import time
import json
import logging
import random
import threading
from datetime import timedelta, datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

# 自定义库导入
import fetch_all_alphas as faa
import wq_login
from session_manager import SessionManager
from session_proxy import SessionProxy

# 配置日志记录
# 统一日志时间为UTC，并在时间后添加Z标记
try:
    from wq_logger import get_logger as _get_logger
    logger = _get_logger(__name__)
    # 保持字段与格式一致：UTC+Z，threadName、levelname、message
    _formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s', '%Y-%m-%d %H:%M:%SZ')
    for _h in logging.getLogger().handlers:
        _h.setFormatter(_formatter)
except Exception:
    logging.Formatter.converter = time.gmtime
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format='%(asctime)sZ - %(threadName)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------------------
# 运行配置加载（优先 async_config.json，不存在时用内置默认）
# --------------------------------------------------------------------------------------
CONFIG_FILE = Path(__file__).with_name("async_config.json")
DEFAULT_CONFIG = {
    "MAX_WORKERS": 3,
    "MAX_RETRIES": 3,
    "INITIAL_BACKOFF": 8,
    "BACKOFF_MULTIPLIER": 2.0,
    "MAX_BACKOFF": 240,
    "MAX_LOGIN_ATTEMPTS": 5,
    "SESSION_CHECK_INTERVAL": 40,
    "SESSION_CHECK_ON_RETRY": 6,     # 重试N次后检查会话
    "SESSION_CHECK_ON_CONSECUTIVE_FAILURES": 3,  # 连续失败N次后检查会话
    "FETCH_MODE": "auto",            # 获取模式：auto, incremental, manual
    "START_TIME": None,              # manual 模式的起始时间
    "END_TIME": None,                # manual 模式的结束时间
    "INTERVAL_HOURS": 1,
    "DAYS": 14,
    "OVERWRITE": True,
    "SKIP_REGULAR": False,
    "SKIP_SUPER": False,
    "BASE_DIRECTORY": None,          # 可选：覆盖数据输出根目录，示例 "./WorldQuant/Data/alphas"
    "RETRY_ROUNDS": 3,               # 失败切片重试轮数
    "RETRY_WORKERS": 2               # 重试时使用的并发worker数量（通常比初次获取少）
}

def _load_runtime_config():
    cfg = DEFAULT_CONFIG.copy()
    try:
        if CONFIG_FILE.exists():
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                file_cfg = json.load(f)
            if isinstance(file_cfg, dict):
                # 支持大小写不敏感合并，以配置文件为准覆盖
                merged = cfg.copy()
                for k, v in file_cfg.items():
                    # 过滤掉注释字段（以_或//开头的键）
                    if not k.startswith('_') and not k.startswith('//'):
                        merged[k.upper()] = v
                cfg = merged
            logger.info(f"已加载配置文件: {CONFIG_FILE}")
        else:
            logger.info("未发现 async_config.json，使用内置默认配置")
    except Exception as e:
        logger.warning(f"加载 async_config.json 失败，使用内置默认配置: {e}")
    return cfg

_CONFIG = _load_runtime_config()

# --------------------------------------------------------------------------------------
# 异步配置参数（由 _CONFIG 赋值，供全局重试/退避逻辑使用）
# --------------------------------------------------------------------------------------
MAX_WORKERS = int(_CONFIG["MAX_WORKERS"])
MAX_RETRIES = int(_CONFIG["MAX_RETRIES"])
INITIAL_BACKOFF = int(_CONFIG["INITIAL_BACKOFF"])
BACKOFF_MULTIPLIER = float(_CONFIG["BACKOFF_MULTIPLIER"])
MAX_BACKOFF = int(_CONFIG["MAX_BACKOFF"])
MAX_LOGIN_ATTEMPTS = int(_CONFIG["MAX_LOGIN_ATTEMPTS"])  # 统一外层登录触发上限

# 会话验证计数器
_SUCCESS_COUNT = 0
_SESSION_CHECK_INTERVAL = int(_CONFIG["SESSION_CHECK_INTERVAL"])  # 每 N 次成功后检查会话
_SESSION_CHECK_ON_RETRY = int(_CONFIG.get("SESSION_CHECK_ON_RETRY", 6))  # 重试N次后检查会话
_SESSION_CHECK_ON_CONSECUTIVE_FAILURES = int(_CONFIG.get("SESSION_CHECK_ON_CONSECUTIVE_FAILURES", 3))  # 连续失败N次后检查会话
_GLOBAL_LOCK = threading.Lock()

# 简化：移除分作用域刷新计数，仅保留统一的会话管理与日志

# --------------------------------------------------------------------------------------
# 异步会话管理
# --------------------------------------------------------------------------------------
# 注意：使用SessionProxy后，不再需要_get_shared_session函数
# SessionProxy会自动在每次请求时获取最新session，确保线程安全

def _increment_success_count():
    """增加成功计数器"""
    global _SUCCESS_COUNT
    with _GLOBAL_LOCK:
        _SUCCESS_COUNT += 1

# --------------------------------------------------------------------------------------
# 异步数据获取核心函数
# --------------------------------------------------------------------------------------

def check_file_exists(userkey, base_directory, slice_start, slice_end, alpha_type="regular"):
    """检查指定时间切片的文件是否已存在"""
    import re
    safe_start_date = re.sub(r'[:T]', '-', slice_start)
    safe_end_date = re.sub(r'[:T]', '-', slice_end)
    filename = f'{userkey}_{safe_start_date}_to_{safe_end_date}_all_{alpha_type}_alphas.json'
    directory = base_directory / f"all_{alpha_type}_alphas"
    file_path = directory / filename
    return file_path.exists(), file_path

def fetch_single_time_slice(session_proxy: SessionProxy, session_manager: SessionManager, 
                          userkey: str, base_directory: Path,
                          slice_start: str, slice_end: str, alpha_type: str = "regular",
                          overwrite: bool = False, worker_id: int = 0) -> Tuple[bool, str, Optional[str]]:
    """
    获取单个时间切片的数据 - 使用SessionProxy确保始终使用最新session
    
    Args:
        session_proxy: SessionProxy实例，自动获取最新session
        session_manager: SessionManager实例，用于在401错误时强制刷新
        userkey: 用户标识
        base_directory: 基础目录
        slice_start: 切片起始时间
        slice_end: 切片结束时间
        alpha_type: alpha类型
        overwrite: 是否覆盖
        worker_id: Worker ID
    
    Returns:
        (success, message, filename)
    """
    thread_name = f"Worker-{worker_id}"
    
    # 检查文件是否已存在
    file_exists, file_path = check_file_exists(userkey, base_directory, slice_start, slice_end, alpha_type)
    
    if file_exists and not overwrite:
        return True, f"[{thread_name}] 文件已存在，跳过: {file_path.name}", file_path.name
    
    if file_exists and overwrite:
        logger.info(f"[{thread_name}] 文件已存在，将覆盖: {file_path.name}")
    
    # 线程间随机错开时间，避免同时请求
    random_delay = random.uniform(0.5, 2.0)  # 0.5-2秒随机延迟
    time.sleep(random_delay)
    
    retries = 0
    consecutive_failures = 0  # 连续失败计数器
    
    while retries < MAX_RETRIES:
        try:
            # 使用SessionProxy，每次请求自动获取最新session
            # 不再需要手动获取和更新session
            
            # 根据类型调用相应的函数
            # 注意：不再返回updated_session，SessionProxy会自动管理session
            if alpha_type == "regular":
                fetched_alphas, filename, info = faa.fetch_and_save_regular_alphas(
                    session_proxy, slice_start, slice_end, userkey, base_directory)
            else:
                fetched_alphas, filename, info = faa.fetch_and_save_super_alphas(
                    session_proxy, slice_start, slice_end, userkey, base_directory)
            
            if fetched_alphas:
                # 如果不全，标记为失败以进入重试，但仍已保存当前数据
                if info and info.get('incomplete'):
                    missing = info.get('skipped_offsets', [])
                    return False, f"[Scope:Slice][{thread_name}] 部分成功，缺失页 offsets: {missing} -> {filename}", filename
                # 完整成功 - 重置连续失败计数器
                consecutive_failures = 0
                _increment_success_count()
                return True, f"[Scope:Slice][{thread_name}] 成功获取 {len(fetched_alphas)} 个{alpha_type} alphas -> {filename}", filename
            else:
                # 返回None表示失败，增加失败计数
                consecutive_failures += 1
                # 检查是否需要验证会话
                # SessionProxy确保每次请求都使用最新session，但连续失败可能表示session问题
                if consecutive_failures >= _SESSION_CHECK_ON_CONSECUTIVE_FAILURES:
                    logger.warning(f"[Scope:Slice][{thread_name}] WARNING: 连续失败{consecutive_failures}次，主动检查会话有效性")
                    try:
                        import wq_login
                        # 修复：在调用check_session_validity之前检查session是否为None
                        if session_manager.session is None:
                            logger.warning(f"[Scope:Slice][{thread_name}] 会话为None，主动刷新")
                            with _GLOBAL_LOCK:
                                session_manager.force_refresh()
                                logger.info(f"[Scope:Slice][{thread_name}] SUCCESS: 会话已刷新，SessionProxy将在下次请求时自动使用新session")
                        elif not wq_login.check_session_validity(session_manager.session, min_remaining_seconds=1800):
                            logger.warning(f"[Scope:Slice][{thread_name}] 会话剩余时间不足30分钟，主动刷新")
                            with _GLOBAL_LOCK:
                                session_manager.force_refresh()
                                logger.info(f"[Scope:Slice][{thread_name}] SUCCESS: 会话已刷新，SessionProxy将在下次请求时自动使用新session")
                        else:
                            logger.info(f"[Scope:Slice][{thread_name}] SUCCESS: 会话验证通过，仍然有效")
                    except Exception as check_error:
                        logger.error(f"[Scope:Slice][{thread_name}] ERROR: 检查会话时出错: {check_error}")
                
                return False, f"[Scope:Slice][{thread_name}] 未获取到数据: {slice_start} 到 {slice_end}", None
                
        except Exception as e:
            retries += 1
            consecutive_failures += 1
            error_msg = str(e)
            
            # 检查错误类型并采取相应措施
            if "401" in error_msg or "403" in error_msg or "unauthorized" in error_msg.lower():
                # 认证错误：会话过期，强制刷新
                # SessionProxy确保下次请求自动使用新session，无需手动更新
                logger.warning(f"[Scope:Slice][{thread_name}] 收到401/403认证错误，强制刷新会话: {e}")
                try:
                    with _GLOBAL_LOCK:
                        session_manager.force_refresh()
                        logger.info(f"[Scope:Slice][{thread_name}] SUCCESS: 会话已刷新，SessionProxy将在下次请求时自动使用新session")
                    # 刷新成功后，不等待直接重试（SessionProxy会获取新session）
                    continue
                except Exception as login_error:
                    logger.error(f"[Scope:Slice][{thread_name}] ERROR: 刷新会话失败: {login_error}")
                    break
                    
            elif retries >= _SESSION_CHECK_ON_RETRY:
                # 重试N次后，检查会话有效性
                # SessionProxy确保每次请求都使用最新session，但多次重试可能表示session问题
                logger.warning(f"[Scope:Slice][{thread_name}] WARNING: 已重试{retries}次，主动检查会话有效性")
                try:
                    import wq_login
                    # 修复：在调用check_session_validity之前检查session是否为None
                    if session_manager.session is None:
                        logger.warning(f"[Scope:Slice][{thread_name}] 会话为None，主动刷新")
                        with _GLOBAL_LOCK:
                            session_manager.force_refresh()
                            logger.info(f"[Scope:Slice][{thread_name}] SUCCESS: 会话已刷新，SessionProxy将在下次请求时自动使用新session")
                    elif not wq_login.check_session_validity(session_manager.session, min_remaining_seconds=1800):
                        logger.warning(f"[Scope:Slice][{thread_name}] 会话剩余时间不足30分钟，主动刷新")
                        with _GLOBAL_LOCK:
                            session_manager.force_refresh()
                            logger.info(f"[Scope:Slice][{thread_name}] SUCCESS: 会话已刷新，SessionProxy将在下次请求时自动使用新session")
                    else:
                        logger.info(f"[Scope:Slice][{thread_name}] SUCCESS: 会话验证通过，继续重试")
                except Exception as check_error:
                    logger.error(f"[Scope:Slice][{thread_name}] ERROR: 检查会话时出错: {check_error}")
                    
            # 等待后重试
            if retries < MAX_RETRIES:
                if "429" in error_msg or "rate limit" in error_msg.lower():
                    # 限流错误：指数退避
                    base_wait = INITIAL_BACKOFF * (BACKOFF_MULTIPLIER ** (retries - 1))
                    wait_time = min(MAX_BACKOFF, base_wait)
                    wait_time *= random.uniform(0.8, 1.2)
                    logger.warning(f"[Scope:Slice][{thread_name}] 429 限流，等待 {wait_time:.1f}s (重试 {retries}/{MAX_RETRIES})")
                else:
                    # 其他错误：线性退避
                    wait_time = min(30, 5 * retries)
                    wait_time *= random.uniform(0.8, 1.2)
                    logger.warning(f"[Scope:Slice][{thread_name}] 获取数据失败: {e}，等待 {wait_time:.1f}s 后重试 ({retries}/{MAX_RETRIES})")
                time.sleep(wait_time)
            else:
                logger.error(f"[Scope:Slice][{thread_name}] 达到最大重试次数，放弃: {e}")
    
    return False, f"[Scope:Slice][{thread_name}] 失败: {slice_start} 到 {slice_end} (重试 {MAX_RETRIES} 次后放弃)", None

# --------------------------------------------------------------------------------------
# 异步批处理函数
# --------------------------------------------------------------------------------------

def fetch_alpha_async(session_proxy: SessionProxy, session_manager: SessionManager,
                     userkey: str, start_date_input: str, 
                     end_date_input: str, base_directory: Path, alpha_type: str = "regular", 
                     overwrite: bool = False, max_workers: int = MAX_WORKERS, interval_hours: int = 2) -> Dict[str, Any]:
    """
    异步获取alpha数据
    
    Args:
        session_proxy: SessionProxy实例，自动获取最新session
        session_manager: SessionManager实例，用于在401错误时强制刷新
        userkey: 用户标识
        start_date_input: 起始时间
        end_date_input: 结束时间
        base_directory: 基础目录
        alpha_type: alpha类型
        overwrite: 是否覆盖
        max_workers: 并发worker数量
        interval_hours: 时间切片间隔（小时）
    
    Returns:
        统计结果字典
    """
    # 生成时间切片
    time_slices = list(faa.generate_time_slices(start_date_input, end_date_input, interval=timedelta(hours=interval_hours)))
    
    logger.info(f"开始异步获取 {alpha_type.upper()} Alphas，共 {len(time_slices)} 个时间切片，使用 {max_workers} 个并发worker")
    
    # 创建结果跟踪
    results = {
        'total': len(time_slices),
        'processed': 0,
        'skipped': 0,
        'failed': 0,
        'incomplete': 0,
        'failed_slices': [],
        'incomplete_slices': [],
        'retry_queue': [],            # 需要进入多轮重试的切片（包含失败与不全）
        'retry_types': {},            # slice_info -> 'failed' | 'incomplete'
        'successful_files': [],
        'failed_reasons': {}
    }
    
    failed_lock = threading.Lock()

    def mark_failed(slice_info: str, error_msg: str = ""):
        with failed_lock:
            results['failed_slices'].append(slice_info)
            results['failed_reasons'][slice_info] = error_msg
            results['retry_queue'].append(slice_info)
            results['retry_types'][slice_info] = 'failed'
            # 统一通过logger记录失败信息
            logger.error(f"[第一轮获取] 切片失败: {slice_info} - {error_msg}")

    def mark_incomplete(slice_info: str, note: str = ""):
        with failed_lock:
            results['incomplete_slices'].append(slice_info)
            results['retry_queue'].append(slice_info)
            results['retry_types'][slice_info] = 'incomplete'
            # 统一通过logger记录不全信息
            logger.warning(f"[第一轮获取] 切片不全: {slice_info} - {note}")
    
    # 使用线程池执行异步获取
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=f"{alpha_type}Worker") as executor:
        # 提交所有任务 - 所有worker共享同一个session_proxy
        future_to_slice = {}
        for i, (slice_start, slice_end) in enumerate(time_slices):
            future = executor.submit(
                fetch_single_time_slice,
                session_proxy, session_manager, userkey, base_directory,  # 传递session_proxy和session_manager
                slice_start, slice_end, alpha_type, overwrite, i + 1
            )
            future_to_slice[future] = (i, slice_start, slice_end)
        
        # 处理完成的任务
        completed = 0
        for future in as_completed(future_to_slice):
            slice_index, slice_start, slice_end = future_to_slice[future]
            completed += 1
            
            try:
                success, message, filename = future.result()
                
                if success:
                    if "跳过" in message:
                        results['skipped'] += 1
                    else:
                        results['processed'] += 1
                        if filename:
                            results['successful_files'].append(filename)
                    logger.info(f"[{completed}/{len(time_slices)}] {message}")
                else:
                    slice_info = f"{slice_start}_to_{slice_end}"
                    # 区分不全 vs 失败
                    if "部分成功" in message or "不全" in message:
                        results['incomplete'] += 1
                        mark_incomplete(slice_info, message)
                        logger.warning(f"[{completed}/{len(time_slices)}] {message}")
                    else:
                        results['failed'] += 1
                        mark_failed(slice_info, message.split('] ')[-1] if '] ' in message else message)
                        logger.error(f"[{completed}/{len(time_slices)}] {message}")
                    
            except Exception as e:
                results['failed'] += 1
                slice_info = f"{slice_start}_to_{slice_end}"
                mark_failed(slice_info, f"Worker异常: {str(e)}")
                logger.error(f"[{completed}/{len(time_slices)}] Worker异常: {slice_start} 到 {slice_end}: {e}")
            
            # 每10个任务显示一次进度
            if completed % 10 == 0:
                logger.info(f"进度: {completed}/{len(time_slices)} 完成，成功计数: {_SUCCESS_COUNT}")
    
    # 第一轮获取完成，开始重试失败的切片
    retry_rounds = int(_CONFIG.get("RETRY_ROUNDS", 3))
    retry_workers = int(_CONFIG.get("RETRY_WORKERS", 2))
    
    # 合并失败和不全的切片进行重试
    all_failed_slices = results['failed_slices'] + results['incomplete_slices']
    
    if all_failed_slices and retry_rounds > 0:
        logger.info(f"\n📋 第一轮获取完成，开始重试 {len(all_failed_slices)} 个失败/不全切片")
        logger.info(f"重试配置：{retry_rounds} 轮重试，每轮使用 {retry_workers} 个worker")
        
        current_failed_slices = all_failed_slices.copy()
        
        for retry_round in range(1, retry_rounds + 1):
            if not current_failed_slices:
                logger.info(f"SUCCESS: 所有切片已成功，无需继续重试")
                break
                
            # 执行重试
            retry_results = retry_failed_slices(
                session_proxy, session_manager, userkey, base_directory,
                current_failed_slices, alpha_type, overwrite, retry_workers, retry_round
            )
            
            # 更新总体统计
            results['processed'] += retry_results['processed']
            results['skipped'] += retry_results['skipped']
            results['successful_files'].extend(retry_results['successful_files'])
            
            # 输出重试轮次结果
            logger.info(f"\n第 {retry_round} 轮重试结果:")
            logger.info(f"  成功: {retry_results['processed']} 个")
            logger.info(f"  跳过: {retry_results['skipped']} 个") 
            logger.info(f"  失败: {retry_results['failed']} 个")
            logger.info(f"  不全: {retry_results['incomplete']} 个")
            
            # 准备下一轮重试的切片（失败+不全）
            current_failed_slices = retry_results['failed_slices'] + retry_results['incomplete_slices']
            
            if not current_failed_slices:
                logger.info(f"SUCCESS: 第 {retry_round} 轮重试后所有切片已成功")
                break
            elif retry_round < retry_rounds:
                logger.info(f"准备第 {retry_round + 1} 轮重试，剩余 {len(current_failed_slices)} 个切片")
                # 重试间隔等待
                time.sleep(5)
        
        # 更新最终的失败统计
        if current_failed_slices:
            # 重新分类最终失败的切片
            final_failed = []
            final_incomplete = []
            for slice_info in current_failed_slices:
                if slice_info in results['failed_slices']:
                    final_failed.append(slice_info)
                else:
                    final_incomplete.append(slice_info)
            
            results['failed_slices'] = final_failed
            results['incomplete_slices'] = final_incomplete
            results['failed'] = len(final_failed)
            results['incomplete'] = len(final_incomplete)
            
            logger.info(f"\nERROR: 经过 {retry_rounds} 轮重试后仍然失败的切片:")
            for s in final_failed:
                reason = results.get('failed_reasons', {}).get(s, '未知原因')
                logger.info(f"- {s}: {reason}")
            
            if final_incomplete:
                logger.info(f"\nWARNING: 经过 {retry_rounds} 轮重试后仍然不全的切片:")
                for s in final_incomplete:
                    logger.info(f"- {s}")
        else:
            # 所有切片都成功了
            results['failed_slices'] = []
            results['incomplete_slices'] = []
            results['failed'] = 0
            results['incomplete'] = 0
            logger.info(f"🎉 所有切片经过重试后均已成功获取！")
    
    elif all_failed_slices:
        logger.info(f"\n📋 有 {len(all_failed_slices)} 个失败/不全切片，但重试功能已禁用 (RETRY_ROUNDS=0)")
        # 输出最终失败原因列表
        if results['failed_slices']:
            logger.info("最终失败切片与原因：")
            for s in results['failed_slices']:
                reason = results.get('failed_reasons', {}).get(s, '未知原因')
                logger.info(f"- {s}: {reason}")

        if results['incomplete_slices']:
            logger.info("最终不全切片：")
            for s in results['incomplete_slices']:
                logger.info(f"- {s}")
    else:
        logger.info(f"🎉 第一轮获取即全部成功，无需重试！")

    return results

def retry_failed_slices(session_proxy: SessionProxy, session_manager: SessionManager,
                       userkey: str, base_directory: Path,
                       failed_slices: List[str], alpha_type: str = "regular", 
                       overwrite: bool = False, max_workers: int = 2, retry_round: int = 1) -> Dict[str, Any]:
    """
    重试失败的时间切片
    
    Args:
        session_proxy: SessionProxy实例，自动获取最新session
        session_manager: SessionManager实例，用于在401错误时强制刷新
        userkey: 用户标识
        base_directory: 基础目录
        failed_slices: 失败的切片列表，格式为 "start_to_end"
        alpha_type: alpha类型
        overwrite: 是否覆盖
        max_workers: 并发worker数量
        retry_round: 重试轮次
    
    Returns:
        重试结果统计
    """
    if not failed_slices:
        return {
            'total': 0, 'processed': 0, 'skipped': 0, 'failed': 0, 'incomplete': 0,
            'failed_slices': [], 'incomplete_slices': [], 'successful_files': [], 'failed_reasons': {}
        }
    
    logger.info(f"\nRetry round {retry_round}: {len(failed_slices)} failed slices, using {max_workers} concurrent workers")
    
    # 解析失败切片的时间范围
    time_slices = []
    for slice_info in failed_slices:
        try:
            start_str, end_str = slice_info.split('_to_')
            time_slices.append((start_str, end_str))
        except ValueError:
            logger.warning(f"无法解析切片格式: {slice_info}")
            continue
    
    if not time_slices:
        logger.warning("没有有效的切片可以重试")
        return {
            'total': 0, 'processed': 0, 'skipped': 0, 'failed': 0, 'incomplete': 0,
            'failed_slices': [], 'incomplete_slices': [], 'successful_files': [], 'failed_reasons': {}
        }
    
    # 创建重试结果跟踪
    retry_results = {
        'total': len(time_slices),
        'processed': 0,
        'skipped': 0,
        'failed': 0,
        'incomplete': 0,
        'failed_slices': [],
        'incomplete_slices': [],
        'successful_files': [],
        'failed_reasons': {}
    }
    
    retry_failed_lock = threading.Lock()

    def mark_retry_failed(slice_info: str, error_msg: str = ""):
        with retry_failed_lock:
            retry_results['failed_slices'].append(slice_info)
            retry_results['failed_reasons'][slice_info] = error_msg
            # 统一通过logger记录，不单独创建文件
            logger.error(f"[重试第{retry_round}轮] 切片失败: {slice_info} - {error_msg}")

    def mark_retry_incomplete(slice_info: str, note: str = ""):
        with retry_failed_lock:
            retry_results['incomplete_slices'].append(slice_info)
            # 统一通过logger记录
            logger.warning(f"[重试第{retry_round}轮] 切片不全: {slice_info} - {note}")
    
    # 使用线程池执行重试
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=f"Retry{alpha_type}Worker") as executor:
        # 提交所有重试任务
        future_to_slice = {}
        for i, (slice_start, slice_end) in enumerate(time_slices):
            future = executor.submit(
                fetch_single_time_slice,
                session_proxy, session_manager, userkey, base_directory,
                slice_start, slice_end, alpha_type, overwrite, i + 1
            )
            future_to_slice[future] = (i, slice_start, slice_end)
        
        # 处理完成的重试任务
        completed = 0
        for future in as_completed(future_to_slice):
            slice_index, slice_start, slice_end = future_to_slice[future]
            completed += 1
            
            try:
                success, message, filename = future.result()
                
                if success:
                    if "跳过" in message:
                        retry_results['skipped'] += 1
                    else:
                        retry_results['processed'] += 1
                        if filename:
                            retry_results['successful_files'].append(filename)
                    logger.info(f"[重试 {completed}/{len(time_slices)}] SUCCESS: {message}")
                else:
                    slice_info = f"{slice_start}_to_{slice_end}"
                    # 区分不全 vs 失败
                    if "部分成功" in message or "不全" in message:
                        retry_results['incomplete'] += 1
                        mark_retry_incomplete(slice_info, message)
                        logger.warning(f"[重试 {completed}/{len(time_slices)}] WARNING: {message}")
                    else:
                        retry_results['failed'] += 1
                        mark_retry_failed(slice_info, message.split('] ')[-1] if '] ' in message else message)
                        logger.error(f"[重试 {completed}/{len(time_slices)}] ERROR: {message}")
                    
            except Exception as e:
                retry_results['failed'] += 1
                slice_info = f"{slice_start}_to_{slice_end}"
                mark_retry_failed(slice_info, f"重试Worker异常: {str(e)}")
                logger.error(f"[重试 {completed}/{len(time_slices)}] ERROR - Worker异常: {slice_start} 到 {slice_end}: {e}")
            
            # 每5个任务显示一次进度（重试时更频繁显示）
            if completed % 5 == 0:
                logger.info(f"重试进度: {completed}/{len(time_slices)} 完成")
    
    # 输出重试结果
    if retry_results['failed_slices']:
        logger.info(f"第 {retry_round} 轮重试仍然失败的切片：")
        for s in retry_results['failed_slices']:
            reason = retry_results.get('failed_reasons', {}).get(s, '未知原因')
            logger.info(f"- {s}: {reason}")

    if retry_results['incomplete_slices']:
        logger.info(f"第 {retry_round} 轮重试仍然不全的切片：")
        for s in retry_results['incomplete_slices']:
            logger.info(f"- {s}")

    return retry_results

# --------------------------------------------------------------------------------------
# 时间范围获取函数（支持3种模式）
# --------------------------------------------------------------------------------------

def get_time_range_by_mode(mode: str, base_directory: Path, userkey: str, 
                          alpha_type: str = "regular") -> Tuple[str, str]:
    """
    根据模式获取时间范围
    
    Args:
        mode: 'auto', 'incremental', 'manual'
        base_directory: 数据根目录
        userkey: 用户标识
        alpha_type: 'regular' 或 'super'
        
    Returns:
        (start_date_str, end_date_str): ISO格式的日期字符串
    """
    mode = mode.lower()
    
    if mode == "auto":
        # 自动模式：基于 DAYS 配置
        days = int(_CONFIG["DAYS"])
        start_date, end_date = faa.get_dates_for_query(days)
        logger.info(f"[{alpha_type.upper()}] 使用 auto 模式：获取过去 {days} 天的数据")
        return start_date, end_date
        
    elif mode == "incremental":
        # 增量模式：从最新文件的起始时间开始（提前一个切片）
        data_dir = base_directory / f"all_{alpha_type}_alphas"
        
        if not data_dir.exists():
            logger.warning(f"[{alpha_type.upper()}] 增量模式：目录不存在 {data_dir}，回退到 auto 模式")
            days = int(_CONFIG["DAYS"])
            return faa.get_dates_for_query(days)
        
        # 查找所有文件
        import re
        pattern = re.compile(
            rf'{userkey}_(\d{{4}}-\d{{2}}-\d{{2}}-\d{{2}}-\d{{2}}-\d{{2}}\.\d{{3}}Z)_to_'
            rf'(\d{{4}}-\d{{2}}-\d{{2}}-\d{{2}}-\d{{2}}-\d{{2}}\.\d{{3}}Z)_all_{alpha_type}_alphas\.json'
        )
        
        files = []
        for f in data_dir.glob(f"{userkey}_*_all_{alpha_type}_alphas.json"):
            match = pattern.match(f.name)
            if match:
                # 文件名格式: 2024-11-27-09-00-00.000Z
                # 转换为: 2024-11-27T09:00:00.000Z
                file_time = match.group(1)
                # 替换前两个 - 后面的 - 为 :
                parts = file_time.split('-')
                if len(parts) >= 6:
                    start_str = f"{parts[0]}-{parts[1]}-{parts[2]}T{parts[3]}:{parts[4]}:{parts[5]}"
                else:
                    continue
                files.append((start_str, f))
        
        if not files:
            logger.warning(f"[{alpha_type.upper()}] 增量模式：未找到已有文件，回退到 auto 模式")
            days = int(_CONFIG["DAYS"])
            return faa.get_dates_for_query(days)
        
        # 找到最新文件的起始时间
        files.sort(key=lambda x: x[0], reverse=True)
        latest_start_time = files[0][0]
        
        # 获取当前时间（对齐到整点）
        now_utc = datetime.now(timezone.utc)
        end_date = now_utc.replace(minute=0, second=0, microsecond=0)
        end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        
        # 检查数据完整性并发出警告
        total_files = len(files)
        oldest_start_time = files[-1][0] if files else None
        
        logger.info(f"[{alpha_type.upper()}] 使用 incremental 模式：从 {latest_start_time} 开始（覆盖最后一个切片）")
        logger.warning("=" * 80)
        logger.warning("WARNING: INCREMENTAL 模式警告 - 请仔细阅读")
        logger.warning("=" * 80)
        logger.warning(f"当前检测到 {total_files} 个已有文件")
        logger.warning(f"最早文件时间: {oldest_start_time}")
        logger.warning(f"最新文件时间: {latest_start_time}")
        logger.warning("")
        logger.warning("WARNING: 重要提示：")
        logger.warning("   1. incremental 模式只从【最新文件】开始获取新数据")
        logger.warning("   2. 不会检查【最新文件之前】的历史数据是否完整")
        logger.warning("   3. 如果历史数据中存在缺失的时间切片，本次运行不会补充")
        logger.warning("")
        logger.warning("WARNING: 潜在风险：")
        logger.warning(f"   - 从 {oldest_start_time} 到 {latest_start_time} 之间可能存在数据缺失")
        logger.warning("   - 如果之前的运行被中断，可能有部分时间切片未获取")
        logger.warning("   - 建议定期使用 auto 模式进行全量检查")
        logger.warning("")
        logger.warning("NOTE: 如需检查并补充历史缺失数据，请使用以下模式之一：")
        logger.warning("   - auto 模式 + OVERWRITE=false（补充所有缺失文件）")
        logger.warning("   - manual 模式指定时间范围 + OVERWRITE=false（补充特定时段）")

        
        return latest_start_time, end_date_str
        
    elif mode == "manual":
        # 手动模式：使用配置文件中的 START_TIME 和 END_TIME
        start_time = _CONFIG.get("START_TIME")
        end_time = _CONFIG.get("END_TIME")
        
        if not start_time or not end_time:
            raise ValueError(
                "manual 模式需要在配置文件中设置 START_TIME 和 END_TIME\n"
                "格式示例: \"2024-11-01T00:00:00.000Z\""
            )
        
        logger.info(f"[{alpha_type.upper()}] 使用 manual 模式：{start_time} 到 {end_time}")
        return start_time, end_time
        
    else:
        raise ValueError(f"不支持的模式: {mode}，支持的模式: auto, incremental, manual")


# --------------------------------------------------------------------------------------
# 主函数
# --------------------------------------------------------------------------------------

def main():
    # 从配置获取运行期参数（若无 async_config.json 则使用 DEFAULT_CONFIG）
    fetch_mode = _CONFIG.get("FETCH_MODE", "auto").lower()  # 获取模式
    overwrite = bool(_CONFIG["OVERWRITE"])     # 是否覆盖已存在的文件
    skip_regular = bool(_CONFIG["SKIP_REGULAR"])   # 是否跳过 Regular Alphas
    skip_super = bool(_CONFIG["SKIP_SUPER"])       # 是否跳过 Super Alphas
    max_workers = int(_CONFIG["MAX_WORKERS"])  # 并发 worker 数量
    interval_hours = int(_CONFIG["INTERVAL_HOURS"])  # 时间切片间隔（小时）
    base_directory_override = _CONFIG.get("BASE_DIRECTORY")  # 可选：覆盖数据输出根目录
    retry_rounds = int(_CONFIG.get("RETRY_ROUNDS", 3))  # 失败切片重试轮数
    retry_workers = int(_CONFIG.get("RETRY_WORKERS", 2))  # 重试时使用的worker数量

    try:
        # 初始化会话管理器
        session_manager = SessionManager()
        
        # 创建SessionProxy，确保每次请求都使用最新session
        session_proxy = SessionProxy(session_manager)
        
        # 提前初始化会话，避免在 worker 线程中初始化导致日志混乱
        logger.info("正在初始化会话...")
        _ = session_manager.get_session()  # 触发会话初始化
        logger.info("会话初始化完成（使用SessionProxy确保始终获取最新session）")

        userkey = wq_login.USER_KEY
        base_directory = faa.base_directory
        # 覆盖数据输出目录（如提供）
        if base_directory_override:
            base_directory = Path(base_directory_override)
            base_directory.mkdir(parents=True, exist_ok=True)
            logger.info(f"使用配置的覆盖数据输出目录: {base_directory}")

        # 所有日志统一通过 wq_logger.py 处理，不再创建独立的日志文件

        logger.info("=" * 80)
        logger.info("WorldQuant Brain Alpha数据获取器 - 异步版本")
        logger.info("=" * 80)
        logger.info(f"获取模式: {fetch_mode.upper()}")
        logger.info(f"用户标识: {userkey}")
        logger.info(f"基础目录: {base_directory}")
        logger.info(f"覆盖模式: {'是' if overwrite else '否'}")
        logger.info(f"并发Worker数: {max_workers}")
        logger.info(f"重试配置: {retry_rounds} 轮重试，每轮 {retry_workers} 个worker")
        logger.info("=" * 80)

        total_start_time = time.time()

        # 获取 Regular Alphas
        if not skip_regular:
            logger.info("\n开始异步获取 Regular Alphas")
            logger.info("=" * 50)

            # 根据模式获取时间范围
            start_date_input, end_date_input = get_time_range_by_mode(
                fetch_mode, base_directory, userkey, "regular"
            )
            logger.info(f"时间范围: {start_date_input} 到 {end_date_input}")

            start_time = time.time()
            regular_results = fetch_alpha_async(
                session_proxy, session_manager, userkey, start_date_input, end_date_input,
                base_directory, "regular", overwrite, max_workers, interval_hours
            )
            end_time = time.time()
            duration = end_time - start_time

            logger.info(f"\nRegular Alphas 处理完成 (耗时: {duration:.1f}秒):")
            logger.info(f"  总计: {regular_results['total']} 个时间切片")
            logger.info(f"  已处理: {regular_results['processed']} 个")
            logger.info(f"  已跳过: {regular_results['skipped']} 个")
            logger.info(f"  失败: {regular_results['failed']} 个")
            logger.info(f"  不全: {regular_results['incomplete']} 个")

        # 获取 Super Alphas
        if not skip_super:
            logger.info("\n开始异步获取 Super Alphas")
            logger.info("=" * 50)

            # 根据模式获取时间范围
            start_date_input, end_date_input = get_time_range_by_mode(
                fetch_mode, base_directory, userkey, "super"
            )
            logger.info(f"时间范围: {start_date_input} 到 {end_date_input}")

            start_time = time.time()
            super_results = fetch_alpha_async(
                session_proxy, session_manager, userkey, start_date_input, end_date_input,
                base_directory, "super", overwrite, max_workers, interval_hours
            )
            end_time = time.time()
            duration = end_time - start_time

            logger.info(f"\nSuper Alphas 处理完成 (耗时: {duration:.1f}秒):")
            logger.info(f"  总计: {super_results['total']} 个时间切片")
            logger.info(f"  已处理: {super_results['processed']} 个")
            logger.info(f"  已跳过: {super_results['skipped']} 个")
            logger.info(f"  失败: {super_results['failed']} 个")
            logger.info(f"  不全: {super_results['incomplete']} 个")

        total_end_time = time.time()
        total_duration = total_end_time - total_start_time

        logger.info(f"\nSUCCESS: 所有任务已完成 (总耗时: {total_duration:.1f}秒)")

        # 显示最终会话状态
        remaining_time = session_manager.get_remaining_time()
        logger.info(f"会话剩余时间: {int(remaining_time/60)} 分钟")

    except KeyboardInterrupt:
        logger.warning("\nWARNING: 程序被用户中断")
    except Exception as e:
        logger.error(f"程序执行过程中发生错误: {e}")
        raise

# --------------------------------------------------------------------------------------
# 入口
# --------------------------------------------------------------------------------------

if __name__ == "__main__":
    main()