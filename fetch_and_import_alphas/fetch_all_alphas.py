# 标准库导入
import json
import os
import time
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pickle
import re
import json
import orjson
import logging
import random

# 第三方库导入
import wq_login
from timezone_utils import generate_time_slices_utc

brain_api_url = os.environ.get("BRAIN_API_URL", "https://api.worldquantbrain.com")
base_directory = wq_login.WQ_DATA_ROOT / wq_login.USER_KEY / "primeval_data"

def get_dates_for_query(days):
    """
    根据天数计算查询的起止日期（UTC时间，对齐到整点小时）
    
    Args:
        days: 获取过去多少天的数据
        
    Returns:
        (start_date_str, end_date_str): ISO格式的日期字符串，精确到小时（秒和毫秒为0）
        格式示例：2024-09-18T14:00:00.000Z
    """
    # 获取当前 UTC 时间
    now_utc = datetime.now(timezone.utc)
    
    # 对齐到当前小时的整点（分钟、秒、微秒归零）
    end_date = now_utc.replace(minute=0, second=0, microsecond=0)
    
    # 向前推算指定天数
    start_date = end_date - timedelta(days=days)
    
    # 返回格式：YYYY-MM-DDTHH:MM:SS.000Z（与稳定版保持一致）
    start_str = start_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    end_str = end_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    
    return start_str, end_str

def generate_time_slices(start_date_str, end_date_str, interval=timedelta(hours=2)):
    """
    生成时间切片的包装函数
    
    Args:
        start_date_str: 起始日期字符串
        end_date_str: 结束日期字符串
        interval: 时间间隔（timedelta对象）
        
    Returns:
        时间切片列表
    """
    interval_hours = interval.total_seconds() / 3600
    return generate_time_slices_utc(start_date_str, end_date_str, interval_hours)

# 运行期配置（与异步入口保持一致，从async_config.json 加载）
from pathlib import Path as _Path
_CONFIG_FILE = _Path(__file__).with_name("async_config.json")
_DEFAULTS = {
    "PAGE_LIMIT": 100,
    "HTTP_TIMEOUT": 30,
    "PAGE_MAX_RETRIES": 7,
    "INITIAL_WAIT": 3,
    "ORDER_FIELD": "dateCreated",
    "BACKOFF_MULTIPLIER": 2.0,
}
def _load_cfg():
    cfg = _DEFAULTS.copy()
    try:
        if _CONFIG_FILE.exists():
            import json as _json
            with open(_CONFIG_FILE, 'r', encoding='utf-8') as f:
                file_cfg = _json.load(f)
            if isinstance(file_cfg, dict):
                for k, v in file_cfg.items():
                    # 过滤掉注释字段（以_或/开头的键）
                    if not k.startswith('_') and not k.startswith('//'):
                        cfg[k.upper()] = v
    except Exception:
        pass
    return cfg
_CFG = _load_cfg()

def get_all_regular_alphas(session, start_date, end_date):
    """
    Fetch all alphas using pagination with improved error handling.
    
    Args:
        session: A requests.Session object (or SessionProxy) to handle the API requests.
        start_date: The start date for filtering alphas.
        end_date: The end date for filtering alphas.

    Returns:
        A tuple of (fetched_alphas, info) when successful, or (None, info) when failed
        
    Note:
        使用SessionProxy时，不再需要返回session，因为SessionProxy会自动管理session生命周期
    """
    fetched_alphas = []  # 确保在函数开始就初始化
    offset = 0
    order = 'dateCreated'
    
    # 生成时间切片标识用于日志（包含年月日和时间）
    slice_id = f"{start_date[0:10]}T{start_date[11:16]}_to_{end_date[0:10]}T{end_date[11:16]}"
    
    # Set default parameters（统一由配置驱动）
    limit = int(_CFG.get("PAGE_LIMIT", 100))
    page_max_retries = int(_CFG.get("PAGE_MAX_RETRIES", 7))    # 单页失败时的最大重试次数
    initial_wait = int(_CFG.get("INITIAL_WAIT", 3))            # 初始等待时间（若未配置则使用默认 3s）
    backoff_multiplier = float(_CFG.get("BACKOFF_MULTIPLIER", 2.0))  # 统一指数退避倍数

    # 轻量级logger 适配（优先使用项目自带wq_logger，否则回退到标准logging）
    try:
        from wq_logger import get_logger as _get_logger  # type: ignore
        logger = _get_logger(__name__)
    except Exception:  # pragma: no cover - 回退到标准库
        import logging as _logging
        logger = _logging.getLogger(__name__)
        if not logger.handlers:
            _handler = _logging.StreamHandler()
            _formatter = _logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
            _handler.setFormatter(_formatter)
            logger.addHandler(_handler)
        logger.setLevel(_logging.INFO)
    # 简化：取消分页全局重试与补偿重试，仅保留页级重试
    # 若任一页在页级重试后仍失败，则返回失败，让外层切片重试处理
    info = {'incomplete': False, 'skipped_offsets': []}
    while True:
        retries = 0
        # 将url变量定义移到try块外面，避免在异常处理中未定义的问题
        url = ""
        while retries < page_max_retries:
            try:
                from timezone_utils import build_api_query_with_time_slice
                query_params = build_api_query_with_time_slice(start_date, end_date, "REGULAR")
                url = f"https://api.worldquantbrain.com/users/self/alphas?limit={limit}&offset={offset}&{query_params}&order={_CFG.get('ORDER_FIELD', 'dateCreated')}"
                
                # 仅使用传入的会话；若无效则直接失败，由外层处理登录
                # 注意：session可能是SessionProxy对象，不需要isinstance检查
                if session is None:
                    logger.error("无效的会话对象，停止分页并返回失败")
                    return None, {'incomplete': True, 'skipped_offsets': []}
                
                response = session.get(url, timeout=int(_CFG.get("HTTP_TIMEOUT", 30)))
                response.raise_for_status()
                alphas = response.json().get("results", [])
                fetched_alphas.extend(alphas)
                
                if len(alphas) < limit:
                    # 已到最后一页
                    return fetched_alphas, info
                
                offset += limit
                break
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    retries += 1
                    if retries >= page_max_retries:
                        logger.warning(f"[{slice_id}] 429 达到页级最大重试次数，返回失败由外层处理 | URL: {url}")
                        return None, {'incomplete': True, 'skipped_offsets': []}
                    base_wait = initial_wait * (backoff_multiplier ** (retries - 1))
                    wait_time = base_wait * random.uniform(0.8, 1.2)
                    logger.warning(
                        f"[{slice_id}] Rate limit 429. Retry {retries}/{page_max_retries}. Waiting {wait_time:.1f}s | URL: {url}"
                    )
                    time.sleep(wait_time)
                    continue
                elif e.response.status_code in {401, 403}:
                    logger.error(f"[{slice_id}] 会话未授权/过期 {e.response.status_code}，停止分页 | URL: {url}")
                    # 抛出异常以便上层刷新会话
                    raise Exception(f"会话未授权/过期，停止分页") from e
                elif e.response.status_code in {500, 502, 503, 504}:
                    retries += 1
                    base_wait = initial_wait * (backoff_multiplier ** (min(retries - 1, 5)))
                    wait_time = base_wait * random.uniform(0.8, 1.2)
                    logger.error(
                        f"[{slice_id}] Server error {e.response.status_code}. Retry {retries}/{page_max_retries}. Waiting {wait_time:.1f}s | URL: {url}"
                    )
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"[{slice_id}] Unexpected HTTP error {e.response.status_code}: {e} | URL: {url}")
                    return None, {'incomplete': True, 'skipped_offsets': []}
            except (requests.exceptions.Timeout, requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
                retries += 1
                base_wait = initial_wait * (backoff_multiplier ** (min(retries - 1, 5)))
                wait_time = base_wait * random.uniform(0.8, 1.2)
                logger.error(
                    f"[{slice_id}] Timeout/Connection error. Retry {retries}/{page_max_retries}. Waiting {wait_time:.1f}s | URL: {url}"
                )
                time.sleep(wait_time)
                continue
            except Exception as e:
                retries += 1
                if retries >= page_max_retries:
                    logger.warning("[Scope:Page] 达到页级最大重试次数，返回失败由外层处理")
                    return None, {'incomplete': True, 'skipped_offsets': []}
                # 未分类的异常也加入轻微随机抖动，避免集中重试
                time.sleep(initial_wait * random.uniform(0.8, 1.2))
                continue

    # 不应到达此处；如到达，视为失败
    return None, {'incomplete': True, 'skipped_offsets': []}

def get_all_super_alphas(session, start_date, end_date):
    """
    Fetch all super alphas using pagination with improved error handling.
    
    Args:
        session: A requests.Session object (or SessionProxy) to handle the API requests.
        start_date: The start date for filtering alphas.
        end_date: The end date for filtering alphas.

    Returns:
        A tuple of (fetched_alphas, info) when successful, or (None, info) when failed
        
    Note:
        使用SessionProxy时，不再需要返回session，因为SessionProxy会自动管理session生命周期
    """
    fetched_alphas = []
    offset = 0
    
    # 生成时间切片标识用于日志（包含年月日和时间）
    slice_id = f"{start_date[0:10]}T{start_date[11:16]}_to_{end_date[0:10]}T{end_date[11:16]}"
    
    limit = int(_CFG.get("PAGE_LIMIT", 100))
    page_max_retries = int(_CFG.get("PAGE_MAX_RETRIES", 7))
    initial_wait = int(_CFG.get("INITIAL_WAIT", 3))
    backoff_multiplier = float(_CFG.get("BACKOFF_MULTIPLIER", 2.0))

    try:
        from wq_logger import get_logger as _get_logger
        logger = _get_logger(__name__)
    except Exception:
        import logging as _logging
        logger = _logging.getLogger(__name__)
        if not logger.handlers:
            _handler = _logging.StreamHandler()
            _formatter = _logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
            _handler.setFormatter(_formatter)
            logger.addHandler(_handler)
        logger.setLevel(_logging.INFO)
    
    info = {'incomplete': False, 'skipped_offsets': []}
    while True:
        retries = 0
        # 将url变量定义移到try块外面，避免在异常处理中未定义的问题
        url = ""
        while retries < page_max_retries:
            try:
                from timezone_utils import build_api_query_with_time_slice
                query_params = build_api_query_with_time_slice(start_date, end_date, "SUPER")
                url = f"https://api.worldquantbrain.com/users/self/alphas?limit={limit}&offset={offset}&{query_params}&order={_CFG.get('ORDER_FIELD', 'dateCreated')}"
                
                # 注意：session可能是SessionProxy对象，不需要isinstance检查
                if session is None:
                    logger.error("无效的会话对象，停止分页并返回失败")
                    return None, {'incomplete': True, 'skipped_offsets': []}
                
                response = session.get(url, timeout=int(_CFG.get("HTTP_TIMEOUT", 30)))
                response.raise_for_status()
                alphas = response.json().get("results", [])
                fetched_alphas.extend(alphas)
                
                if len(alphas) < limit:
                    return fetched_alphas, info
                
                offset += limit
                break
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    retries += 1
                    if retries >= page_max_retries:
                        logger.warning(f"[{slice_id}] 429 达到页级最大重试次数，返回失败由外层处理 | URL: {url}")
                        return None, {'incomplete': True, 'skipped_offsets': []}
                    base_wait = initial_wait * (backoff_multiplier ** (retries - 1))
                    wait_time = base_wait * random.uniform(0.8, 1.2)
                    logger.warning(f"[{slice_id}] Rate limit 429. Retry {retries}/{page_max_retries}. Waiting {wait_time:.1f}s | URL: {url}")
                    time.sleep(wait_time)
                    continue
                elif e.response.status_code in {401, 403}:
                    logger.error(f"[{slice_id}] 会话未授权/过期 {e.response.status_code}，停止分页 | URL: {url}")
                    # 抛出异常以便上层刷新会话
                    raise Exception(f"会话未授权/过期，停止分页") from e
                elif e.response.status_code in {500, 502, 503, 504}:
                    retries += 1
                    base_wait = initial_wait * (backoff_multiplier ** (min(retries - 1, 5)))
                    wait_time = base_wait * random.uniform(0.8, 1.2)
                    logger.error(f"[{slice_id}] Server error {e.response.status_code}. Retry {retries}/{page_max_retries}. Waiting {wait_time:.1f}s | URL: {url}")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"[{slice_id}] Unexpected HTTP error {e.response.status_code}: {e} | URL: {url}")
                    return None, {'incomplete': True, 'skipped_offsets': []}
            except (requests.exceptions.Timeout, requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
                retries += 1
                base_wait = initial_wait * (backoff_multiplier ** (min(retries - 1, 5)))
                wait_time = base_wait * random.uniform(0.8, 1.2)
                logger.error(f"[{slice_id}] Timeout/Connection error. Retry {retries}/{page_max_retries}. Waiting {wait_time:.1f}s | URL: {url}")
                time.sleep(wait_time)
                continue
            except Exception as e:
                retries += 1
                if retries >= page_max_retries:
                    logger.warning("[Scope:Page] 达到页级最大重试次数，返回失败由外层处理")
                    return None, {'incomplete': True, 'skipped_offsets': []}
                time.sleep(initial_wait * random.uniform(0.8, 1.2))
                continue

    return None, {'incomplete': True, 'skipped_offsets': []}

def fetch_and_save_regular_alphas(session, start_date, end_date, userkey, base_directory):
    """
    获取并保存regular alphas
    
    Returns:
        (fetched_alphas, filename, session, info)
    """
    fetched_alphas, info = get_all_regular_alphas(session, start_date, end_date)
    
    if fetched_alphas is None:
        return None, None, info
    
    # 生成文件名（与稳定版保持一致）
    safe_start_date = re.sub(r'[:T]', '-', start_date)
    safe_end_date = re.sub(r'[:T]', '-', end_date)
    filename = f'{userkey}_{safe_start_date}_to_{safe_end_date}_all_regular_alphas.json'
    
    # 创建目录（与稳定版保持一致）
    directory = base_directory / "all_regular_alphas"
    directory.mkdir(parents=True, exist_ok=True)
    
    # 使用临时文件保存，确保原子性
    file_path = directory / filename
    temp_file = file_path.with_suffix('.tmp')
    
    try:
        # 写入临时文件
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(fetched_alphas, f, ensure_ascii=False, indent=2)
        
        # 原子性重命名（如果目标文件存在会被覆盖）
        temp_file.replace(file_path)
        
    except Exception as e:
        # 清理临时文件
        if temp_file.exists():
            try:
                temp_file.unlink()
            except:
                pass
        raise Exception(f"保存文件失败: {e}") from e
    
    return fetched_alphas, filename, info

def fetch_and_save_super_alphas(session, start_date, end_date, userkey, base_directory):
    """
    获取并保存super alphas
    
    Returns:
        (fetched_alphas, filename, session, info)
    """
    fetched_alphas, info = get_all_super_alphas(session, start_date, end_date)
    
    if fetched_alphas is None:
        return None, None, info
    
    # 生成文件名（与稳定版保持一致）
    safe_start_date = re.sub(r'[:T]', '-', start_date)
    safe_end_date = re.sub(r'[:T]', '-', end_date)
    filename = f'{userkey}_{safe_start_date}_to_{safe_end_date}_all_super_alphas.json'
    
    # 创建目录（与稳定版保持一致）
    directory = base_directory / "all_super_alphas"
    directory.mkdir(parents=True, exist_ok=True)
    
    # 使用临时文件保存，确保原子性
    file_path = directory / filename
    temp_file = file_path.with_suffix('.tmp')
    
    try:
        # 写入临时文件
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(fetched_alphas, f, ensure_ascii=False, indent=2)
        
        # 原子性重命名（如果目标文件存在会被覆盖）
        temp_file.replace(file_path)
        
    except Exception as e:
        # 清理临时文件
        if temp_file.exists():
            try:
                temp_file.unlink()
            except:
                pass
        raise Exception(f"保存文件失败: {e}") from e
    
    return fetched_alphas, filename, info