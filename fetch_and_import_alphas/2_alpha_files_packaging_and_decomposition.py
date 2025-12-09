#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Alpha文件打包和分解工具
支持两种处理模式：
1. full - 全量模式：处理所有文件，覆盖输出
2. incremental - 增量模式：从最新时间点往前推1小时开始处理
"""

import os
import re
import pathlib
import json
from datetime import datetime
from multiprocessing import Pool, cpu_count
from typing import Tuple, Optional, List, Dict
import time
import sys
import gc

# 导入日志和状态管理
try:
    from wq_logger import get_logger
    logger = get_logger(__name__)
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

try:
    from efficient_state_manager import SQLiteStateManager
    USE_STATE_MANAGER = True
except ImportError:
    logger.warning("无法导入 efficient_state_manager，将使用简单文件记录")
    USE_STATE_MANAGER = False


class ProcessingStats:
    """处理统计信息"""
    
    def __init__(self):
        self.processed_alphas = 0   # 处理的alpha总数
        self.failed_count = 0       # 失败的alpha
        self.total_files = 0        # 总文件数
        self.processed_files = 0    # 已处理文件数
        self.start_time = time.time()
        
        # 新增：用于检测数据完整性
        self.total_alphas_in_json = 0   # 从JSON文件中读取的alpha总数
        self.total_files_written = 0    # 实际写入的文件数量
        
        # 详细记录
        self.failed_items = []
    
    def add_processed(self, count: int):
        self.processed_alphas += count
    
    def add_failed(self, item: str, error: str):
        self.failed_count += 1
        self.failed_items.append((item, error))
    
    def print_summary(self):
        """打印统计摘要"""
        elapsed = time.time() - self.start_time
        
        logger.info("=" * 80)
        logger.info("处理统计摘要")
        logger.info("=" * 80)
        logger.info(f"总耗时: {elapsed:.2f} 秒")
        logger.info(f"处理文件: {self.processed_files}/{self.total_files}")
        logger.info(f"处理 Alpha: {self.processed_alphas}")
        logger.info(f"失败: {self.failed_count}")
        
        # 数据完整性检查
        if self.total_alphas_in_json > 0:
            diff = self.total_alphas_in_json - self.total_files_written
            if diff == 0:
                logger.info(f"Alpha处理: 读取{self.total_alphas_in_json}个, 写入{self.total_files_written}个文件")
            else:
                logger.warning(f"Alpha处理: 读取{self.total_alphas_in_json}个, 写入{self.total_files_written}个文件, 差异{abs(diff)}个")
        
        if self.failed_items:
            logger.info("\n失败详情:")
            for item, error in self.failed_items[:10]:  # 只显示前10个
                logger.info(f"  - {item}: {error}")
            if len(self.failed_items) > 10:
                logger.info(f"  ... 还有 {len(self.failed_items) - 10} 个失败项")
        
        logger.info("=" * 80)


def load_mongo_config():
    """
    Load workers configuration from mongo_config.json (same as Stage 3).
    Returns the number of workers to use.
    """
    config_file = pathlib.Path(__file__).parent / "mongo_config.json"
    
    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            workers_raw = config.get('workers')
            
            # Use same default calculation as Stage 3: CPU cores - 2
            if workers_raw is None:
                try:
                    default_workers = max(1, len(os.sched_getaffinity(0)) - 2)
                except AttributeError:
                    cpu_count_value = cpu_count()
                    default_workers = max(1, cpu_count_value - 2 if cpu_count_value and cpu_count_value > 2 else 1)
                return default_workers
            else:
                return max(1, int(workers_raw))
        except Exception as e:
            logger.warning(f"Failed to load mongo_config.json: {e}, using default")
    
    # Fallback: CPU cores - 2 (same as Stage 3)
    try:
        return max(1, len(os.sched_getaffinity(0)) - 2)
    except AttributeError:
        cpu_count_value = cpu_count()
        return max(1, cpu_count_value - 2 if cpu_count_value and cpu_count_value > 2 else 1)


def build_target(
    path: Optional[pathlib.Path], 
    meta: dict, 
    final_root: pathlib.Path
) -> pathlib.Path:
    """根据时间戳拼出目标目录"""
    # 处理时间戳：移除 .000Z 或 Z 后缀
    start_time = meta["start"].replace('.000Z', '').rstrip('Z')
    ts = datetime.strptime(start_time, "%Y-%m-%d-%H-%M-%S")
    month_folder = f"{meta['id']}_{ts:%Y-%m}_all_{meta['kind']}_alphas"
    day_folder   = f"{meta['id']}_{ts:%Y-%m-%d}_all_{meta['kind']}_alphas"
    target_dir   = final_root / month_folder / day_folder
    target_dir.mkdir(parents=True, exist_ok=True)
    return target_dir


def get_correct_target_dir_for_alpha(
    alpha: dict, 
    meta: dict, 
    final_root: pathlib.Path
) -> pathlib.Path:
    """根据Alpha的实际时间戳确定正确的目标目录（考虑时区转换）"""
    try:
        from timezone_utils import get_file_date_from_utc_timestamp
        
        date_created = alpha.get('dateCreated')
        if date_created:
            if date_created.endswith('Z'):
                file_date = get_file_date_from_utc_timestamp(date_created)
            else:
                from timezone_utils import convert_eastern_to_utc
                utc_dt = convert_eastern_to_utc(date_created)
                file_date = get_file_date_from_utc_timestamp(utc_dt.isoformat() + 'Z')
            
            file_dt = datetime.strptime(file_date, "%Y-%m-%d")
            month_folder = f"{meta['id']}_{file_dt:%Y-%m}_all_{meta['kind']}_alphas"
            day_folder = f"{meta['id']}_{file_dt:%Y-%m-%d}_all_{meta['kind']}_alphas"
            target_dir = final_root / month_folder / day_folder
            target_dir.mkdir(parents=True, exist_ok=True)
            return target_dir
            
    except Exception as e:
        logger.debug(f"时区转换失败，使用原始目录: {e}")
    
    # 传递None作为path参数，因为在这个上下文中我们没有具体的path对象
    return build_target(None, meta, final_root)


def split_and_copy_file(
    file_info: Tuple[pathlib.Path, re.Pattern, pathlib.Path]
) -> Tuple[bool, int, int, str]:
    """
    拆分文件并保存（multiprocessing-safe version）
    
    Args:
        file_info: Tuple of (path, re_name, final_root)
    
    Returns:
        Tuple of (success, alpha_count, alphas_in_json, error_message)
    """
    import hashlib
    
    path, re_name, final_root = file_info
    
    m = re_name.match(path.name)
    if not m:
        return False, 0, 0, f"Filename pattern mismatch: {path.name}"
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
            if not content:
                return True, 0, 0, ""
            data = json.loads(content)
        
        if not isinstance(data, list):
            return True, 0, 0, ""
        
        # Count alphas in JSON
        alphas_in_json = len(data)
        
        # Pre-extract metadata to avoid repeated calculations
        meta = m.groupdict()
        kind = meta.get('kind', 'regular')
        suffix = '_regular_alpha' if kind == 'regular' else '_super_alpha'
        base_filename = re.sub(r'_all_(regular|super)_alphas', suffix, path.name)
        name_part, ext = os.path.splitext(base_filename)
        
        # Pre-import timezone module
        try:
            from timezone_utils import get_file_date_from_utc_timestamp, convert_eastern_to_utc
            has_tz_utils = True
        except ImportError:
            has_tz_utils = False
        
        count = 0
        
        for alpha in data:
            # Determine target directory
            if has_tz_utils:
                correct_target_dir = get_correct_target_dir_for_alpha(alpha, meta, final_root)
            else:
                correct_target_dir = build_target(path, meta, final_root)
            
            author = str(alpha.get('author', 'unknown')).replace(os.sep, '_')
            alpha_id = str(alpha.get('id', 'unknown')).replace(os.sep, '_')
            
            # Generate 12-char MD5 hash (required by Stage 3's filename pattern)
            alpha_id_hash = hashlib.md5(alpha_id.encode('utf-8')).hexdigest()[:12]
            
            # Filename format MUST match Stage 3's regex: ..._<alpha_id>_<hash>.json
            new_filename = f"{name_part}_{author}_{alpha_id}_{alpha_id_hash}{ext}"
            new_path = correct_target_dir / new_filename
            
            # Write directly (keep formatted for readability)
            try:
                with open(new_path, 'w', encoding='utf-8') as f_out:
                    json.dump(alpha, f_out, indent=2, ensure_ascii=False)
                count += 1
            except Exception as write_err:
                return False, count, alphas_in_json, f"Failed to write {new_path}: {write_err}"
        
        return True, count, alphas_in_json, ""
        
    except json.JSONDecodeError as exc:
        return False, 0, 0, f"JSON decode error in {path.name}: {exc}"
    except Exception as exc:
        return False, 0, 0, f"Unexpected error in {path.name}: {exc}"


def find_latest_output_time(final_root: pathlib.Path) -> Optional[datetime]:
    """
    扫描输出目录，找到最新的文件时间（用于incremental模式）
    往前推1小时作为起始时间
    
    优化策略：
    1. 扫描目录名，找到最新的日期目录
    2. 检查该目录是否有文件，如果为空则往前推一个目录
    3. 找到有文件的目录后，扫描该目录获取最新文件时间
    4. 往前推1小时作为起始时间
    
    Returns:
        最新文件时间往前推1小时的时间戳，如果没有文件则返回None
    """
    logger.info("扫描输出目录，查找最新文件...")
    
    # 正则匹配目录名中的日期：lk_2025-10-20_all_regular_alphas
    day_dir_pattern = re.compile(r'[A-Za-z0-9]+_(\d{4}-\d{2}-\d{2})_all_(regular|super)_alphas')
    time_pattern = re.compile(r'(\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2})')
    
    try:
        # 步骤1：收集所有日期目录并排序
        day_dirs = []
        for month_dir in final_root.iterdir():
            if not month_dir.is_dir():
                continue
            
            for day_dir in month_dir.iterdir():
                if not day_dir.is_dir():
                    continue
                
                match = day_dir_pattern.match(day_dir.name)
                if match:
                    try:
                        date_str = match.group(1)  # 2025-10-20
                        dir_date = datetime.strptime(date_str, "%Y-%m-%d")
                        day_dirs.append((dir_date, day_dir))
                    except Exception as e:
                        logger.debug(f"解析目录日期失败 {day_dir.name}: {e}")
        
        if not day_dirs:
            logger.info("未找到任何输出目录，将处理所有文件")
            return None
        
        # 按日期降序排序（最新的在前）
        day_dirs.sort(key=lambda x: x[0], reverse=True)
        logger.info(f"扫描了 {len(day_dirs)} 个日期目录")
        
        # 步骤2：从最新目录开始，找到第一个有文件的目录
        latest_time = None
        checked_dirs = 0
        
        for dir_date, day_dir in day_dirs:
            checked_dirs += 1
            
            # 检查目录中是否有文件
            json_files = list(day_dir.glob("*.json"))
            
            if json_files:
                # 找到有文件的目录，扫描该目录获取最新文件时间
                logger.info(f"检查目录: {day_dir.name} (有 {len(json_files)} 个文件)")
                
                for json_file in json_files:
                    match = time_pattern.search(json_file.name)
                    if match:
                        try:
                            file_time = datetime.strptime(match.group(1), "%Y-%m-%d-%H-%M-%S")
                            if latest_time is None or file_time > latest_time:
                                latest_time = file_time
                        except:
                            pass
                
                # 找到最新时间后退出
                if latest_time:
                    break
            else:
                logger.debug(f"目录为空，跳过: {day_dir.name}")
            
            # 最多检查最近1000个目录
            if checked_dirs >= 1000:
                logger.warning("已检查1000个目录，停止扫描")
                break
        
        if latest_time:
            # 往前推1小时
            from datetime import timedelta
            start_time = latest_time - timedelta(hours=1)
            logger.info(f"找到最新文件时间: {latest_time}")
            logger.info(f"往前推1小时，起始时间: {start_time}")
            logger.info(f"检查了 {checked_dirs} 个目录")
            return start_time
        else:
            logger.info("未找到任何文件，将处理所有文件")
            return None
            
    except Exception as e:
        logger.warning(f"扫描目录时出错，回退到文件扫描: {e}")
        return find_latest_output_time_fallback(final_root)


def find_latest_output_time_fallback(final_root: pathlib.Path) -> Optional[datetime]:
    """
    回退方法：扫描文件（限制范围）
    只扫描最近3个月的数据
    """
    logger.info("使用回退方法：扫描最近3个月的文件...")
    
    latest_time = None
    file_count = 0
    
    # 只扫描最近3个月
    from datetime import timedelta
    now = datetime.now()
    three_months_ago = now - timedelta(days=90)
    cutoff_month = three_months_ago.strftime("%Y-%m")
    
    time_pattern = re.compile(r'(\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2})')
    
    for month_dir in final_root.iterdir():
        if not month_dir.is_dir():
            continue
        
        # 跳过太旧的月份目录
        if cutoff_month in month_dir.name or month_dir.name > cutoff_month:
            for json_file in month_dir.rglob("*.json"):
                file_count += 1
                match = time_pattern.search(json_file.name)
                if match:
                    try:
                        file_time = datetime.strptime(match.group(1), "%Y-%m-%d-%H-%M-%S")
                        if latest_time is None or file_time > latest_time:
                            latest_time = file_time
                    except:
                        pass
                
                # 限制扫描数量
                if file_count > 10000:
                    logger.warning("扫描文件数超过1万，停止扫描")
                    break
    
    if latest_time:
        from datetime import timedelta
        start_time = latest_time - timedelta(hours=1)
        logger.info(f"找到最新文件时间: {latest_time}")
        logger.info(f"往前推1小时，起始时间: {start_time}")
        logger.info(f"扫描了 {file_count} 个文件（限制范围）")
        return start_time
    else:
        logger.info("未找到任何文件")
        return None


def group_and_split_by_date(
    root: pathlib.Path, 
    final_root: pathlib.Path, 
    re_name: re.Pattern, 
    workers: int,
    mode: str = 'full',
    start_time_str: Optional[str] = None,
    end_time_str: Optional[str] = None
):
    """
    按日期分组批量拆分并复制文件
    
    Args:
        mode: 处理模式 'full' | 'incremental' | 'manual'
        start_time_str: 手动模式的起始时间 (格式: YYYY-MM-DD-HH-MM-SS)
        end_time_str: 手动模式的结束时间 (格式: YYYY-MM-DD-HH-MM-SS)
    """
    stats = ProcessingStats()
    
    logger.info("=" * 80)
    logger.info(f"开始处理 - 模式: {mode.upper()}")
    logger.info("=" * 80)
    
    # 步骤1：收集需要处理的文件
    files_to_process: List[pathlib.Path] = []
    
    # 初始化时间过滤变量
    cutoff_str = None
    end_cutoff_str = None
    
    if mode == 'incremental':
        # 增量模式：找到最新时间点，往前推1小时
        start_time = find_latest_output_time(final_root)
        
        if start_time:
            cutoff_str = start_time.strftime("%Y-%m-%d-%H-%M-%S")
        # 增量模式没有结束时间限制
        end_cutoff_str = None
    elif mode == 'manual':
        # 手动模式：使用指定的时间范围
        cutoff_str = start_time_str
        end_cutoff_str = end_time_str
        if cutoff_str:
            logger.info(f"手动指定起始时间: {cutoff_str}")
        if end_cutoff_str:
            logger.info(f"手动指定结束时间: {end_cutoff_str}")
    else:
        # full模式：处理所有文件，无时间限制
        cutoff_str = None
        end_cutoff_str = None
    
    logger.info("扫描输入文件...")
    for kind in ("all_regular_alphas", "all_super_alphas"):
        kind_dir = root / kind
        if not kind_dir.exists():
            logger.warning(f"目录不存在，跳过: {kind_dir}")
            continue
        
        for file in kind_dir.glob("*.json"):
            if not file.is_file() or not re_name.match(file.name):
                continue
            
            # 时间过滤
            if mode in ('incremental', 'manual') and (cutoff_str or end_cutoff_str):
                # 提取文件名中的时间戳
                match = re.search(r'(\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2})', file.name)
                if match:
                    file_time_str = match.group(1)
                    
                    # 起始时间过滤（适用于增量模式和手动模式）
                    if cutoff_str and file_time_str < cutoff_str:
                        continue  # 跳过旧文件
                    
                    # 结束时间过滤（仅手动模式）
                    if mode == 'manual' and end_cutoff_str and file_time_str > end_cutoff_str:
                        continue  # 跳过太新的文件
            
            files_to_process.append(file)
    
    if not files_to_process:
        logger.info("未找到需要处理的文件")
        return
    
    stats.total_files = len(files_to_process)
    logger.info(f"发现 {stats.total_files} 个文件需要处理")
    logger.info(f"使用 {workers} 个工作进程 (multiprocessing, 真正并行)")
    
    # Check Python 3.14 free-threading
    if hasattr(sys, '_is_gil_enabled'):
        gil_enabled = sys._is_gil_enabled()
        if not gil_enabled:
            logger.info("Python 3.14+ free-threading detected (GIL disabled)")
        else:
            logger.info(f"Python 3.14 detected with GIL enabled (using multiprocessing for true parallelism)")
    else:
        logger.info(f"Python {sys.version_info.major}.{sys.version_info.minor} (using multiprocessing for true parallelism)")
    
    logger.info("")
    
    # 步骤2：并发处理 (multiprocessing for true parallelism, same as Stage 3)
    # Prepare arguments for each file
    file_args = [(file_path, re_name, final_root) for file_path in files_to_process]
    
    with Pool(processes=workers) as pool:
        # Use imap_unordered for better progress tracking
        results_iterator = pool.imap_unordered(split_and_copy_file, file_args)
        
        for success, alpha_count, alphas_in_json, error_msg in results_iterator:
            stats.processed_files += 1
            stats.total_alphas_in_json += alphas_in_json
            
            if success:
                stats.add_processed(alpha_count)
                stats.total_files_written += alpha_count
            else:
                stats.failed_count += 1
                if error_msg:
                    stats.add_failed(f"File {stats.processed_files}", error_msg)
                    logger.warning(f"Processing error: {error_msg}")
            
            # Real-time progress display
            progress = (stats.processed_files / stats.total_files) * 100
            print(f"\r进度: {stats.processed_files}/{stats.total_files} ({progress:.1f}%) | "
                  f"已处理:{stats.processed_alphas} 个alpha", 
                  end="", flush=True)
    
    print()  # Newline
    logger.info("")
    
    # 步骤3：打印统计摘要
    stats.print_summary()
    
    # 步骤4：程序运行完毕，清理所有累积的内存
    del file_args, files_to_process, stats
    gc.collect()
    logger.info("内存清理完成")


if __name__ == "__main__":
    import argparse
    from wq_login import USER_KEY, WQ_DATA_ROOT
    
    # Load workers from mongo_config.json (same as Stage 3)
    DEFAULT_WORKERS = load_mongo_config()
    
    # Load mode from async_config.json for backward compatibility
    ASYNC_CONFIG_FILE = pathlib.Path(__file__).parent / "async_config.json"
    DEFAULT_MODE = 'incremental'
    
    if ASYNC_CONFIG_FILE.exists():
        try:
            with open(ASYNC_CONFIG_FILE, 'r', encoding='utf-8') as f:
                config = json.load(f)
            DEFAULT_MODE = config.get('PACKAGING_MODE', 'incremental')
            logger.info(f"已加载模式配置: {ASYNC_CONFIG_FILE}")
        except Exception as e:
            logger.warning(f"加载async_config.json失败: {e}")
    
    parser = argparse.ArgumentParser(
        description="Alpha文件打包和分解工具 (使用multiprocessing实现真正并行)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
处理模式说明:
  full        - 全量模式：处理所有文件，覆盖已存在的输出
  incremental - 增量模式：从最新时间点往前推1小时开始处理
  manual      - 手动模式：指定起始和结束时间范围

工作进程数量:
  默认从 mongo_config.json 的 workers 参数读取 (与第三阶段一致)
  如果未配置，则使用 CPU核心数-2
  
手动模式示例:
  python 2_alpha_files_packaging_and_decomposition.py --mode manual --start-time 2024-09-18-14-00-00 --end-time 2024-09-25-14-00-00
        """
    )
    
    parser.add_argument(
        '--mode', 
        type=str, 
        choices=['full', 'incremental', 'manual'],
        default=DEFAULT_MODE,
        help=f'处理模式（默认: {DEFAULT_MODE}，可在 async_config.json 中配置）'
    )
    
    parser.add_argument(
        '--workers', 
        type=int, 
        default=DEFAULT_WORKERS,
        help=f'工作进程数量（默认: {DEFAULT_WORKERS}，从mongo_config.json读取）'
    )
    
    parser.add_argument(
        '--start-time',
        type=str,
        help='手动模式：起始时间（格式: YYYY-MM-DD-HH-MM-SS，例如: 2024-09-18-14-00-00）'
    )
    
    parser.add_argument(
        '--end-time',
        type=str,
        help='手动模式：结束时间（格式: YYYY-MM-DD-HH-MM-SS，例如: 2024-09-25-14-00-00）'
    )
    
    args = parser.parse_args()
    
    # 验证手动模式参数
    if args.mode == 'manual':
        if not args.start_time:
            parser.error("手动模式需要指定 --start-time 参数")
        # 验证时间格式
        try:
            datetime.strptime(args.start_time, "%Y-%m-%d-%H-%M-%S")
            if args.end_time:
                datetime.strptime(args.end_time, "%Y-%m-%d-%H-%M-%S")
        except ValueError as e:
            parser.error(f"时间格式错误，应为 YYYY-MM-DD-HH-MM-SS: {e}")
    
    # 路径配置
    PRIMEVAL_ROOT = WQ_DATA_ROOT / USER_KEY / "primeval_data"
    FINAL_ROOT = WQ_DATA_ROOT / USER_KEY / "final_data"
    FINAL_ROOT.mkdir(exist_ok=True, parents=True)
    
    # Determine workers (using multiprocessing, same calculation as Stage 3)
    if args.workers:
        WORKERS = max(1, args.workers)
    else:
        # Use value from mongo_config.json (already loaded)
        WORKERS = DEFAULT_WORKERS
    
    # 文件名正则
    RE_NAME = re.compile(
        r'^(?P<id>[A-Za-z0-9@._]+)'
        r'_(?P<start>\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}\.000Z)_to_'
        r'(?P<end>\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}\.000Z)_'
        r'all_(?P<kind>regular|super)_alphas\.json$'
    )
    
    # 开始处理
    group_and_split_by_date(
        PRIMEVAL_ROOT, 
        FINAL_ROOT, 
        RE_NAME, 
        WORKERS, 
        args.mode,
        args.start_time if args.mode == 'manual' else None,
        args.end_time if args.mode == 'manual' else None
    )
    
    # 程序完全结束，最终清理
    gc.collect()
    logger.info("程序结束，已释放内存")
