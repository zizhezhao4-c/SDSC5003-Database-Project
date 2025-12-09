#!/usr/bin/env python3
"""
MongoDB Alpha Import Tool v4.0
High-performance, modular batch import system for WorldQuant alpha JSON files.

Architecture:
- 1 main process + N worker processes (multiprocessing.Pool)
- Sequential month processing (all regular, then all super)
- Deep merge strategy preserving existing fields
- Optimized memory management with batch processing
- Supports Python 3.14+ free-threading (GIL disabled)
"""

import gc
import json
import logging
import argparse
import time
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Tuple, Optional, Set
from collections import defaultdict

from pymongo import MongoClient, UpdateOne, ReplaceOne
from pymongo.errors import ConnectionFailure, BulkWriteError
from multiprocessing import Pool, cpu_count
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Check for Python 3.14+ free-threading support (GIL disabled)
GIL_ENABLED = True
if hasattr(sys, '_is_gil_enabled'):
    GIL_ENABLED = sys._is_gil_enabled()
    if not GIL_ENABLED:
        logger_early = logging.getLogger('MongoDB_Importer_v4')
        logger_early.info("Python 3.14+ free-threading detected (GIL disabled) - multiprocessing will be used")

# Import project modules (unchanged)
import wq_login
from wq_logger import get_logger

# Ensure project root is on sys.path so spawned workers can import helpers
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from timezone_utils import convert_eastern_to_utc  # type: ignore
    TIMEZONE_UTILS_AVAILABLE = True
except ImportError:  # pragma: no cover - optional dependency
    TIMEZONE_UTILS_AVAILABLE = False

# Try to import efficient state manager (after sys.path setup)
STATE_MANAGER_AVAILABLE = False
STATE_MANAGER_WARNING_EMITTED = False
try:
    from efficient_state_manager import create_state_manager, SQLiteStateManager
    STATE_MANAGER_AVAILABLE = True
except ImportError:
    pass

# Constants
DAY_PARALLEL_FILE_THRESHOLD = 20000
MAX_PARALLEL_DAYS = 4

# Initialize logger
logger = get_logger('MongoDB_Importer_v4')

# ============================================================================
# PHASE 1: CORE MODULE DESIGN
# ============================================================================

# ----------------------------------------------------------------------------
# 1.1 Configuration & Initialization
# ----------------------------------------------------------------------------

class Config:
    """Configuration container with validation"""
    
    def __init__(self, config_dict: dict):
        # MongoDB settings
        self.db_host = config_dict.get('db_host', 'localhost')
        raw_db_port = config_dict.get('db_port')
        if raw_db_port is None:
            raw_db_port = config_dict.get('db_port_27017', 27017)
        self.db_port = self._coerce_int(raw_db_port, 27017)
        self.db_port_27017 = self._coerce_int(config_dict.get('db_port_27017', self.db_port), self.db_port)
        raw_port_27018 = config_dict.get('db_port_27018')
        self.db_port_27018 = self._coerce_int(raw_port_27018, 27018) if raw_port_27018 is not None else None
        self.db_name_regular = config_dict.get('db_name_regular', 'regular_alphas')
        self.db_name_super = config_dict.get('db_name_super', 'super_alphas')
        
        # Data paths
        self.data_base_path = config_dict.get('data_base_path') or \
                             str(wq_login.WQ_DATA_ROOT / wq_login.USER_KEY / "final_data")
        self.regular_collection = config_dict.get('regular_collection') or wq_login.USER_KEY
        self.super_collection = config_dict.get('super_collection') or wq_login.USER_KEY
        
        # Performance settings
        default_workers = self._calculate_default_workers()
        raw_workers = config_dict.get('workers')
        self.workers = self._coerce_int(raw_workers, default_workers) if raw_workers is not None else default_workers
        raw_batch_size = config_dict.get('batch_size')
        self.batch_size = self._coerce_int(raw_batch_size, 1000) if raw_batch_size is not None else 1000
        
        # Advanced toggles
        self.fast_mode = self._coerce_bool(config_dict.get('FAST_MODE', False))
        self.fast_mode_months = self._coerce_int(config_dict.get('FAST_MODE_MONTHS', 3), 3)
        self.use_preload = self._coerce_bool(config_dict.get('USE_PRELOAD', True))
        self.use_batch_query = self._coerce_bool(config_dict.get('USE_BATCH_QUERY', False))
        self.parallel_days_mode = self._coerce_bool(config_dict.get('PARALLEL_DAYS_MODE', False))
        self.parallel_days_workers = self._coerce_int(
            config_dict.get('PARALLEL_DAYS_WORKERS', 2), 2
        )
        
        # Time-range settings
        self.time_range_hours = self._coerce_int(
            config_dict.get('TIME_RANGE_HOURS', 25), 25
        )
        self.time_range_buffer_days = self._coerce_int(
            config_dict.get('TIME_RANGE_BUFFER_DAYS', 3), 3
        )
        
        # Other settings
        self.key_field = config_dict.get('key_field', 'id')
        self.state_file = config_dict.get('state_file') or \
                         str(Path(wq_login.WQ_LOGS_ROOT) / wq_login.USER_KEY / 
                             "fetch_and_import_alphas" / f"processed_files_{wq_login.USER_KEY}.log")
        self.use_smart_validation = self._coerce_bool(config_dict.get('use_smart_validation', False))
        self.log_path = config_dict.get('log_path') or config_dict.get('LOG_PATH')
        
        # Import mode
        self.import_mode = config_dict.get('IMPORT_MODE', 'incremental')
        
        # Validate configuration
        self._validate()
    
    @staticmethod
    def _coerce_bool(value, default: bool = False) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {'true', '1', 'yes', 'on'}:
                return True
            if normalized in {'false', '0', 'no', 'off'}:
                return False
        return bool(value)
    
    @staticmethod
    def _coerce_int(value, default: int) -> int:
        if value is None:
            return default
        try:
            return int(value)
        except (TypeError, ValueError):
            raise ValueError(f"Expected integer value, got {value!r}")
    
    def _calculate_default_workers(self) -> int:
        """Calculate default number of workers"""
        try:
            return max(1, len(os.sched_getaffinity(0)) - 2)
        except AttributeError:
            cpu_count_value = cpu_count()
            return max(1, cpu_count_value - 2 if cpu_count_value and cpu_count_value > 2 else 1)
    
    def _validate(self):
        """Validate configuration"""
        if not Path(self.data_base_path).exists():
            raise ValueError(f"Data base path does not exist: {self.data_base_path}")
        
        if self.workers < 1:
            raise ValueError(f"Workers must be >= 1, got {self.workers}")
        
        if self.batch_size < 1:
            raise ValueError(f"Batch size must be >= 1, got {self.batch_size}")
        
        valid_modes = ['incremental_fast', 'incremental_slow', 
                      'overwrite', 'rebuild', 'time_range', 'smart_merge', 'smart_incremental_slow']
        if self.import_mode not in valid_modes:
            raise ValueError(f"Invalid import mode: {self.import_mode}")
        
        if self.time_range_hours <= 0:
            raise ValueError(f"TIME_RANGE_HOURS must be > 0, got {self.time_range_hours}")
        
        if self.time_range_buffer_days < 0:
            raise ValueError(f"TIME_RANGE_BUFFER_DAYS must be >= 0, got {self.time_range_buffer_days}")
        
        if self.log_path is not None and not isinstance(self.log_path, str):
            raise ValueError("log_path must be a string path or null")
        
        if self.import_mode == 'rebuild':
            if self.fast_mode:
                raise ValueError("FAST_MODE cannot be enabled when IMPORT_MODE is 'rebuild'")
            if self.use_batch_query:
                raise ValueError("USE_BATCH_QUERY is not supported in rebuild mode (rebuild replaces entire documents)")
        
        if self.fast_mode and self.fast_mode_months < 1:
            raise ValueError(f"FAST_MODE_MONTHS must be >= 1, got {self.fast_mode_months}")

        # Note: FAST_MODE now takes precedence over time_range/smart_merge month filtering
        # No conflict check needed - FAST_MODE will override other month filters
        
        if self.db_port is None:
            raise ValueError("MongoDB port must be specified via db_port or db_port_27017")
        if self.db_port <= 0:
            raise ValueError(f"MongoDB port must be positive, got {self.db_port}")
        if self.db_port_27017 <= 0:
            raise ValueError(f"db_port_27017 must be positive, got {self.db_port_27017}")
        if self.db_port_27018 is not None and self.db_port_27018 <= 0:
            raise ValueError(f"db_port_27018 must be positive, got {self.db_port_27018}")

        if self.parallel_days_workers < 1:
            raise ValueError("PARALLEL_DAYS_WORKERS must be >= 1")
    
    def __repr__(self):
        return (f"Config(workers={self.workers}, batch_size={self.batch_size}, "
                f"mode={self.import_mode}, db={self.db_host}:{self.db_port})")


def load_config(config_path: str = "mongo_config.json") -> Config:
    """Load and validate configuration from JSON file
    
    Searches for config file in the following order:
    1. Absolute path (if provided)
    2. Relative to script directory
    3. Relative to current working directory
    """
    config_file = Path(config_path)
    
    # If not absolute, try script directory first
    if not config_file.is_absolute():
        script_dir = Path(__file__).resolve().parent
        script_relative = script_dir / config_path
        if script_relative.exists():
            config_file = script_relative
        elif not config_file.exists():
            raise FileNotFoundError(
                f"Configuration file not found: {config_path}\n"
                f"Searched in:\n"
                f"  - {script_relative}\n"
                f"  - {Path.cwd() / config_path}"
            )
    elif not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_file, 'r', encoding='utf-8') as f:
        config_dict = json.load(f)
    logger.info(f"Loaded configuration from {config_file}")
    return Config(config_dict)


def _add_boolean_cli_flag(
    parser: argparse.ArgumentParser,
    *,
    option: str,
    dest: str,
    default: bool,
    help_enable: str,
    help_disable: str
) -> None:
    """Register paired --foo / --no-foo CLI flags."""

    group = parser.add_mutually_exclusive_group()
    group.add_argument(f"--{option}", dest=dest, action='store_true', help=help_enable)
    group.add_argument(f"--no-{option}", dest=dest, action='store_false', help=help_disable)
    parser.set_defaults(**{dest: default})


def test_mongodb_connection(host: str, port: int) -> bool:
    """Test MongoDB connection"""
    try:
        with MongoClient(host=host, port=port, serverSelectionTimeoutMS=5000) as client:
            client.admin.command('ping')
        logger.info(f"MongoDB connection successful: {host}:{port}")
        return True
    except ConnectionFailure as e:
        logger.error(f"MongoDB connection failed: {e}")
        return False


def validate_document_smart(doc: dict) -> Tuple[bool, str]:
    """Lightweight smart validation for alpha documents."""

    required_fields = ['id', 'type', 'author']
    for field in required_fields:
        if field not in doc:
            return False, f"Missing required field '{field}'"

    alpha_type = doc.get('type')
    if alpha_type not in {'REGULAR', 'SUPER'}:
        return False, f"Invalid type value '{alpha_type}'"

    return True, ""


def parse_alpha_timestamp(date_str: Optional[str]) -> Optional[datetime]:
    """Parse an alpha timestamp string into a UTC datetime."""

    if not date_str:
        return None

    if TIMEZONE_UTILS_AVAILABLE:
        try:
            dt = convert_eastern_to_utc(date_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt
        except Exception:
            pass

    try:
        normalized = date_str.replace('Z', '+00:00') if date_str.endswith('Z') else date_str
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        return None


def extract_timestamp_from_file(file_path: Path) -> Optional[datetime]:
    """Extract the most relevant timestamp from an alpha JSON file."""

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            doc = json.load(f)
    except Exception:
        return None

    for key in ('dateModified', 'dateCreated'):
        dt = parse_alpha_timestamp(doc.get(key))
        if dt:
            return dt

    return None


def filter_files_by_time_window(
    files: List[Path],
    window_start: Optional[datetime],
    window_end: Optional[datetime]
) -> List[Path]:
    """Filter files to those whose timestamps fall within the window."""

    if window_start is None and window_end is None:
        return files

    kept = []
    dropped = 0

    for file_path in files:
        doc_time = extract_timestamp_from_file(file_path)
        if doc_time is None:
            kept.append(file_path)
            continue

        if window_start and doc_time < window_start:
            dropped += 1
            continue
        if window_end and doc_time > window_end:
            dropped += 1
            continue

        kept.append(file_path)

    if dropped:
        logger.info(f"Time window filter dropped {dropped} files out of {len(files)} candidates")

    return kept


def extract_month_from_dirname(directory: Path) -> Optional[datetime]:
    """Extract YYYY-MM from directory name."""

    try:
        parts = directory.name.split('_')
        for part in parts:
            if len(part) == 7 and part[4] == '-':
                month_dt = datetime.strptime(part, '%Y-%m')
                return month_dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None
    return None


def filter_month_dirs_by_window(
    month_dirs: List[Path],
    window_start: Optional[datetime]
) -> List[Path]:
    """Filter month directories so they start at or after window_start."""

    if window_start is None:
        return month_dirs

    filtered = []
    for month_dir in month_dirs:
        month_dt = extract_month_from_dirname(month_dir)
        if month_dt is None or month_dt >= window_start.replace(day=1, hour=0, minute=0, second=0, microsecond=0):
            filtered.append(month_dir)

    return filtered


def configure_logging(config: Config) -> None:
    """Configure optional file logging based on config.log_path."""

    if not config.log_path:
        return

    try:
        log_path = Path(config.log_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        root_logger = logging.getLogger()
        existing_paths = {
            getattr(handler, 'baseFilename', None)
            for handler in root_logger.handlers
            if hasattr(handler, 'baseFilename')
        }

        if str(log_path) in existing_paths:
            return

        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        formatter = logging.Formatter(
            '%(asctime)s - %(processName)s - %(levelname)s - %(message)s',
            '%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
        logger.info(f"File logging enabled: {log_path}")
    except Exception as e:
        logger.warning(f"Failed to configure log file '{config.log_path}': {e}")


def get_latest_alpha_timestamp(collection) -> Optional[datetime]:
    """Fetch the latest alpha timestamp (dateModified/dateCreated) from MongoDB."""

    try:
        latest_doc = collection.find_one(
            {"dateModified": {"$exists": True}},
            sort=[("dateModified", -1)]
        )
        if not latest_doc:
            latest_doc = collection.find_one(
                {"dateCreated": {"$exists": True}},
                sort=[("dateCreated", -1)]
            )

        if not latest_doc:
            return None

        for field in ("dateModified", "dateCreated"):
            timestamp = parse_alpha_timestamp(latest_doc.get(field))
            if timestamp:
                return timestamp
    except Exception as e:
        logger.warning(f"Failed to query latest timestamp from MongoDB: {e}")

    return None


def estimate_avg_files_per_day(day_dirs: List[Path], sample_size: int = 3) -> float:
    """Estimate average number of JSON files per day directory."""

    counts: List[int] = []
    for day_dir in day_dirs[:sample_size]:
        try:
            count = sum(1 for _ in day_dir.rglob('*.json'))
            counts.append(count)
        except Exception as e:
            logger.warning(f"Failed to count files in {day_dir}: {e}")

    if not counts:
        return 0.0

    return sum(counts) / len(counts)


def process_month_parallel_days(
    month_path: Path,
    collection,
    config: Config,
    state_manager,
    alpha_type: str,
    time_window_start: Optional[datetime],
    time_window_end: Optional[datetime]
) -> Optional[dict]:
    """Process a month directory day-by-day when parallel day mode is enabled."""

    try:
        day_dirs = sorted([d for d in month_path.iterdir() if d.is_dir()])
    except Exception as e:
        logger.warning(f"Failed to enumerate day directories in {month_path}: {e}")
        return None

    if not day_dirs:
        return None

    avg_files = estimate_avg_files_per_day(day_dirs)
    if avg_files >= DAY_PARALLEL_FILE_THRESHOLD:
        logger.info(
            "Parallel days mode skipped for %s: avg %.0f files/day >= %d",
            month_path.name,
            avg_files,
            DAY_PARALLEL_FILE_THRESHOLD
        )
        return None

    logger.info(
        "Parallel days mode active for %s: %d day directories, %.0f files/day",
        month_path.name,
        len(day_dirs),
        avg_files
    )

    aggregated = {'updated': 0, 'inserted': 0, 'errors': 0, 'processed': 0}

    day_tasks: List[Tuple[str, List[str]]] = []

    for day_dir in day_dirs:
        try:
            day_paths = list(day_dir.rglob('*.json'))
        except Exception as e:
            logger.warning(f"Failed to scan {day_dir}: {e}")
            continue

        if time_window_start is not None or time_window_end is not None:
            day_paths = filter_files_by_time_window(day_paths, time_window_start, time_window_end)
        if not day_paths:
            continue

        day_files = filter_files_by_state(day_paths, state_manager, config.import_mode, config.workers)
        if not day_files:
            continue

        day_tasks.append((day_dir.name, [str(p) for p in day_files]))

    if not day_tasks:
        logger.info("No eligible day directories after filtering")
        return aggregated

    max_workers = min(config.parallel_days_workers, len(day_tasks))

    def _process_single_day(day_name: str, files: List[str]):
        logger.info("Parallel day start: %s (%d files)", day_name, len(files))
        stats = process_batch(files, collection, config, None, show_progress=False)
        stats['day_name'] = day_name
        logger.info(
            "Parallel day finished: %s | processed=%d updated=%d inserted=%d errors=%d",
            day_name,
            stats['processed'],
            stats['updated'],
            stats['inserted'],
            stats['errors']
        )
        return stats

    all_processed_paths: List[str] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_process_single_day, day_name, file_list): (day_name, file_list)
            for day_name, file_list in day_tasks
        }

        for future in as_completed(future_map):
            day_name, file_list = future_map[future]
            try:
                day_stats = future.result()
            except Exception as exc:
                logger.error(f"Parallel day failed: {day_name} ({exc})")
                aggregated['errors'] += 1
                continue

            for key in aggregated:
                aggregated[key] += day_stats.get(key, 0)

            processed_paths = day_stats.get('processed_paths', [])
            if processed_paths:
                all_processed_paths.extend(processed_paths)

    if state_manager and all_processed_paths:
        state_manager.mark_processed(all_processed_paths)

    return aggregated


# ----------------------------------------------------------------------------
# 1.2 Deep Merge Engine (ported from old code, proven logic)
# ----------------------------------------------------------------------------

def deep_merge_dicts(old_dict: dict, new_dict: dict) -> dict:
    """
    Deep recursive merge of two dictionaries.
    - New values override old values
    - Old keys not in new dict are retained
    - Recursively handles nested dictionaries
    - Intelligently handles arrays (merge by unique key if possible)
    
    Args:
        old_dict: Existing document from MongoDB
        new_dict: New document from JSON file
    
    Returns:
        Merged dictionary
    """
    if not isinstance(old_dict, dict) or not isinstance(new_dict, dict):
        return new_dict
    
    result = old_dict.copy()
    
    for key, new_value in new_dict.items():
        if key not in result:
            # New field: add it
            result[key] = new_value
        else:
            old_value = result[key]
            
            # Recursively handle nested dictionaries
            if isinstance(old_value, dict) and isinstance(new_value, dict):
                result[key] = deep_merge_dicts(old_value, new_value)
            # Intelligently handle arrays
            elif isinstance(old_value, list) and isinstance(new_value, list):
                result[key] = merge_arrays(old_value, new_value)
            # Other cases: new value overrides old value
            else:
                result[key] = new_value
    
    return result


def merge_arrays(old_array: list, new_array: list) -> list:
    """
    Intelligently merge two arrays.
    - If arrays contain objects with unique keys ('id' or 'name'), merge by that key
    - Otherwise, replace with new array
    
    Optimization: Only check first element to determine array type
    
    Args:
        old_array: Existing array from MongoDB
        new_array: New array from JSON file
    
    Returns:
        Merged array
    """
    # If new array is empty, keep old array
    if not new_array:
        return old_array
    
    # If old array is empty, use new array
    if not old_array:
        return new_array
    
    # Check array element types (only first element)
    first_new = new_array[0] if new_array else None
    first_old = old_array[0] if old_array else None
    
    # If not object arrays, use new array (simple types)
    if not isinstance(first_new, dict) and not isinstance(first_old, dict):
        return new_array
    
    # Object array: detect unique key (only check first element)
    unique_key = None
    if isinstance(first_old, dict) and isinstance(first_new, dict):
        if 'id' in first_old and 'id' in first_new:
            unique_key = 'id'
        elif 'name' in first_old and 'name' in first_new:
            unique_key = 'name'
    
    if unique_key:
        # Merge by unique key
        items_dict = {}
        for item in old_array:
            if isinstance(item, dict) and unique_key in item:
                items_dict[item[unique_key]] = item.copy()
        
        for item in new_array:
            if isinstance(item, dict) and unique_key in item:
                item_key = item[unique_key]
                if item_key in items_dict:
                    # Exists: deep merge
                    items_dict[item_key] = deep_merge_dicts(items_dict[item_key], item)
                else:
                    # New element: add
                    items_dict[item_key] = item.copy()
        
        return list(items_dict.values())
    else:
        # No unique key, replace with new array
        return new_array


# ----------------------------------------------------------------------------
# 1.3 File Scanner Module
# ----------------------------------------------------------------------------

def scan_directory_for_json(directory: Path) -> List[Path]:
    """
    Recursively scan directory for all .json files.
    
    Args:
        directory: Directory to scan
    
    Returns:
        List of Path objects for JSON files
    """
    if not directory.exists() or not directory.is_dir():
        return []
    
    return list(directory.rglob('*.json'))


def filter_files_by_state(files: List[Path], state_manager, mode: str, workers: int = 32) -> List[str]:
    """
    Filter files based on processing state and mode.
    
    Args:
        files: List of file paths
        state_manager: State manager for tracking processed files
        mode: Import mode ('incremental', 'overwrite', etc.)
        workers: Number of parallel workers for filtering operations
    
    Returns:
        List of file path strings to process
    """
    # Convert to strings
    file_strs = [str(f) for f in files]
    
    # Only incremental modes check state manager
    if mode in ('incremental_fast', 'incremental_slow', 'smart_incremental_slow') and state_manager:
        # Use batch checking for performance (single query instead of N queries)
        if hasattr(state_manager, 'is_processed_batch'):
            processed_set = state_manager.is_processed_batch(file_strs, workers=workers)
            return [f for f in file_strs if f not in processed_set]
        else:
            # Fallback to individual checks (slow)
            return [f for f in file_strs if not state_manager.is_processed(f)]
    
    # Other modes process all files
    return file_strs


# ----------------------------------------------------------------------------
# 1.4 ID Extraction Module
# ----------------------------------------------------------------------------

def _extract_id_worker(args: Tuple[str, str]) -> Optional[str]:
    """
    Worker function to extract document ID from a single JSON file.
    Used by multiprocessing.Pool.
    
    Args:
        args: Tuple of (file_path, key_field)
    
    Returns:
        Document ID or None if extraction fails
    """
    file_path, key_field = args
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            doc = json.load(f)
            return doc.get(key_field)
    except Exception:
        return None


def extract_document_ids_parallel(file_paths: List[str], workers: int, key_field: str) -> List[str]:
    """
    Extract document IDs from JSON files in parallel.
    
    Args:
        file_paths: List of file paths
        workers: Number of worker processes
        key_field: Field name for document ID (default: 'id')
    
    Returns:
        List of unique document IDs
    """
    if not file_paths:
        return []
    
    logger.info(f"Extracting document IDs from {len(file_paths):,} files using {workers} workers...")
    start_time = time.time()
    
    # Prepare arguments
    args_list = [(fp, key_field) for fp in file_paths]
    
    # Calculate optimal chunksize
    chunksize = max(1, len(file_paths) // (workers * 4))
    
    # Parallel extraction
    with Pool(processes=workers) as pool:
        doc_ids_raw = pool.map(_extract_id_worker, args_list, chunksize=chunksize)
    
    # Filter out None values and get unique IDs
    doc_ids = list(set(doc_id for doc_id in doc_ids_raw if doc_id is not None))
    
    extraction_time = time.time() - start_time
    errors = len(doc_ids_raw) - len([x for x in doc_ids_raw if x is not None])
    
    logger.info(f"Extracted {len(doc_ids):,} unique IDs in {extraction_time:.2f}s "
                f"({errors} errors)")
    
    return doc_ids


# ----------------------------------------------------------------------------
# 1.5 Database Preloader Module
# ----------------------------------------------------------------------------

def _query_batch_worker(args: Tuple) -> Dict[str, dict]:
    """
    Worker function to query a batch of document IDs from MongoDB.
    Used by multiprocessing.Pool.
    
    Args:
        args: Tuple of (batch_ids, key_field, db_host, db_port, db_name, collection_name)
    
    Returns:
        Dictionary of {doc_id: document}
    """
    batch_ids, key_field, db_host, db_port, db_name, collection_name = args
    batch_docs = {}
    
    try:
        # Each worker creates its own MongoDB connection
        client = MongoClient(host=db_host, port=db_port, serverSelectionTimeoutMS=10000)
        collection = client[db_name][collection_name]
        
        # Query this batch of IDs
        query = {key_field: {"$in": batch_ids}}
        cursor = collection.find(query)
        
        for doc in cursor:
            doc_id = doc.get(key_field)
            if doc_id:
                # Remove _id field to avoid conflicts
                doc.pop('_id', None)
                batch_docs[doc_id] = doc
        
        # Close connection
        client.close()
    except Exception as e:
        logger.error(f"Batch query failed: {e}")
    
    return batch_docs


def preload_documents_from_db(
    collection,
    doc_ids: List[str],
    key_field: str,
    workers: int
) -> Dict[str, dict]:
    """
    Preload documents from MongoDB using parallel $in queries.
    
    Args:
        collection: MongoDB collection object
        doc_ids: List of document IDs to query
        key_field: Field name for document ID
        workers: Number of worker processes for parallel queries
    
    Returns:
        Dictionary of {doc_id: document}
    """
    if not doc_ids:
        return {}
    
    logger.info(f"Preloading {len(doc_ids):,} documents from MongoDB...")
    start_time = time.time()
    
    # MongoDB $in query optimal batch size
    query_batch_size = 1000
    
    # Split doc_ids into batches
    batch_id_list = []
    for i in range(0, len(doc_ids), query_batch_size):
        batch_ids = doc_ids[i:i + query_batch_size]
        batch_id_list.append(batch_ids)
    
    total_batches = len(batch_id_list)
    logger.info(f"Split into {total_batches} query batches ({query_batch_size} IDs per batch)")
    
    # Prepare query arguments
    db_host, db_port = collection.database.client.address
    query_args = [
        (batch_ids, key_field, db_host, db_port, 
         collection.database.name, collection.name)
        for batch_ids in batch_id_list
    ]
    
    # Parallel queries
    query_workers = min(workers, total_batches)
    with Pool(processes=query_workers) as pool:
        batch_results = pool.map(_query_batch_worker, query_args)
    
    # Merge all batch results
    preloaded_docs = {}
    for batch_docs in batch_results:
        preloaded_docs.update(batch_docs)
    
    preload_time = time.time() - start_time
    hit_rate = (len(preloaded_docs) / len(doc_ids)) * 100 if doc_ids else 0
    
    logger.info(f"Preloaded {len(preloaded_docs):,} documents in {preload_time:.2f}s "
                f"(hit rate: {hit_rate:.1f}%)")
    
    return preloaded_docs


# ============================================================================
# PHASE 2: WORKER PROCESS IMPLEMENTATION
# ============================================================================

# Thread-local storage for worker connections
_worker_db_client = None
_worker_collection = None

def _init_worker_connection(db_host: str, db_port: int, db_name: str, collection_name: str):
    """Initialize MongoDB connection for this worker process (called once per worker)"""
    global _worker_db_client, _worker_collection
    try:
        _worker_db_client = MongoClient(host=db_host, port=db_port, 
                                        serverSelectionTimeoutMS=10000,
                                        maxPoolSize=1)  # Each worker needs only 1 connection
        _worker_collection = _worker_db_client[db_name][collection_name]
    except Exception as e:
        logger.error(f"Worker connection initialization failed: {e}")
        _worker_db_client = None
        _worker_collection = None

def worker_process(
    file_chunk: List[str],
    key_field: str,
    import_mode: str,
    db_connection_info: Tuple[str, int, str, str],
    preloaded_docs: Optional[Dict[str, dict]],
    use_batch_query: bool,
    use_smart_validation: bool
) -> Tuple[List, List[str], int]:
    """
    Worker process to handle a chunk of JSON files.
    
    Process:
    1. Read each JSON file
    2. Optionally validate document structure
    3. Look up existing document (preloaded, batched query, or per-doc query)
    4. Deep merge if exists (for non-rebuild modes)
    5. Generate UpdateOne or ReplaceOne operation
    6. Return operations, processed paths, and error count
    
    Args:
        file_chunk: List of file paths to process
        key_field: Document ID field name
        import_mode: Import mode ('incremental', 'overwrite', 'rebuild', etc.)
        db_connection_info: Tuple of (db_host, db_port, db_name, collection_name)
        preloaded_docs: Dictionary of preloaded documents {doc_id: doc}
        use_batch_query: Whether to batch-fetch existing docs from MongoDB
        use_smart_validation: Whether to validate docs before processing
    
    Returns:
        Tuple of (operations, processed_paths, error_count)
    """
    operations: List = []
    processed_paths: List[str] = []
    errors_in_chunk = 0

    preload_hits = 0
    preload_misses = 0

    should_deep_merge = import_mode != 'rebuild'
    should_use_batch_query = use_batch_query and should_deep_merge and preloaded_docs is None

    # Use global persistent connection instead of creating new one
    global _worker_db_client, _worker_collection
    collection = None
    db_client_temp = None  # For fallback temporary connections
    use_fallback = should_deep_merge and preloaded_docs is None

    if use_fallback:
        # Reuse persistent connection if available
        if _worker_collection is not None:
            collection = _worker_collection
        else:
            # Fallback: create temporary connection (shouldn't happen if initializer worked)
            logger.warning("Worker persistent connection not found, creating temporary connection")
            try:
                db_host, db_port, db_name, collection_name = db_connection_info
                db_client_temp = MongoClient(host=db_host, port=db_port, serverSelectionTimeoutMS=10000)
                collection = db_client_temp[db_name][collection_name]
            except Exception as e:
                logger.warning(f"Worker DB connection failed, skipping fallback: {e}")
                collection = None
                should_use_batch_query = False

    # First pass: load docs, validate, and collect metadata
    doc_entries: List[Tuple[str, str, dict]] = []
    for file_path_str in file_chunk:
        try:
            with open(file_path_str, 'r', encoding='utf-8') as f:
                new_doc = json.load(f)
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON file: {file_path_str}, skipped")
            errors_in_chunk += 1
            continue
        except Exception as e:
            logger.error(f"Unexpected error reading {file_path_str}: {e}")
            errors_in_chunk += 1
            continue

        doc_id = new_doc.get(key_field)
        if not doc_id:
            logger.warning(f"Missing key field '{key_field}' in {file_path_str}, skipped")
            errors_in_chunk += 1
            continue

        if use_smart_validation:
            is_valid, reason = validate_document_smart(new_doc)
            if not is_valid:
                logger.warning(f"Smart validation failed for {file_path_str}: {reason}, skipped")
                errors_in_chunk += 1
                continue

        doc_entries.append((file_path_str, doc_id, new_doc))

    if not doc_entries:
        if db_client_temp:
            db_client_temp.close()
        return operations, processed_paths, errors_in_chunk

    batch_lookup: Dict[str, dict] = {}
    if should_use_batch_query and collection is not None:
        doc_ids = [doc_id for _, doc_id, _ in doc_entries]
        try:
            query = {key_field: {"$in": doc_ids}}
            cursor = collection.find(query)
            for doc in cursor:
                doc_id = doc.get(key_field)
                if doc_id:
                    doc.pop('_id', None)
                    batch_lookup[doc_id] = doc
            logger.debug(f"Batch queried {len(batch_lookup)} existing docs for {len(doc_entries)} candidates")
        except Exception as e:
            error_msg = str(e).lower()
            # Check if it's a resource/connection error (don't fallback, fail fast)
            if any(keyword in error_msg for keyword in ['memory', 'resource', 'connection', '10054', '10061', 'refused']):
                logger.error(f"Batch query failed due to system resource/connection issue: {e}")
                logger.error("Failing fast instead of falling back to avoid resource exhaustion")
                return ([], [], 1)  # Return error immediately
            else:
                # Only fallback for query-related errors
                logger.warning(f"Batch query failed, falling back to per-document lookups: {e}")
                batch_lookup = {}
                should_use_batch_query = False

    for file_path_str, doc_id, new_doc in doc_entries:
        try:
            if import_mode == 'rebuild':
                op = ReplaceOne({key_field: doc_id}, new_doc, upsert=True)
            else:
                old_doc = None

                if preloaded_docs and doc_id in preloaded_docs:
                    old_doc = preloaded_docs[doc_id]
                    preload_hits += 1
                elif batch_lookup and doc_id in batch_lookup:
                    old_doc = batch_lookup[doc_id]
                    preload_hits += 1
                elif collection is not None:
                    try:
                        fetched = collection.find_one({key_field: doc_id})
                        if fetched:
                            fetched.pop('_id', None)
                            old_doc = fetched
                        preload_misses += 1
                    except Exception as e:
                        error_msg = str(e).lower()
                        # Fail fast on resource/connection errors
                        if any(keyword in error_msg for keyword in ['memory', 'resource', 'connection', '10054', '10061', 'refused']):
                            logger.error(f"DB query failed for {doc_id} due to resource/connection issue: {e}")
                            logger.error("Aborting worker to prevent resource exhaustion")
                            return ([], [], 1)  # Fail fast
                        else:
                            logger.warning(f"DB query failed for {doc_id}: {e}")

                if old_doc:
                    try:
                        merged_doc = deep_merge_dicts(old_doc, new_doc)
                        merged_doc.pop('_id', None)
                        op = UpdateOne({key_field: doc_id}, {"$set": merged_doc}, upsert=True)
                        # Temporary debug: verify deep merge is being called
                        if len(doc_entries) < 10:  # Only log for small batches
                            logger.debug(f"Deep merged doc {doc_id}: {len(old_doc)} old fields + {len(new_doc)} new fields = {len(merged_doc)} merged fields")
                    except Exception as e:
                        logger.warning(f"Deep merge failed for {file_path_str}: {e}, using simple $set")
                        op = UpdateOne({key_field: doc_id}, {"$set": new_doc}, upsert=True)
                else:
                    op = UpdateOne({key_field: doc_id}, {"$set": new_doc}, upsert=True)

            operations.append(op)
            processed_paths.append(file_path_str)
        except Exception as e:
            logger.error(f"Unexpected error processing {file_path_str}: {e}")
            errors_in_chunk += 1

    # Note: Don't close global _worker_db_client - it's reused across chunks
    # Only close if we created a temporary connection (which shouldn't happen)
    if 'db_client_temp' in locals() and db_client_temp:
        db_client_temp.close()

    if preloaded_docs or batch_lookup:
        total_lookups = preload_hits + preload_misses
        if total_lookups > 0:
            hit_rate = (preload_hits / total_lookups) * 100
            logger.debug(f"Worker lookup hit rate: {hit_rate:.1f}% ({preload_hits}/{total_lookups})")

    return operations, processed_paths, errors_in_chunk


# ============================================================================
# PHASE 3: BATCH PROCESSING ORCHESTRATION
# ============================================================================

def chunk_generator(items: list, chunk_size: int):
    """
    Generate chunks of specified size from a list.
    
    Args:
        items: List to chunk
        chunk_size: Size of each chunk
    
    Yields:
        Chunks of items
    """
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


def process_batch(
    files: List[str],
    collection,
    config: Config,
    state_manager,
    show_progress: bool = True
) -> dict:
    """
    Process a batch of files: preload, distribute to workers, bulk write.
    
    Process:
    1. Extract document IDs from all files (parallel)
    2. Preload documents from MongoDB (parallel, optional)
    3. Create worker pool
    4. Distribute files to workers in chunks of batch_size
    5. Collect operations and bulk write to MongoDB
    6. Update state manager
    7. Return statistics
    
    Args:
        files: List of file paths to process
        collection: MongoDB collection object
        config: Configuration object
        state_manager: State manager for tracking processed files
    
    Returns:
        Statistics dictionary
    """
    if not files:
        return {'updated': 0, 'inserted': 0, 'errors': 0, 'processed': 0}
    
    total_files = len(files)
    logger.info(f"Processing batch of {total_files:,} files...")
    batch_start_time = time.time()
    
    # Step 1: Extract document IDs (if preloading is enabled)
    preloaded_docs = None
    if config.use_preload and config.import_mode != 'rebuild':
        doc_ids = extract_document_ids_parallel(files, config.workers, config.key_field)
        
        if doc_ids:
            # Step 2: Preload documents from MongoDB
            preloaded_docs = preload_documents_from_db(
                collection, doc_ids, config.key_field, config.workers
            )
    
    # Step 3: Prepare database connection info for workers
    db_connection_info = (
        config.db_host,
        config.db_port,
        collection.database.name,
        collection.name
    )
    
    # Step 4: Distribute files to workers
    file_chunks = list(chunk_generator(files, config.batch_size))
    batch_query_enabled = (
        config.use_batch_query
        and config.import_mode != 'rebuild'
        and preloaded_docs is None
    )

    logger.info(f"Distributing {len(file_chunks)} chunks to {config.workers} workers...")
    
    stats = {'updated': 0, 'inserted': 0, 'errors': 0, 'processed': 0}
    
    # Step 5: Process with worker pool
    # Log memory usage if preloading large dataset
    if preloaded_docs and len(preloaded_docs) > 10000:
        est_memory_mb = len(preloaded_docs) * 3 / 1024  # ~3KB per doc
        logger.warning(
            f"Preloaded {len(preloaded_docs):,} docs (~{est_memory_mb:.0f}MB) will be copied to {config.workers} workers. "
            f"Estimated memory usage: {est_memory_mb * config.workers / 1024:.1f}GB. "
            f"Consider USE_PRELOAD=false, USE_BATCH_QUERY=true to save memory."
        )
    
    all_processed_paths: List[str] = []
    
    # Initialize pool with persistent DB connections for each worker (BEFORE progress bar)
    pool_initializer = None
    pool_initargs = ()
    if not config.use_preload and config.import_mode != 'rebuild':
        # Only initialize connections if workers will need to query DB
        pool_initializer = _init_worker_connection
        pool_initargs = db_connection_info
        logger.info(f"Initializing {config.workers} workers with persistent MongoDB connections")
    
    progress_iter = tqdm(total=total_files, desc="Processing files", unit="file") if show_progress else None
    try:
        with Pool(processes=config.workers, initializer=pool_initializer, initargs=pool_initargs) as pool:
            # Prepare arguments for each chunk
            args_for_map = [
                (
                    chunk,
                    config.key_field,
                    config.import_mode,
                    db_connection_info,
                    preloaded_docs,
                    batch_query_enabled,
                    config.use_smart_validation
                )
                for chunk in file_chunks
            ]
            
            results_iterator = pool.starmap(worker_process, args_for_map, chunksize=1)

            # Circuit breaker: stop processing if too many consecutive failures
            consecutive_failures = 0
            MAX_CONSECUTIVE_FAILURES = 5
            total_chunks = len(file_chunks)
            completed_chunks = 0

            for ops, paths, errors in results_iterator:
                completed_chunks += 1
                stats['errors'] += errors
                
                # Circuit breaker: detect systemic failure
                if errors > 0 and len(ops) == 0 and len(paths) == 0:
                    consecutive_failures += 1
                    logger.error(f"Worker returned error with no results (consecutive failures: {consecutive_failures}/{MAX_CONSECUTIVE_FAILURES})")
                    
                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                        logger.critical("")
                        logger.critical("="*80)
                        logger.critical("CIRCUIT BREAKER TRIGGERED: System resource exhaustion detected!")
                        logger.critical("="*80)
                        logger.critical(f"Failed chunks: {consecutive_failures} consecutive worker failures")
                        logger.critical(f"Progress: {completed_chunks}/{total_chunks} chunks ({completed_chunks/total_chunks*100:.1f}%)")
                        logger.critical(f"Files processed before failure: {stats['processed']:,}/{total_files:,}")
                        logger.critical("")
                        logger.critical("LIKELY CAUSE: Out of memory or system resources")
                        logger.critical(f"Current config: workers={config.workers}, batch_size={config.batch_size:,}")
                        logger.critical(f"Memory pressure: {config.workers} × {config.batch_size:,} = {config.workers * config.batch_size:,} files in flight")
                        logger.critical("")
                        logger.critical("RECOMMENDED ACTIONS:")
                        logger.critical(f"  1. Reduce workers to {config.workers // 2} or {config.workers // 4}")
                        logger.critical(f"  2. Reduce batch_size to {config.batch_size // 2:,} or {config.batch_size // 4:,}")
                        logger.critical(f"  3. Enable USE_BATCH_QUERY=true if not already (reduces memory)")
                        logger.critical(f"  4. Process in smaller month batches")
                        logger.critical("="*80)
                        logger.critical("")
                        
                        pool.terminate()  # Force kill all workers
                        raise RuntimeError(
                            f"Circuit breaker triggered: {consecutive_failures} consecutive worker failures. "
                            f"System resources exhausted. Reduce workers ({config.workers}) or batch_size ({config.batch_size})"
                        )
                else:
                    # Reset consecutive failures on any success
                    if len(ops) > 0 or len(paths) > 0:
                        consecutive_failures = 0
                
                # Debug: Log if operations were generated but no changes
                if ops and len(ops) > 0:
                    logger.debug(f"Generated {len(ops)} operations for {len(paths)} files")
                
                if ops:
                    try:
                        # Bulk write to MongoDB
                        bulk_result = collection.bulk_write(ops, ordered=False)
                        stats['updated'] += bulk_result.modified_count
                        stats['inserted'] += bulk_result.upserted_count
                        
                        # Log when documents are unchanged (deep merge produced identical result)
                        if bulk_result.modified_count == 0 and bulk_result.upserted_count == 0 and len(ops) > 0:
                            logger.debug(f"Bulk write: {len(ops)} ops executed but 0 changes (documents identical after merge)")
                        
                        # Update state manager (mark files as processed)
                        if state_manager and paths:
                            state_manager.mark_processed(paths)
                        
                        stats['processed'] += len(paths)
                        all_processed_paths.extend(paths)
                        
                    except BulkWriteError as bwe:
                        # Partial failure: record successful operations only
                        stats['updated'] += bwe.details.get('nModified', 0)
                        stats['inserted'] += bwe.details.get('nUpserted', 0)
                        
                        # Find failed operations
                        failed_indices = {err['index'] for err in bwe.details.get('writeErrors', [])}
                        success_paths = [path for i, path in enumerate(paths) if i not in failed_indices]
                        
                        # Record successful files only
                        if success_paths:
                            if state_manager:
                                state_manager.mark_processed(success_paths)
                            stats['processed'] += len(success_paths)
                            all_processed_paths.extend(success_paths)
                        
                        # Log errors
                        stats['errors'] += len(failed_indices)
                        logger.error(f"Bulk write partial failure: {len(success_paths)} succeeded, "
                                   f"{len(failed_indices)} failed")
                
                if progress_iter is not None:
                    progress_iter.update(len(paths) + errors)
                
                # Periodic memory cleanup for large batches
                if len(all_processed_paths) % 64000 == 0 and len(all_processed_paths) > 0:
                    gc.collect()

    finally:
        if progress_iter is not None:
            progress_iter.close()
        # Final cleanup
        if preloaded_docs:
            preloaded_docs.clear()
        gc.collect()

    stats['processed_paths'] = all_processed_paths

    batch_time = time.time() - batch_start_time
    logger.info(f"Batch completed in {batch_time:.2f}s: "
                f"{stats['processed']} processed, {stats['updated']} updated, "
                f"{stats['inserted']} inserted, {stats['errors']} errors")

    return stats


def process_month(
    month_path: Path,
    collection,
    config: Config,
    state_manager,
    alpha_type: str,
    time_window_start: Optional[datetime] = None,
    time_window_end: Optional[datetime] = None
) -> dict:
    """
    Process all files in a month directory with automatic batching.
    
    Process:
    1. Scan month directory for all JSON files
    2. Filter by state manager (if incremental mode)
    3. Calculate batch count based on threshold (workers × batch_size)
    4. Process each batch sequentially
    5. Explicit memory cleanup between batches
    6. Return accumulated statistics
    
    Args:
        month_path: Path to month directory
        collection: MongoDB collection object
        config: Configuration object
        state_manager: State manager for tracking processed files
        alpha_type: "regular" or "super" (for logging)
    
    Returns:
        Statistics dictionary
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"Processing {alpha_type} month: {month_path.name}")
    logger.info(f"{'='*60}")
    
    month_start_time = time.time()

    if config.parallel_days_mode:
        day_stats = process_month_parallel_days(
            month_path,
            collection,
            config,
            state_manager,
            alpha_type,
            time_window_start,
            time_window_end
        )
        if day_stats is not None:
            logger.info(f"Month {month_path.name} (day mode) completed in {time.time() - month_start_time:.2f}s")
            return day_stats
    
    # Step 1: Scan directory for JSON files
    all_files = scan_directory_for_json(month_path)
    # 按父目录排序，让同批次的文件来自相近位置，减少磁盘随机读取
    all_files.sort(key=lambda p: (p.parent, p.name))
    logger.info(f"Found {len(all_files):,} JSON files in month directory")
    
    if not all_files:
        logger.info("No files to process")
        return {'updated': 0, 'inserted': 0, 'errors': 0, 'processed': 0}
    
    # Step 2: Filter by state manager
    filter_start = time.time()
    files_to_process = filter_files_by_state(all_files, state_manager, config.import_mode, config.workers)
    filter_time = time.time() - filter_start

    if time_window_start is not None or time_window_end is not None:
        filtered_paths = filter_files_by_time_window(
            [Path(f) for f in files_to_process],
            time_window_start,
            time_window_end
        )
        if len(filtered_paths) != len(files_to_process):
            logger.info(
                f"Time-window filtered files: {len(filtered_paths):,} / {len(files_to_process):,}"
            )
        files_to_process = [str(p) for p in filtered_paths]

    logger.info(f"After filtering: {len(files_to_process):,} files to process (filtering took {filter_time:.2f}s)")
    
    if not files_to_process:
        logger.info("All files already processed")
        return {'updated': 0, 'inserted': 0, 'errors': 0, 'processed': 0}
    
    # Step 3: Calculate batching
    threshold = config.workers * config.batch_size
    total_files = len(files_to_process)
    
    if total_files <= threshold:
        # Single batch
        logger.info(f"Processing in single batch ({total_files:,} ≤ {threshold:,})")
        stats = process_batch(files_to_process, collection, config, state_manager)
    else:
        # Multiple batches
        num_batches = (total_files + threshold - 1) // threshold
        logger.info(f"Processing in {num_batches} batches "
                   f"({total_files:,} > {threshold:,} = {config.workers} workers × "
                   f"{config.batch_size} batch_size)")
        
        # Accumulate statistics
        total_stats = {'updated': 0, 'inserted': 0, 'errors': 0, 'processed': 0}
        
        # Process each batch sequentially
        for batch_idx in range(num_batches):
            start_idx = batch_idx * threshold
            end_idx = min(start_idx + threshold, total_files)
            batch_files = files_to_process[start_idx:end_idx]
            
            logger.info(f"\nBatch {batch_idx + 1}/{num_batches}: "
                       f"files {start_idx + 1}-{end_idx}")
            
            # Process batch
            batch_stats = process_batch(batch_files, collection, config, state_manager)
            
            # Accumulate statistics
            for key in total_stats:
                total_stats[key] += batch_stats.get(key, 0)
            
            # Step 5: Explicit memory cleanup between batches
            try:
                gc.collect()
                logger.debug(f"Memory cleanup after batch {batch_idx + 1}")
            except Exception as e:
                logger.warning(f"Memory cleanup failed: {e}")
        
        stats = total_stats
    
    month_time = time.time() - month_start_time
    logger.info(f"\n{'='*60}")
    logger.info(f"Month {month_path.name} completed in {month_time:.2f}s")
    logger.info(f"Processed: {stats['processed']:,}, Updated: {stats['updated']:,}, "
                f"Inserted: {stats['inserted']:,}, Errors: {stats['errors']:,}")
    logger.info(f"{'='*60}\n")
    
    return stats


# ============================================================================
# PHASE 4: MAIN ORCHESTRATION
# ============================================================================

# ----------------------------------------------------------------------------
# 4.1 Month Traversal Logic
# ----------------------------------------------------------------------------

def find_month_directories(
    base_path: Path,
    userkey: str,
    alpha_type: str
) -> List[Path]:
    """
    Find and sort month directories for given alpha type.
    
    Pattern: {userkey}_YYYY-MM_all_{regular|super}_alphas
    
    Args:
        base_path: Base data directory
        userkey: User key from wq_login
        alpha_type: "regular" or "super"
    
    Returns:
        Sorted list of Path objects (oldest to newest)
    """
    pattern = f"{userkey}_*_all_{alpha_type}_alphas"
    month_dirs = list(base_path.glob(pattern))
    
    # Sort by directory name (which contains date)
    month_dirs.sort()
    
    logger.info(f"Found {len(month_dirs)} {alpha_type} month directories")
    return month_dirs


def process_all_months(
    base_path: Path,
    alpha_type: str,
    config: Config,
    state_manager
) -> dict:
    """
    Process all months for given alpha type sequentially.
    
    Args:
        base_path: Base data directory
        alpha_type: "regular" or "super"
        config: Configuration object
        state_manager: State manager for tracking processed files
    
    Returns:
        Accumulated statistics dictionary
    """
    logger.info(f"\n{'#'*60}")
    logger.info(f"PHASE: Processing {alpha_type.upper()} Alphas")
    logger.info(f"{'#'*60}\n")
    
    phase_start_time = time.time()
    
    # Find month directories
    month_dirs = find_month_directories(base_path, wq_login.USER_KEY, alpha_type)
    
    if not month_dirs:
        logger.warning(f"No {alpha_type} month directories found")
        return {'updated': 0, 'inserted': 0, 'errors': 0, 'processed': 0}

    # Get appropriate collection
    if alpha_type == 'regular':
        db_name = config.db_name_regular
        collection_name = config.regular_collection
    else:  # super
        db_name = config.db_name_super
        collection_name = config.super_collection
    
    # Connect to MongoDB
    with MongoClient(host=config.db_host, port=config.db_port) as client:
        db = client[db_name]
        collection = db[collection_name]
        logger.info(f"Connected to MongoDB: {db_name}.{collection_name}")
        
        # Create index on key field (if it doesn't exist)
        collection.create_index(config.key_field, unique=True)
        
        time_window_start: Optional[datetime] = None
        time_window_end: Optional[datetime] = None

        # FAST_MODE takes absolute precedence - skip other month filtering
        if config.fast_mode:
            if len(month_dirs) > config.fast_mode_months:
                original_count = len(month_dirs)
                month_dirs = month_dirs[-config.fast_mode_months:]
                logger.info(
                    f"FAST_MODE enabled: limiting month scan from {original_count} to {len(month_dirs)} (last {config.fast_mode_months} months)"
                )
            else:
                logger.info(f"FAST_MODE enabled but only {len(month_dirs)} months available")
        else:
            # Only apply import_mode filters if FAST_MODE is disabled
            if config.import_mode in ('smart_merge', 'smart_incremental_slow'):
                latest_ts = get_latest_alpha_timestamp(collection)
                if latest_ts:
                    time_window_start = latest_ts - timedelta(days=config.time_range_buffer_days)
                    logger.info(
                        f"Smart merge window start (UTC): {time_window_start.isoformat()}"
                    )
                    month_dirs = filter_month_dirs_by_window(month_dirs, time_window_start)
                else:
                    logger.info("Smart merge found no existing documents; processing all months")
            elif config.import_mode == 'time_range':
                now_utc = datetime.now(timezone.utc)
                time_window_start = now_utc - timedelta(hours=config.time_range_hours) - \
                    timedelta(days=config.time_range_buffer_days)
                time_window_end = now_utc + timedelta(days=config.time_range_buffer_days)
                logger.info(
                    "Time range window (UTC): %s to %s",
                    time_window_start.isoformat(),
                    time_window_end.isoformat()
                )
                month_dirs = filter_month_dirs_by_window(month_dirs, time_window_start)

        if not month_dirs:
            logger.warning("No month directories meet the selected mode criteria")
            return {'updated': 0, 'inserted': 0, 'errors': 0, 'processed': 0}

        logger.info(f"Processing {len(month_dirs)} month directories after mode filters")

        # Accumulate statistics
        total_stats = {'updated': 0, 'inserted': 0, 'errors': 0, 'processed': 0}
        
        # Process each month sequentially
        for month_idx, month_path in enumerate(month_dirs, 1):
            logger.info(f"\n>>> Processing month {month_idx}/{len(month_dirs)}: {month_path.name}")
            
            try:
                month_stats = process_month(
                    month_path,
                    collection,
                    config,
                    state_manager,
                    alpha_type,
                    time_window_start=time_window_start,
                    time_window_end=time_window_end
                )
                
                # Accumulate statistics
                for key in total_stats:
                    total_stats[key] += month_stats.get(key, 0)
                
            except Exception as e:
                logger.error(f"Failed to process month {month_path.name}: {e}", exc_info=True)
                total_stats['errors'] += 1
                # Continue to next month
                continue
        
        phase_time = time.time() - phase_start_time
        
        logger.info(f"\n{'#'*60}")
        logger.info(f"PHASE COMPLETED: {alpha_type.upper()} Alphas")
        logger.info(f"Time: {phase_time:.2f}s")
        logger.info(f"Total Processed: {total_stats['processed']:,}")
        logger.info(f"Total Updated: {total_stats['updated']:,}")
        logger.info(f"Total Inserted: {total_stats['inserted']:,}")
        logger.info(f"Total Errors: {total_stats['errors']:,}")
        logger.info(f"{'#'*60}\n")
    
    return total_stats


# ----------------------------------------------------------------------------
# 4.2 Main Entry Point
# ----------------------------------------------------------------------------

def print_final_summary(regular_stats: dict, super_stats: dict):
    """Print final summary of all processing"""
    print("\n" + "="*70)
    print(" " * 20 + "FINAL SUMMARY")
    print("="*70)
    
    if regular_stats:
        print(f"\nREGULAR ALPHAS:")
        print(f"  Processed: {regular_stats.get('processed', 0):,}")
        print(f"  Updated:   {regular_stats.get('updated', 0):,}")
        print(f"  Inserted:  {regular_stats.get('inserted', 0):,}")
        print(f"  Errors:    {regular_stats.get('errors', 0):,}")
    else:
        print("\nREGULAR ALPHAS: No files processed")
    
    if super_stats:
        print(f"\nSUPER ALPHAS:")
        print(f"  Processed: {super_stats.get('processed', 0):,}")
        print(f"  Updated:   {super_stats.get('updated', 0):,}")
        print(f"  Inserted:  {super_stats.get('inserted', 0):,}")
        print(f"  Errors:    {super_stats.get('errors', 0):,}")
    else:
        print("\nSUPER ALPHAS: No files processed")
    
    total_processed = (regular_stats.get('processed', 0) + super_stats.get('processed', 0))
    total_updated = (regular_stats.get('updated', 0) + super_stats.get('updated', 0))
    total_inserted = (regular_stats.get('inserted', 0) + super_stats.get('inserted', 0))
    total_errors = (regular_stats.get('errors', 0) + super_stats.get('errors', 0))
    
    print(f"\nTOTAL:")
    print(f"  Processed: {total_processed:,}")
    print(f"  Updated:   {total_updated:,}")
    print(f"  Inserted:  {total_inserted:,}")
    print(f"  Errors:    {total_errors:,}")
    print("="*70 + "\n")


def parse_arguments(config: Config) -> argparse.Namespace:
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description="MongoDB Alpha Import Tool v4.0 - High-performance batch importer",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # MongoDB settings
    parser.add_argument('--db-host', type=str, default=config.db_host,
                       help='MongoDB server address')
    parser.add_argument('--db-port', type=int, default=config.db_port,
                       help='MongoDB server port')
    parser.add_argument('--db-port-27017', type=int, default=config.db_port_27017,
                       help='MongoDB server port for 27017-compatible imports')
    parser.add_argument('--db-port-27018', type=int,
                       default=config.db_port_27018 if config.db_port_27018 is not None else 27018,
                       help='MongoDB server port for 27018-compatible imports')
    parser.add_argument('--db-name-regular', type=str, default=config.db_name_regular,
                       help='Regular Alphas database name')
    parser.add_argument('--db-name-super', type=str, default=config.db_name_super,
                       help='Super Alphas database name')
    
    # Data paths
    parser.add_argument('--data-base-path', type=str, default=config.data_base_path,
                       help='Base directory containing all alpha data')
    parser.add_argument('--regular-collection', type=str, default=config.regular_collection,
                       help='Regular Alphas collection name')
    parser.add_argument('--super-collection', type=str, default=config.super_collection,
                       help='Super Alphas collection name')
    
    # Performance settings
    parser.add_argument('--workers', type=int, default=config.workers,
                       help='Number of worker processes')
    parser.add_argument('--batch-size', type=int, default=config.batch_size,
                       help='Number of files per worker batch')
    
    # Other settings
    parser.add_argument('--key-field', type=str, default=config.key_field,
                       help='Document unique identifier field')
    parser.add_argument('--state-file', type=str, default=config.state_file,
                       help='State file for tracking processed files')
    parser.add_argument('--import-mode', type=str, default=config.import_mode,
                       choices=['incremental', 'overwrite', 'rebuild', 'time_range', 'smart_merge', 'smart_incremental_slow'],
                       help='Import mode')
    parser.add_argument('--time-range-hours', type=int, default=config.time_range_hours,
                       help='Time window (hours) for time_range import mode')
    parser.add_argument('--time-range-buffer-days', type=int, default=config.time_range_buffer_days,
                       help='Additional buffer days when scanning for time_range mode')
    parser.add_argument('--log-path', type=str, default=config.log_path,
                       help='Custom log file path (overrides default handler configuration)')

    _add_boolean_cli_flag(
        parser,
        option='preload',
        dest='use_preload',
        default=config.use_preload,
        help_enable='Enable document preloading (load existing docs before merge)',
        help_disable='Disable document preloading and query MongoDB per document'
    )

    _add_boolean_cli_flag(
        parser,
        option='fast-mode',
        dest='fast_mode',
        default=config.fast_mode,
        help_enable='Enable fast mode (scan only recent directories)',
        help_disable='Disable fast mode and scan full history'
    )

    parser.add_argument('--fast-mode-months', type=int, default=config.fast_mode_months,
                       help='Number of recent months to scan in fast mode')

    _add_boolean_cli_flag(
        parser,
        option='batch-query',
        dest='use_batch_query',
        default=config.use_batch_query,
        help_enable='Enable batched MongoDB lookups during merge',
        help_disable='Disable batched MongoDB lookups'
    )

    _add_boolean_cli_flag(
        parser,
        option='parallel-days-mode',
        dest='parallel_days_mode',
        default=config.parallel_days_mode,
        help_enable='Enable per-day parallelism when daily folders are small',
        help_disable='Process months sequentially without per-day parallelism'
    )

    parser.add_argument('--parallel-days-workers', type=int, default=config.parallel_days_workers,
                       help='Number of day batches to process concurrently when parallel-days mode is enabled')

    _add_boolean_cli_flag(
        parser,
        option='smart-validation',
        dest='use_smart_validation',
        default=config.use_smart_validation,
        help_enable='Enable smart validation before writing to MongoDB',
        help_disable='Disable smart validation checks'
    )
    
    args = parser.parse_args()
    
    # Update config with command-line arguments
    config.db_host = args.db_host
    config.db_port = args.db_port
    config.db_port_27017 = args.db_port_27017
    config.db_port_27018 = args.db_port_27018
    config.db_name_regular = args.db_name_regular
    config.db_name_super = args.db_name_super
    config.data_base_path = args.data_base_path
    config.regular_collection = args.regular_collection
    config.super_collection = args.super_collection
    config.workers = args.workers
    config.batch_size = args.batch_size
    config.key_field = args.key_field
    config.state_file = args.state_file
    config.import_mode = args.import_mode
    config.time_range_hours = args.time_range_hours
    config.time_range_buffer_days = args.time_range_buffer_days
    config.log_path = args.log_path
    config.use_preload = args.use_preload
    config.fast_mode = args.fast_mode
    config.fast_mode_months = args.fast_mode_months
    config.use_batch_query = args.use_batch_query
    config.parallel_days_mode = args.parallel_days_mode
    config.use_smart_validation = args.use_smart_validation
    config.parallel_days_workers = args.parallel_days_workers

    config._validate()
    
    return args


def get_state_manager(state_file: str, import_mode: str = "incremental"):
    """
    Initialize state manager for tracking processed files.
    
    Args:
        state_file: Path to state file
        import_mode: Import mode (determines state manager behavior)
    
    Returns:
        State manager instance or None if unavailable
    """
    global STATE_MANAGER_WARNING_EMITTED

    if not STATE_MANAGER_AVAILABLE:
        if not STATE_MANAGER_WARNING_EMITTED:
            logger.warning("State manager not available, will process all files")
            STATE_MANAGER_WARNING_EMITTED = True
        return None
    
    try:
        state_dir = Path(state_file).parent / "state_db"
        
        # Determine state manager mode based on import_mode
        if import_mode in ('incremental_fast', 'incremental_slow', 'smart_incremental_slow'):
            # These modes actively use state manager for filtering
            state_mode = 'incremental_slow' if import_mode == 'smart_incremental_slow' else import_mode
            state_usage = "active filtering"
        else:
            # Other modes only use state manager for tracking, not filtering
            # Default to fast mode (doesn't matter for these modes)
            state_mode = "incremental_fast"
            state_usage = "tracking only"
        
        state_manager = create_state_manager(state_dir, "sqlite", mode=state_mode)
        
        # Log with clear indication of whether state manager affects file filtering
        if import_mode in ('incremental_fast', 'incremental_slow', 'smart_incremental_slow'):
            logger.info(f"State manager initialized: {state_dir} (mode: {state_mode}, {state_usage})")
        else:
            logger.info(f"State manager initialized: {state_dir} ({state_usage} for '{import_mode}' mode, no filtering)")
        
        # 智能获取统计信息：数据库小就查全表，数据库大就只显示大小
        try:
            db_path = state_dir / "processed_files.db"
            if db_path.exists():
                db_size_bytes = db_path.stat().st_size
                db_size_mb = db_size_bytes / (1024 * 1024)
                
                # 如果数据库小于100MB，查询完整统计信息
                if db_size_mb < 100:
                    stats = state_manager.get_stats(quick=False)
                    logger.info(f"Processed files tracked: {stats.get('total_files', 0):,} (DB size: {db_size_mb:.1f} MB)")
                else:
                    # 数据库太大，跳过全表扫描
                    logger.info(f"State DB size: {db_size_mb:.1f} MB (skipping full scan for large DB)")
            else:
                logger.info("State DB is new (empty)")
        except Exception as e:
            logger.warning(f"Failed to get state statistics: {e}")
        
        return state_manager
        
    except Exception as e:
        logger.error(f"Failed to initialize state manager: {e}")
        return None


def main():
    """Main entry point"""
    print("\n" + "="*70)
    print(" " * 15 + "MongoDB Alpha Import Tool v4.0")
    print("="*70 + "\n")
    
    start_time = time.time()
    
    try:
        # Step 1: Load configuration
        logger.info("Step 1: Loading configuration...")
        config = load_config("mongo_config.json")
        logger.info(f"Configuration: {config}")
        
        # Step 2: Parse command-line arguments
        logger.info("\nStep 2: Parsing command-line arguments...")
        args = parse_arguments(config)
        configure_logging(config)
        
        # Step 3: Test MongoDB connection
        logger.info("\nStep 3: Testing MongoDB connection...")
        if not test_mongodb_connection(config.db_host, config.db_port):
            logger.error("MongoDB connection failed, aborting")
            return 1
        
        # Step 4: Initialize state manager
        logger.info("\nStep 4: Initializing state manager...")
        state_manager = get_state_manager(config.state_file, config.import_mode)
        
        # Step 5: Process Regular Alphas
        logger.info("\nStep 5: Processing Regular Alphas...")
        regular_stats = process_all_months(
            Path(config.data_base_path),
            "regular",
            config,
            state_manager
        )
        
        # Step 6: Process Super Alphas
        logger.info("\nStep 6: Processing Super Alphas...")
        super_stats = process_all_months(
            Path(config.data_base_path),
            "super",
            config,
            state_manager
        )
        
        # Step 7: Print final summary
        logger.info("\nStep 7: Generating final summary...")
        print_final_summary(regular_stats, super_stats)
        
        # Step 8: Cleanup
        logger.info("Step 8: Cleaning up...")
        if state_manager:
            try:
                state_manager.cleanup()
                logger.info("State manager cleanup completed")
            except Exception as e:
                logger.warning(f"State manager cleanup failed: {e}")
        
        # Final memory cleanup
        try:
            gc.collect()
            logger.info("Final memory cleanup completed")
        except Exception as e:
            logger.warning(f"Final memory cleanup failed: {e}")
        
        total_time = time.time() - start_time
        logger.info(f"\nTotal execution time: {total_time:.2f}s")
        logger.info("Import process completed successfully!")
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("\n\nProcess interrupted by user")
        return 130
        
    except Exception as e:
        logger.error(f"\n\nFatal error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

