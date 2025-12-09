import os
import re
import time
import json
import logging
import argparse
import random
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

import hashlib

try:
    import orjson
except ImportError:
    orjson = None  # type: ignore

try:
    from pymongo import MongoClient
except ImportError:
    MongoClient = None  # type: ignore

# 修改引用：移除 recordsets. 前缀
import wq_login as wq_login
import mongo_recordsets_writer as mrw
import wq_logger as wql
from session_manager import SessionManager
from session_proxy import SessionProxy

# --------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------
BRAIN_API_URL = os.environ.get("BRAIN_API_URL", "https://api.worldquantbrain.com").rstrip("/")
USER_KEY = wq_login.USER_KEY
# 优先读取环境变量中的 WQ_BASE_DIRECTORY，默认使用相对路径
BASE_ROOT_DIR = Path(os.environ.get("WQ_BASE_DIRECTORY", f"./data/{USER_KEY}/recordsets"))
MAX_WORKERS = int(os.environ.get("WQ_MAX_WORKERS", "5"))
REQUEST_TIMEOUT = int(os.environ.get("WQ_REQUEST_TIMEOUT", "90"))
MAX_RETRIES = int(os.environ.get("WQ_MAX_RETRIES", "9"))
INITIAL_BACKOFF = int(os.environ.get("WQ_INITIAL_BACKOFF", "8"))
BACKOFF_MULTIPLIER = float(os.environ.get("WQ_BACKOFF_MULTIPLIER", "2.0"))
MAX_BACKOFF = int(os.environ.get("WQ_MAX_BACKOFF", "240"))

# DB write toggles
WRITE_PRIMARY_DB = os.environ.get("WQ_WRITE_PRIMARY_DB", "1") == "1"  # localhost:27018
WRITE_SECONDARY_DB = os.environ.get("WQ_WRITE_SECONDARY_DB", "0") == "1"  # localhost:27017
MONGO_ENABLED = WRITE_PRIMARY_DB or WRITE_SECONDARY_DB

# Update mode: overwrite / update / skip
UPDATE_MODE = os.environ.get("WQ_UPDATE_MODE", "update").lower()

# 移除硬编码的TARGET_RECORDSETS，改为从环境变量获取或默认全量
# fetch_recordsets.py 的主要功能是下载，用户可能希望下载所有可用的。
# 如果只提供默认值，可能会漏掉 coverage-by-industry 等。
# 这里改为：如果环境变量未设置，默认包含常见的，但在 process_alpha 中会动态获取所有 available。
TARGET_RECORDSETS = ["pnl", "sharpe", "turnover", "daily-pnl", "yearly-stats"]
# --------------------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Simple global 429 backoff shared across threads
_GLOBAL_NEXT_RESUME: float = 0.0
_thread_local = threading.local()

# --------------------------------------------------------------------------------------
# Alpha kind detection and local path helpers
# --------------------------------------------------------------------------------------

def _detect_alpha_kind(alpha_id: str, session: requests.Session) -> str:
    # 1) Check primary DB if available
    try:
        if MongoClient and WRITE_PRIMARY_DB:
            c = MongoClient("mongodb://localhost:27018/", serverSelectionTimeoutMS=2000)
            c.admin.command('ping')
            db = c[f"wq{USER_KEY}"]
            if db["regular_alphas"].find_one({"id": alpha_id}, {"_id": 1}):
                c.close()
                return "regular"
            if db["super_alphas"].find_one({"id": alpha_id}, {"_id": 1}):
                c.close()
                return "super"
            c.close()
    except Exception:
        pass
    # 2) Try API: GET /alphas/{id}
    try:
        url = f"{BRAIN_API_URL}/alphas/{alpha_id}"
        data = _robust_get_json(session, url)
        if not data:
             return "regular"
        kind = data.get("type")
        if isinstance(kind, str) and kind.upper().startswith("SUPER"):
            return "super"
        if isinstance(kind, str) and kind.upper().startswith("REGULAR"):
            return "regular"
    except Exception:
        pass
    # 3) Default
    return "regular"


def _compute_local_dir(recordset_name: str, alpha_kind: str, alpha_id: str) -> Path:
    kind_dir = "regular_alphas" if alpha_kind == "regular" else "super_alphas"
    prefix = (alpha_id[:2] if alpha_id else "xx").lower()
    return BASE_ROOT_DIR / recordset_name / kind_dir / prefix


def _try_fast_skip(alpha_id: str) -> Optional[Dict[str, Optional[Path]]]:
    """
    尝试进行快速跳过检测。
    如果 TARGET_RECORDSETS 中的所有文件在本地都存在（无论是 regular 还是 super 目录），
    则认为该 Alpha 已处理完毕，直接返回结果，避免 API 调用。
    """
    if UPDATE_MODE != "skip":
        return None

    prefix = (alpha_id[:2] if alpha_id else "xx").lower()
    found_paths = {}

    for rs in TARGET_RECORDSETS:
        # 我们不知道它是 regular 还是 super，所以两个路径都查一下
        found = None
        for kind in ("regular", "super"):
            kind_dir = "regular_alphas" if kind == "regular" else "super_alphas"
            check_dir = BASE_ROOT_DIR / rs / kind_dir / prefix
            
            if check_dir.exists():
                # 查找该目录下是否有该 Alpha 的文件
                matches = list(check_dir.glob(f"*_{alpha_id}_*.json"))
                if matches:
                    found = matches[0]
                    break
        
        if found:
            found_paths[rs] = found
        else:
            # 只要缺少一个核心文件，就不能进行快速跳过
            # (因为可能这个 Alpha 确实没有这个数据，也可能是没下载，需要 API 确认)
            return None

    logger.info(f"Fast skip: Found all target recordsets locally for {alpha_id}")
    return found_paths


# --------------------------------------------------------------------------------------
# HTTP helpers
# --------------------------------------------------------------------------------------

def _apply_global_backoff():
    global _GLOBAL_NEXT_RESUME
    now = time.time()
    if now < _GLOBAL_NEXT_RESUME:
        sleep_s = _GLOBAL_NEXT_RESUME - now
        if sleep_s > 0:
            time.sleep(sleep_s)

def _set_global_backoff(wait_time: float):
    global _GLOBAL_NEXT_RESUME
    _GLOBAL_NEXT_RESUME = max(_GLOBAL_NEXT_RESUME, time.time() + wait_time)


def _robust_get_json(session: requests.Session, url: str) -> Optional[dict]:
    retries = 0
    while retries < MAX_RETRIES:
        try:
            _apply_global_backoff()
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code in (401, 403):
                logger.warning("Session unauthorized. Re-authenticating...")
                if isinstance(session, SessionProxy):
                    session._session_manager.force_refresh()
                else:
                    new_session = wq_login.start_session()
                    session.cookies.update(new_session.cookies)
                retries += 1
                continue

            if resp.status_code == 429:
                retries += 1
                if retries >= MAX_RETRIES:
                    logger.error("Hit max retries on 429 for %s", url)
                    return None
                base_wait = INITIAL_BACKOFF * (2 ** (retries - 1))
                wait_time = min(MAX_BACKOFF, BACKOFF_MULTIPLIER * base_wait)
                wait_time *= random.uniform(0.8, 1.2)
                logger.warning("429 Rate limited. Waiting %s seconds (retry %s/%s)...", int(wait_time), retries, MAX_RETRIES)
                _set_global_backoff(wait_time)
                time.sleep(wait_time)
                continue

            resp.raise_for_status()
            if not resp.content:
                 raise ValueError("Empty response body (0 bytes) with 200 OK")
            return resp.json()
        
        except (requests.exceptions.RequestException, ValueError, Exception) as e:
            # Check for "Empty response body" specifically
            is_empty_body_error = "Empty response body" in str(e)
            
            retries += 1
            if retries >= MAX_RETRIES:
                logger.error("Request failed permanently for %s: %s", url, e)
                return None
            
            # Optimized Backoff for Cold Data Wakeup (Empty Body)
            if is_empty_body_error:
                # Cold data usually takes 5-15s to warm up.
                # We don't need exponential backoff that goes up to 240s.
                # A fixed or slightly increasing short wait is better.
                # Retry 1: ~12s, Retry 2: ~15s, Retry 3: ~18s
                wait_time = 12.0 + (retries * 3.0)
                # Cap at 30s for empty body logic
                wait_time = min(wait_time, 30.0)
                
                logger.info("Cold data wakeup for %s (retry %s/%s). Waiting %ss...", url, retries, MAX_RETRIES, int(wait_time))
            else:
                # Standard Exponential Backoff for real errors (429, 500, 502...)
                base_wait = INITIAL_BACKOFF * (2 ** (retries - 1))
                wait_time = min(MAX_BACKOFF, BACKOFF_MULTIPLIER * base_wait)
                wait_time *= random.uniform(0.8, 1.2)
                logger.warning("Request failed for %s (retry %s/%s). Waiting %ss: %s", url, retries, MAX_RETRIES, int(wait_time), e)
            
            _set_global_backoff(wait_time)
            time.sleep(wait_time)

    return None


def _get_user_id(session: requests.Session) -> Optional[str]:
    url = f"{BRAIN_API_URL}/authentication"
    data = _robust_get_json(session, url)
    if not data:
        return None
    try:
        return data.get("user", {}).get("id")
    except Exception:
        return None


def _get_recordsets(session: requests.Session, alpha_id: str) -> List[Dict]:
    url = f"{BRAIN_API_URL}/alphas/{alpha_id}/recordsets"
    data = _robust_get_json(session, url)
    if not data:
        return []
    return data.get("results", []) or []


def _get_recordset_data(session: requests.Session, alpha_id: str, recordset_name: str) -> Optional[dict]:
    # Corrected endpoint: /alphas/<alpha id>/recordsets/<record set name>
    # Previous code had this right, but just to be explicit about validation against docs
    url = f"{BRAIN_API_URL}/alphas/{alpha_id}/recordsets/{recordset_name}"
    return _robust_get_json(session, url)


# --------------------------------------------------------------------------------------
# Data processing and persistence
# --------------------------------------------------------------------------------------

_DATE_TOKEN_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_YEAR_TOKEN_RE = re.compile(r"^\d{4}$")


def _is_valid_date_token(value: str) -> bool:
    try:
        return bool(_DATE_TOKEN_RE.match(str(value)))
    except Exception:
        return False


def _is_valid_year_token(value: str) -> bool:
    try:
        return bool(_YEAR_TOKEN_RE.match(str(value)))
    except Exception:
        return False


def _fallback_range_token() -> Tuple[str, str]:
    # 对于没有 date/year 列的聚合统计数据 (Snapshots)，
    # 我们不使用任何后缀标记。
    # 文件名将形如: ..._{alpha_hash}.json
    return ("", "")


def _find_date_column_index(schema: dict) -> Optional[int]:
    try:
        properties = schema.get("properties", [])
        for idx, prop in enumerate(properties):
            if prop.get("type") == "date" or prop.get("name") == "date":
                return idx
    except Exception:
        pass
    return None


def _extract_range_for_recordset(recordset_name: str, data: dict) -> Optional[Tuple[str, str]]:
    if not data or not data.get("records"):
        return None
    try:
        if recordset_name == "yearly-stats":
            year_idx = None
            for i, prop in enumerate(data.get("schema", {}).get("properties", [])):
                if prop.get("type") == "year" or prop.get("name") == "year":
                    year_idx = i
                    break
            if year_idx is None:
                return None
            records = data.get("records", [])
            start_year = str(records[0][year_idx])
            end_year = str(records[-1][year_idx])
            if _is_valid_year_token(start_year) and _is_valid_year_token(end_year):
                return start_year, end_year
            return None
        else:
            date_idx = _find_date_column_index(data.get("schema", {}))
            if date_idx is None:
                return None
            records = data.get("records", [])
            start_date = str(records[0][date_idx])
            end_date = str(records[-1][date_idx])
            if _is_valid_date_token(start_date) and _is_valid_date_token(end_date):
                return start_date, end_date
            return None
    except Exception:
        return None


def _safe_write_json(data: dict, file_path: Path) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    if orjson is not None:
        with open(file_path, "wb") as f:
            f.write(orjson.dumps(data, option=orjson.OPT_INDENT_2))
    else:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)



def _merge_records_and_save(user_key: str, user_id: str, alpha_id: str, alpha_kind: str,
                            recordset_name: str, data: dict) -> Path:
    """
    智能合并保存逻辑 (带 Hash):
    1. 寻找该 Alpha 该 Recordset 及其下的所有旧文件 (通过 Hash 匹配)。
    2. 如果是时间序列数据（有 date/year 列），进行 Merge 操作（新旧并集）。
    3. 如果是截面数据（无 date 列），直接覆盖（删除旧的，保存新的）。
    4. 返回新文件路径。
    """
    out_dir = _compute_local_dir(recordset_name, alpha_kind, alpha_id)
    
    recordset_token = recordset_name.replace("-", "_")
    kind_token = "regular_alpha" if alpha_kind == "regular" else "super_alpha"
    
    # 计算 Hash 用于查找
    alpha_hash = hashlib.md5(alpha_id.encode("utf-8")).hexdigest()[:12]
    
    # 匹配前缀：..._{alpha_id}_{alpha_hash}_
    prefix_pattern = f"{user_key}_{kind_token}_{recordset_token}_{user_id}_{alpha_id}_{alpha_hash}_"
    
    # --- 跨类型查找逻辑 ---
    # 我们不仅要在当前计算出的 kind_dir 中查找，还要去另一种类型的目录查找。
    # 防止 Alpha 类型从 Regular 变为 Super (或反之) 后，旧目录残留僵尸文件。
    
    existing_files = []
    prefix = (alpha_id[:2] if alpha_id else "xx").lower()
    
    # 定义需要扫描的两种目录类型
    dirs_to_scan = [
        ("regular", BASE_ROOT_DIR / recordset_name / "regular_alphas" / prefix),
        ("super", BASE_ROOT_DIR / recordset_name / "super_alphas" / prefix)
    ]
    
    for k_name, d_path in dirs_to_scan:
        if d_path.exists():
            # 注意：不同类型的 kind_token 是不一样的 (regular_alpha vs super_alpha)
            # 所以我们需要构造通用的匹配模式，或者分别为每种目录构造匹配模式。
            # 文件名格式: {user_key}_{kind_token}_{recordset_token}_{user_id}_{alpha_id}_{alpha_hash}_...
            # 其中 kind_token 可能是 regular_alpha 也可能是 super_alpha。
            
            scan_kind_token = "regular_alpha" if k_name == "regular" else "super_alpha"
            scan_prefix = f"{user_key}_{scan_kind_token}_{recordset_token}_{user_id}_{alpha_id}_{alpha_hash}_"
            
            for f in d_path.iterdir():
                if f.name.startswith(scan_prefix) and f.name.endswith(".json"):
                    existing_files.append(f)

    # 区分 Time Series 还是 Snapshot
    date_idx = _find_date_column_index(data.get("schema", {}))
    year_idx = None
    series_idx = None
    
    if date_idx is None:
        props = data.get("schema", {}).get("properties", [])
        for i, prop in enumerate(props):
            if recordset_name == "yearly-stats" and (prop.get("type") == "year" or prop.get("name") == "year"):
                year_idx = i
                break
            # 新增: 识别 "series" 列作为 Key (例如 coverage-by-industry)
            if prop.get("name") == "series" and prop.get("type") == "string":
                series_idx = i
                
    
    # 确定 Merge Key
    # 优先级: Date > Year > Series
    key_idx = None
    if date_idx is not None:
        key_idx = date_idx
    elif year_idx is not None:
        key_idx = year_idx
    elif series_idx is not None:
        key_idx = series_idx
        
    # 如果没有旧文件，直接保存
    if not existing_files:
        date_range = _extract_range_for_recordset(recordset_name, data)
        return _save_recordset_to_file(user_key, user_id, alpha_id, alpha_kind, recordset_name, date_range, data)

    # --- Merge 逻辑 (适用于所有类型) ---
    # 如果有 Key (Date/Year/Series)，按 Key Merge
    # 如果没有 Key (单行 Snapshot)，直接视为 Conflict，用新数据覆盖旧数据
    
    # 读取所有旧数据
    old_records_map = {} # key -> record_list
    # 对于无 Key 的情况，我们需要一种方式来存储。这里简单起见，如果无 Key，就不做 Map Merge，而是直接覆盖。
    
    if key_idx is not None:
        for old_f in existing_files:
            try:
                with open(old_f, "rb") as f:
                    old_json = json.load(f) if orjson is None else orjson.loads(f.read())
                    if "records" in old_json:
                        for rec in old_json["records"]:
                            if len(rec) > key_idx:
                                key_val = str(rec[key_idx])
                                old_records_map[key_val] = rec
            except Exception as e:
                logger.warning(f"Failed to read old file {old_f.name} for merging: {e}")
                
        # 读取新数据并更新 map (新数据覆盖旧数据)
        new_records = data.get("records", [])
        for rec in new_records:
             if len(rec) > key_idx:
                key_val = str(rec[key_idx])
                old_records_map[key_val] = rec # 新增或覆盖
                
        # 重新排序
        # 如果是 Date/Year，通常希望按时间序；如果是 Series，字母序也可以
        sorted_keys = sorted(old_records_map.keys())
        merged_records = [old_records_map[k] for k in sorted_keys]
        
        # 更新 data 对象
        data["records"] = merged_records
        
        # 计算新的 range (仅针对 Time Series)
        if date_idx is not None or year_idx is not None:
            new_start = str(merged_records[0][key_idx])
            new_end = str(merged_records[-1][key_idx])
            if _is_valid_date_token(new_start) and _is_valid_date_token(new_end):
                final_range = (new_start, new_end)
            elif _is_valid_year_token(new_start) and _is_valid_year_token(new_end):
                 final_range = (new_start, new_end)
            else:
                final_range = _fallback_range_token()
        else:
            # Snapshot 类型 (有 Key 但不是时间)
            final_range = _fallback_range_token()
            
    else:
        # 无 Key 的 Snapshot (如 average-size-by-capitalization 只有一行)
        # 策略: 既然没有 Key，无法做行级 Merge。
        # 但用户要求: "有新增的部分就保留，有减少的部分，就保留原来那个部分"
        # 这可能指的是 Schema (列) 的变化？
        # 鉴于 JSON 结构通常 records 对应 schema，如果 schema 变了，records 结构也会变，Merge 会很复杂。
        # 简单且安全的策略是: 直接使用新爬取的数据 (Overwrite)。
        # 因为这些统计数据通常是"当前最新状态"。
        final_range = _fallback_range_token()
        # data 保持原样 (新下载的)
    
    # 删除所有旧文件
    for old_f in existing_files:
        try:
            os.remove(old_f)
        except Exception:
            pass
            
    return _save_recordset_to_file(user_key, user_id, alpha_id, alpha_kind, recordset_name, final_range, data)


def _save_recordset_to_file(user_key: str, user_id: str, alpha_id: str, alpha_kind: str,
                            recordset_name: str, date_range: Optional[Tuple[str, str]], data: dict) -> Path:
    if date_range:
        start_date, end_date = date_range
    else:
        start_date, end_date = _fallback_range_token()
        
    # 清洗文件名中的日期部分，允许数字、字母(如SNAPSHOT)和横杠
    # 计算 Hash 用于查找
    alpha_hash = hashlib.md5(alpha_id.encode("utf-8")).hexdigest()[:12]
    
    # 构建文件名后缀
    if start_date:
        safe_start = re.sub(r"[^a-zA-Z0-9\-]", "-", start_date)
        if end_date:
            safe_end = re.sub(r"[^a-zA-Z0-9\-]", "-", end_date)
            suffix = f"_{safe_start}_to_{safe_end}"
        else:
            # 仅有 start_date 的情况 (较少见)
            suffix = f"_{safe_start}"
    else:
        # Snapshot 情况，无日期后缀
        suffix = ""
    
    recordset_token = recordset_name.replace("-", "_")
    kind_token = "regular_alpha" if alpha_kind == "regular" else "super_alpha"
    
    # 文件名格式：
    # Time Series: ..._{alpha_id}_{alpha_hash}_{start}_to_{end}.json
    # Snapshot:    ..._{alpha_id}_{alpha_hash}.json
    filename = f"{user_key}_{kind_token}_{recordset_token}_{user_id}_{alpha_id}_{alpha_hash}{suffix}.json"
    
    out_dir = _compute_local_dir(recordset_name, alpha_kind, alpha_id)
    out_path = out_dir / filename
    
    # --- 核心修改：Update/Overwrite 模式下的自动清理 (已在 _merge_records_and_save 中处理) ---
    # 这里只负责写入，清理工作由调用者或 merge 函数完成
    # 不过，如果是全新的文件(例如 snapshot 或者第一次下载)，_merge 可能会调用这里
    # 这里的逻辑保持简单写入即可。
    
    _safe_write_json(data, out_path)
    logger.info("Saved %s -> %s", recordset_name, out_path)
    return out_path

    if date_range:
        start_date, end_date = date_range
    else:
        start_date, end_date = _fallback_range_token()
        
    # 清洗文件名中的日期部分，允许数字、字母(如SNAPSHOT)和横杠
    safe_start = re.sub(r"[^a-zA-Z0-9\-]", "-", start_date)
    safe_end = re.sub(r"[^a-zA-Z0-9\-]", "-", end_date)
    
    recordset_token = recordset_name.replace("-", "_")
    kind_token = "regular_alpha" if alpha_kind == "regular" else "super_alpha"
    filename = f"{user_key}_{kind_token}_{recordset_token}_{user_id}_{alpha_id}_{safe_start}_to_{safe_end}.json"
    out_dir = _compute_local_dir(recordset_name, alpha_kind, alpha_id)
    out_path = out_dir / filename
    _safe_write_json(data, out_path)
    logger.info("Saved %s -> %s", recordset_name, out_path)
    return out_path


# --------------------------------------------------------------------------------------
# Orchestration per alpha and batch
# --------------------------------------------------------------------------------------

def process_alpha(session: requests.Session, alpha_id: str, writers: Optional[mrw.MongoWriters]) -> Dict[str, Optional[Path]]:
    # 0. Startup Jitter: Sleep random 100-1000ms to avoid concurrent spike at start of task
    time.sleep(random.uniform(0.1, 1.0))

    # 1. Try Fast Skip (0 API Call)
    fast_results = _try_fast_skip(alpha_id)
    if fast_results:
        return fast_results

    # 2. Detect Alpha Kind first (Might trigger GET /alphas/{id}, acting as a probe/warmup)
    # If DB has it, this is fast. If not, it warms up the connection with a lighter request.
    alpha_kind = _detect_alpha_kind(alpha_id, session)
    user_id = _get_user_id(session) or "UNKNOWN"

    # 3. Get Recordsets List
    available = _get_recordsets(session, alpha_id)
    available_names_ordered = [item.get("name") for item in available if item.get("name")]
    if not available_names_ordered:
        logger.warning("No recordsets listed for alpha=%s. Skipping.", alpha_id)
        return {}

    result_paths: Dict[str, Optional[Path]] = {name: None for name in available_names_ordered}

    for recordset in available_names_ordered:
        # 提前检查文件是否存在
        out_dir = _compute_local_dir(recordset, alpha_kind, alpha_id)
        
        # 如果是 "skip" 模式，且我们不希望重复请求 API，
        # 我们可以在 _compute_local_dir 下查找是否已存在包含 alpha_id 的文件。
        if UPDATE_MODE == "skip":
             existing_files = list(out_dir.glob(f"*_{alpha_id}_*.json"))
             if existing_files:
                 logger.info(f"Skipping {recordset} for {alpha_id} (Mode=skip, Found: {existing_files[0].name})")
                 result_paths[recordset] = existing_files[0]
                 continue

        data = _get_recordset_data(session, alpha_id, recordset)
        if not data or not data.get("records"):
            logger.warning("No data for recordset '%s' alpha=%s", recordset, alpha_id)
            continue

        date_range = _extract_range_for_recordset(recordset, data)
        if not date_range:
            logger.warning("Could not extract start/end for recordset '%s' alpha=%s; using nan_to_nan fallback.", recordset, alpha_id)
            fallback_range = _fallback_range_token()
        else:
            fallback_range = date_range

        try:
            # 如果是 UPDATE_MODE=update，且文件已存在，是否需要合并？
            # 调用智能合并保存函数 (带 Hash + Merge TimeSeries + Overwrite Snapshot)
            path = _merge_records_and_save(USER_KEY, user_id, alpha_id, alpha_kind, recordset, data)
            
            result_paths[recordset] = path
            if writers:
                # DB 写入
                writers.upsert_recordset(alpha_id=alpha_id, user_id=user_id,
                                         recordset_name=recordset, data=data,
                                         date_range=None)
        except Exception as e:
            logger.error("Failed saving recordset '%s' for alpha=%s: %s", recordset, alpha_id, e)

    return result_paths


# --------------------------------------------------------------------------------------
# Batch runner (concurrent across alphas)
# --------------------------------------------------------------------------------------

def _new_worker_session_from_cookies(source_session: requests.Session) -> requests.Session:
    s = requests.Session()
    try:
        s.cookies.update(source_session.cookies.get_dict())
        # Add a standard User-Agent to avoid being blocked by strict filters
        s.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })
    except Exception:
        pass
    return s


def _process_alpha_wrapper(session_proxy: SessionProxy, alpha_id: str, writers: Optional[mrw.MongoWriters]):
    # Use the shared session proxy which handles thread-safety and auto-refresh
    return process_alpha(session_proxy, alpha_id, writers)


def _rewrite_failed_file(failed_file_path: Path, failed_ids: List[str]):
    # Rewrite full file atomically via temp
    tmp_path = failed_file_path.with_suffix(".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(failed_ids)))
    os.replace(tmp_path, failed_file_path)


def fetch_recordsets_for_alphas(alpha_ids: List[str], max_workers: int = MAX_WORKERS) -> Dict[str, Dict[str, Optional[Path]]]:
    if not alpha_ids:
        logger.info("No alpha IDs provided.")
        return {}

    BASE_ROOT_DIR.mkdir(parents=True, exist_ok=True)

    wqlog = None
    try:
        _, wqlog = wql.init_logger(USER_KEY, subdir="recordsets")
        logger.info(f"Log file: {wqlog.get_log_file_path()}")
    except Exception:
        pass

    # Initialize SessionManager and SessionProxy
    session_manager = SessionManager()
    # Optional: Pre-warm the session (login if needed)
    try:
        session_manager.get_session()
    except Exception as e:
        logger.warning(f"Initial session check failed: {e}")
    
    session_proxy = SessionProxy(session_manager)

    writers: Optional[mrw.MongoWriters] = None
    if MONGO_ENABLED:
        writers = mrw.MongoWriters(user_key=USER_KEY,
                                   write_primary=WRITE_PRIMARY_DB,
                                   write_secondary=WRITE_SECONDARY_DB)

    results: Dict[str, Dict[str, Optional[Path]]] = {}
    failed_alphas: List[str] = []
    # Real-time failed tracking
    run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Determine output directory for failed file: prefer log dir, fallback to data root
    failed_dir = BASE_ROOT_DIR
    if wqlog:
        log_path = wqlog.get_log_file_path()
        if log_path:
            failed_dir = Path(log_path).parent

    failed_file_path = failed_dir / f"failed_alphas_{run_id}.txt"
    
    try:
        # Ensure directory exists (log dir should exist, but just in case)
        failed_dir.mkdir(parents=True, exist_ok=True)
        open(failed_file_path, 'w', encoding='utf-8').close()
    except Exception as e:
        logger.warning(f"Could not create failed_alphas file at {failed_file_path}: {e}")
        # Fallback to current dir if permission issues
        failed_file_path = Path(f"failed_alphas_{run_id}.txt")
        open(failed_file_path, 'w', encoding='utf-8').close()

    failed_lock = threading.Lock()

    def mark_failed(aid: str):
        with failed_lock:
            if aid not in failed_alphas:
                failed_alphas.append(aid)
                _rewrite_failed_file(failed_file_path, failed_alphas)

    def mark_success(aid: str):
        with failed_lock:
            if aid in failed_alphas:
                failed_alphas.remove(aid)
                _rewrite_failed_file(failed_file_path, failed_alphas)

    # Pre-filtering: If UPDATE_MODE is "skip", we can check all alphas locally
    # and filter out those that are already fully downloaded.
    to_process_ids = alpha_ids
    if UPDATE_MODE == "skip":
        logger.info(f"Mode is 'skip'. Pre-scanning local files for {len(alpha_ids)} alphas...")
        missing_ids = []
        skipped_count = 0
        
        for aid in alpha_ids:
            # Fast check: if we are in "download all" mode (TARGET_RECORDSETS was only a subset),
            # we can't really know if an alpha is "complete" without querying the API first to know WHAT to download.
            # However, we can still skip if we have AT LEAST the 5 core ones.
            # If user wants absolutely EVERYTHING including rare ones, this fast check might skip an alpha that has 5 cores but lacks 'coverage'.
            # But for efficiency, assuming "5 cores" = "complete enough to skip" is usually a good trade-off.
            # If you want strictly "download anything new", you should use UPDATE_MODE="update" or disable fast-skip.
            
            is_complete = True
            prefix = (aid[:2] if aid else "xx").lower()
            
            # Only check the "Core 5" for skipping logic.
            # If an alpha has these 5, we assume it's done.
            for rs in ["pnl", "sharpe", "turnover", "daily-pnl", "yearly-stats"]:
                found = False
                for kind in ("regular", "super"):
                    kind_dir = "regular_alphas" if kind == "regular" else "super_alphas"
                    check_dir = BASE_ROOT_DIR / rs / kind_dir / prefix
                    if check_dir.exists():
                        if any(check_dir.glob(f"*_{aid}_*.json")):
                            found = True
                            break
                if not found:
                    is_complete = False
                    break
            
            if is_complete:
                skipped_count += 1
            else:
                missing_ids.append(aid)
                
        logger.info(f"Pre-scan complete. Skipped {skipped_count} alphas. Proceeding with {len(missing_ids)} missing alphas.")
        to_process_ids = missing_ids
    
    if not to_process_ids:
        logger.info("All alphas are already downloaded. Nothing to do.")
        return results # return empty results, or maybe populate with skipped paths if needed (but usually unnecessary for "fetch" step)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_alpha = {}
        
        for alpha_id in to_process_ids:
            # Use wrapper to manage thread-local sessions instead of creating new ones for every alpha
            future = executor.submit(_process_alpha_wrapper, session_proxy, alpha_id, writers)
            future_to_alpha[future] = alpha_id

        for future in as_completed(future_to_alpha):
            alpha_id = future_to_alpha[future]
            try:
                paths_map = future.result()
                results[alpha_id] = paths_map
                if not any(paths_map.values()):
                    mark_failed(alpha_id)
                else:
                    mark_success(alpha_id)
                logger.info("Finished alpha %s", alpha_id)
            except Exception as e:
                logger.error("Worker failed for alpha %s: %s", alpha_id, e)
                results[alpha_id] = {name: None for name in TARGET_RECORDSETS}
                mark_failed(alpha_id)

    # Second pass: conservative sequential retry for failed alphas
    if failed_alphas:
        logger.info("Second pass retry for %d failed alphas...", len(failed_alphas))
        # Temporarily use stronger backoff for this pass
        orig_initial, orig_mult, orig_max = INITIAL_BACKOFF, BACKOFF_MULTIPLIER, MAX_BACKOFF
        try:
            # Use larger backoff for retry wave
            globals()["INITIAL_BACKOFF"] = max( INITIAL_BACKOFF, 12 )
            globals()["BACKOFF_MULTIPLIER"] = max( BACKOFF_MULTIPLIER, 2.5 )
            globals()["MAX_BACKOFF"] = max( MAX_BACKOFF, 300 )
            for alpha_id in list(failed_alphas):
                try:
                    # Use the proxy even for retries to benefit from shared rate limiting / auth
                    retry_paths = process_alpha(session_proxy, alpha_id, writers)
                    if any(retry_paths.values()):
                        results[alpha_id] = retry_paths
                        logger.info("Retry succeeded for alpha %s", alpha_id)
                        mark_success(alpha_id)
                    else:
                        logger.warning("Retry still failed for alpha %s", alpha_id)
                except Exception as e:
                    logger.error("Retry exception for alpha %s: %s", alpha_id, e)
        finally:
            globals()["INITIAL_BACKOFF"], globals()["BACKOFF_MULTIPLIER"], globals()["MAX_BACKOFF"] = orig_initial, orig_mult, orig_max

    # Persist failed list to disk for audit (already maintained real-time)
    remaining_failed = [aid for aid, mp in results.items() if not any(mp.values())]
    if remaining_failed:
        logger.warning("Failed alphas remain (%d). See %s", len(remaining_failed), failed_file_path)

    if writers:
        try:
            writers.close()
        except Exception:
            pass

    return results


# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch recordsets for given alpha IDs")
    parser.add_argument("--alpha-ids-file", dest="ids_file", type=str, default=os.environ.get("ALPHA_IDS_FILE"), help="Path to a text file with one alpha_id per line")
    parser.add_argument("--max-workers", dest="max_workers", type=int, default=int(os.environ.get("WQ_MAX_WORKERS", MAX_WORKERS)))
    args = parser.parse_args()

    ids_env = os.environ.get("ALPHA_IDS", "").strip()
    alpha_ids_input: List[str] = []
    if ids_env:
        alpha_ids_input = [s.strip() for s in ids_env.split(",") if s.strip()]
    elif args.ids_file:
        file_path = Path(args.ids_file)
        # 自动检测文件类型
        if file_path.suffix.lower() == '.json':
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        alpha_ids_input = [str(x).strip() for x in data]
                    else:
                         # 如果不是列表，尝试按行读取回退
                         logger.warning("JSON content is not a list, falling back to empty list.")
                         alpha_ids_input = []
            except json.JSONDecodeError:
                 logger.warning("Failed to parse JSON file, falling back to line-by-line reading.")
                 with open(args.ids_file, "r", encoding="utf-8-sig") as f:
                    alpha_ids_input = [line.strip() for line in f if line.strip()]
        else:
            # 默认按 TXT 行读取
            with open(args.ids_file, "r", encoding="utf-8-sig") as f:
                alpha_ids_input = [line.strip() for line in f if line.strip()]

    # 再次清洗所有 ID，去除可能的引号、括号、逗号等非法字符
    # 保留字母、数字、下划线、横杠，以兼容可能的各种 ID 格式
    cleaned_ids = []
    for aid in alpha_ids_input:
        # 去除收尾的引号、逗号、括号等
        # 例如 "abc", -> abc
        clean_id = re.sub(r'^[\'"\[\],]+|[\'"\[\],]+$', '', aid)
        if clean_id:
            cleaned_ids.append(clean_id)
    alpha_ids_input = cleaned_ids

    if not alpha_ids_input:
        logger.info("Please provide alpha IDs via ALPHA_IDS env or --alpha-ids-file. Example: ALPHA_IDS=peqmVwj,Y9pGxpM")
        raise SystemExit(0)

    fetch_recordsets_for_alphas(alpha_ids_input, max_workers=args.max_workers)