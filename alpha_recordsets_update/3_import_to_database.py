#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
3_import_to_database.py

步骤3：从本地文件导入到数据库
- 统一登录会话（wq_login）
- 读取 async_config.json 控制导入范围与更新策略（覆盖/更新/跳过）
- 共享 SHARED_UPDATE_MODE（overwrite/update/skip）
- 支持 IMPORT_SCOPE（full_local/default_list/custom_list）与 IMPORT_IDS_FILE
- 支持多库写入：WRITE_DB（单值或列表）与 CHECK_DB（单值或列表）
"""

import os
import json
import subprocess
import sys
import re
from pathlib import Path
from typing import List, Union, Optional, Dict
from datetime import datetime

import wq_login
import mongo_recordsets_writer as mrw
import fetch_recordsets as frs
import sqlite_state_manager as ssm

# 移除旧的导入
# from wq_login import start_session, save_cookie_to_file, WQ_DATA_ROOT, USER_KEY

# 使用基于脚本所在目录的绝对路径
SCRIPT_DIR = Path(__file__).parent
CONFIG_PATH = SCRIPT_DIR / "3_import_config.json"
# 移除硬编码路径，使用相对路径作为备选
ABS_CONFIG_PATH = CONFIG_PATH  # Fallback to relative path
ALPHA_IDS_DIR = SCRIPT_DIR / "alpha_ids_to_fetch_list"  # 待爬取的Alpha ID列表目录
IMPORT_IDS_DIR = SCRIPT_DIR / "alpha_ids_to_import_list" # 待导入的自定义Alpha ID列表目录
DEFAULT_IDS_TXT = "alpha_recordsets_update/filtered_alpha_ids.txt"
DEFAULT_IDS_JSON = "alpha_recordsets_update/filtered_alpha_ids.json"


def find_latest_alpha_ids_file_in_dir(target_dir: Path) -> Optional[str]:
    """
    通用函数：在指定目录下查找最新的 alpha_ids_USER_KEY_*.json/txt 文件
    """
    if not target_dir.exists():
        return None
    
    current_user_key = wq_login.USER_KEY
    candidate_files = []
    
    # 扩展匹配模式，兼容 json 和 txt
    for file in target_dir.glob("alpha_ids_*.*"):
        if file.suffix.lower() not in ('.json', '.txt'):
            continue
            
        parts = file.stem.split('_')
        timestamp_str = None
        file_user = None
        
        if len(parts) >= 3:
            ts_part = f"{parts[-2]}_{parts[-1]}"
            try:
                datetime.strptime(ts_part, "%Y%m%d_%H%M%S")
                timestamp_str = ts_part
                if len(parts) > 3:
                    file_user = "_".join(parts[2:-2])
            except ValueError:
                pass
        
        if timestamp_str:
             candidate_files.append({
                 "file": file,
                 "time": timestamp_str,
                 "user": file_user
             })

    if not candidate_files:
        return None
        
    if current_user_key:
        user_files = [f for f in candidate_files if f["user"] == current_user_key]
        if user_files:
            candidate_files = user_files
        else:
            # 如果在该目录下没找到属于当前用户的文件，返回 None
            return None
    
    candidate_files.sort(key=lambda x: x["time"], reverse=True)
    latest_file = candidate_files[0]["file"]
    
    print(f"在 {target_dir.name} 中找到最新文件: {latest_file.name}")
    return str(latest_file)

# 兼容旧函数名，指向新通用函数（默认查 ALPHA_IDS_DIR）
def find_latest_alpha_ids_file() -> Optional[str]:
    return find_latest_alpha_ids_file_in_dir(ALPHA_IDS_DIR)


def load_config() -> dict:
    path = CONFIG_PATH if CONFIG_PATH.exists() else ABS_CONFIG_PATH
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


# 删除登录相关逻辑，步骤3仅执行本地导入


def normalize_db_value(v: Union[str, int, List[Union[str, int]]]) -> str:
    """将配置中的数据库值规范化为逗号分隔字符串，供环境变量传递。"""
    if isinstance(v, list):
        return ",".join(str(x) for x in v)
    return str(v)


def build_env(cfg: dict):
    env = os.environ.copy()
    
    # 修改：简化 PYTHONPATH 设置
    local_root = str(Path(__file__).parent)
    existing_pp = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{existing_pp};{local_root}" if existing_pp else local_root

    # 并发/重试策略
    for k in (
        "MAX_WORKERS",
        "REQUEST_TIMEOUT",
        "MAX_RETRIES",
        "INITIAL_BACKOFF",
        "BACKOFF_MULTIPLIER",
        "MAX_BACKOFF",
        "MAX_RETRY_ROUNDS",
        "SESSION_CHECK_INTERVAL",
    ):
        if k in cfg:
            env[f"WQ_{k}"] = str(cfg[k])
    # 文件/数据库更新策略（共享 2/3）
    # 优先使用 DB_UPDATE_MODE，回退到 SHARED_UPDATE_MODE
    env["WQ_UPDATE_MODE"] = str(cfg.get("DB_UPDATE_MODE", cfg.get("SHARED_UPDATE_MODE", "update"))).lower()
    if "SQLITE_DB_PATH" in cfg:
        env["WQ_SQLITE_DB_PATH"] = str(cfg["SQLITE_DB_PATH"])

    # 导入范围标识
    env["WQ_IMPORT_SCOPE"] = str(cfg.get("IMPORT_SCOPE", "default_list")).lower()
    # 写库与检查库
    if "WRITE_DB" in cfg:
        env["WQ_WRITE_DB"] = normalize_db_value(cfg["WRITE_DB"]) 
        try:
            w = cfg["WRITE_DB"]
            w_list = w if isinstance(w, list) else [w]
            w_list = [int(x) for x in w_list]
            
            # Defaults
            p_port = 27018
            s_port = 27017
            write_p = "0"
            write_s = "0"
            
            remaining = []
            if 27018 in w_list:
                write_p = "1"
            if 27017 in w_list:
                write_s = "1"
                
            for p in w_list:
                if p != 27018 and p != 27017:
                    remaining.append(p)
            
            if remaining:
                if write_p == "0":
                    write_p = "1"
                    p_port = remaining.pop(0)
            
            if remaining:
                 if write_s == "0":
                    write_s = "1"
                    s_port = remaining.pop(0)
            
            env["WQ_WRITE_PRIMARY_DB"] = write_p
            env["WQ_WRITE_SECONDARY_DB"] = write_s
            env["WQ_PRIMARY_PORT"] = str(p_port)
            env["WQ_SECONDARY_PORT"] = str(s_port)
        except Exception:
            env["WQ_WRITE_PRIMARY_DB"] = "0"
            env["WQ_WRITE_SECONDARY_DB"] = "0"
            env["WQ_PRIMARY_PORT"] = "27018"
            env["WQ_SECONDARY_PORT"] = "27017"
    if "CHECK_DB" in cfg:
        env["WQ_CHECK_DB"] = normalize_db_value(cfg["CHECK_DB"])  # 如 27017 或 27017,27018
    # 其他控制项（保留必要项）
    for k in ("SKIP_REGULAR", "SKIP_SUPER", "BASE_DIRECTORY"):
        if k in cfg:
            env[f"WQ_{k}"] = str(cfg[k])
    # 将 BASE_DIRECTORY 同时映射到 WQ_BASE_DIRECTORY（部分 recordsets 版本读该名）
    base_dir_cfg = cfg.get("BASE_DIRECTORY", None)
    if base_dir_cfg:
        # 如果是相对路径，基于脚本目录解析
        base_path = Path(base_dir_cfg)
        if not base_path.is_absolute():
            base_path = (SCRIPT_DIR / base_path).resolve()
        env["WQ_BASE_DIRECTORY"] = str(base_path)
    else:
        if wq_login.WQ_DATA_ROOT and wq_login.USER_KEY:
            computed = str(Path(wq_login.WQ_DATA_ROOT) / wq_login.USER_KEY / "recordsets")
            env["WQ_BASE_DIRECTORY"] = computed
        else:
            existing = env.get("WQ_BASE_DIRECTORY")
            if not existing:
                raise RuntimeError("缺少 BASE_DIRECTORY，请在配置或环境变量 WQ_BASE_DIRECTORY 中提供本地记录集根目录。")
    return env


def read_ids_from_file(path_str: str) -> List[str]:
    p = Path(path_str)
    if not p.exists():
        for alt in (DEFAULT_IDS_JSON, DEFAULT_IDS_TXT):
            ap = Path(alt)
            if ap.exists():
                p = ap
                break
    ids: List[str] = []
    if p.suffix.lower() == ".json":
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    ids = [str(x).strip() for x in data if str(x).strip()]
        except Exception as e:
            print(f"读取 JSON ID 列表失败: {e}")
    else:
        try:
            with open(p, "r", encoding="utf-8") as f:
                ids = [line.strip() for line in f.readlines() if line.strip()]
        except Exception as e:
            print(f"读取 TXT ID 列表失败: {e}")
    print(f"使用 ID 列表文件: {p}，共 {len(ids)} 条")
    return ids


def _read_ids_from_file_or_default(path_str: str) -> List[str]:
    p = Path(path_str)
    if not p.exists():
        for alt in (DEFAULT_IDS_JSON, DEFAULT_IDS_TXT):
            ap = Path(alt)
            if ap.exists():
                p = ap
                break
    ids: List[str] = []
    if p.suffix.lower() == ".json":
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    ids = [str(x).strip() for x in data if str(x).strip()]
        except Exception:
            ids = []
    else:
        try:
            with open(p, "r", encoding="utf-8") as f:
                ids = [line.strip() for line in f.readlines() if line.strip()]
        except Exception:
            ids = []
    return ids

def _scan_alpha_ids_from_local(base_dir: Path) -> List[str]:
    ids: set[str] = set()
    pattern = re.compile(r"^[^_]+_(regular_alpha|super_alpha)_[^_]+_[^_]+_([^_]+)_[0-9a-f]{12}")
    for rs_dir in base_dir.glob("*"):
        if not rs_dir.is_dir():
            continue
        for kind_dir in (rs_dir / "regular_alphas", rs_dir / "super_alphas"):
            if not kind_dir.exists():
                continue
            for prefix_dir in kind_dir.glob("*"):
                if not prefix_dir.is_dir():
                    continue
                for fp in prefix_dir.glob("*.json"):
                    m = pattern.match(fp.stem)
                    if m:
                        ids.add(m.group(2))
    return sorted(ids)

def _compute_local_dir_with_base(base_dir: Path, recordset_name: str, alpha_kind: str, alpha_id: str) -> Path:
    """使用指定的 base_dir 计算本地目录路径"""
    kind_dir = "regular_alphas" if alpha_kind == "regular" else "super_alphas"
    prefix = (alpha_id[:2] if alpha_id else "xx").lower()
    return base_dir / recordset_name / kind_dir / prefix


def _pick_latest_file(alpha_id: str, recordset_name: str, base_dir: Path) -> Optional[Path]:
    """在指定的 base_dir 下查找最新的文件"""
    candidates: List[Path] = []
    for kind in ("regular", "super"):
        d = _compute_local_dir_with_base(base_dir, recordset_name, kind, alpha_id)
        if d.exists():
            candidates.extend([p for p in d.glob(f"*_{alpha_id}_*.json") if p.is_file()])
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]

def _detect_user_key(base_dir: Path) -> Optional[str]:
    pattern = re.compile(r"^(?P<user>.+?)_(regular_alpha|super_alpha)_")
    for rs_dir in base_dir.glob("*"):
        if not rs_dir.is_dir():
            continue
        for kind_dir in (rs_dir / "regular_alphas", rs_dir / "super_alphas"):
            if not kind_dir.exists():
                continue
            for prefix_dir in kind_dir.glob("*"):
                if not prefix_dir.is_dir():
                    continue
                for fp in prefix_dir.glob("*.json"):
                    m = pattern.match(fp.stem)
                    if m:
                        return m.group("user")
    return None

def _import_local_to_db(alpha_ids: List[str], write_primary: bool, write_secondary: bool, base_dir: Path) -> Dict[str, int]:
    user_key = wq_login.USER_KEY or _detect_user_key(base_dir)
    if not user_key:
        raise RuntimeError("无法确定用户Key。请在 wq_login.py 中配置 USER_KEY，或确保本地文件名包含 user_key 前缀。")
    user_id = "UNKNOWN"
    
    # 显式读取更新策略
    update_mode = os.environ.get("WQ_UPDATE_MODE", "update").lower()
    sqlite_db_path = os.environ.get("WQ_SQLITE_DB_PATH", "recordsets_state.db")
    primary_port = int(os.environ.get("WQ_PRIMARY_PORT", "27018"))
    secondary_port = int(os.environ.get("WQ_SECONDARY_PORT", "27017"))
    
    # 如果开启 smart_merge，初始化 SQLiteStateDB
    state_db = None
    if update_mode == "smart_merge":
        state_db = ssm.SQLiteStateDB(sqlite_db_path)
        writer_mode = "update"
    else:
        writer_mode = update_mode

    # 传递 update_mode 到 MongoWriters
    writers = mrw.MongoWriters(user_key=user_key, 
                               write_primary=write_primary, 
                               write_secondary=write_secondary, 
                               update_mode=writer_mode,
                               primary_port=primary_port,
                               secondary_port=secondary_port)
    
    total_map: Dict[str, int] = {}
    for aid in alpha_ids:
        cnt = 0
        
        # 动态扫描该 Alpha 的所有本地文件，而不仅仅是 TARGET_RECORDSETS
        # 我们需要在两个目录（regular/super）中查找所有以 aid 为 ID 的文件
        found_files = []
        prefix = (aid[:2] if aid else "xx").lower()
        
        # 扫描 regular 和 super 目录
        # 假设我们不知道有哪些 recordset，只能遍历所有 recordset 目录？
        # 不，我们可以遍历 base_dir 下的一级目录（recordset name），然后再深入查找。
        # 这样做比硬编码 TARGET_RECORDSETS 更全面，但可能会慢一些。
        
        # 方法: 遍历 base_dir 的子目录 (recordset names)
        for recordset_dir in base_dir.iterdir():
            if not recordset_dir.is_dir():
                continue
            
            recordset_name = recordset_dir.name
            # 忽略非 recordset 目录（如果有的话，比如 __pycache__）
            if recordset_name.startswith(".") or recordset_name.startswith("_"):
                continue

            # 使用 _pick_latest_file 逻辑来找该 Alpha 在该 recordset 下的最新文件
            p = _pick_latest_file(aid, recordset_name, base_dir)
            if p:
                found_files.append((recordset_name, p))

        if not found_files:
            # 如果一个文件都没找到，可能是 ID 不对或者真的没下载
            continue

        for name, p in found_files:
            # Smart Merge 逻辑
            if update_mode == "smart_merge" and state_db:
                current_hash = ssm.compute_file_hash(p)
                stored_hash = state_db.get_state(aid, name)
                
                if current_hash == stored_hash:
                    # 哈希一致，文件未变，跳过
                    continue
            
            try:
                with open(p, "rb") as f:
                    raw = f.read()
                try:
                    import orjson
                    data = orjson.loads(raw)
                except Exception:
                    data = json.loads(raw.decode("utf-8"))
                rng = frs._extract_range_for_recordset(name, data)
                if not rng:
                    rng = None
                
                # 执行写入
                writers.upsert_recordset(alpha_id=aid, user_id=user_id, recordset_name=name, data=data, date_range=rng)
                
                # 写入成功后，更新 SQLite 状态
                if update_mode == "smart_merge" and state_db:
                    state_db.update_state(aid, name, current_hash)
                
                cnt += 1
            except Exception:
                pass
        total_map[aid] = cnt
    try:
        writers.close()
    except Exception:
        pass
    return total_map


def main():
    cfg = load_config()
    env = build_env(cfg)
    for k, v in env.items():
        if k.startswith("WQ_"):
            print(f"  {k}={v}")

    import_scope = str(cfg.get("IMPORT_SCOPE", "default_list")).lower()
    
    # 警告检查：overwrite模式需要强确认
    update_mode = str(env.get("WQ_UPDATE_MODE", "update")).lower()
    
    if update_mode == "overwrite":
        print("\n" + "!"*60)
        print("警告: 当前处于 [OVERWRITE] 覆盖模式！")
        print("警告: 这将清除数据库中已有的 Recordset 历史数据，仅保留本次导入的内容。")
        print("警告: 如果您只是想追加新数据，请使用 'update' 模式。")
        print("!"*60 + "\n")
        
        confirm = input("您确定要执行覆盖操作吗? (请输入 'overwrite' 确认): ").strip()
        if confirm != "overwrite":
            print("确认失败，已取消执行。")
            return
    elif bool(cfg.get("CONFIRM_BEFORE_RUN", True)):
        resp = input("是否开始从本地导入数据库? (y/N): ").strip().lower()
        if resp not in ("y", "yes"):
            print("取消执行")
            return

    base_dir = Path(env.get("WQ_BASE_DIRECTORY", ""))
    write_primary = env.get("WQ_WRITE_PRIMARY_DB", "0") == "1"
    write_secondary = env.get("WQ_WRITE_SECONDARY_DB", "0") == "1"

    if import_scope == "full_local":
        ids = _scan_alpha_ids_from_local(base_dir)
    else:
        # 简化合并逻辑：
        # 1. 优先读取 IMPORT_IDS_FILE
        # 2. 如果未配置或文件不存在，自动扫描 ALPHA_IDS_DIR 下最新的同用户文件
        target_file = None
        
        cfg_file = cfg.get("IMPORT_IDS_FILE")
        if cfg_file and Path(cfg_file).exists():
            print(f"正在使用配置的列表文件: {cfg_file}")
            target_file = cfg_file
        else:
            if cfg_file:
                print(f"配置的 IMPORT_IDS_FILE ({cfg_file}) 不存在，尝试自动扫描...")
            else:
                print("IMPORT_IDS_FILE 未配置，尝试自动扫描...")
            
            # 自动扫描 fetch 目录
            target_file = find_latest_alpha_ids_file_in_dir(ALPHA_IDS_DIR)
            
        if target_file:
            ids = _read_ids_from_file_or_default(target_file)
        else:
            print(f"错误: 既未指定有效 IMPORT_IDS_FILE，也未在 {ALPHA_IDS_DIR} 找到可用列表。")
            ids = []

    if not ids:
        print("无可导入的 Alpha ID")
        return

    stats = _import_local_to_db(ids, write_primary=write_primary, write_secondary=write_secondary, base_dir=base_dir)
    total = sum(stats.values())
    print(f"导入完成，写入记录集文件数: {total}")


if __name__ == "__main__":
    main()
