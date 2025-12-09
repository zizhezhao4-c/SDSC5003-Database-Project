#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
2_fetch_and_store_local.py

步骤2：数据爬取与本地存储
- 统一登录会话（wq_login）
- 读取 async_config.json 以控制并发、重试与覆盖/更新/跳过策略
- 支持 SHARED_UPDATE_MODE：overwrite/update/skip
- 统一调用 recordsets.fetch_recordsets
- 输出：本地 recordsets 文件
"""

import os
import sys
import json
import subprocess
import re
from pathlib import Path
from typing import List, Optional
from datetime import datetime

import wq_login
from wq_login import start_session

# 统一登录
# 移除旧的导入
# from wq_login import start_session, save_cookie_to_file, WQ_DATA_ROOT, USER_KEY

# 使用基于脚本所在目录的绝对路径
SCRIPT_DIR = Path(__file__).parent
CONFIG_PATH = SCRIPT_DIR / "2_fetch_config.json"
# 移除硬编码路径，使用相对路径作为备选
ABS_CONFIG_PATH = CONFIG_PATH  # Fallback to relative path
ALPHA_IDS_DIR = SCRIPT_DIR / "alpha_ids_to_fetch_list"  # 待爬取的Alpha ID列表目录


def find_latest_alpha_ids_file() -> Optional[Path]:
    """
    自动查找 alpha_ids_to_fetch_list 目录中最新的 alpha_ids_USER_KEY_*.json 文件
    文件命名格式: alpha_ids_USER_KEY_YYYYMMDD_HHMMSS.json
    """
    if not ALPHA_IDS_DIR.exists():
        print(f"Alpha IDs 目录不存在: {ALPHA_IDS_DIR}")
        return None
    
    # 尝试从 USER_KEY 环境变量或 wq_login 中获取当前用户
    # 注意：这里最好与 1_select_alpha_range.py 使用的 USER_KEY 一致。
    # 1_select_alpha_range.py 使用的是 1_select_alpha_config.json 中的 SOURCE_COLLECTION_USER。
    # 但 2_fetch_and_store_local.py 是读取 async_config.json。
    # 为了安全起见，我们先尝试匹配包含当前登录 USER_KEY 的文件。
    # 如果没找到，再尝试通用匹配（为了兼容旧文件，或者如果用户想跑别人的列表）。
    
    current_user_key = wq_login.USER_KEY
    if not current_user_key:
        print("错误: 未在 wq_login.py 或环境变量中找到 USER_KEY")
        return None

    # 优先查找包含当前 USER_KEY 的文件
    candidate_files = []
    
    # 模式1: alpha_ids_{USER_KEY}_YYYYMMDD_HHMMSS.json
    # 模式2: alpha_ids_YYYYMMDD_HHMMSS.json (旧格式)
    
    for file in ALPHA_IDS_DIR.glob("alpha_ids_*.json"):
        # 尝试解析时间戳
        # 文件名可能是:
        # 1. alpha_ids_20241124_100000.json (旧)
        # 2. alpha_ids_zzz_20241124_100000.json (新)
        
        parts = file.stem.split('_')
        timestamp_str = None
        file_user = None
        
        if len(parts) >= 3:
            # 检查最后两部分是否组成时间戳
            ts_part = f"{parts[-2]}_{parts[-1]}"
            try:
                dt = datetime.strptime(ts_part, "%Y%m%d_%H%M%S")
                timestamp_str = ts_part
                # 如果有更多部分，中间的就是 user_key
                if len(parts) > 3:
                    file_user = "_".join(parts[2:-2])
            except ValueError:
                pass
        
        if timestamp_str:
             candidate_files.append({
                 "file": file,
                 "time": timestamp_str, # 字符串比较即可，ISO格式
                 "user": file_user
             })

    if not candidate_files:
        print(f"未在 {ALPHA_IDS_DIR} 中找到符合命名规则的 Alpha IDs 文件")
        return None
    
    # 过滤逻辑：
    # 如果 current_user_key 存在，必须只看该用户的文件
    # 严格限制：不回退到其他用户，防止跨账号操作 Alpha
    if current_user_key:
        user_files = [f for f in candidate_files if f["user"] == current_user_key]
        if user_files:
            candidate_files = user_files
        else:
            print(f"错误: 未找到用户 '{current_user_key}' 的 Alpha ID 列表文件")
            print(f"当前目录下仅有以下用户的文件: {list(set(f['user'] for f in candidate_files if f['user']))}")
            return None
        
    # 按时间戳排序
    candidate_files.sort(key=lambda x: x["time"], reverse=True)
    latest_file = candidate_files[0]["file"]
    
    print(f"找到最新的 Alpha IDs 文件: {latest_file.name}")
    # print(f"  生成时间: {json_files[0][0].strftime('%Y-%m-%d %H:%M:%S')}")
    
    return latest_file


def load_config() -> dict:
    # 优先使用相对路径；如果不存在则回退到绝对路径
    path = CONFIG_PATH if CONFIG_PATH.exists() else ABS_CONFIG_PATH
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"读取配置失败({path}): {e}")
        return {}


def ensure_login_and_cookie():
    if not start_session:
        print("警告：未找到 wq_login.start_session，将直接执行（可能导致认证问题）")
        return
    try:
        # start_session 内部会自动处理：读取标准路径的 cookie -> 验证 -> 无效则重新登录 -> 保存到标准路径
        session = start_session(min_remaining_seconds=120)
        print(f"登录成功，当前用户: {wq_login.USER_KEY}")
        
        # 不再需要在当前目录手动保存 session_cookie.json
        # if save_cookie_to_file: ...
        # with open("session_cookie.json", ...) 
        
    except Exception as e:
        print(f"登录流程失败：{e}")
        raise


def build_env(cfg: dict):
    env = os.environ.copy()
    
    # 修改：不再需要添加 recordsets 包路径，只需确保当前目录在 PYTHONPATH 中
    # (其实 Python 默认包含当前目录，但显式添加更稳妥)
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
    # 更新策略（共享给 recordsets），用于决定文件存在时的动作
    # 优先使用 FETCH_UPDATE_MODE，回退到 SHARED_UPDATE_MODE
    mode = str(cfg.get("FETCH_UPDATE_MODE", cfg.get("SHARED_UPDATE_MODE", "update"))).lower()
    env["WQ_UPDATE_MODE"] = mode  # overwrite/update/skip
    # 写库/检查库（步骤2通常不写库，但保留传递以兼容老版本逻辑）
    if "WRITE_DB" in cfg:
        val = cfg["WRITE_DB"]
        env["WQ_WRITE_DB"] = ",".join(str(x) for x in (val if isinstance(val, list) else [val]))
        # 兼容旧版变量：主/备库
        w_list = val if isinstance(val, list) else [val]
        if len(w_list) >= 1:
            env["WQ_WRITE_PRIMARY_DB"] = str(w_list[0])
        if len(w_list) >= 2:
            env["WQ_WRITE_SECONDARY_DB"] = str(w_list[1])
    if "CHECK_DB" in cfg:
        val = cfg["CHECK_DB"]
        env["WQ_CHECK_DB"] = ",".join(str(x) for x in (val if isinstance(val, list) else [val]))
    # 其他控制项
    for k in ("SKIP_REGULAR", "SKIP_SUPER", "BASE_DIRECTORY"):
        if k in cfg:
            env[f"WQ_{k}"] = str(cfg[k])
    # 输出根目录（可选）。当 BASE_DIRECTORY 为 null/缺失时，按 wq_login 规则拼接: {WQ_DATA_ROOT}\{USER_KEY}\recordsets
    base_dir_cfg = cfg.get("BASE_DIRECTORY", None)
    if base_dir_cfg:
        # 如果是相对路径，基于脚本目录解析
        base_path = Path(base_dir_cfg)
        if not base_path.is_absolute():
            base_path = (SCRIPT_DIR / base_path).resolve()
        env["WQ_BASE_DIRECTORY"] = str(base_path)
    else:
        # 使用 wq_login 中的 WQ_DATA_ROOT 和 当前 USER_KEY
        if wq_login.WQ_DATA_ROOT and wq_login.USER_KEY:
            computed = str(Path(wq_login.WQ_DATA_ROOT) / wq_login.USER_KEY / "recordsets")
            env["WQ_BASE_DIRECTORY"] = computed
        else:
            raise RuntimeError("缺少 WQ_DATA_ROOT 或 USER_KEY，请在 wq_login.py 中配置。")
    return env


def read_ids_from_file(path: Path) -> List[str]:
    """从 JSON 文件读取 Alpha IDs"""
    ids: List[str] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                ids = [str(x).strip() for x in data if str(x).strip()]
    except Exception as e:
        print(f"读取 Alpha IDs 失败: {e}")
    
    print(f"加载了 {len(ids)} 个 Alpha ID")
    return ids


def build_cmd(cfg: dict, alpha_ids_file: Path):
    # 步骤2统一按 ID 列表抓取并本地存储
    max_workers = str(cfg.get("MAX_WORKERS", 3))
    
    # 修改：直接调用同级目录下的 fetch_recordsets.py
    cmd = [
        sys.executable, "fetch_recordsets.py",
        "--alpha-ids-file", str(alpha_ids_file),
        "--max-workers", max_workers
    ]
    
    # 若配置提供了 RECORDSETS_FETCH_SCRIPT，则优先使用该路径
    cfg_script = Path(str(cfg.get("RECORDSETS_FETCH_SCRIPT", ""))) if cfg.get("RECORDSETS_FETCH_SCRIPT") else None
    if cfg_script and cfg_script.exists():
        cmd = [
            sys.executable, str(cfg_script),
            "--alpha-ids-file", str(alpha_ids_file),
            "--max-workers", max_workers
        ]
        
    return cmd


def main():
    print("="*60)
    print("步骤2: 数据抓取与本地存储")
    print("="*60)
    
    # 1. 加载配置
    cfg = load_config()
    
    # 2. 自动查找最新的 Alpha IDs 文件
    print("\n[1/4] 查找 Alpha IDs 文件...")
    
    # 直接使用 find_latest_alpha_ids_file，它会根据当前 wq_login.USER_KEY 进行严格匹配
    alpha_ids_file = find_latest_alpha_ids_file()
    
    if not alpha_ids_file:
        print("\n错误: 未找到 Alpha IDs 文件")
        print("\n请先运行步骤1生成 Alpha IDs，或手动将 JSON 文件放入 alpha_ids_to_fetch_list 目录")
        print("文件命名格式: alpha_ids_YYYYMMDD_HHMMSS.json")
        print("示例: alpha_ids_20241124_153045.json")
        return
    
    # 3. 读取 Alpha IDs
    print("\n[2/4] 读取 Alpha IDs...")
    alpha_ids = read_ids_from_file(alpha_ids_file)
    
    if not alpha_ids:
        print("\n错误: Alpha IDs 文件为空")
        return
    
    # 4. 登录并保存 Cookie
    print("\n[3/4] 检查登录会话...")
    ensure_login_and_cookie()
    
    # 5. 构建执行环境和命令
    print("\n[4/4] 准备执行抓取...")
    env = build_env(cfg)
    cmd = build_cmd(cfg, alpha_ids_file)

    # 警告检查：overwrite模式需要强确认
    update_mode = str(env.get("WQ_UPDATE_MODE", "update")).lower()
    
    if update_mode == "overwrite":
        print("\n" + "!"*60)
        print("警告: 当前处于 [OVERWRITE] 覆盖模式！")
        print("警告: 这将重新下载并覆盖本地已有的 Recordset 文件。")
        print("警告: 如果您只是想下载缺失的文件，请使用 'skip' 或 'update' 模式。")
        print("!"*60 + "\n")
        
        # 即使 CONFIRM_BEFORE_RUN=false，overwrite 模式也强制确认，除非明确知晓风险（这里为了安全强制确认）
        confirm = input("您确定要执行覆盖操作吗? (请输入 'overwrite' 确认): ").strip()
        if confirm != "overwrite":
            print("确认失败，已取消执行。")
            return
    elif bool(cfg.get("CONFIRM_BEFORE_RUN", True)):
        resp = input("是否开始执行抓取? (y/N): ").strip().lower()
        if resp not in ("y", "yes"):
            print("取消执行")
            return

    # 展示关键环境变量
    print("环境变量 (WQ_*):")
    for k, v in env.items():
        if k.startswith("WQ_"):
            print(f"  {k}={v}")

    print(f"执行命令: {' '.join(cmd)}")
    root_dir = Path(__file__).parent
    print(f"工作目录: {root_dir}")

    pass

    try:
        result = subprocess.run(cmd, env=env, cwd=root_dir, text=True)
        print(f"进程结束，返回码: {result.returncode}")
    except KeyboardInterrupt:
        print("\n用户中断执行")
    except Exception as e:
        print(f"\n执行出错: {e}")

    print("\n步骤2完成：数据已抓取并存储到本地。")


if __name__ == "__main__":
    main()
