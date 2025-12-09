#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生产环境健康检查脚本

检查项目:
1. Python 环境和依赖包
2. MongoDB 连接状态
3. WorldQuant API 认证
4. 配置文件完整性
5. 必要目录和权限
6. 日志系统
"""

import sys
import os
import json
from pathlib import Path
from typing import Dict, List, Tuple

# 颜色输出
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    END = '\033[0m'
    BOLD = '\033[1m'

def print_header(text: str):
    """打印标题"""
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{text}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.END}\n")

def print_check(name: str, status: bool, message: str = ""):
    """打印检查结果"""
    if status:
        icon = f"{Colors.GREEN}✓{Colors.END}"
        status_text = f"{Colors.GREEN}通过{Colors.END}"
    else:
        icon = f"{Colors.RED}✗{Colors.END}"
        status_text = f"{Colors.RED}失败{Colors.END}"
    
    print(f"{icon} {name:.<50} {status_text}")
    if message:
        print(f"  {Colors.YELLOW}→ {message}{Colors.END}")

def check_python_version() -> Tuple[bool, str]:
    """检查 Python 版本"""
    version = sys.version_info
    if version.major >= 3 and version.minor >= 8:
        return True, f"Python {version.major}.{version.minor}.{version.micro}"
    else:
        return False, f"Python {version.major}.{version.minor}.{version.micro} (需要 3.8+)"

def check_dependencies() -> Dict[str, Tuple[bool, str]]:
    """检查依赖包"""
    deps = {}
    
    # pymongo
    try:
        import pymongo
        deps["pymongo"] = (True, f"版本 {pymongo.__version__}")
    except ImportError:
        deps["pymongo"] = (False, "未安装")
    
    # requests
    try:
        import requests
        deps["requests"] = (True, f"版本 {requests.__version__}")
    except ImportError:
        deps["requests"] = (False, "未安装")
    
    # orjson (可选)
    try:
        import orjson
        deps["orjson"] = (True, f"版本 {orjson.__version__}")
    except ImportError:
        deps["orjson"] = (False, "未安装 (可选，但推荐)")
    
    return deps

def check_mongodb_connection(port: int) -> Tuple[bool, str]:
    """检查 MongoDB 连接"""
    try:
        from pymongo import MongoClient
        client = MongoClient(f"mongodb://localhost:{port}/", serverSelectionTimeoutMS=3000)
        client.admin.command('ping')
        client.close()
        return True, f"端口 {port} 连接成功"
    except Exception as e:
        return False, f"端口 {port} 连接失败: {str(e)[:50]}"

def check_wq_auth() -> Tuple[bool, str]:
    """检查 WorldQuant 认证"""
    try:
        import wq_login
        session = wq_login.start_session(min_remaining_seconds=60)
        
        # 尝试获取用户信息
        auth_url = f"{wq_login.BRAIN_API_URL}/authentication"
        response = session.get(auth_url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            user_id = data.get("user", {}).get("id", "Unknown")
            expiry = data.get("token", {}).get("expiry", 0)
            return True, f"用户 {user_id}, 会话剩余 {int(expiry)} 秒"
        else:
            return False, f"认证失败: HTTP {response.status_code}"
    except Exception as e:
        return False, f"认证错误: {str(e)[:50]}"

def check_config_file() -> Tuple[bool, str]:
    """检查配置文件"""
    config_path = Path("async_config.json")
    
    if not config_path.exists():
        return False, "配置文件不存在"
    
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        
        # 检查必要字段
        required_fields = [
            "ALPHA_IDS_FILE",
            "MAX_WORKERS",
            "SHARED_UPDATE_MODE",
            "WRITE_DB"
        ]
        
        missing = [f for f in required_fields if f not in config]
        if missing:
            return False, f"缺少字段: {', '.join(missing)}"
        
        return True, f"配置完整，包含 {len(config)} 个字段"
    except json.JSONDecodeError as e:
        return False, f"JSON 格式错误: {str(e)[:50]}"
    except Exception as e:
        return False, f"读取错误: {str(e)[:50]}"

def check_directories() -> Dict[str, Tuple[bool, str]]:
    """检查必要目录"""
    dirs = {}
    
    # 检查项目数据目录
    project_data = Path(__file__).resolve().parent.parent / "WorldQuant" / "Data"
    if project_data.exists():
        dirs["项目数据目录"] = (True, str(project_data))
    else:
        dirs["项目数据目录"] = (False, f"{project_data} 不存在（将自动创建）")
    
    # 检查数据目录
    try:
        import wq_login
        data_root = wq_login.WQ_DATA_ROOT
        if Path(data_root).exists():
            dirs["数据根目录"] = (True, str(data_root))
        else:
            dirs["数据根目录"] = (False, f"{data_root} 不存在")
    except Exception as e:
        dirs["数据根目录"] = (False, f"检查失败: {str(e)[:30]}")
    
    # 检查日志目录
    try:
        import wq_login
        logs_root = wq_login.WQ_LOGS_ROOT
        if Path(logs_root).exists():
            dirs["日志根目录"] = (True, str(logs_root))
        else:
            # 尝试创建
            try:
                Path(logs_root).mkdir(parents=True, exist_ok=True)
                dirs["日志根目录"] = (True, f"{logs_root} (已创建)")
            except Exception:
                dirs["日志根目录"] = (False, f"{logs_root} 不存在且无法创建")
    except Exception as e:
        dirs["日志根目录"] = (False, f"检查失败: {str(e)[:30]}")
    
    return dirs

def check_scripts() -> Dict[str, Tuple[bool, str]]:
    """检查脚本文件"""
    scripts = {}
    
    script_files = [
        "1_select_alpha_range.py",
        "2_fetch_and_store_local.py",
        "3_import_to_database.py",
        "wq_login.py",
        "wq_logger.py"
    ]
    
    for script in script_files:
        path = Path(script)
        if path.exists():
            scripts[script] = (True, f"{path.stat().st_size} 字节")
        else:
            scripts[script] = (False, "文件不存在")
    
    return scripts

def check_recordsets_module() -> Tuple[bool, str]:
    """检查 recordsets 相关核心模块"""
    # 修改：不再检查 recordsets 子目录，而是检查当前目录下的核心文件
    required_files = [
        "fetch_recordsets.py",
        "mongo_recordsets_writer.py",
        # "__init__.py" # 扁平化后通常不需要这个
    ]
    
    current_dir = Path(".")
    missing = [f for f in required_files if not (current_dir / f).exists()]
    
    if missing:
        return False, f"缺少文件: {', '.join(missing)}"
    
    return True, f"核心模块完整: {', '.join(required_files)}"

def main():
    """主函数"""
    print_header("Alpha Recordsets 生产环境健康检查")
    
    all_passed = True
    
    # 1. Python 环境
    print_header("1. Python 环境检查")
    status, msg = check_python_version()
    print_check("Python 版本", status, msg)
    all_passed = all_passed and status
    
    # 2. 依赖包
    print_header("2. 依赖包检查")
    deps = check_dependencies()
    for name, (status, msg) in deps.items():
        print_check(name, status, msg)
        if name != "orjson":  # orjson 是可选的
            all_passed = all_passed and status
    
    # 3. MongoDB 连接
    print_header("3. MongoDB 连接检查")
    for port in [27017, 27018]:
        status, msg = check_mongodb_connection(port)
        print_check(f"MongoDB {port}", status, msg)
        # 至少一个端口可用即可
    
    # 4. WorldQuant 认证
    print_header("4. WorldQuant API 认证检查")
    status, msg = check_wq_auth()
    print_check("API 认证", status, msg)
    all_passed = all_passed and status
    
    # 5. 配置文件
    print_header("5. 配置文件检查")
    status, msg = check_config_file()
    print_check("async_config.json", status, msg)
    all_passed = all_passed and status
    
    # 6. 目录检查
    print_header("6. 目录和权限检查")
    dirs = check_directories()
    for name, (status, msg) in dirs.items():
        print_check(name, status, msg)
        # R盘不是必须的
        if name != "R盘":
            all_passed = all_passed and status
    
    # 7. 脚本文件
    print_header("7. 脚本文件检查")
    scripts = check_scripts()
    for name, (status, msg) in scripts.items():
        print_check(name, status, msg)
        all_passed = all_passed and status
    
    # 8. recordsets 模块
    print_header("8. Recordsets 模块检查")
    status, msg = check_recordsets_module()
    print_check("recordsets 模块", status, msg)
    all_passed = all_passed and status
    
    # 总结
    print_header("健康检查总结")
    if all_passed:
        print(f"{Colors.GREEN}{Colors.BOLD}✓ 所有关键检查项通过，系统可以正常运行{Colors.END}\n")
        sys.exit(0)
    else:
        print(f"{Colors.RED}{Colors.BOLD}✗ 部分检查项失败，请修复后再运行{Colors.END}\n")
        print(f"{Colors.YELLOW}提示: 运行以下命令安装缺失的依赖:{Colors.END}")
        print(f"  pip install pymongo requests orjson\n")
        sys.exit(1)

if __name__ == "__main__":
    main()
