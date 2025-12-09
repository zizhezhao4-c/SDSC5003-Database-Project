#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多账号批处理脚本 v2.0 - WorldQuant Alpha 数据获取、打包和导入
支持从配置文件读取账号列表

遍历多个用户账号，依次执行：
1. 步骤1：获取 Alpha 数据 (1_fetch_all_alpha_main_async.py)
2. 步骤2：数据打包和分解 (2_alpha_files_packaging_and_decomposition.py)
3. 步骤3：导入到 MongoDB (3_mongo_import_v4.py)
"""

import sys
import os
import time
import json
import logging
import argparse
import csv
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Optional
import traceback

# 确保项目根目录在 sys.path 中
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# 导入项目模块
import wq_login
from wq_logger import get_logger

# 初始化日志
logger = get_logger(__name__)

# 导入获取更新alpha的功能
try:
    from fetch_updated_alphas import fetch_updated_alphas_for_user
    FETCH_UPDATED_ALPHAS_AVAILABLE = True
except ImportError as e:
    logger.debug(f"无法导入fetch_updated_alphas模块: {e}")
    FETCH_UPDATED_ALPHAS_AVAILABLE = False
    fetch_updated_alphas_for_user = None

# ============================================================================
# 配置加载
# ============================================================================

def load_accounts_from_csv_excel(file_path: str) -> List[Dict]:
    """
    从CSV或Excel文件读取账号列表
    
    格式：user_key, email, password
    
    Args:
        file_path: CSV/Excel文件路径
        
    Returns:
        账号列表
    """
    accounts = []
    file_path_obj = Path(file_path)
    
    if not file_path_obj.exists():
        logger.warning(f"账号文件不存在: {file_path}")
        return accounts
    
    try:
        # 判断文件类型
        if file_path.endswith(('.xlsx', '.xls')):
            # Excel文件
            try:
                import importlib
                openpyxl = importlib.import_module("openpyxl")
                workbook = openpyxl.load_workbook(file_path)
                sheet = workbook.active
                
                for row_idx, row in enumerate(sheet.iter_rows(min_row=1, values_only=True), 1):
                    if not row or len(row) < 3:
                        continue
                    
                    user_key = str(row[0]).strip() if row[0] else None
                    email = str(row[1]).strip() if row[1] else None
                    password = str(row[2]).strip() if row[2] else None
                    
                    if user_key and email and password:
                        # 自动创建凭据文件
                        credentials_dir = Path.home() / "secrets"
                        credentials_dir.mkdir(exist_ok=True)
                        credentials_file = credentials_dir / f"{user_key}_platform-brain.json"
                        
                        with open(credentials_file, 'w', encoding='utf-8') as f:
                            json.dump({"email": email, "password": password}, f, indent=2)
                        
                        accounts.append({
                            "user_key": user_key,
                            "description": f"从Excel导入 (行{row_idx})",
                            "enabled": True
                        })
                        logger.info(f"从Excel读取账号: {user_key}")
                    
            except ImportError:
                logger.error("需要安装 openpyxl 库来读取Excel文件: pip install openpyxl")
                return accounts
        else:
            # CSV文件
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                for row_idx, row in enumerate(reader, 1):
                    if len(row) < 3:
                        continue
                    
                    user_key = row[0].strip()
                    email = row[1].strip()
                    password = row[2].strip()
                    
                    if user_key and email and password:
                        # 自动创建凭据文件
                        credentials_dir = Path.home() / "secrets"
                        credentials_dir.mkdir(exist_ok=True)
                        credentials_file = credentials_dir / f"{user_key}_platform-brain.json"
                        
                        with open(credentials_file, 'w', encoding='utf-8') as f:
                            json.dump({"email": email, "password": password}, f, indent=2)
                        
                        accounts.append({
                            "user_key": user_key,
                            "description": f"从CSV导入 (行{row_idx})",
                            "enabled": True
                        })
                        logger.info(f"从CSV读取账号: {user_key}")
        
        logger.info(f"成功从文件读取 {len(accounts)} 个账号")
        
    except Exception as e:
        logger.error(f"读取账号文件失败: {e}")
        logger.debug(traceback.format_exc())
    
    return accounts


def load_user_config(config_file: str = "user_accounts_config.json") -> Dict:
    """
    从配置文件加载用户账号配置
    
    Args:
        config_file: 配置文件路径
        
    Returns:
        配置字典
    """
    config_path = Path(config_file)
    
    # 默认配置
    default_config = {
        "USER_ACCOUNTS": [
            {"user_key": "zzz", "description": "主账号", "enabled": True}
        ],
        "EXECUTE_STEPS": {
            "step1_fetch": True,
            "step2_package": True,
            "step3_import": True,
        },
        "CONTINUE_ON_ERROR": True
    }
    
    if not config_path.exists():
        logger.warning(f"配置文件不存在: {config_file}，使用默认配置")
        return default_config
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        
        # 过滤注释字段
        config = {}
        for key, value in config_data.items():
            if not key.startswith('_') and not key.startswith('//'):
                config[key] = value
        
        # 合并默认配置
        for key in default_config:
            if key not in config:
                config[key] = default_config[key]
        
        # 检查是否需要从CSV/Excel读取账号
        accounts_file = config.get("ACCOUNTS_FILE")
        if accounts_file:
            logger.info(f"检测到账号文件配置: {accounts_file}")
            accounts_from_file = load_accounts_from_csv_excel(accounts_file)
            if accounts_from_file:
                config["USER_ACCOUNTS"] = accounts_from_file
                logger.info(f"已从文件加载 {len(accounts_from_file)} 个账号")
        
        logger.info(f"已加载配置文件: {config_file}")
        return config
        
    except Exception as e:
        logger.error(f"加载配置文件失败: {e}，使用默认配置")
        return default_config


# ============================================================================
# 辅助函数
# ============================================================================

def format_time(seconds: float) -> str:
    """将秒数格式化为可读的时间字符串"""
    if seconds < 60:
        return f"{seconds:.1f}秒"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}分钟"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}小时"


def setup_user_environment(user_key: str) -> None:
    """
    设置指定用户的环境变量
    
    Args:
        user_key: 用户标识
    """
    # 修改 wq_login 模块的全局变量
    wq_login.USER_KEY = user_key
    
    # 更新相关的路径配置
    wq_login.COOKIE_FILE_PATH = os.path.join(
        wq_login.COOKIES_FOLDER_PATH,
        f"{user_key}_session_cookie.json"
    )
    
    logger.info(f"环境已切换到用户: {user_key}")


def check_step_prerequisites(step_name: str, user_key: str) -> Tuple[bool, str]:
    """
    检查步骤的前置条件是否满足
    
    Args:
        step_name: 步骤名称 ('step1_fetch', 'step2_package', 'step3_import')
        user_key: 用户标识
        
    Returns:
        (is_ready, message)
    """
    if step_name == "step1_fetch":
        # 步骤1：检查凭据文件是否存在
        credentials_file = Path.home() / "secrets" / f"{user_key}_platform-brain.json"
        if not credentials_file.exists():
            return False, f"凭据文件不存在: {credentials_file}"
        return True, "前置条件满足"
    
    elif step_name == "step2_package":
        # 步骤2：检查原始数据目录是否存在
        primeval_dir = wq_login.WQ_DATA_ROOT / user_key / "primeval_data"
        if not primeval_dir.exists():
            return False, f"原始数据目录不存在: {primeval_dir}"
        
        # 检查是否有数据文件
        regular_dir = primeval_dir / "all_regular_alphas"
        super_dir = primeval_dir / "all_super_alphas"
        
        has_files = False
        if regular_dir.exists() and list(regular_dir.glob("*.json")):
            has_files = True
        if super_dir.exists() and list(super_dir.glob("*.json")):
            has_files = True
        
        if not has_files:
            return False, f"原始数据目录为空: {primeval_dir}"
        
        return True, "前置条件满足"
    
    elif step_name == "step3_import":
        # 步骤3：检查最终数据目录是否存在
        final_dir = wq_login.WQ_DATA_ROOT / user_key / "final_data"
        if not final_dir.exists():
            return False, f"最终数据目录不存在: {final_dir}"
        
        # 检查是否有数据文件
        has_files = False
        for item in final_dir.iterdir():
            if item.is_dir() and list(item.rglob("*.json")):
                has_files = True
                break
        
        if not has_files:
            return False, f"最终数据目录为空: {final_dir}"
        
        return True, "前置条件满足"
    
    return False, f"未知步骤: {step_name}"


def execute_step1_fetch(user_key: str) -> Dict:
    """
    执行步骤1：获取 Alpha 数据
    
    Args:
        user_key: 用户标识
        
    Returns:
        执行结果字典
    """
    logger.info("="*80)
    logger.info(f"步骤1：获取 Alpha 数据 (用户: {user_key})")
    logger.info("="*80)
    
    result = {
        "step": "step1_fetch",
        "user_key": user_key,
        "success": False,
        "start_time": time.time(),
        "end_time": None,
        "duration": None,
        "error": None,
        "details": {}
    }
    
    try:
        # 检查前置条件
        is_ready, message = check_step_prerequisites("step1_fetch", user_key)
        if not is_ready:
            raise Exception(f"前置条件检查失败: {message}")
        
        # 导入并执行步骤1的主函数
        # 注意：需要重新导入以获取更新后的 USER_KEY
        import importlib
        import sys
        
        # 移除旧的模块缓存
        modules_to_reload = [
            '1_fetch_all_alpha_main_async',
            'fetch_all_alphas',
            'session_manager',
            'session_proxy'
        ]
        for module in modules_to_reload:
            if module in sys.modules:
                del sys.modules[module]
        
        # 重新导入
        step1_module = __import__('1_fetch_all_alpha_main_async')
        
        # 执行主函数
        step1_module.main()
        
        result["success"] = True
        logger.info(f"步骤1完成 (用户: {user_key})")
        
    except Exception as e:
        result["success"] = False
        result["error"] = str(e)
        logger.error(f"步骤1失败 (用户: {user_key}): {e}")
        logger.debug(traceback.format_exc())
    
    finally:
        result["end_time"] = time.time()
        result["duration"] = result["end_time"] - result["start_time"]
    
    return result


def execute_step2_package(user_key: str) -> Dict:
    """
    执行步骤2：数据打包和分解
    
    Args:
        user_key: 用户标识
        
    Returns:
        执行结果字典
    """
    logger.info("="*80)
    logger.info(f"步骤2：数据打包和分解 (用户: {user_key})")
    logger.info("="*80)
    
    result = {
        "step": "step2_package",
        "user_key": user_key,
        "success": False,
        "start_time": time.time(),
        "end_time": None,
        "duration": None,
        "error": None,
        "details": {}
    }
    
    try:
        # 检查前置条件
        is_ready, message = check_step_prerequisites("step2_package", user_key)
        if not is_ready:
            raise Exception(f"前置条件检查失败: {message}")
        
        # 导入步骤2模块
        import importlib
        import sys
        
        # 移除旧的模块缓存
        if '2_alpha_files_packaging_and_decomposition' in sys.modules:
            del sys.modules['2_alpha_files_packaging_and_decomposition']
        
        # 重新导入
        step2_module = __import__('2_alpha_files_packaging_and_decomposition')
        
        # 准备参数
        primeval_root = wq_login.WQ_DATA_ROOT / user_key / "primeval_data"
        final_root = wq_login.WQ_DATA_ROOT / user_key / "final_data"
        final_root.mkdir(exist_ok=True, parents=True)
        
        # 加载配置
        config_file = Path(__file__).parent / "async_config.json"
        mode = 'incremental'
        workers = None
        
        if config_file.exists():
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                mode = config.get('PACKAGING_MODE', 'incremental')
                workers = config.get('PACKAGING_WORKERS', None)
            except Exception:
                pass
        
        # 计算工作线程数
        if workers is None:
            try:
                cpu_cores = len(os.sched_getaffinity(0))
                workers = max(8, int(cpu_cores * 2))
            except AttributeError:
                cpu_cores = os.cpu_count() or 8
                workers = max(8, int(cpu_cores * 2))
        
        # 文件名正则
        import re
        re_name = re.compile(
            r'^(?P<id>[A-Za-z0-9@._]+)'
            r'_(?P<start>\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}\.000Z)_to_'
            r'(?P<end>\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}\.000Z)_'
            r'all_(?P<kind>regular|super)_alphas\.json$'
        )
        
        # 执行打包
        step2_module.group_and_split_by_date(
            primeval_root,
            final_root,
            re_name,
            workers,
            mode
        )
        
        result["success"] = True
        logger.info(f"步骤2完成 (用户: {user_key})")
        
    except Exception as e:
        result["success"] = False
        result["error"] = str(e)
        logger.error(f"步骤2失败 (用户: {user_key}): {e}")
        logger.debug(traceback.format_exc())
    
    finally:
        result["end_time"] = time.time()
        result["duration"] = result["end_time"] - result["start_time"]
    
    return result


def execute_step3_import(user_key: str) -> Dict:
    """
    执行步骤3：导入到 MongoDB
    
    Args:
        user_key: 用户标识
        
    Returns:
        执行结果字典
    """
    logger.info("="*80)
    logger.info(f"步骤3：导入到 MongoDB (用户: {user_key})")
    logger.info("="*80)
    
    result = {
        "step": "step3_import",
        "user_key": user_key,
        "success": False,
        "start_time": time.time(),
        "end_time": None,
        "duration": None,
        "error": None,
        "details": {}
    }
    
    try:
        # 检查前置条件
        is_ready, message = check_step_prerequisites("step3_import", user_key)
        if not is_ready:
            raise Exception(f"前置条件检查失败: {message}")
        
        # 导入步骤3模块
        import importlib
        import sys
        
        # 移除旧的模块缓存
        if '3_mongo_import_v4' in sys.modules:
            del sys.modules['3_mongo_import_v4']
        
        # 重新导入
        step3_module = __import__('3_mongo_import_v4')
        
        # 执行主函数
        exit_code = step3_module.main()
        
        if exit_code == 0:
            result["success"] = True
            logger.info(f"步骤3完成 (用户: {user_key})")
        else:
            raise Exception(f"MongoDB导入返回非零退出码: {exit_code}")
        
    except Exception as e:
        result["success"] = False
        result["error"] = str(e)
        logger.error(f"步骤3失败 (用户: {user_key}): {e}")
        logger.debug(traceback.format_exc())
    
    finally:
        result["end_time"] = time.time()
        result["duration"] = result["end_time"] - result["start_time"]
    
    return result


def process_user(user_account: Dict, execute_steps: Dict, continue_on_error: bool) -> Dict:
    """
    处理单个用户账号（执行完整的3个步骤）
    
    Args:
        user_account: 用户账号信息字典
        execute_steps: 执行步骤配置
        continue_on_error: 遇到错误是否继续
        
    Returns:
        处理结果字典
    """
    user_key = user_account["user_key"]
    description = user_account.get("description", "")
    
    logger.info("\n" + "="*80)
    logger.info(f"开始处理用户: {user_key}" + (f" ({description})" if description else ""))
    logger.info("="*80 + "\n")
    
    user_start_time = time.time()
    
    # 用户处理结果
    user_result = {
        "user_key": user_key,
        "description": description,
        "success": False,
        "start_time": user_start_time,
        "end_time": None,
        "duration": None,
        "steps": {},
        "error": None
    }
    
    try:
        # 设置用户环境
        setup_user_environment(user_key)
        
        # 步骤1：获取数据
        if execute_steps.get("step1_fetch", True):
            step1_result = execute_step1_fetch(user_key)
            user_result["steps"]["step1_fetch"] = step1_result
            
            if not step1_result["success"]:
                if not continue_on_error:
                    raise Exception(f"步骤1失败: {step1_result['error']}")
                else:
                    logger.warning(f"步骤1失败，但继续执行后续步骤")
        
        # 步骤2：打包分解
        if execute_steps.get("step2_package", True):
            step2_result = execute_step2_package(user_key)
            user_result["steps"]["step2_package"] = step2_result
            
            if not step2_result["success"]:
                if not continue_on_error:
                    raise Exception(f"步骤2失败: {step2_result['error']}")
                else:
                    logger.warning(f"步骤2失败，但继续执行后续步骤")
        
        # 步骤3：导入MongoDB
        if execute_steps.get("step3_import", True):
            step3_result = execute_step3_import(user_key)
            user_result["steps"]["step3_import"] = step3_result
            
            if not step3_result["success"]:
                if not continue_on_error:
                    raise Exception(f"步骤3失败: {step3_result['error']}")
                else:
                    logger.warning(f"步骤3失败")
            
            # 步骤3成功后，获取最近更新的alpha JSON
            if step3_result["success"] and FETCH_UPDATED_ALPHAS_AVAILABLE:
                try:
                    logger.info(f"\n{'='*80}")
                    logger.info(f"获取用户 {user_key} 最近更新的Alpha JSON数据")
                    logger.info(f"{'='*80}")
                    
                    # 加载MongoDB配置
                    mongo_config = None
                    try:
                        import json
                        config_path = Path("mongo_config.json")
                        if config_path.exists():
                            with open(config_path, 'r', encoding='utf-8') as f:
                                config_data = json.load(f)
                            # 过滤注释字段
                            mongo_config = {}
                            for key, value in config_data.items():
                                if not key.startswith('_') and not key.startswith('//'):
                                    mongo_config[key] = value
                    except Exception as e:
                        logger.debug(f"加载mongo_config.json失败，使用默认配置: {e}")
                    
                    # 调用获取更新的alpha函数（output_dir=None会使用wq_logger的日志目录）
                    fetch_result = fetch_updated_alphas_for_user(
                        user_key=user_key,
                        limit=5,
                        mongo_config=mongo_config,
                        output_dir=None  # None表示使用wq_logger的日志目录
                    )
                    
                    if fetch_result["success"]:
                        if fetch_result["saved_count"] > 0:
                            logger.info(f"成功保存 {fetch_result['saved_count']} 个更新的alpha JSON文件到: {fetch_result['output_dir']}")
                        else:
                            logger.info("未找到需要保存的更新alpha")
                    else:
                        logger.warning(f"获取更新的alpha失败: {fetch_result.get('error', '未知错误')}")
                    
                except Exception as e:
                    # 获取更新的alpha失败不影响主流程
                    logger.warning(f"获取更新的alpha时出错（不影响主流程）: {e}")
                    logger.debug(traceback.format_exc())
        
        # 检查所有步骤是否成功
        all_success = all(
            result["success"]
            for result in user_result["steps"].values()
        )
        
        user_result["success"] = all_success
        
        if all_success:
            logger.info(f"用户 {user_key} 处理完成 - 所有步骤成功")
        else:
            logger.warning(f"用户 {user_key} 处理完成 - 部分步骤失败")
    
    except Exception as e:
        user_result["success"] = False
        user_result["error"] = str(e)
        logger.error(f"用户 {user_key} 处理失败: {e}")
        logger.debug(traceback.format_exc())
    
    finally:
        user_result["end_time"] = time.time()
        user_result["duration"] = user_result["end_time"] - user_start_time
    
    logger.info(f"\n用户 {user_key} 总耗时: {format_time(user_result['duration'])}\n")
    
    return user_result


def print_summary(all_results: List[Dict], total_duration: float) -> None:
    """
    打印所有用户的处理摘要
    
    Args:
        all_results: 所有用户的处理结果列表
        total_duration: 总耗时（秒）
    """
    print("\n" + "="*80)
    print(" "*30 + "执行摘要")
    print("="*80)
    
    print(f"\n总耗时: {format_time(total_duration)}")
    print(f"处理用户数: {len(all_results)}")
    
    # 统计成功和失败的用户
    success_users = [r for r in all_results if r["success"]]
    failed_users = [r for r in all_results if not r["success"]]
    
    print(f"成功: {len(success_users)} 个用户")
    print(f"失败: {len(failed_users)} 个用户")
    
    # 详细信息
    print("\n" + "-"*80)
    print("详细信息:")
    print("-"*80)
    
    for result in all_results:
        user_key = result["user_key"]
        description = result.get("description", "")
        status = "[成功]" if result["success"] else "[失败]"
        duration = format_time(result["duration"])
        
        print(f"\n用户: {user_key}" + (f" ({description})" if description else ""))
        print(f"状态: {status}")
        print(f"耗时: {duration}")
        
        if result["error"]:
            print(f"错误: {result['error']}")
        
        # 步骤详情
        if result["steps"]:
            print("步骤详情:")
            for step_name, step_result in result["steps"].items():
                step_status = "[OK]" if step_result["success"] else "[FAIL]"
                step_duration = format_time(step_result["duration"])
                step_label = {
                    "step1_fetch": "步骤1-获取数据",
                    "step2_package": "步骤2-打包分解",
                    "step3_import": "步骤3-导入MongoDB"
                }.get(step_name, step_name)
                
                print(f"  {step_status} {step_label}: {step_duration}")
                if not step_result["success"] and step_result["error"]:
                    print(f"     错误: {step_result['error']}")
    
    print("\n" + "="*80)
    
    # 如果有失败的用户，单独列出
    if failed_users:
        print("\n[警告] 失败的用户:")
        for result in failed_users:
            user_key = result["user_key"]
            error = result.get("error", "未知错误")
            print(f"  - {user_key}: {error}")
    
    print("\n")


def save_results_to_json(all_results: List[Dict], output_file: str = "run_all_users_results.json") -> None:
    """
    将结果保存到JSON文件
    
    Args:
        all_results: 所有用户的处理结果列表
        output_file: 输出文件名
    """
    try:
        output_path = Path(__file__).parent / output_file
        
        # 准备输出数据
        output_data = {
            "timestamp": datetime.now().isoformat(),
            "total_users": len(all_results),
            "success_count": sum(1 for r in all_results if r["success"]),
            "failed_count": sum(1 for r in all_results if not r["success"]),
            "results": all_results
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"结果已保存到: {output_path}")
        
    except Exception as e:
        logger.warning(f"保存结果文件失败: {e}")


# ============================================================================
# 主函数
# ============================================================================

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="多账号批处理脚本 - WorldQuant Alpha 数据获取、打包和导入",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python run_all_users_v2.py                              # 使用默认配置文件
  python run_all_users_v2.py --config my_config.json     # 使用自定义配置文件
  python run_all_users_v2.py --user zzz                  # 只处理指定用户
  python run_all_users_v2.py --skip-step1                # 跳过步骤1
        """
    )
    
    parser.add_argument(
        '--config',
        type=str,
        default='user_accounts_config.json',
        help='配置文件路径 (默认: user_accounts_config.json)'
    )
    
    parser.add_argument(
        '--user',
        type=str,
        help='只处理指定的用户 (覆盖配置文件中的账号列表)'
    )
    
    parser.add_argument(
        '--skip-step1',
        action='store_true',
        help='跳过步骤1（获取数据）'
    )
    
    parser.add_argument(
        '--skip-step2',
        action='store_true',
        help='跳过步骤2（打包分解）'
    )
    
    parser.add_argument(
        '--skip-step3',
        action='store_true',
        help='跳过步骤3（导入MongoDB）'
    )
    
    parser.add_argument(
        '--stop-on-error',
        action='store_true',
        help='遇到错误立即停止（覆盖配置文件设置）'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default='run_all_users_results.json',
        help='结果输出文件名 (默认: run_all_users_results.json)'
    )
    
    return parser.parse_args()


def main():
    """主函数"""
    print("\n" + "="*80)
    print(" "*15 + "多账号批处理脚本 v2.0")
    print(" "*10 + "WorldQuant Alpha 数据获取、打包和导入")
    print("="*80 + "\n")
    
    # 解析命令行参数
    args = parse_arguments()
    
    # 加载配置
    config = load_user_config(args.config)
    
    # 处理命令行覆盖
    execute_steps = config["EXECUTE_STEPS"].copy()
    if args.skip_step1:
        execute_steps["step1_fetch"] = False
    if args.skip_step2:
        execute_steps["step2_package"] = False
    if args.skip_step3:
        execute_steps["step3_import"] = False
    
    continue_on_error = not args.stop_on_error if args.stop_on_error else config["CONTINUE_ON_ERROR"]
    
    # 确定要处理的账号列表
    if args.user:
        # 只处理指定用户
        user_accounts = [{"user_key": args.user, "description": "命令行指定", "enabled": True}]
    else:
        # 使用配置文件中的账号列表（只处理 enabled=true 的账号）
        user_accounts = [
            acc for acc in config["USER_ACCOUNTS"]
            if acc.get("enabled", True)
        ]
    
    if not user_accounts:
        logger.error("没有要处理的账号，请检查配置")
        return 1
    
    global_start_time = time.time()
    
    # 显示配置
    logger.info("配置信息:")
    logger.info(f"配置文件: {args.config}")
    logger.info(f"待处理用户数: {len(user_accounts)}")
    logger.info(f"执行步骤: {[k for k, v in execute_steps.items() if v]}")
    logger.info(f"失败处理策略: {'继续' if continue_on_error else '立即停止'}")
    logger.info(f"输出文件: {args.output}")
    
    # 显示用户列表
    logger.info("\n用户列表:")
    for i, account in enumerate(user_accounts, 1):
        user_key = account["user_key"]
        description = account.get("description", "")
        logger.info(f"  {i}. {user_key}" + (f" - {description}" if description else ""))
    
    print("\n")
    
    # 处理所有用户
    all_results = []
    
    for i, account in enumerate(user_accounts, 1):
        logger.info(f"\n{'='*80}")
        logger.info(f"进度: {i}/{len(user_accounts)}")
        logger.info(f"{'='*80}")
        
        user_result = process_user(account, execute_steps, continue_on_error)
        all_results.append(user_result)
        
        # 如果失败且不继续，则停止
        if not user_result["success"] and not continue_on_error:
            logger.error("遇到错误，停止处理")
            break
    
    # 计算总耗时
    total_duration = time.time() - global_start_time
    
    # 打印摘要
    print_summary(all_results, total_duration)
    
    # 保存结果到JSON
    save_results_to_json(all_results, args.output)
    
    # 返回退出码
    all_success = all(r["success"] for r in all_results)
    return 0 if all_success else 1


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.warning("\n\n程序被用户中断")
        sys.exit(130)
    except Exception as e:
        logger.error(f"\n\n致命错误: {e}", exc_info=True)
        sys.exit(1)

