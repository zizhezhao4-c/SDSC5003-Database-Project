#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
获取最近更新的Alpha JSON数据

功能：
1. 从MongoDB查询最近更新的alpha_id（最多5个）
2. 从WorldQuant API获取这些alpha的完整JSON数据
3. 保存到根目录
"""

import json
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from pymongo import MongoClient
import requests

# 导入项目模块
try:
    import wq_logger
    logger = wq_logger.get_logger(__name__)
except ImportError:
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%SZ'
    )
    logger = logging.getLogger(__name__)

# 导入登录模块
try:
    import wq_login
    from session_manager import SessionManager
except ImportError as e:
    logger.error(f"导入模块失败: {e}")
    sys.exit(1)


def load_mongo_config(config_file: str = "mongo_config.json") -> Dict:
    """加载MongoDB配置"""
    config_path = Path(config_file)
    if not config_path.exists():
        logger.error(f"配置文件不存在: {config_file}")
        sys.exit(1)
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config_data = json.load(f)
    
    # 过滤注释字段
    config = {}
    for key, value in config_data.items():
        if not key.startswith('_') and not key.startswith('//'):
            config[key] = value
    
    return config


def get_recently_updated_alphas(
    db_host: str,
    db_port: int,
    db_name: str,
    collection_name: str,
    limit: int = 5,
    hours: int = 24
) -> List[str]:
    """
    从MongoDB获取最近更新的alpha_id列表
    
    策略：
    1. 优先按_id排序（MongoDB的_id包含时间戳，最近插入/更新的文档_id更大）
    2. 如果有dateModified字段，也可以按dateModified排序
    
    Args:
        db_host: MongoDB主机
        db_port: MongoDB端口
        db_name: 数据库名称
        collection_name: 集合名称
        limit: 返回的最大数量
        hours: 查询最近N小时内的更新（用于过滤）
    
    Returns:
        alpha_id列表
    """
    try:
        client = MongoClient(f"mongodb://{db_host}:{db_port}/")
        db = client[db_name]
        collection = db[collection_name]
        
        logger.info(f"查询最近更新的alpha（最多{limit}个）...")
        
        # 方法1: 直接按_id排序获取最新的文档（最可靠）
        # MongoDB的_id是ObjectId，包含时间戳，最近插入/更新的文档_id更大
        cursor = collection.find({}).sort("_id", -1).limit(limit * 2)  # 多取一些以防有无效的
        alphas = list(cursor)
        
        # 提取有效的alpha_id
        alpha_ids = []
        for alpha in alphas:
            alpha_id = alpha.get("id")
            if alpha_id and alpha_id not in alpha_ids:
                alpha_ids.append(alpha_id)
                if len(alpha_ids) >= limit:
                    break
        
        # 如果还是不足，尝试按dateModified查询
        if len(alpha_ids) < limit:
            logger.info(f"按_id找到{len(alpha_ids)}个，尝试按dateModified查询...")
            time_threshold = datetime.utcnow() - timedelta(hours=hours)
            cursor = collection.find(
                {"dateModified": {"$gte": time_threshold.isoformat() + "Z"}}
            ).sort("dateModified", -1).limit(limit)
            
            additional_alphas = list(cursor)
            for alpha in additional_alphas:
                alpha_id = alpha.get("id")
                if alpha_id and alpha_id not in alpha_ids:
                    alpha_ids.append(alpha_id)
                    if len(alpha_ids) >= limit:
                        break
        
        client.close()
        
        logger.info(f"找到 {len(alpha_ids)} 个alpha_id: {alpha_ids}")
        return alpha_ids[:limit]
        
    except Exception as e:
        logger.error(f"查询MongoDB失败: {e}", exc_info=True)
        return []


def fetch_alpha_from_api(session: requests.Session, alpha_id: str) -> Optional[Dict]:
    """
    从WorldQuant API获取单个alpha的完整数据
    
    Args:
        session: 已认证的requests Session
        alpha_id: Alpha ID
    
    Returns:
        Alpha的JSON数据，失败返回None
    """
    # 方法1: 尝试直接获取单个alpha（如果API支持）
    url = f"https://api.worldquantbrain.com/users/self/alphas/{alpha_id}"
    
    try:
        response = session.get(url, timeout=30)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            # API可能不支持单个alpha端点，返回None让调用者尝试备用方法
            return None
        else:
            logger.warning(f"获取alpha {alpha_id} 失败: HTTP {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        logger.debug(f"直接获取alpha {alpha_id} 失败: {e}，尝试备用方法")
        return None


def fetch_alpha_from_list_api(session: requests.Session, alpha_id: str) -> Optional[Dict]:
    """
    从列表API查询特定alpha（备用方法）
    
    Args:
        session: 已认证的requests Session
        alpha_id: Alpha ID
    
    Returns:
        Alpha的JSON数据，失败返回None
    """
    # 通过列表API查询，使用id过滤
    url = "https://api.worldquantbrain.com/users/self/alphas"
    params = {
        "id": alpha_id,
        "limit": 1
    }
    
    try:
        response = session.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            results = data.get("results", [])
            if results:
                return results[0]
            else:
                logger.warning(f"Alpha {alpha_id} 在API中未找到")
                return None
        else:
            logger.warning(f"查询alpha {alpha_id} 失败: HTTP {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"请求alpha {alpha_id} 时出错: {e}")
        return None


def save_alpha_json(alpha_data: Dict, output_dir: Path, alpha_id: str):
    """
    保存alpha JSON到文件
    
    Args:
        alpha_data: Alpha的JSON数据
        output_dir: 输出目录
        alpha_id: Alpha ID
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    filename = f"{alpha_id}_alpha.json"
    filepath = output_dir / filename
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(alpha_data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"已保存: {filepath}")


def fetch_updated_alphas_for_user(
    user_key: str,
    limit: int = 5,
    mongo_config: Optional[Dict] = None,
    output_dir: Optional[Path] = None
) -> Dict:
    """
    为指定用户获取最近更新的Alpha JSON数据（独立函数，可被其他模块调用）
    
    Args:
        user_key: 用户标识
        limit: 获取的alpha数量（默认5）
        mongo_config: MongoDB配置字典（如果为None，则从文件加载）
        output_dir: 输出目录（如果为None，则使用当前工作目录）
    
    Returns:
        结果字典，包含：
        - success: 是否成功
        - alpha_ids: 找到的alpha_id列表
        - saved_count: 成功保存的数量
        - failed_count: 失败的数量
        - output_dir: 输出目录
        - error: 错误信息（如果有）
    """
    result = {
        "success": False,
        "alpha_ids": [],
        "saved_count": 0,
        "failed_count": 0,
        "output_dir": None,
        "error": None
    }
    
    try:
        # 加载MongoDB配置
        if mongo_config is None:
            mongo_config = load_mongo_config()
        
        # 获取配置
        db_host = mongo_config.get("db_host", "localhost")
        db_port = mongo_config.get("db_port_27017", 27017)
        db_name_regular = mongo_config.get("db_name_regular", "regular_alphas")
        regular_collection = mongo_config.get("regular_collection")
        
        if not regular_collection:
            regular_collection = user_key
        
        logger.info(f"获取用户 {user_key} 最近更新的 {limit} 个alpha...")
        logger.debug(f"MongoDB配置: {db_host}:{db_port}/{db_name_regular}/{regular_collection}")
        
        # 从MongoDB获取最近更新的alpha_id
        alpha_ids = get_recently_updated_alphas(
            db_host=db_host,
            db_port=db_port,
            db_name=db_name_regular,
            collection_name=regular_collection,
            limit=limit,
            hours=24
        )
        
        if not alpha_ids:
            logger.info(f"用户 {user_key} 未找到最近更新的alpha")
            result["success"] = True  # 没有找到不算失败
            return result
        
        result["alpha_ids"] = alpha_ids
        logger.info(f"找到 {len(alpha_ids)} 个最近更新的alpha: {alpha_ids}")
        
        # 初始化session
        from session_manager import SessionManager
        session_manager = SessionManager(user_key=user_key)
        session = session_manager.get_session()
        
        if not session:
            raise Exception("无法获取有效的session")
        
        # 输出目录：使用wq_logger获取日志目录，并添加日期子目录
        if output_dir is None:
            try:
                # 使用wq_logger的_determine_log_dir函数获取日志目录
                from wq_logger import _determine_log_dir
                log_dir = _determine_log_dir(user_key, "fetch_and_import_alphas")
                # 获取当前系统日期（格式：YYYY-MM-DD）
                current_date = datetime.now().strftime("%Y-%m-%d")
                # 在日志目录下创建updated_alphas/日期子目录
                output_dir = Path(log_dir) / "updated_alphas" / current_date
                output_dir.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                logger.warning(f"无法使用wq_logger获取日志目录: {e}，使用当前目录")
                current_date = datetime.now().strftime("%Y-%m-%d")
                output_dir = Path.cwd() / "updated_alphas" / current_date
                output_dir.mkdir(parents=True, exist_ok=True)
        else:
            output_dir = Path(output_dir)
            # 如果指定了output_dir，也添加日期子目录
            current_date = datetime.now().strftime("%Y-%m-%d")
            output_dir = output_dir / "updated_alphas" / current_date
            output_dir.mkdir(parents=True, exist_ok=True)
        
        result["output_dir"] = str(output_dir)
        
        # 获取并保存每个alpha的JSON
        for i, alpha_id in enumerate(alpha_ids, 1):
            logger.info(f"[{i}/{len(alpha_ids)}] 正在获取 alpha: {alpha_id}")
            
            # 尝试方法1: 直接获取
            alpha_data = fetch_alpha_from_api(session, alpha_id)
            
            # 如果失败，尝试方法2: 从列表API查询
            if not alpha_data:
                logger.debug(f"  尝试备用方法...")
                alpha_data = fetch_alpha_from_list_api(session, alpha_id)
            
            if alpha_data:
                save_alpha_json(alpha_data, output_dir, alpha_id)
                result["saved_count"] += 1
            else:
                logger.warning(f"  获取失败: {alpha_id}")
                result["failed_count"] += 1
        
        result["success"] = True
        logger.info(f"完成：成功保存 {result['saved_count']}/{len(alpha_ids)} 个alpha")
        
    except Exception as e:
        result["success"] = False
        result["error"] = str(e)
        logger.error(f"获取更新的alpha失败: {e}", exc_info=True)
    
    return result


def main():
    """主函数（命令行入口）"""
    # 解析命令行参数
    import argparse
    parser = argparse.ArgumentParser(description="获取最近更新的Alpha JSON数据")
    parser.add_argument('--user', type=str, help='用户key（覆盖默认值）')
    parser.add_argument('--limit', type=int, default=5, help='获取的alpha数量（默认5）')
    args = parser.parse_args()
    
    print("\n" + "="*80)
    print(" "*20 + "获取最近更新的Alpha JSON数据")
    print("="*80 + "\n")
    
    # 获取用户key
    user_key = args.user if args.user else wq_login.USER_KEY
    limit = args.limit
    
    # 调用独立函数
    result = fetch_updated_alphas_for_user(user_key=user_key, limit=limit)
    
    # 打印摘要
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"成功: {result['saved_count']}/{len(result['alpha_ids'])}")
    print(f"失败: {result['failed_count']}/{len(result['alpha_ids'])}")
    if result['output_dir']:
        print(f"输出目录: {result['output_dir']}")
    if result['error']:
        print(f"错误: {result['error']}")
    print("="*80 + "\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("\n\n程序被用户中断")
        sys.exit(130)
    except Exception as e:
        logger.error(f"\n\n致命错误: {e}", exc_info=True)
        sys.exit(1)

