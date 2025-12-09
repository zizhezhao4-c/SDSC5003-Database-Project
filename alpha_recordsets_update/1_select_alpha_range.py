#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
1_select_alpha_range.py

步骤1：确定需要爬取的 Alpha 范围
- 连接 MongoDB(27018) 的 wqzzz.regular_alphas
- 根据既定规则筛选
- 输出：filtered_alpha_ids.txt 与 filtered_alphas_details.json
"""

import json
from pathlib import Path
from typing import List, Dict

from pymongo import MongoClient

from datetime import datetime

import wq_login

# 使用基于脚本所在目录的绝对路径，避免因运行目录不同而找不到配置文件
SCRIPT_DIR = Path(__file__).parent
CONFIG_PATH = SCRIPT_DIR / "1_select_alpha_config.json"
# OUTPUT_DIR 也建议基于 SCRIPT_DIR，或者在配置中写绝对路径。
# 这里保持相对路径逻辑，但让它相对于 SCRIPT_DIR (如果配置里是相对路径)
# 但注意：OUTPUT_DIR 在下面代码中是直接使用的，如果是 Path("xxx")，它还是会基于 CWD。
# 我们稍后在 main 函数或 save_outputs 中处理 OUTPUT_DIR 的绝对化。
DEFAULT_OUTPUT_DIR_NAME = "alpha_ids_to_fetch_list"


def load_config() -> dict:
    cfg = {}
    try:
        if not CONFIG_PATH.exists():
            print(f"警告: 配置文件未找到: {CONFIG_PATH}")
            return cfg
            
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            user_cfg = json.load(f)
            cfg.update(user_cfg or {})
    except Exception as e:
        print(f"读取配置文件出错: {e}")
    return cfg


def connect_to_mongo(cfg: dict):
    host = cfg.get("SOURCE_DB_HOST", "localhost")
    port = cfg.get("SOURCE_DB_PORT", 27017)
    try:
        client = MongoClient(f"mongodb://{host}:{port}/", serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        print(f"成功连接到 MongoDB ({host}:{port})")
        return client
    except Exception as e:
        print(f"连接失败: {e}")
        return None


def build_filter_query(cfg: dict):
    # 读取配置中的筛选条件
    enable_region_filter = cfg.get("ENABLE_REGION_FILTER", False)
    region_input = cfg.get("REGION_INPUT")
    filter_universe = cfg.get("FILTER_UNIVERSE")
    filter_delay = cfg.get("FILTER_DELAY")

    # 读取基础指标配置 (带默认值)
    min_fitness = cfg.get("MIN_FITNESS", 0.8)
    min_sharpe = cfg.get("MIN_SHARPE", 1)
    min_long_count = cfg.get("MIN_LONG_COUNT", 50)
    min_short_count = cfg.get("MIN_SHORT_COUNT", 50)
    min_pnl = cfg.get("MIN_PNL", 1000000)
    max_turnover = cfg.get("MAX_TURNOVER", 0.7)
    alpha_type = cfg.get("ALPHA_TYPE", "REGULAR")
    exclude_status = cfg.get("EXCLUDE_STATUS", "ACTIVE")

    query = {
        "type": alpha_type,
        "is.fitness": {"$gt": min_fitness},
        "is.sharpe": {"$gt": min_sharpe},
        "is.longCount": {"$gt": min_long_count},
        "is.shortCount": {"$gt": min_short_count},
        "is.pnl": {"$gt": min_pnl},
        "is.turnover": {"$lt": max_turnover},
    }

    if exclude_status:
        query["status"] = {"$ne": exclude_status}
    
    # 动态添加筛选条件
    
    # 如果开启了 Region 筛选 (ENABLE_REGION_FILTER=true)，
    # 则同时应用 Region, Universe, Delay 筛选。
    # 如果为 false，则视为"全量/无限制"模式，这三项都不筛选。
    if enable_region_filter:
        # Region (settings.region)
        if region_input:
            query["settings.region"] = region_input
        
        # Universe (settings.universe)
        if filter_universe:
            query["settings.universe"] = filter_universe
        
        # Delay (settings.delay)
        if filter_delay is not None and filter_delay != -1:
            query["settings.delay"] = filter_delay

    print("构建的筛选条件:")
    print(json.dumps(query, indent=2, ensure_ascii=False))
    
    return query


def filter_alphas_with_check_condition(client, base_query, db_name="regular_alphas", collection_user="alphas") -> List[Dict]:
    db = client[db_name]
    collection = db[collection_user]

    print(f"正在从数据库 {db_name}.{collection_user} 筛选符合条件的 alpha ...")

    query_with_sharpe_checks = base_query.copy()
    query_with_sharpe_checks["$or"] = [
        {"is.checks": {"$elemMatch": {"name": "LOW_2Y_SHARPE", "value": {"$gt": 1}}}},
        {"is.checks": {"$elemMatch": {"name": "IS_LADDER_SHARPE", "value": {"$gt": 1}}}},
    ]

    print("查询包含 LOW_2Y_SHARPE > 1 或 IS_LADDER_SHARPE > 1 的 alpha ...")
    alphas_with_sharpe_checks = list(
        collection.find(
            query_with_sharpe_checks,
            {"id": 1, "author": 1, "dateCreated": 1, "is": 1, "settings": 1, "_id": 0},
        )
    )

    query_without_sharpe_checks = base_query.copy()
    query_without_sharpe_checks["$and"] = [
        {"is.checks": {"$not": {"$elemMatch": {"name": "LOW_2Y_SHARPE"}}}},
        {"is.checks": {"$not": {"$elemMatch": {"name": "IS_LADDER_SHARPE"}}}},
    ]

    print("查询不包含 LOW_2Y_SHARPE 和 IS_LADDER_SHARPE 字段的 alpha ...")
    alphas_without_sharpe_checks = list(
        collection.find(
            query_without_sharpe_checks,
            {"id": 1, "author": 1, "dateCreated": 1, "is": 1, "settings": 1, "_id": 0},
        )
    )

    all_alphas = alphas_with_sharpe_checks + alphas_without_sharpe_checks

    print(f"包含 Sharpe 检查的: {len(alphas_with_sharpe_checks)} 个")
    print(f"不包含 Sharpe 检查的: {len(alphas_without_sharpe_checks)} 个")
    print(f"总计: {len(all_alphas)} 个")

    return all_alphas


def save_outputs(alphas: List[Dict], cfg: dict):
    # 确定输出目录：优先使用配置，否则使用默认，并确保是基于脚本目录的绝对路径
    out_dir_name = cfg.get("OUTPUT_DIR", DEFAULT_OUTPUT_DIR_NAME)
    # 如果是绝对路径则直接用，否则拼接到脚本目录下
    out_path = Path(out_dir_name)
    if not out_path.is_absolute():
        out_path = SCRIPT_DIR / out_dir_name
        
    out_path.mkdir(parents=True, exist_ok=True)

    # 获取 USER_KEY，用于文件名
    user_key = wq_login.USER_KEY

    # 生成带时间戳的文件名: alpha_ids_USER_KEY_YYYYMMDD_HHMMSS.json
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    ids_json_file = out_path / f"alpha_ids_{user_key}_{timestamp}.json"
    
    # 提取 ID 列表
    id_list = [a.get("id") for a in alphas if a.get("id")]
    
    # 写入 JSON 文件
    with open(ids_json_file, "w", encoding="utf-8") as jf:
        json.dump(id_list, jf, ensure_ascii=False, indent=2)

    # 禁用详情文件输出
    # details_file = out_path / f"alpha_details_{timestamp}.json"
    # with open(details_file, "w", encoding="utf-8") as f:
    #     json.dump(alphas, f, ensure_ascii=False, indent=2)

    print(f"\nAlpha IDs 已保存到: {ids_json_file}")
    # print(f"Alpha详细信息已保存到: {details_file}")
    print(f"\n共筛选出 {len(id_list)} 个 Alpha ID")
    
    return ids_json_file


def display_summary(alphas: List[Dict]):
    print("\n" + "=" * 60)
    print("筛选结果摘要")
    print("=" * 60)
    print(f"总数量: {len(alphas)}")

    authors = {}
    for a in alphas:
        author = a.get("author", "Unknown")
        authors[author] = authors.get(author, 0) + 1

    print("\n按作者分布:")
    for author, count in sorted(authors.items(), key=lambda x: x[1], reverse=True):
        print(f"  {author}: {count} 个")

    print("\n示例前10条:")
    for i, a in enumerate(alphas[:10]):
        print(f"  {i+1}. {a.get('id')} (作者: {a.get('author', 'N/A')})")


def main():
    cfg = load_config()

    client = connect_to_mongo(cfg)
    if not client:
        return

    base_query = build_filter_query(cfg)
    
    # 从配置中读取数据库和集合名称
    db_name = cfg.get("SOURCE_DB_NAME", "regular_alphas")
    
    # 直接使用 wq_login 中的 USER_KEY 作为集合名称
    collection_user = wq_login.USER_KEY
    print(f"使用当前配置的用户Key作为集合名称: {collection_user}")

    alphas = filter_alphas_with_check_condition(client, base_query, db_name, collection_user)

    if len(alphas) == 0:
        print("\n警告: 未筛选到符合条件的Alpha，请检查筛选条件或数据库数据。")
        return

    output_file = save_outputs(alphas, cfg)
    display_summary(alphas)

    print("\n" + "="*60)
    print("步骤1完成：已生成待抓取的 Alpha 范围列表")
    print("="*60)
    print(f"\n输出文件: {output_file}")
    print(f"下一步: 运行步骤2将自动读取最新的Alpha ID列表")


if __name__ == "__main__":
    main()