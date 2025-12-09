#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Alpha 筛选、排序与可视化工具

功能：
1. 根据配置文件筛选 Alpha（支持 Sharpe、Fitness、Turnover 等条件）
2. 按 Sharpe 从高到低排序
3. 自动可视化 Sharpe 最高的 Alpha（需有 recordsets 数据）

配置文件：使用 alpha_recordsets_update/1_select_alpha_config.json
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Any

from pymongo import MongoClient

# 路径配置
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
CONFIG_PATH = PROJECT_ROOT / "alpha_recordsets_update" / "1_select_alpha_config.json"
VISUALIZER_DIR = PROJECT_ROOT / "alpha_visualizer"
OUTPUT_DIR = SCRIPT_DIR / "filter_results"

# 添加 visualizer 到路径
sys.path.insert(0, str(VISUALIZER_DIR))


def load_config() -> Dict[str, Any]:
    """加载筛选配置"""
    if not CONFIG_PATH.exists():
        print(f"错误: 配置文件不存在: {CONFIG_PATH}")
        sys.exit(1)
    
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    print(f"已加载配置: {CONFIG_PATH}")
    return config


def build_query(config: Dict[str, Any]) -> Dict:
    """根据配置构建 MongoDB 查询条件"""
    query = {}
    
    # 区域筛选
    if config.get("ENABLE_REGION_FILTER", False):
        region = config.get("REGION_INPUT")
        if region:
            query["settings.region"] = region
    
    # Universe 筛选
    universe = config.get("FILTER_UNIVERSE")
    if universe:
        query["settings.universe"] = universe
    
    # Delay 筛选
    delay = config.get("FILTER_DELAY", -1)
    if delay >= 0:
        query["settings.delay"] = delay
    
    # Alpha 类型
    alpha_type = config.get("ALPHA_TYPE")
    if alpha_type:
        query["type"] = alpha_type
    
    # 排除状态
    exclude_status = config.get("EXCLUDE_STATUS")
    if exclude_status:
        query["status"] = {"$ne": exclude_status}
    
    # 数值条件
    min_fitness = config.get("MIN_FITNESS", 0)
    if min_fitness > 0:
        query["is.fitness"] = {"$gt": min_fitness}
    
    min_sharpe = config.get("MIN_SHARPE", 0)
    if min_sharpe > 0:
        query["is.sharpe"] = {"$gt": min_sharpe}
    
    min_long_count = config.get("MIN_LONG_COUNT", 0)
    if min_long_count > 0:
        query["is.longCount"] = {"$gt": min_long_count}
    
    min_short_count = config.get("MIN_SHORT_COUNT", 0)
    if min_short_count > 0:
        query["is.shortCount"] = {"$gt": min_short_count}
    
    min_pnl = config.get("MIN_PNL", 0)
    if min_pnl > 0:
        query["is.pnl"] = {"$gt": min_pnl}
    
    max_turnover = config.get("MAX_TURNOVER", 1.0)
    if max_turnover < 1.0:
        query["is.turnover"] = {"$lt": max_turnover}
    
    return query


def filter_and_sort_alphas(config: Dict[str, Any]) -> List[Dict]:
    """筛选并按 Sharpe 排序"""
    # 连接数据库
    host = config.get("SOURCE_DB_HOST", "localhost")
    port = config.get("SOURCE_DB_PORT", 27017)
    db_name = config.get("SOURCE_DB_NAME", "regular_alphas")
    
    # 集合名使用 wq_login 中的 USER_KEY，这里默认 test
    collection_name = "test"
    
    client = MongoClient(f"mongodb://{host}:{port}/")
    db = client[db_name]
    collection = db[collection_name]
    
    # 构建查询
    query = build_query(config)
    
    print("\n" + "=" * 60)
    print("筛选条件:")
    print("=" * 60)
    print(json.dumps(query, indent=2, ensure_ascii=False, default=str))
    
    # 执行查询并排序
    pipeline = [
        {"$match": query},
        {"$sort": {"is.sharpe": -1}},
        {"$project": {
            "_id": 0,
            "id": 1,
            "author": 1,
            "status": 1,
            "type": 1,
            "is.sharpe": 1,
            "is.fitness": 1,
            "is.turnover": 1,
            "is.pnl": 1,
            "is.longCount": 1,
            "is.shortCount": 1,
            "settings.region": 1,
            "settings.universe": 1,
            "settings.delay": 1,
            "has_recordsets": {
                "$cond": [
                    {"$gt": [{"$size": {"$objectToArray": {"$ifNull": ["$recordsets", {}]}}}, 0]},
                    True,
                    False
                ]
            }
        }}
    ]
    
    results = list(collection.aggregate(pipeline))
    client.close()
    
    return results


def display_results(alphas: List[Dict], top_n: int = 30):
    """显示筛选结果"""
    print("\n" + "=" * 100)
    print(f"筛选结果: 共 {len(alphas)} 个 Alpha (按 Sharpe 从高到低)")
    print("=" * 100)
    
    if not alphas:
        print("未找到符合条件的 Alpha")
        return
    
    # 表头
    print(f"{'排名':<5}{'Alpha ID':<12}{'Sharpe':<8}{'Fitness':<8}{'Turnover':<10}{'PnL':<14}{'Long':<6}{'Short':<6}{'Region':<8}{'RS':<4}")
    print("-" * 100)
    
    display_count = min(top_n, len(alphas))
    for i, alpha in enumerate(alphas[:display_count], 1):
        is_data = alpha.get('is', {})
        settings = alpha.get('settings', {})
        
        alpha_id = alpha.get('id', 'N/A')
        sharpe = is_data.get('sharpe', 0) or 0
        fitness = is_data.get('fitness', 0) or 0
        turnover = is_data.get('turnover', 0) or 0
        pnl = is_data.get('pnl', 0) or 0
        long_count = is_data.get('longCount', 0) or 0
        short_count = is_data.get('shortCount', 0) or 0
        region = settings.get('region', 'N/A')
        has_rs = "✓" if alpha.get('has_recordsets') else ""
        
        print(f"{i:<5}{alpha_id:<12}{sharpe:<8.2f}{fitness:<8.2f}{turnover:<10.4f}{pnl:<14,.0f}{long_count:<6}{short_count:<6}{region:<8}{has_rs:<4}")
    
    if len(alphas) > display_count:
        print(f"... 还有 {len(alphas) - display_count} 个")
    
    print("-" * 100)
    
    # 统计有 recordsets 的数量
    with_rs = sum(1 for a in alphas if a.get('has_recordsets'))
    print(f"其中有 Recordsets 数据的: {with_rs} 个")


def save_results(alphas: List[Dict], config: Dict[str, Any]) -> str:
    """保存筛选结果"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # 构建文件名
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    min_sharpe = config.get("MIN_SHARPE", 0)
    region = config.get("REGION_INPUT", "ALL") if config.get("ENABLE_REGION_FILTER") else "ALL"
    
    filename = f"filtered_sharpe{min_sharpe}_region{region}_{timestamp}.json"
    filepath = OUTPUT_DIR / filename
    
    # 准备数据
    alpha_ids = [a['id'] for a in alphas]
    output_data = {
        "filter_time": datetime.now().isoformat(),
        "config_file": str(CONFIG_PATH),
        "total_count": len(alphas),
        "with_recordsets_count": sum(1 for a in alphas if a.get('has_recordsets')),
        "alpha_ids": alpha_ids,
        "top_10_details": alphas[:10]
    }
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"\n结果已保存: {filepath}")
    return str(filepath)


def visualize_top_alphas(alphas: List[Dict], top_n: int = 3, auto: bool = False):
    """可视化 Sharpe 最高的前 N 个有 recordsets 的 Alpha"""
    # 找到有 recordsets 的前 N 个 alpha
    top_alphas = []
    for i, alpha in enumerate(alphas, 1):
        if alpha.get('has_recordsets'):
            top_alphas.append((i, alpha))  # (排名, alpha)
            if len(top_alphas) >= top_n:
                break
    
    if not top_alphas:
        print("\n没有找到有 recordsets 数据的 Alpha，无法可视化")
        return
    
    print(f"\n有 Recordsets 的前 {len(top_alphas)} 个最高 Sharpe Alpha:")
    for rank, alpha in top_alphas:
        alpha_id = alpha['id']
        sharpe = alpha.get('is', {}).get('sharpe', 0)
        fitness = alpha.get('is', {}).get('fitness', 0)
        print(f"  #{rank}: {alpha_id} (Sharpe: {sharpe:.2f}, Fitness: {fitness:.2f})")
    
    if not auto:
        response = input(f"\n是否可视化这 {len(top_alphas)} 个 Alpha? (Y/n): ").strip().lower()
        if response in ('n', 'no'):
            return
    
    # 执行可视化
    try:
        from visualize_alpha import visualize_alpha
        
        output_subdir = SCRIPT_DIR / "visualizations"
        output_subdir.mkdir(parents=True, exist_ok=True)
        
        print(f"\n正在生成 {len(top_alphas)} 张可视化图表...")
        
        for rank, alpha in top_alphas:
            alpha_id = alpha['id']
            sharpe = alpha.get('is', {}).get('sharpe', 0)
            
            # 文件名前缀格式: rank{排名}_sharpe{值}
            filename_prefix = f"rank{rank:03d}_sharpe{sharpe:.2f}"
            
            print(f"\n[{rank}/{len(top_alphas)}] 正在可视化 {alpha_id}...")
            saved_path = visualize_alpha(
                alpha_id, 
                output_dir=str(output_subdir), 
                show=False,  # 批量生成时不显示窗口
                filename_prefix=filename_prefix
            )
            
            if saved_path:
                print(f"  已保存: {Path(saved_path).name}")
        
        print(f"\n可视化完成! 共生成 {len(top_alphas)} 张图表")
        print(f"保存目录: {output_subdir}")
        
    except ImportError as e:
        print(f"错误: 无法导入可视化模块: {e}")
    except Exception as e:
        print(f"可视化失败: {e}")


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Alpha 筛选、排序与可视化工具')
    parser.add_argument('--top', '-n', type=int, default=30, help='显示前 N 个结果 (默认 30)')
    parser.add_argument('--auto-visualize', '-v', action='store_true', help='自动可视化最高 Sharpe 的 Alpha')
    parser.add_argument('--no-save', action='store_true', help='不保存结果文件')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Alpha 筛选、排序与可视化工具")
    print("=" * 60)
    
    # 1. 加载配置
    config = load_config()
    
    # 2. 筛选并排序
    alphas = filter_and_sort_alphas(config)
    
    # 3. 显示结果
    display_results(alphas, top_n=args.top)
    
    if not alphas:
        return
    
    # 4. 保存结果
    if not args.no_save:
        save_results(alphas, config)
    
    # 5. 输出 Alpha ID 列表
    print("\nAlpha ID 列表 (按 Sharpe 从高到低):")
    alpha_ids = [a['id'] for a in alphas]
    if len(alpha_ids) <= 50:
        print(alpha_ids)
    else:
        print(alpha_ids[:50])
        print(f"... 共 {len(alpha_ids)} 个")
    
    # 6. 可视化前三名
    visualize_top_alphas(alphas, top_n=3, auto=args.auto_visualize)


if __name__ == "__main__":
    main()
