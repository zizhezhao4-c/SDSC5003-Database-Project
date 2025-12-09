#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Alpha Recordsets 可视化工具

输入 alpha_id，从 MongoDB 读取 recordsets 数据并生成可视化图表：
- PnL (累计收益)
- Sharpe (夏普比率)
- Turnover (换手率)
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List, Any

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pymongo import MongoClient
import pandas as pd

# 配置
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "regular_alphas"
COLLECTION_NAME = "test"

# 中文字体支持 (macOS)
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'PingFang SC', 'Heiti TC']
plt.rcParams['axes.unicode_minus'] = False


def get_alpha_recordsets(alpha_id: str) -> Optional[Dict[str, Any]]:
    """从 MongoDB 获取指定 alpha 的 recordsets 数据"""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    alpha = collection.find_one({'id': alpha_id})
    client.close()
    
    if not alpha:
        return None
    return alpha.get('recordsets', {})


def recordset_to_dataframe(recordset: Dict, date_col: str = 'date') -> pd.DataFrame:
    """将 recordset 转换为 DataFrame"""
    if not recordset or 'records' not in recordset or 'schema' not in recordset:
        return pd.DataFrame()
    
    # 获取列名
    columns = [prop['name'] for prop in recordset['schema']['properties']]
    
    # 创建 DataFrame
    df = pd.DataFrame(recordset['records'], columns=columns)
    
    # 转换日期列
    if date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.sort_values(date_col)
    
    return df


def plot_pnl(ax, df: pd.DataFrame, alpha_id: str):
    """绘制 PnL 图表"""
    if df.empty or 'pnl' not in df.columns:
        ax.text(0.5, 0.5, 'No PnL data', ha='center', va='center', transform=ax.transAxes)
        return
    
    ax.plot(df['date'], df['pnl'], color='#2E86AB', linewidth=1.2)
    ax.fill_between(df['date'], df['pnl'], alpha=0.3, color='#2E86AB')
    ax.axhline(y=0, color='gray', linestyle='--', linewidth=0.8, alpha=0.5)
    
    ax.set_title(f'Cumulative PnL - {alpha_id}', fontsize=12, fontweight='bold')
    ax.set_xlabel('Date')
    ax.set_ylabel('PnL ($)')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    ax.grid(True, alpha=0.3)
    
    # 添加统计信息
    final_pnl = df['pnl'].iloc[-1]
    max_pnl = df['pnl'].max()
    min_pnl = df['pnl'].min()
    stats_text = f'Final: ${final_pnl:,.0f}\nMax: ${max_pnl:,.0f}\nMin: ${min_pnl:,.0f}'
    ax.text(0.02, 0.98, stats_text, transform=ax.transAxes, fontsize=9,
            verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))


def plot_sharpe(ax, df: pd.DataFrame, alpha_id: str):
    """绘制 Sharpe 图表"""
    if df.empty or 'sharpe' not in df.columns:
        ax.text(0.5, 0.5, 'No Sharpe data', ha='center', va='center', transform=ax.transAxes)
        return
    
    # 过滤掉 None 值
    df_valid = df.dropna(subset=['sharpe'])
    if df_valid.empty:
        ax.text(0.5, 0.5, 'No valid Sharpe data', ha='center', va='center', transform=ax.transAxes)
        return
    
    ax.plot(df_valid['date'], df_valid['sharpe'], color='#28A745', linewidth=1.2)
    ax.axhline(y=0, color='gray', linestyle='--', linewidth=0.8, alpha=0.5)
    ax.axhline(y=1, color='green', linestyle=':', linewidth=0.8, alpha=0.7, label='Sharpe=1')
    ax.axhline(y=2, color='darkgreen', linestyle=':', linewidth=0.8, alpha=0.7, label='Sharpe=2')
    
    ax.set_title(f'Rolling Sharpe Ratio - {alpha_id}', fontsize=12, fontweight='bold')
    ax.set_xlabel('Date')
    ax.set_ylabel('Sharpe Ratio')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    ax.grid(True, alpha=0.3)
    ax.legend(loc='upper right', fontsize=8)
    
    # 添加统计信息
    final_sharpe = df_valid['sharpe'].iloc[-1]
    avg_sharpe = df_valid['sharpe'].mean()
    stats_text = f'Final: {final_sharpe:.2f}\nAvg: {avg_sharpe:.2f}'
    ax.text(0.02, 0.98, stats_text, transform=ax.transAxes, fontsize=9,
            verticalalignment='top', bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.5))


def plot_turnover(ax, df: pd.DataFrame, alpha_id: str):
    """绘制 Turnover 图表"""
    if df.empty or 'turnover' not in df.columns:
        ax.text(0.5, 0.5, 'No Turnover data', ha='center', va='center', transform=ax.transAxes)
        return
    
    # 转换为百分比
    turnover_pct = df['turnover'] * 100
    
    ax.plot(df['date'], turnover_pct, color='#FD7E14', linewidth=1.2)
    ax.fill_between(df['date'], turnover_pct, alpha=0.3, color='#FD7E14')
    
    ax.set_title(f'Daily Turnover - {alpha_id}', fontsize=12, fontweight='bold')
    ax.set_xlabel('Date')
    ax.set_ylabel('Turnover (%)')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    ax.grid(True, alpha=0.3)
    
    # 添加统计信息
    avg_turnover = turnover_pct.mean()
    max_turnover = turnover_pct.max()
    stats_text = f'Avg: {avg_turnover:.2f}%\nMax: {max_turnover:.2f}%'
    ax.text(0.02, 0.98, stats_text, transform=ax.transAxes, fontsize=9,
            verticalalignment='top', bbox=dict(boxstyle='round', facecolor='moccasin', alpha=0.5))


def visualize_alpha(alpha_id: str, output_dir: Optional[str] = None, show: bool = True, 
                    filename_prefix: Optional[str] = None) -> Optional[str]:
    """
    主可视化函数
    
    Args:
        alpha_id: Alpha ID
        output_dir: 输出目录
        show: 是否显示图表窗口
        filename_prefix: 自定义文件名前缀，如 "sharpe2.30_rank1"
    
    Returns:
        保存的文件路径，如果未保存则返回 None
    """
    print(f"正在获取 Alpha {alpha_id} 的数据...")
    
    recordsets = get_alpha_recordsets(alpha_id)
    if not recordsets:
        print(f"错误: 未找到 Alpha {alpha_id} 的数据")
        return None
    
    print(f"找到 {len(recordsets)} 个 recordsets: {list(recordsets.keys())}")
    
    # 转换数据
    pnl_df = recordset_to_dataframe(recordsets.get('pnl', {}))
    sharpe_df = recordset_to_dataframe(recordsets.get('sharpe', {}))
    turnover_df = recordset_to_dataframe(recordsets.get('turnover', {}))
    
    # 创建图表
    fig, axes = plt.subplots(3, 1, figsize=(14, 12))
    fig.suptitle(f'Alpha Performance Dashboard: {alpha_id}', fontsize=14, fontweight='bold', y=0.98)
    
    plot_pnl(axes[0], pnl_df, alpha_id)
    plot_sharpe(axes[1], sharpe_df, alpha_id)
    plot_turnover(axes[2], turnover_df, alpha_id)
    
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    
    # 保存图表
    if output_dir:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 使用自定义前缀或默认格式
        if filename_prefix:
            filename = output_path / f"{filename_prefix}_{alpha_id}_{timestamp}.png"
        else:
            filename = output_path / f"alpha_{alpha_id}_{timestamp}.png"
        
        plt.savefig(filename, dpi=150, bbox_inches='tight')
        saved_path = str(filename)
        print(f"图表已保存: {saved_path}")
    
    if show:
        plt.show()
    else:
        plt.close()
    
    return saved_path


def main():
    parser = argparse.ArgumentParser(description='Alpha Recordsets 可视化工具')
    parser.add_argument('alpha_id', help='Alpha ID')
    parser.add_argument('-o', '--output', help='输出目录', default='./output')
    parser.add_argument('--no-show', action='store_true', help='不显示图表窗口')
    
    args = parser.parse_args()
    
    success = visualize_alpha(
        alpha_id=args.alpha_id,
        output_dir=args.output,
        show=not args.no_show
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
