# Alpha Filter

基于 JSON 配置的通用 Alpha 筛选系统，支持灵活的多条件筛选和结果可视化。

## 功能

- JSON 配置驱动：通过配置文件定义筛选条件
- 多维度筛选：Sharpe、Fitness、Turnover、PnL、Region、Universe 等
- Pyramid 匹配：按策略金字塔分类筛选
- 检查项筛选：排除 FAIL 状态的 Alpha
- 结果可视化：生成分布图表和统计信息
- 自动清理：保留最近 N 个结果文件

## 使用方法

### 基本筛选

```bash
# 使用默认配置文件
python universal_alpha_filter.py

# 使用自定义配置文件
python universal_alpha_filter.py my_config.json
```

### 筛选并可视化

```bash
# 筛选、排序并可视化 Sharpe 最高的 Alpha
python filter_sort_visualize.py

# 自动可视化模式
python filter_sort_visualize.py --auto-visualize

# 显示前 50 个结果
python filter_sort_visualize.py --top 50
```

## 配置文件

### alpha_filter_config.json

```json
{
    "filter_name": "我的筛选配置",
    "database": "regular_alphas",
    "collection": "test",
    "filters": {
        "status": {
            "enabled": true,
            "operator": "exclude",
            "values": ["ACTIVE"]
        },
        "sharpe": {
            "is_sharpe": {
                "enabled": true,
                "operator": "gt",
                "value": 1.5
            },
            "any_sharpe_gt": {
                "enabled": true,
                "value": 1.5
            }
        },
        "region": {
            "enabled": false,
            "operator": "in",
            "values": ["USA", "EUR"]
        },
        "pyramids": {
            "enabled": false,
            "operator": "contains_any",
            "values": ["Momentum", "Value"]
        }
    },
    "output": {
        "save_to_file": true,
        "save_ids_only": false,
        "include_timestamp": true,
        "max_keep_files": 10,
        "include_fields": ["id", "author", "status", "settings", "is_sharpe", "sharpe_2y", "ladder_sharpe"]
    },
    "display": {
        "show_summary": true,
        "show_statistics": true,
        "max_detail_display": 10
    }
}
```

### 筛选条件说明

| 条件 | 说明 |
|------|------|
| `status` | 按状态筛选（exclude/in/equals） |
| `sharpe.is_sharpe` | 按 is.sharpe 字段筛选 |
| `sharpe.any_sharpe_gt` | 2Y Sharpe 或 Ladder Sharpe 任一满足 |
| `sharpe.sharpe_2y` | 按 LOW_2Y_SHARPE 检查项筛选 |
| `sharpe.ladder_sharpe` | 按 IS_LADDER_SHARPE 检查项筛选 |
| `region` | 按区域筛选 |
| `universe` | 按 Universe 筛选 |
| `pyramids` | 按金字塔分类筛选 |
| `delay` | 按延迟筛选 |
| `checks.no_fail` | 排除有 FAIL 检查项的 Alpha |
| `exclude_red_tagged` | 排除红色标记的 Alpha |

## 输出文件

筛选结果保存在脚本所在目录：

```
{collection}_filtered_alphas_{database}_{pyramid}_{status}_{timestamp}.json
```

示例：`test_filtered_alphas_regular_alphas_exclude_ACTIVE_20251208T120000.json`

## 项目结构

```
alpha_filter/
├── universal_alpha_filter.py    # 主筛选程序
├── filter_sort_visualize.py     # 筛选+排序+可视化
├── alpha_filter_config.json     # 筛选配置文件
├── filter_results/              # 筛选结果输出目录
└── visualizations/              # 可视化图表输出目录
```

## 依赖

```bash
pip install pymongo matplotlib pandas
```
