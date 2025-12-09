# Alpha Visualizer

从 MongoDB 读取 Alpha Recordsets 数据并生成可视化图表。

## 功能

- PnL (累计收益曲线)
- Sharpe (滚动夏普比率)
- Turnover (每日换手率)

## 使用方法

```bash
# 基本用法
python visualize_alpha.py <alpha_id>

# 指定输出目录
python visualize_alpha.py <alpha_id> -o ./charts

# 只保存不显示
python visualize_alpha.py <alpha_id> --no-show

# 交互模式
python interactive.py
```

## 配置

默认连接 `mongodb://localhost:27017/`，数据库 `regular_alphas`，集合 `test`。

如需修改，编辑 `visualize_alpha.py` 中的配置常量：

```python
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "regular_alphas"
COLLECTION_NAME = "test"
```

## 依赖

```bash
pip install pymongo pandas matplotlib
```
