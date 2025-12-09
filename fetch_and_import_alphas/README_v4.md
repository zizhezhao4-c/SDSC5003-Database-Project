# MongoDB Alpha Import Tool v4.0

高性能、模块化的 Alpha JSON 文件批量导入系统。

## 概述

本工具提供完整的 Alpha 数据导入流程：

1. **数据抓取**：从 API 异步获取 Alpha 元数据
2. **文件整理**：打包和分解数据文件
3. **数据库导入**：高性能并行导入 MongoDB

## 核心特性

- 多进程并行处理（支持 64+ worker）
- 智能深度合并（保留历史字段）
- 多种导入模式（incremental/overwrite/rebuild/time_range/smart_merge）
- SQLite 状态追踪（支持断点续传）
- 内存优化（批处理 + 显式清理）
- 支持 Python 3.14+ free-threading

## 快速开始

### 完整流程

```bash
# 步骤1：从 API 抓取 Alpha 数据
python 1_fetch_all_alpha_main_async.py

# 步骤2：打包和整理数据文件
python 2_alpha_files_packaging_and_decomposition.py

# 步骤3：导入到 MongoDB
python 3_mongo_import_v4.py
```

### 多用户批量运行

```bash
# 使用脚本批量运行所有用户
python run_all_users_v2.py

# 或使用 shell 脚本
./run_all_users.sh
```

## 配置

### mongo_config.json

```json
{
    "db_host": "localhost",
    "db_port": 27017,
    "db_name_regular": "regular_alphas",
    "db_name_super": "super_alphas",
    "workers": 64,
    "batch_size": 1000,
    "IMPORT_MODE": "incremental",
    "USE_PRELOAD": true,
    "FAST_MODE": false
}
```

### 导入模式

| 模式 | 说明 |
|------|------|
| `incremental` | 增量导入，跳过已处理文件，深度合并 |
| `overwrite` | 处理所有文件，深度合并（保留旧字段） |
| `rebuild` | 完全替换（警告：会删除旧字段） |
| `time_range` | 按时间范围导入 |
| `smart_merge` | 智能合并，查询数据库最新时间戳 |

### 命令行参数

```bash
python 3_mongo_import_v4.py \
    --workers 32 \
    --batch-size 500 \
    --import-mode incremental \
    --db-host localhost \
    --db-port 27017
```

## 架构

### 处理流程

```
Init → Regular Months (sequential) → Super Months (sequential) → Finalize
         ↓                              ↓
    Month → Batches → Worker Pool → Bulk Write → Cleanup
```

### 核心原则

- 1 主进程 + N 工作进程（multiprocessing.Pool）
- 顺序处理月份（先 Regular，后 Super）
- 自动批次划分：文件数 > workers × batch_size 时分批
- 深度合并策略：递归合并，保留旧字段，更新新值

## 深度合并示例

### 嵌套对象合并

```python
# 旧文档
{"settings": {"region": "USA", "delay": 0}}

# 新文档
{"settings": {"region": "ASI", "decay": 5}}

# 合并结果
{"settings": {"region": "ASI", "delay": 0, "decay": 5}}
```

### 数组合并（按唯一键）

```python
# 旧文档
{"checks": [{"name": "LOW_SHARPE", "value": 1.0}]}

# 新文档
{"checks": [{"name": "LOW_SHARPE", "value": 2.5}, {"name": "NEW_CHECK", "value": 1.0}]}

# 合并结果（按 name 字段合并）
{"checks": [{"name": "LOW_SHARPE", "value": 2.5}, {"name": "NEW_CHECK", "value": 1.0}]}
```

## 性能优化

### 批处理策略

- 阈值 = workers × batch_size
- 文件数 ≤ 阈值：单批处理
- 文件数 > 阈值：分多批顺序处理

### 内存管理

- 批次级清理：每批后 `gc.collect()`
- 显式删除：处理完成后删除 preloaded_docs
- 顺序处理：一次只处理一个批次

## 项目结构

```
fetch_and_import_alphas/
├── 1_fetch_all_alpha_main_async.py    # 异步数据抓取
├── 2_alpha_files_packaging_and_decomposition.py  # 文件整理
├── 3_mongo_import_v4.py               # MongoDB 导入（主程序）
├── efficient_state_manager.py         # SQLite 状态追踪
├── run_all_users_v2.py                # 多用户批量运行
├── async_config.json                  # 抓取配置
├── mongo_config.json                  # 导入配置
├── user_accounts_config.json          # 用户账户配置
├── session_manager.py                 # 会话管理
├── session_proxy.py                   # 会话代理
├── wq_login.py                        # API 登录
├── wq_logger.py                       # 日志系统
└── timezone_utils.py                  # 时区工具
```

## 故障排除

### MongoDB 连接失败

```bash
# 检查 MongoDB 是否运行
mongosh --host localhost --port 27017
```

### 内存不足

1. 减少 `batch_size`
2. 减少 `workers` 数量
3. 使用 `--no-preload` 禁用预加载

### 性能慢

1. 增加 `workers`（如果 CPU 核心足够）
2. 增加 `batch_size`（如果内存足够）
3. 使用 SSD 存储数据
4. 使用 `incremental` 模式跳过已处理文件

## 依赖

- pymongo
- tqdm
- orjson (可选，提升 JSON 解析性能)
