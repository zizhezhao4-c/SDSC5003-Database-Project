# Alpha Recordsets Update Pipeline

三步骤分离的 Alpha Recordsets 数据管理系统，用于筛选高质量 Alpha、抓取 Recordsets 数据并导入数据库。

## 核心特性

- 步骤分离：筛选、抓取、导入三步独立，失败可单独重跑
- 智能更新：支持 overwrite/update/skip 三种更新策略
- 多库支持：可同时写入 27017 和 27018 端口数据库
- 容错机制：自动重试、会话管理、失败追踪
- SQLite 状态追踪：支持 smart_merge 模式避免重复导入

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 运行完整流程

```bash
# 一键运行全部三个步骤
python run_all.py --all

# 或分步运行
python 1_select_alpha_range.py
python 2_fetch_and_store_local.py
python 3_import_to_database.py
```

## 三步骤工作流程

### 步骤1：筛选 Alpha 范围 (`1_select_alpha_range.py`)

从 MongoDB 筛选符合条件的 Alpha 并生成 ID 列表。

配置文件：`1_select_alpha_config.json`

筛选条件示例：
- `type = "REGULAR"`
- `status != "ACTIVE"`
- `is.fitness > 0.8`
- `is.sharpe > 1`
- `is.turnover < 0.7`

输出：`alpha_ids_to_fetch_list/alpha_ids_{USER_KEY}_{timestamp}.json`

### 步骤2：抓取数据到本地 (`2_fetch_and_store_local.py`)

读取步骤1生成的 Alpha ID 列表，从 API 抓取 Recordsets 数据并存储到本地。

配置文件：`2_fetch_config.json`

支持的 Recordsets：
- pnl (累计收益)
- sharpe (夏普比率)
- turnover (换手率)
- daily-pnl (每日收益)
- yearly-stats (年度统计)

输出目录：`WorldQuant/Data/{USER_KEY}/recordsets/`

### 步骤3：导入到数据库 (`3_import_to_database.py`)

将本地 Recordsets 文件导入到 MongoDB。

配置文件：`3_import_config.json`

更新模式：
- `overwrite`：覆盖已有数据
- `update`：增量更新（保留历史）
- `skip`：跳过已存在的记录
- `smart_merge`：基于文件哈希智能判断是否需要更新

## 项目结构

```
alpha_recordsets_update/
├── 1_select_alpha_range.py      # 步骤1：筛选 Alpha
├── 1_select_alpha_config.json   # 步骤1 配置
├── 2_fetch_and_store_local.py   # 步骤2：抓取数据
├── 2_fetch_config.json          # 步骤2 配置
├── 3_import_to_database.py      # 步骤3：导入数据库
├── 3_import_config.json         # 步骤3 配置
├── run_all.py                   # 统一运行脚本
├── fetch_recordsets.py          # Recordsets 抓取核心模块
├── mongo_recordsets_writer.py   # MongoDB 写入模块
├── sqlite_state_manager.py      # SQLite 状态追踪
├── health_check.py              # 健康检查脚本
├── alpha_ids_to_fetch_list/     # 待抓取的 Alpha ID 列表
└── requirements.txt             # 依赖列表
```

## 配置说明

### 1_select_alpha_config.json

```json
{
    "SOURCE_DB_HOST": "localhost",
    "SOURCE_DB_PORT": 27017,
    "SOURCE_DB_NAME": "regular_alphas",
    "MIN_FITNESS": 0.8,
    "MIN_SHARPE": 1,
    "MAX_TURNOVER": 0.7,
    "ALPHA_TYPE": "REGULAR",
    "EXCLUDE_STATUS": "ACTIVE"
}
```

### 2_fetch_config.json

```json
{
    "MAX_WORKERS": 3,
    "FETCH_UPDATE_MODE": "update",
    "CONFIRM_BEFORE_RUN": true
}
```

### 3_import_config.json

```json
{
    "WRITE_DB": [27017, 27018],
    "DB_UPDATE_MODE": "update",
    "IMPORT_SCOPE": "default_list"
}
```

## 注意事项

1. 运行前确保 MongoDB 服务已启动
2. 步骤2需要有效的 API 登录凭证
3. `overwrite` 模式会清除历史数据，请谨慎使用
4. 建议使用 `update` 或 `smart_merge` 模式进行增量更新
