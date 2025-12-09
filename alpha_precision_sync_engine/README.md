# Alpha Precision Sync Engine

针对单个或指定列表的 Alpha 进行精准同步，支持从 API 抓取最新数据并智能合并到 MongoDB。

## 功能

- 单 Alpha 同步：按需更新指定的 Alpha
- 批量 ID 支持：从 JSON 文件读取 Alpha ID 列表
- 智能深度合并：保留历史数据，只更新变化的字段
- 状态追踪：与主流程的 efficient_state_manager 集成
- 文件备份：保存 JSON 到 final_data 目录

## 使用方法

### 命令行指定 Alpha ID

```bash
# 同步单个 Alpha
python sync_single_alphas.py <alpha_id>

# 同步多个 Alpha
python sync_single_alphas.py <alpha_id_1> <alpha_id_2> <alpha_id_3>
```

### 从 JSON 文件读取

```bash
# 使用指定文件
python sync_single_alphas.py --file my_alphas.json

# 使用默认配置文件 (target_alpha_ids_to_sync.json)
python sync_single_alphas.py
```

### JSON 文件格式

支持两种格式：

```json
// 格式1：纯数组
["alpha_id_1", "alpha_id_2", "alpha_id_3"]

// 格式2：带元数据的对象
{
    "alpha_ids": ["alpha_id_1", "alpha_id_2", "alpha_id_3"],
    "description": "可选的描述信息"
}
```

## 配置文件

### target_alpha_ids_to_sync.json

```json
{
    "_comment": "要同步的 Alpha ID 列表",
    "_usage": "python sync_single_alphas.py (无参数时自动读取此文件)",
    "alpha_ids": [
        "替换成你要同步的alpha_id_1",
        "替换成你要同步的alpha_id_2"
    ]
}
```

### mongo_config.json

```json
{
    "db_host": "localhost",
    "db_port": 27017,
    "db_name_regular": "regular_alphas",
    "db_name_super": "super_alphas"
}
```

## 数据保存位置

同步的数据保存到：
```
WorldQuant/Data/{USER_KEY}/final_data/{YYYY-MM}/{alpha_type}/{prefix}/{alpha_id}.json
```

## 深度合并策略

- 新字段：直接添加
- 已有字段：用新值覆盖
- 嵌套对象：递归合并
- 数组：按唯一键（id/name）合并

## 项目结构

```
alpha_precision_sync_engine/
├── sync_single_alphas.py           # 主程序
├── efficient_state_manager.py      # 状态追踪
├── target_alpha_ids_to_sync.json   # 默认 Alpha ID 列表
├── mongo_config.json               # MongoDB 配置
├── async_config.json               # 异步配置
├── user_accounts_config.json       # 用户账户配置
├── session_manager.py              # 会话管理
├── session_proxy.py                # 会话代理
├── wq_login.py                     # API 登录
├── wq_logger.py                    # 日志系统
└── timezone_utils.py               # 时区工具
```

## 依赖

```bash
pip install pymongo requests
```

## 与其他模块的关系

- 使用与 `fetch_and_import_alphas` 相同的 `efficient_state_manager`
- 数据保存格式与主流程一致，可被 `3_mongo_import_v4.py` 识别
- 共享 `wq_shared` 中的登录和日志模块
