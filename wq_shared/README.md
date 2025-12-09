# WQ Shared Module

项目公共模块，包含各子模块共享的核心功能。

## 包含模块

| 文件 | 功能 |
|------|------|
| `wq_login.py` | WorldQuant Brain API 登录和会话管理 |
| `wq_logger.py` | 统一日志系统（UTC 时间、文件+控制台输出） |
| `timezone_utils.py` | 美东/UTC 时区转换工具 |
| `session_manager.py` | 多账户会话管理器 |
| `session_proxy.py` | 会话代理（自动重试、限流处理） |

## 使用方式

### 方式1：直接导入（推荐）

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from wq_shared import start_session, get_logger, convert_eastern_to_utc
```

### 方式2：设置环境变量

```bash
export PYTHONPATH="/path/to/project:$PYTHONPATH"
```

### 方式3：使用本地桥接文件

各子模块保留的 `wq_login.py` 等文件会自动桥接到共享模块。

## 导出的主要接口

```python
# 登录相关
from wq_shared import start_session, get_credentials, check_session_validity, clear_credentials
from wq_shared import WQ_DATA_ROOT, WQ_LOGS_ROOT, USER_KEY, BRAIN_API_URL

# 日志相关
from wq_shared import get_logger, WQLogger, init_logger, quick_setup, setup_root_logging

# 时区相关
from wq_shared import convert_eastern_to_utc, get_utc_date_range_for_eastern_date
from wq_shared import get_file_date_from_utc_timestamp, generate_time_slices_utc
from wq_shared import EASTERN_TZ, UTC_TZ

# 会话管理
from wq_shared import SessionManager, SessionProxy
```

## 配置

### 环境变量

```bash
# 数据和日志目录
export WQ_DATA_ROOT=/path/to/data
export WQ_LOGS_ROOT=/path/to/logs

# API 凭证
export BRAIN_CREDENTIAL_EMAIL=your_email
export BRAIN_CREDENTIAL_PASSWORD=your_password
```

### 凭证存储

凭证文件存储在 `~/secrets/` 目录：
- `{USER_KEY}_platform-brain.json` - 登录凭证
- `wq_cookies/{USER_KEY}_session_cookie.json` - 会话 Cookie

## 日志格式

```
2025-12-08 12:00:00Z - INFO - ModuleName - Message
```

- 时间统一使用 UTC
- 同时输出到控制台和文件
- 日志文件位置：`{WQ_LOGS_ROOT}/{USER_KEY}/`
