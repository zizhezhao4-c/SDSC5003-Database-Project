# Bridge to shared module - 桥接到共享模块
import sys as _sys
import importlib.util as _importlib_util
from pathlib import Path as _Path

# 直接加载共享模块，避免循环导入
_shared_file = _Path(__file__).resolve().parent.parent / "wq_shared" / "wq_login.py"
_spec = _importlib_util.spec_from_file_location("_wq_login_shared", _shared_file)
_module = _importlib_util.module_from_spec(_spec)
_spec.loader.exec_module(_module)

# 导出所有公共接口
WQ_DRIVE = _module.WQ_DRIVE
WQ_LOGS_ROOT = _module.WQ_LOGS_ROOT
WQ_DATA_ROOT = _module.WQ_DATA_ROOT
BRAIN_API_URL = _module.BRAIN_API_URL
brain_api_url = _module.brain_api_url
USER_KEY = _module.USER_KEY
MIN_REMAINING_SECONDS = _module.MIN_REMAINING_SECONDS
COOKIES_FOLDER_PATH = _module.COOKIES_FOLDER_PATH
COOKIE_FILE_PATH = _module.COOKIE_FILE_PATH
get_credentials = _module.get_credentials
check_session_validity = _module.check_session_validity
save_cookie_to_file = _module.save_cookie_to_file
perform_full_login = _module.perform_full_login
perform_full_login_with_retry_limit = _module.perform_full_login_with_retry_limit
start_session = _module.start_session
check_session_timeout = _module.check_session_timeout
clear_credentials = _module.clear_credentials

__all__ = [
    'WQ_DRIVE', 'WQ_LOGS_ROOT', 'WQ_DATA_ROOT',
    'BRAIN_API_URL', 'brain_api_url', 'USER_KEY',
    'MIN_REMAINING_SECONDS', 'COOKIES_FOLDER_PATH', 'COOKIE_FILE_PATH',
    'get_credentials', 'check_session_validity', 'save_cookie_to_file',
    'perform_full_login', 'perform_full_login_with_retry_limit',
    'start_session', 'check_session_timeout', 'clear_credentials',
]
