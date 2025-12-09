# Bridge to shared module - 桥接到共享模块
import sys as _sys
import importlib.util as _importlib_util
from pathlib import Path as _Path

# 直接加载共享模块，避免循环导入
_shared_file = _Path(__file__).resolve().parent.parent / "wq_shared" / "wq_logger.py"
_spec = _importlib_util.spec_from_file_location("_wq_logger_shared", _shared_file)
_module = _importlib_util.module_from_spec(_spec)
_spec.loader.exec_module(_module)

# 导出所有公共接口
DEFAULT_SUBDIR = _module.DEFAULT_SUBDIR
WQ_DRIVE = _module.WQ_DRIVE
WQ_LOGS_ROOT = _module.WQ_LOGS_ROOT
LOG_FORMAT = _module.LOG_FORMAT
DATE_FORMAT = _module.DATE_FORMAT
setup_root_logging = _module.setup_root_logging
WQLogger = _module.WQLogger
init_logger = _module.init_logger
quick_setup = _module.quick_setup
get_logger = _module.get_logger

__all__ = [
    'DEFAULT_SUBDIR', 'WQ_DRIVE', 'WQ_LOGS_ROOT', 'LOG_FORMAT', 'DATE_FORMAT',
    'setup_root_logging', 'WQLogger', 'init_logger', 'quick_setup', 'get_logger',
]
