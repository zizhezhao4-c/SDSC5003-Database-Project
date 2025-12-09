# Bridge to shared module - 桥接到共享模块
import sys as _sys
import importlib.util as _importlib_util
from pathlib import Path as _Path

# 直接加载共享模块，避免循环导入
_shared_file = _Path(__file__).resolve().parent.parent / "wq_shared" / "session_proxy.py"
_spec = _importlib_util.spec_from_file_location("_session_proxy_shared", _shared_file)
_module = _importlib_util.module_from_spec(_spec)
_spec.loader.exec_module(_module)

# 导出所有公共接口
SessionProxy = _module.SessionProxy

__all__ = ['SessionProxy']
