# Bridge to shared module - 桥接到共享模块
import sys as _sys
import importlib.util as _importlib_util
from pathlib import Path as _Path

# 直接加载共享模块，避免循环导入
_shared_file = _Path(__file__).resolve().parent.parent / "wq_shared" / "timezone_utils.py"
_spec = _importlib_util.spec_from_file_location("_timezone_utils_shared", _shared_file)
_module = _importlib_util.module_from_spec(_spec)
_spec.loader.exec_module(_module)

# 导出所有公共接口
EASTERN_TZ = _module.EASTERN_TZ
UTC_TZ = _module.UTC_TZ
get_eastern_offset_for_date = _module.get_eastern_offset_for_date
convert_eastern_to_utc = _module.convert_eastern_to_utc
get_utc_date_range_for_eastern_date = _module.get_utc_date_range_for_eastern_date
build_api_query_with_correct_timezone = _module.build_api_query_with_correct_timezone
build_api_query_with_time_slice = _module.build_api_query_with_time_slice
get_file_date_from_utc_timestamp = _module.get_file_date_from_utc_timestamp
convert_alpha_timestamps_to_utc = _module.convert_alpha_timestamps_to_utc
generate_time_slices_utc = _module.generate_time_slices_utc

__all__ = [
    'EASTERN_TZ', 'UTC_TZ',
    'get_eastern_offset_for_date', 'convert_eastern_to_utc',
    'get_utc_date_range_for_eastern_date', 'build_api_query_with_correct_timezone',
    'build_api_query_with_time_slice', 'get_file_date_from_utc_timestamp',
    'convert_alpha_timestamps_to_utc', 'generate_time_slices_utc',
]
