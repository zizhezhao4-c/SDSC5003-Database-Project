#!/usr/bin/env python3
"""
时区处理工具模块
解决美国东部时间夏令时/冬令时转换问题
"""

from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Tuple, Optional
import logging

__all__ = [
    'EASTERN_TZ', 'UTC_TZ',
    'get_eastern_offset_for_date', 'convert_eastern_to_utc',
    'get_utc_date_range_for_eastern_date', 'build_api_query_with_correct_timezone',
    'build_api_query_with_time_slice', 'get_file_date_from_utc_timestamp',
    'convert_alpha_timestamps_to_utc', 'generate_time_slices_utc',
]

logger = logging.getLogger(__name__)

# 美国东部时区与UTC（使用标准库 zoneinfo/timezone）
# 注意：在 Windows 上需要安装 tzdata 包：pip install tzdata
EASTERN_TZ = ZoneInfo('America/New_York')
UTC_TZ = timezone.utc

def get_eastern_offset_for_date(dt: datetime) -> str:
    """
    获取指定日期的美国东部时区偏移量
    
    Args:
        dt: 日期时间对象
        
    Returns:
        str: 时区偏移量字符串，如 "-05:00" 或 "-04:00"
    """
    # 确保输入是naive datetime，则视为东部本地时间；否则转换到东部时区
    if dt.tzinfo is None:
        eastern_dt = dt.replace(tzinfo=EASTERN_TZ)
    else:
        eastern_dt = dt.astimezone(EASTERN_TZ)
    
    # 获取UTC偏移量
    offset = eastern_dt.utcoffset()
    
    # 检查offset是否为None
    if offset is None:
        raise ValueError(f"无法获取 {eastern_dt} 的UTC偏移量")
    
    # 转换为字符串格式
    total_seconds = int(offset.total_seconds())
    hours, remainder = divmod(abs(total_seconds), 3600)
    minutes = remainder // 60
    
    sign = '-' if total_seconds < 0 else '+'
    return f"{sign}{hours:02d}:{minutes:02d}"

def convert_eastern_to_utc(dt_str: str) -> datetime:
    """
    将美国东部时间字符串转换为UTC时间
    
    Args:
        dt_str: 东部时间字符串，如 "2025-09-16T00:59:44-04:00"
        
    Returns:
        datetime: UTC时间对象
    """
    try:
        # 解析带时区的时间字符串
        dt = datetime.fromisoformat(dt_str)
        
        # 转换为UTC
        if dt.tzinfo is None:
            # 如果没有时区信息，假设为东部时间
            eastern_dt = dt.replace(tzinfo=EASTERN_TZ)
            return eastern_dt.astimezone(UTC_TZ)
        else:
            return dt.astimezone(UTC_TZ)
            
    except Exception as e:
        logger.error(f"时间转换失败: {dt_str}, 错误: {e}")
        raise

def get_utc_date_range_for_eastern_date(date_str: str) -> Tuple[str, str]:
    """
    获取东部时间日期对应的UTC时间范围
    
    Args:
        date_str: 日期字符串，如 "2025-09-16" 或 "2025-09-16T00:00:00"
        
    Returns:
        tuple: (UTC开始时间, UTC结束时间)
    """
    try:
        # 解析日期 - 支持多种格式
        if 'T' in date_str:
            # ISO格式: "2025-09-16T00:00:00"
            date_obj = datetime.fromisoformat(date_str.split('T')[0])
        else:
            # 简单格式: "2025-09-16"
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        
        # 东部时间的一天开始和结束（使用 zoneinfo）
        eastern_start = date_obj.replace(hour=0, minute=0, second=0, tzinfo=EASTERN_TZ)
        eastern_end = date_obj.replace(hour=23, minute=59, second=59, tzinfo=EASTERN_TZ)
        
        # 转换为UTC
        utc_start = eastern_start.astimezone(UTC_TZ)
        utc_end = eastern_end.astimezone(UTC_TZ)
        
        return (
            utc_start.strftime("%Y-%m-%dT%H:%M:%S.000"),
            utc_end.strftime("%Y-%m-%dT%H:%M:%S.000")
        )
        
    except Exception as e:
        logger.error(f"日期范围转换失败: {date_str}, 错误: {e}")
        raise

def build_api_query_with_correct_timezone(start_date: str, end_date: str, alpha_type: str = "REGULAR") -> str:
    """
    构建带有正确时区的API查询URL
    
    Args:
        start_date: 开始日期 "YYYY-MM-DD"
        end_date: 结束日期 "YYYY-MM-DD"
        alpha_type: Alpha类型 "REGULAR" 或 "SUPER"
        
    Returns:
        str: API查询URL的时间部分
    """
    try:
        # 获取UTC时间范围
        utc_start, utc_end = get_utc_date_range_for_eastern_date(start_date)
        
        # 构建查询参数 - utc_start和utc_end已经包含毫秒精度
        query_params = (
            f"type={alpha_type}&"
            f"dateCreated%3E={utc_start}Z&"
            f"dateCreated%3C{utc_end}Z"
        )
        
        return query_params
        
    except Exception as e:
        logger.error(f"构建API查询失败: {start_date} to {end_date}, 错误: {e}")
        raise

def build_api_query_with_time_slice(start_timestamp: str, end_timestamp: str, alpha_type: str = "REGULAR") -> str:
    """
    构建带有精确时间切片的API查询URL
    
    Args:
        start_timestamp: 开始时间戳 "YYYY-MM-DDTHH:MM:SS.000Z"
        end_timestamp: 结束时间戳 "YYYY-MM-DDTHH:MM:SS.000Z"
        alpha_type: Alpha类型 "REGULAR" 或 "SUPER"
        
    Returns:
        str: API查询URL的时间部分
    """
    try:
        # 直接使用时间戳，去掉末尾的Z
        start_clean = start_timestamp.rstrip('Z')
        end_clean = end_timestamp.rstrip('Z')
        
        # 构建查询参数
        query_params = (
            f"type={alpha_type}&"
            f"dateCreated%3E={start_clean}Z&"
            f"dateCreated%3C{end_clean}Z"
        )
        
        return query_params
        
    except Exception as e:
        logger.error(f"构建时间切片API查询失败: {start_timestamp} to {end_timestamp}, 错误: {e}")
        raise

def get_file_date_from_utc_timestamp(utc_timestamp: str) -> str:
    """
    从UTC时间戳获取应归档的文件日期（按UTC自然日）
    
    Args:
        utc_timestamp: UTC时间戳字符串
        
    Returns:
        str: 文件日期 "YYYY-MM-DD"（UTC）
    """
    try:
        # 解析UTC时间
        if utc_timestamp.endswith('Z'):
            utc_dt = datetime.fromisoformat(utc_timestamp[:-1]).replace(tzinfo=UTC_TZ)
        else:
            utc_dt = datetime.fromisoformat(utc_timestamp)
            if utc_dt.tzinfo is None:
                utc_dt = utc_dt.replace(tzinfo=UTC_TZ)
            else:
                utc_dt = utc_dt.astimezone(UTC_TZ)
        
        # 返回UTC日期
        return utc_dt.strftime("%Y-%m-%d")
        
    except Exception as e:
        logger.error(f"时间戳转换失败: {utc_timestamp}, 错误: {e}")
        raise

def convert_alpha_timestamps_to_utc(alpha_data: dict) -> dict:
    """
    将Alpha数据中的时间戳转换为UTC格式
    
    Args:
        alpha_data: Alpha数据字典
        
    Returns:
        dict: 转换后的Alpha数据
    """
    alpha_copy = alpha_data.copy()
    
    # 需要转换的时间字段
    time_fields = ['dateCreated', 'dateModified', 'dateSubmitted']
    
    for field in time_fields:
        if field in alpha_copy and alpha_copy[field]:
            try:
                # 转换为UTC时间
                utc_dt = convert_eastern_to_utc(alpha_copy[field])
                alpha_copy[field] = utc_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            except Exception as e:
                logger.warning(f"转换字段 {field} 失败: {alpha_copy[field]}, 错误: {e}")
    
    return alpha_copy

def generate_time_slices_utc(start_date_str: str, end_date_str: str, interval_hours: float = 2) -> list:
    """
    生成基于UTC自然日的时间切片（使用半开区间 [start, end)）
    
    Args:
        start_date_str: 开始时间或日期（UTC） "YYYY-MM-DD" 或 "YYYY-MM-DDTHH:MM:SS[.fff]Z"
        end_date_str: 结束时间或日期（UTC，作为半开区间的上界）
        interval_hours: 时间间隔（小时）
        
    Returns:
        list: 时间切片列表 [(start, end), ...]，时间格式为 "YYYY-MM-DDT%H:%M:%S.000Z"
    """
    try:
        def _parse_utc(s: str) -> datetime:
            if 'T' in s:
                if s.endswith('Z'):
                    base = s[:-1]
                    dt = datetime.fromisoformat(base)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=UTC_TZ)
                    else:
                        dt = dt.astimezone(UTC_TZ)
                    return dt
                else:
                    dt = datetime.fromisoformat(s)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=UTC_TZ)
                    else:
                        dt = dt.astimezone(UTC_TZ)
                    return dt
            else:
                d = datetime.strptime(s, "%Y-%m-%d")
                return d.replace(tzinfo=UTC_TZ)

        start_dt = _parse_utc(start_date_str)
        end_dt = _parse_utc(end_date_str)

        slices = []
        current_start = start_dt
        interval = timedelta(hours=int(interval_hours))

        while current_start < end_dt:
            current_end = min(current_start + interval, end_dt)
            slices.append((
                current_start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                current_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            ))
            current_start = current_end

        return slices
    except Exception as e:
        logger.error(f"生成UTC时间切片失败: {e}")
        raise

if __name__ == "__main__":
    # 测试代码
    print("Testing timezone handling functions...")
    
    # 测试1: 夏令时偏移量
    summer_date = datetime(2025, 9, 16)  # 夏令时
    winter_date = datetime(2025, 1, 16)  # 冬令时
    
    print(f"夏令时偏移量: {get_eastern_offset_for_date(summer_date)}")
    print(f"冬令时偏移量: {get_eastern_offset_for_date(winter_date)}")
    
    # 测试2: 时间转换
    eastern_time = "2025-09-16T00:59:44-04:00"
    utc_time = convert_eastern_to_utc(eastern_time)
    print(f"东部时间: {eastern_time}")
    print(f"UTC时间: {utc_time}")
    
    # 测试3: 文件日期归档
    file_date = get_file_date_from_utc_timestamp(utc_time.isoformat() + "Z")
    print(f"应归档到: {file_date}")
    
    # 测试4: UTC日期范围
    utc_start, utc_end = get_utc_date_range_for_eastern_date("2025-09-16")
    print(f"UTC范围: {utc_start} 到 {utc_end}")
    
    print("Timezone handling tests completed successfully")