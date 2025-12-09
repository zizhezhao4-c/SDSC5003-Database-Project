# Canonical wq_logger.py (project-wide)
import logging
import time
import os
import sys
import tempfile
import re
from datetime import datetime, timezone
from typing import Tuple, Optional

__all__ = [
    'DEFAULT_SUBDIR', 'WQ_DRIVE', 'WQ_LOGS_ROOT', 'LOG_FORMAT', 'DATE_FORMAT',
    'setup_root_logging', 'WQLogger', 'init_logger', 'quick_setup', 'get_logger',
]

# === 日志系统配置常量 ===
# 默认子目录，可通过环境变量覆盖
DEFAULT_SUBDIR = os.environ.get("WQ_DEFAULT_SUBDIR", "simulate")

# 项目根目录（wq_shared 的父目录）
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 默认日志根目录配置 - 使用项目目录下的 WorldQuant/Logs
WQ_DRIVE = os.environ.get("WQ_DRIVE", _PROJECT_ROOT)
WQ_LOGS_ROOT = os.environ.get("WQ_LOGS_ROOT", os.path.join(_PROJECT_ROOT, "WorldQuant", "Logs"))

# 统一日志格式（UTC + Z）
# 说明：通过设置 logging.Formatter.converter = time.gmtime 强制使用UTC；
# DATE_FORMAT 包含 'Z' 以明确时区。
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
DATE_FORMAT = '%Y-%m-%d %H:%M:%SZ'

# 强制所有 Formatter 使用 UTC 时间
logging.Formatter.converter = time.gmtime


def _sanitize_path_component(component: str) -> str:
    """
    清理路径组件，防止路径遍历攻击和非法字符
    """
    # Remove any path separators and parent directory references
    component = component.replace('\\', '_').replace('/', '_')
    component = component.replace('..', '_')
    # Remove other potentially problematic characters
    component = re.sub(r'[<>:"|?*\x00-\x1f]', '_', component)
    # Ensure not empty
    return component.strip() or 'default'


def _determine_log_dir(userkey: str, subdir: Optional[str]) -> str:
    r"""
    确定日志目录，优先级：
    1. 环境变量 WQ_LOG_DIR (完全自定义)
    2. 配置的根目录 + 用户键 + 子目录
    3. 系统备用路径 (Windows: C:\WorldQuant\Logs, Unix: ~/WorldQuant/Logs)
    4. 当前目录备用路径
    5. 系统临时目录 (最后备用)
    """
    # Sanitize inputs to prevent path traversal
    safe_userkey = _sanitize_path_component(userkey)
    chosen_subdir = _sanitize_path_component(subdir or DEFAULT_SUBDIR)
    
    # Get a temporary logger for early error reporting (before main logger is set up)
    temp_logger = logging.getLogger('wq_logger_init')
    
    # 环境变量完全覆盖
    env_dir = os.environ.get("WQ_LOG_DIR")
    if env_dir:
        try:
            os.makedirs(env_dir, exist_ok=True)
            return env_dir
        except (OSError, PermissionError) as e:
            temp_logger.warning(f"Cannot use WQ_LOG_DIR={env_dir}: {e}")
    
    # 使用配置的根目录
    primary_path = os.path.join(WQ_LOGS_ROOT, safe_userkey, chosen_subdir)
    try:
        os.makedirs(primary_path, exist_ok=True)
        return primary_path
    except (OSError, PermissionError) as e:
        temp_logger.warning(f"Cannot create primary log directory {primary_path}: {e}")
    
    # 系统备用路径 (Windows: C:\WorldQuant\Logs, Unix: ~/WorldQuant/Logs)
    if os.name == 'nt':  # Windows
        backup_root = r"C:\WorldQuant\Logs"
    else:  # Unix-like (Linux, macOS)
        backup_root = os.path.expanduser("~/WorldQuant/Logs")
    
    backup_path = os.path.join(backup_root, safe_userkey, chosen_subdir)
    try:
        os.makedirs(backup_path, exist_ok=True)
        temp_logger.info(f"Using backup log directory: {backup_path}")
        return backup_path
    except (OSError, PermissionError) as e:
        temp_logger.warning(f"Cannot create backup log directory {backup_path}: {e}")
    
    # 当前目录备用路径
    try:
        current_backup_path = os.path.join(os.getcwd(), "Logs", safe_userkey, chosen_subdir)
        os.makedirs(current_backup_path, exist_ok=True)
        temp_logger.info(f"Using current directory log path: {current_backup_path}")
        return current_backup_path
    except (OSError, PermissionError) as e:
        temp_logger.warning(f"Cannot create current directory log path: {e}")
    
    # 最后备用：系统临时目录
    try:
        temp_log_path = os.path.join(tempfile.gettempdir(), "WorldQuant", "Logs", safe_userkey, chosen_subdir)
        os.makedirs(temp_log_path, exist_ok=True)
        temp_logger.warning(f"Using system temp directory for logs: {temp_log_path}")
        return temp_log_path
    except (OSError, PermissionError) as e:
        # This should almost never happen, but if it does, raise an error
        raise RuntimeError(f"Cannot create log directory anywhere: {e}") from e


def setup_root_logging(
    userkey: str,
    subdir: Optional[str] = None,
    filename_prefix: str = "run_log",
    level: int = logging.INFO,
) -> Tuple[logging.Logger, str]:
    # _determine_log_dir already creates the directory
    log_dir = _determine_log_dir(userkey, subdir)

    # 使用UTC时间生成日志文件名，并在秒级末尾加 'Z'
    # 使用时区感知的UTC时间，避免 utcnow() 的弃用警告
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    log_filename = f"{filename_prefix}_{timestamp}.log"
    log_file_path = os.path.join(log_dir, log_filename)

    formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    # Clear existing handlers to avoid duplicates
    for h in list(root_logger.handlers):
        root_logger.removeHandler(h)

    # Create file handler with immediate write (unbuffered mode for real-time logging)
    file_handler = logging.FileHandler(log_file_path, encoding='utf-8', mode='a', delay=False)
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    
    # Force unbuffered writes for real-time logging
    # Override emit to flush after every log message
    original_emit = file_handler.emit
    def emit_with_flush(record):
        original_emit(record)
        if file_handler.stream:
            try:
                file_handler.stream.flush()
                os.fsync(file_handler.stream.fileno())  # Ensure OS writes to disk
            except (OSError, AttributeError):
                pass  # Handle closed streams gracefully
    file_handler.emit = emit_with_flush
    
    root_logger.addHandler(file_handler)

    # Console handler with auto-flush
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    
    # Console also gets immediate flush
    original_console_emit = console_handler.emit
    def console_emit_with_flush(record):
        original_console_emit(record)
        if console_handler.stream:
            try:
                console_handler.stream.flush()
            except (OSError, AttributeError):
                pass
    console_handler.emit = console_emit_with_flush
    
    root_logger.addHandler(console_handler)

    return root_logger, log_file_path


class WQLogger:
    def __init__(self, userkey: str, subdir: Optional[str] = None, filename_prefix: str = "simulate_log"):
        self.userkey = userkey
        self.subdir = subdir or DEFAULT_SUBDIR
        self.filename_prefix = filename_prefix
        self.log_file_path: Optional[str] = None
        self.logger: Optional[logging.Logger] = None
        # default levels policy: INFO for normal run; override via env
        env_level = os.environ.get("WQ_LOG_LEVEL")
        self.default_level = getattr(logging, env_level.upper(), logging.INFO) if env_level else logging.INFO

    def setup_logging(self) -> logging.Logger:
        logger, path = setup_root_logging(self.userkey, self.subdir, self.filename_prefix, self.default_level)
        self.logger = logger
        self.log_file_path = path
        return logger

    def get_logger(self) -> logging.Logger:
        if self.logger is None:
            self.setup_logging()
        return self.logger  # type: ignore

    def get_log_file_path(self) -> Optional[str]:
        return self.log_file_path
    
    def _flush_handlers(self):
        """强制刷新所有日志处理器"""
        if self.logger:
            for handler in self.logger.handlers:
                if hasattr(handler, 'flush'):
                    handler.flush()

    def _ensure_logger(self) -> logging.Logger:
        """确保logger已初始化并返回"""
        if self.logger is None:
            self.setup_logging()
        return self.logger  # type: ignore

    def info(self, message):
        """带自动刷新的info日志"""
        logger = self._ensure_logger()
        logger.info(message)
        self._flush_handlers()

    def warning(self, message):
        """带自动刷新的warning日志"""
        logger = self._ensure_logger()
        logger.warning(message)
        self._flush_handlers()

    def error(self, message):
        """带自动刷新的error日志"""
        logger = self._ensure_logger()
        logger.error(message)
        self._flush_handlers()

    def log_program_start(self, params_dict=None):
        logger = self.get_logger()
        logger.info("=" * 50)
        logger.info("程序开始执行")
        logger.info(f"用户键: {self.userkey}")
        logger.info(f"日志文件: {self.log_file_path}")
        for key, value in (params_dict or {}).items():
            logger.info(f"参数 {key}: {value}")
        logger.info("=" * 50)

    def log_program_end(self):
        logger = self.get_logger()
        logger.info("=" * 50)
        logger.info("程序执行结束")
        logger.info(f"完整日志已保存到: {self.log_file_path}")
        logger.info("=" * 50)


def init_logger(userkey: str, subdir: Optional[str] = None) -> Tuple[logging.Logger, WQLogger]:
    """Init and return logger with centralized format/level policy.
    Level can be overridden via env WQ_LOG_LEVEL.
    """
    wq_logger = WQLogger(userkey, subdir=subdir)
    return wq_logger.get_logger(), wq_logger


def quick_setup(
    userkey: str,
    params_dict=None,
    subdir: Optional[str] = None,
    filename_prefix: str = "run_log",
) -> Tuple[logging.Logger, WQLogger]:
    wq_logger = WQLogger(userkey, subdir=subdir, filename_prefix=filename_prefix)
    logger = wq_logger.get_logger()
    if params_dict:
        wq_logger.log_program_start(params_dict)
    return logger, wq_logger


def get_logger(name: Optional[str] = None, level: Optional[int] = None) -> logging.Logger:
    """Provide a project-wide logger accessor compatible with lightweight adapters.
    - If root logger has no handlers yet, attach a stdout handler with centralized format.
    - Level resolves to env WQ_LOG_LEVEL, or the provided level, default INFO.
    - Returning a named logger allows per-module names while sharing root handlers.
    """
    # resolve level (env override wins when no explicit level supplied)
    env_level = os.environ.get("WQ_LOG_LEVEL")
    resolved_level = (
        level
        if level is not None
        else (getattr(logging, env_level.upper(), logging.INFO) if env_level else logging.INFO)
    )

    root_logger = logging.getLogger()
    if not root_logger.handlers:
        # lazy minimal setup to avoid per-module duplicate handlers
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(resolved_level)
        console_handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
        
        # Add auto-flush for real-time console output
        original_emit = console_handler.emit
        def emit_with_flush(record):
            original_emit(record)
            if console_handler.stream:
                try:
                    console_handler.stream.flush()
                except (OSError, AttributeError):
                    pass
        console_handler.emit = emit_with_flush
        
        root_logger.setLevel(resolved_level)
        root_logger.addHandler(console_handler)

    logger = logging.getLogger(name) if name else root_logger
    if level is not None:
        logger.setLevel(resolved_level)
    return logger