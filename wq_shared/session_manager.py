import time
import threading
# 轻量 logger 适配：优先使用上级共享的 wq_logger，否则回退到标准 logging
try:
    from wq_logger import get_logger as _get_logger
    logger = _get_logger(__name__)
except Exception:
    import logging as _logging
    if not _logging.getLogger().handlers:
        _logging.basicConfig(level=_logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    logger = _logging.getLogger(__name__)

from datetime import datetime, timedelta
import wq_login
from typing import Optional
import requests

__all__ = ['SessionManager']

class SessionManager:
    """
    智能会话管理器，避免频繁检查会话有效性
    
    优化特性：
    - 快速路径：大部分情况下无锁检查session有效性
    - 慢速路径：只在需要刷新时才获取锁
    - 双重检查锁定：避免多线程重复刷新
    - 可重入锁：支持同一线程多次获取锁
    
    线程安全策略：
    1. 读取session：快速路径，无锁检查（如果session有效）
    2. 刷新session：慢速路径，获取锁后双重检查
    3. 使用RLock：允许同一线程递归调用
    """
    
    def __init__(self):
        self.session: Optional[requests.Session] = None
        self.session_expiry_time: Optional[float] = None
        self.last_check_time: Optional[float] = None
        self.buffer_time: int = 1800  # 保留30分钟的缓冲时间，确保分页过程中的旧session有足够时间完成
        self._lock = threading.RLock()  # 使用可重入锁，支持递归调用
        logger.debug("SessionManager initialized with 30-minute buffer and thread-safe lock.")

    def get_session(self) -> requests.Session:
        """
        获取有效的会话，只在必要时检查或刷新
        
        优化策略：
        1. 快速路径（无锁）：如果session存在且有效，直接返回
        2. 慢速路径（有锁）：需要刷新时才获取锁，使用双重检查避免重复刷新
        
        性能：
        - 快速路径：~15ns（无锁检查 + 返回引用）
        - 慢速路径：~100ms（登录API调用）
        - 大部分请求走快速路径，性能影响可忽略
        
        Returns:
            requests.Session: 有效的session对象
        """
        current_time = time.time()
        
        # ===== 快速路径：无锁检查 =====
        # 如果session存在且有预估的过期时间
        if self.session is not None and self.session_expiry_time is not None:
            remaining_time = self.session_expiry_time - current_time
            
            # 如果剩余时间充足（超过缓冲时间），直接返回
            if remaining_time > self.buffer_time:
                # 偶尔记录一下状态（避免日志过多）
                if remaining_time > 3600:  # 如果剩余时间超过1小时
                    if not self.last_check_time or (current_time - self.last_check_time) > 1800:  # 每30分钟记录一次
                        logger.info(f"会话仍然有效，剩余时间: {int(remaining_time/60)} 分钟")
                        self.last_check_time = current_time
                return self.session
        
        # ===== 慢速路径：需要刷新，获取锁 =====
        with self._lock:
            # 双重检查：可能其他线程已经刷新了session
            if self.session is not None and self.session_expiry_time is not None:
                remaining_time = self.session_expiry_time - current_time
                if remaining_time > self.buffer_time:
                    logger.debug("其他线程已刷新session，直接使用")
                    return self.session
            
            # 确实需要刷新
            if self.session is None:
                logger.info("会话不存在，创建新会话……")
            elif self.session_expiry_time is None:
                logger.info("会话无过期时间信息，检查有效性……")
                return self._check_and_refresh_if_needed()
            else:
                remaining_time = self.session_expiry_time - current_time
                logger.warning(f"会话剩余时间不足 {int(remaining_time/60)} 分钟（{int(remaining_time)}秒），主动刷新会话……")
            
            return self._create_new_session()

    def _update_expiry_time(self, verbose: bool = True):
        """更新会话过期时间
        
        Args:
            verbose: 是否输出详细日志（默认True）
        """
        try:
            expiry_seconds = wq_login.check_session_timeout(self.session)
            if expiry_seconds and expiry_seconds > 0:
                self.session_expiry_time = time.time() + expiry_seconds
                if verbose:
                    logger.info(
                        f"会话有效，剩余时间: {int(expiry_seconds/3600)} 小时 {int((expiry_seconds%3600)/60)} 分钟 ({int(expiry_seconds)}秒)"
                    )
            else:
                # 设置保守估计
                self.session_expiry_time = time.time() + 6 * 3600
                if verbose:
                    logger.warning("无法获取会话剩余时间，使用默认6小时")
            self.last_check_time = time.time()
        except Exception as e:
            if verbose:
                logger.warning(f"获取会话剩余时间失败，使用默认6小时: {e}")
            self.session_expiry_time = time.time() + 6 * 3600
            self.last_check_time = time.time()

    def _create_new_session(self) -> requests.Session:
        """
        创建新会话并获取过期时间
        
        注意：此方法必须在锁内调用，确保线程安全
        
        Returns:
            requests.Session: 新创建的session对象
        
        Raises:
            Exception: 创建会话失败时抛出
        """
        try:
            logger.info("初始化会话（准备获取可用会话）……")
            # 使用30分钟阈值，避免拿到"即将过期"的旧会话
            self.session = wq_login.start_session(min_remaining_seconds=self.buffer_time)
            
            # 更新过期时间
            self._update_expiry_time()
            logger.info("会话就绪（来源：缓存或登录）")
            return self.session
            
        except Exception as e:
            logger.error(f"创建会话失败: {e}", exc_info=True)
            self.session = None
            self.session_expiry_time = None
            raise

    def _check_and_refresh_if_needed(self) -> requests.Session:
        """
        检查会话有效性，如果需要则刷新
        
        注意：此方法必须在锁内调用，确保线程安全
        
        Returns:
            requests.Session: 有效的session对象
        """
        try:
            # 使用30分钟阈值验证有效性
            if self.session is not None and wq_login.check_session_validity(self.session, min_remaining_seconds=self.buffer_time):
                # 会话有效，获取并更新过期时间
                self._update_expiry_time()
                return self.session
            else:
                # 会话无效，创建新会话
                logger.info("会话剩余时间不足30分钟，创建新会话……")
                return self._create_new_session()
                
        except Exception as e:
            logger.error(f"检查会话时出错: {e}", exc_info=True)
            return self._create_new_session()

    def force_refresh(self) -> requests.Session:
        """
        强制刷新会话
        
        线程安全：使用锁确保只有一个线程执行刷新操作
        
        Returns:
            requests.Session: 新创建的session对象
        """
        logger.info("强制刷新会话……")
        with self._lock:
            return self._create_new_session()

    def update_session(self, new_session: requests.Session, force_check: bool = False) -> requests.Session:
        """
        在外部（如API调用返回了新的Session）更新共享会话，并同步过期时间估计
        
        注意：使用SessionProxy后，此方法通常不再需要，保留是为了向后兼容
        
        线程安全：使用锁确保更新操作的原子性
        
        Args:
            new_session: 新的会话对象
            force_check: 是否强制检查过期时间（默认False，避免频繁检查）
        
        Returns:
            requests.Session: 更新后的session对象
        """
        with self._lock:
            self.session = new_session
            
            # 只在强制检查或没有过期时间信息时才更新
            if force_check or self.session_expiry_time is None:
                self._update_expiry_time()
                logger.info("已更新共享会话")
            
            return self.session
    
    def get_remaining_time(self) -> float:
        """获取会话剩余时间（秒）"""
        if self.session_expiry_time:
            remaining = self.session_expiry_time - time.time()
            return max(0, remaining)
        return 0.0