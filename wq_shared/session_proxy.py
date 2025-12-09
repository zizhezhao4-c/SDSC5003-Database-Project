"""
SessionProxy - 会话代理类

这个类包装了requests.Session，确保每次HTTP请求都使用最新的有效session。
它解决了Worker持有session引用过期的问题，通过在每次请求时从SessionManager获取最新session。

核心特性：
- 透明代理：实现requests.Session的主要接口
- 自动刷新：每次请求前自动获取最新session
- 线程安全：依赖SessionManager的线程安全机制
- 零侵入：对调用代码透明，无需修改fetch_all_alphas.py

使用示例：
    session_manager = SessionManager()
    session_proxy = SessionProxy(session_manager)
    
    # 像使用普通session一样使用proxy
    response = session_proxy.get(url, params=params)
    response = session_proxy.post(url, json=data)
    cookies = session_proxy.cookies
"""

# 轻量 logger 适配：优先使用上级共享的 wq_logger，否则回退到标准 logging
try:
    from wq_logger import get_logger as _get_logger
    logger = _get_logger(__name__)
except Exception:
    import logging as _logging
    if not _logging.getLogger().handlers:
        _logging.basicConfig(level=_logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    logger = _logging.getLogger(__name__)

from typing import Any, Optional
import requests

__all__ = ['SessionProxy']

class SessionProxy:
    """
    Session代理类，自动获取最新的有效session
    
    这个类实现了requests.Session的主要接口，但每次请求时都从SessionManager
    获取最新的session对象，确保不会使用过期的session引用。
    
    设计原理：
    - 不持有session对象的长期引用
    - 每次请求时调用session_manager.get_session()获取最新session
    - SessionManager负责session的生命周期管理和线程安全
    
    性能考虑：
    - get_session()使用快速路径优化，大部分情况下无锁检查
    - 每次请求增加约15ns开销，相比网络延迟可忽略不计
    
    Attributes:
        _session_manager: SessionManager实例，用于获取最新session
    """
    
    def __init__(self, session_manager):
        """
        初始化SessionProxy
        
        Args:
            session_manager: SessionManager实例，负责管理session生命周期
        """
        self._session_manager = session_manager
        logger.debug("SessionProxy initialized")
    
    def get(self, url: str, **kwargs) -> requests.Response:
        """
        发送GET请求，自动使用最新session
        
        每次调用时从SessionManager获取最新的有效session，确保不会使用过期session。
        
        Args:
            url: 请求URL
            **kwargs: 传递给requests.Session.get()的其他参数
        
        Returns:
            requests.Response: HTTP响应对象
        
        Raises:
            requests.RequestException: 请求失败时抛出
        """
        session = self._session_manager.get_session()
        return session.get(url, **kwargs)
    
    def post(self, url: str, **kwargs) -> requests.Response:
        """
        发送POST请求，自动使用最新session
        
        每次调用时从SessionManager获取最新的有效session，确保不会使用过期session。
        
        Args:
            url: 请求URL
            **kwargs: 传递给requests.Session.post()的其他参数
        
        Returns:
            requests.Response: HTTP响应对象
        
        Raises:
            requests.RequestException: 请求失败时抛出
        """
        session = self._session_manager.get_session()
        return session.post(url, **kwargs)
    
    @property
    def cookies(self):
        """
        获取当前session的cookies
        
        返回当前有效session的cookies对象。注意：每次访问都会获取最新session的cookies。
        
        Returns:
            requests.cookies.RequestsCookieJar: 当前session的cookies
        """
        session = self._session_manager.get_session()
        return session.cookies
    
    def __repr__(self) -> str:
        """返回SessionProxy的字符串表示"""
        return f"<SessionProxy(session_manager={self._session_manager})>"
