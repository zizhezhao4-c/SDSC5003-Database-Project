import os
import json
import time
import getpass
import requests
from pathlib import Path
from urllib.parse import urljoin, urlparse
from typing import Optional

__all__ = [
    'WQ_DRIVE', 'WQ_LOGS_ROOT', 'WQ_DATA_ROOT', 
    'BRAIN_API_URL', 'brain_api_url', 'USER_KEY',
    'MIN_REMAINING_SECONDS', 'COOKIES_FOLDER_PATH', 'COOKIE_FILE_PATH',
    'get_credentials', 'check_session_validity', 'save_cookie_to_file',
    'perform_full_login', 'perform_full_login_with_retry_limit',
    'start_session', 'check_session_timeout', 'clear_credentials',
]

# === 统一常量定义区域 ===
# 项目根目录（wq_shared 的父目录，即 Group Assignment）
_PROJECT_ROOT = Path(__file__).resolve().parent.parent

# 默认数据和日志根目录 - 使用项目目录下的 WorldQuant/
WQ_DRIVE = Path(os.environ.get("WQ_DRIVE", str(_PROJECT_ROOT)))
WQ_DATA_ROOT = Path(os.environ.get("WQ_DATA_ROOT", str(_PROJECT_ROOT / "WorldQuant" / "Data")))
WQ_LOGS_ROOT = Path(os.environ.get("WQ_LOGS_ROOT", str(_PROJECT_ROOT / "WorldQuant" / "Logs")))

# 使用项目统一的日志系统
try:
    import wq_logger
    logger = wq_logger.get_logger(__name__)
except ImportError:
    import logging as _logging
    import time as _time
    # 统一为UTC并添加 Z 标记
    _logging.Formatter.converter = _time.gmtime
    if not _logging.getLogger().handlers:
        _logging.basicConfig(
            level=_logging.INFO,
            format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%SZ'
        )
    logger = _logging.getLogger(__name__)

# API配置
BRAIN_API_URL = os.environ.get("brain_api_url", "https://api.worldquantbrain.com")
brain_api_url = BRAIN_API_URL

# 用户配置
USER_KEY = 'test'

# 默认会话最小剩余时间（秒）
MIN_REMAINING_SECONDS = 600

COOKIES_FOLDER_PATH = os.path.join(os.path.expanduser("~"), "secrets", "wq_cookies")
os.makedirs(COOKIES_FOLDER_PATH, exist_ok=True)
COOKIE_FILE_PATH = os.path.join(COOKIES_FOLDER_PATH, f"{USER_KEY}_session_cookie.json")

def get_credentials():
    """
    获取平台凭证（优先环境变量，其次本地文件，否则交互输入）。
    """
    credential_email = os.environ.get('BRAIN_CREDENTIAL_EMAIL')
    credential_password = os.environ.get('BRAIN_CREDENTIAL_PASSWORD')

    credentials_folder_path = os.path.join(os.path.expanduser("~"), "secrets")
    credentials_file_path = os.path.join(credentials_folder_path, f"{USER_KEY}_platform-brain.json")

    if (
        Path(credentials_file_path).exists()
        and os.path.getsize(credentials_file_path) > 2
    ):
        with open(credentials_file_path) as file:
            data = json.loads(file.read())
    else:
        os.makedirs(credentials_folder_path, exist_ok=True)
        if credential_email and credential_password:
            email = credential_email
            password = credential_password
        else:
            email = input("Email:\n")
            password = getpass.getpass(prompt="Password:")
        data = {"email": email, "password": password}
        with open(credentials_file_path, "w") as file:
            json.dump(data, file)
    return (data["email"], data["password"])

# --- wq_login.py ---
# 请用这个新版本替换旧的 check_session_validity 函数

def check_session_validity(s: requests.Session, min_remaining_seconds: int = 60) -> bool:
    """
    检查当前会话是否有效。
    - 增加重试逻辑来处理网络抖动 (如 SSLError)。
    - 只有在多次网络尝试失败，或明确收到401/过期响应时，才认为会话无效。
    - 当剩余有效期大于 min_remaining_seconds 时，才视为"满足需求的有效会话"。
    """
    logger.info("正在检查已缓存会话的有效性...")
    authentication_url = f"{brain_api_url}/authentication"
    
    max_retries = 5  # 最多重试5次
    base_delay = 5  # 线性退避：5/10/15/20/25 秒

    for attempt in range(max_retries):
        try:
            response = s.get(authentication_url, timeout=15)  # 增加超时时间

            # 如果收到 401 Unauthorized，说明 cookie 确定无疑无效的
            if response.status_code == 401:
                logger.warning("会话明确无效 (收到 401 Unauthorized)。")
                return False

            response.raise_for_status()  # 检查其他HTTP错误 (如 500, 502)
            
            expiry_seconds = response.json().get("token", {}).get("expiry", 0)
            if expiry_seconds and expiry_seconds > min_remaining_seconds:
                logger.info(f"会话有效，剩余时间: {int(expiry_seconds)} 秒。")
                return True
            else:
                logger.info("会话已过期或即将过期。")
                return False
        
        # --- 这是关键的修改 ---
        # 只捕获网络连接相关的异常进行重试
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"检查会话时发生网络错误 (第 {attempt + 1}/{max_retries} 次): {e}")
            if attempt < max_retries - 1:
                time.sleep(base_delay * (attempt + 1))
            continue  # 继续下一次重试

        # 捕获其他类型的异常（如超时、JSON解析错误），这些不应该重试
        except (requests.exceptions.RequestException, KeyError, json.JSONDecodeError) as e:
            logger.warning(f"检查会话时发生决定性错误: {e}")
            return False  # 遇到这些错误直接判定为失败

    # 如果五次尝试后仍因网络问题无法验证，最终放弃
    logger.warning("因持续的网络错误，无法验证会话有效性。")
    return False


def save_cookie_to_file(s: requests.Session):
    """保存requests会话中的cookie到文件。"""
    try:
        with open(COOKIE_FILE_PATH, 'w') as f:
            json.dump(s.cookies.get_dict(), f)
        logger.info(f"会话 Cookie 已成功保存至 {COOKIE_FILE_PATH}")
    except IOError as e:
        logger.warning(f"无法保存 Cookie 文件: {e}")


def perform_full_login_with_retry_limit(max_credential_retries: int = 2) -> requests.Session:
    """
    执行完整登录流程，带有凭据重试限制，避免无限递归。
    
    Args:
        max_credential_retries: 最大凭据重试次数，防止无限递归
    """
    if max_credential_retries <= 0:
        raise Exception("凭据错误次数过多，请检查您的邮箱和密码后重新运行程序。")
    
    logger.info(f"需要执行完整登录流程... (剩余凭据重试次数: {max_credential_retries})")
    s = requests.Session()
    s.auth = get_credentials()

    max_retries = 5
    initial_backoff = 5  # 初始等待5秒

    for attempt in range(max_retries):
        try:
            logger.info(f"正在尝试登录 (第 {attempt + 1}/{max_retries} 次)...")
            response = s.post(f'{brain_api_url}/authentication', timeout=30)
            
            if response.status_code == 201:
                logger.info("登录成功！")
                save_cookie_to_file(s)
                return s

            elif response.status_code == 401:
                if response.headers.get("WWW-Authenticate") == "persona":
                    persona_url = urljoin(response.url, response.headers.get("Location", ""))
                    # 以下为与用户交互的提示，保留 print
                    print("\n" + "="*50)
                    print("检测到需要生物识别验证。")
                    print("请在浏览器中打开以下链接完成验证，然后按 Enter 键继续：")
                    print(persona_url)
                    print("="*50 + "\n")
                    input()
                    # 循环确认生物识别是否完成
                    while True:
                        confirm_response = s.post(persona_url)
                        if confirm_response.status_code == 201:
                            logger.info("生物识别验证成功！")
                            save_cookie_to_file(s)
                            return s
                        else:
                            input("验证尚未完成或已失败。请确保您已在浏览器中确认，然后按 Enter 键重试。\n")
                else:
                    print("\n邮箱或密码错误。请检查后重试。")  # 与用户交互的提示
                    # 清除错误的凭据，以便下次重新输入
                    credentials_file_path = os.path.join(os.path.expanduser("~"), "secrets", f"{USER_KEY}_platform-brain.json")
                    if os.path.exists(credentials_file_path):
                        os.remove(credentials_file_path)
                    # 使用受保护的递归调用，避免无限递归
                    return perform_full_login_with_retry_limit(max_credential_retries - 1)

            else:
                response.raise_for_status()

        except requests.exceptions.RequestException as e:
            logger.warning(f"登录时发生网络或请求错误: {e}")
            if attempt < max_retries - 1:
                wait_time = initial_backoff * (2 ** attempt)
                logger.info(f"将在 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            else:
                logger.warning("达到最大重试次数，登录失败。")
                raise Exception("无法登录，请检查您的网络连接和凭据。") from e
    
    raise Exception("登录流程未能返回会话，这是一个意外错误。")


def perform_full_login() -> requests.Session:
    """
    执行完整登录流程的公共接口，内部调用带保护的版本。
    """
    return perform_full_login_with_retry_limit(max_credential_retries=2)


def start_session(min_remaining_seconds: Optional[int] = None) -> requests.Session:
    """
    智能获取已认证的 requests 会话对象。
    - 如果存在缓存 Cookie，且其剩余有效期大于 min_remaining_seconds，则直接复用。
    - 否则执行完整登录以获取新会话。
    """
    # 使用默认值如果没有提供参数
    if min_remaining_seconds is None:
        min_remaining_seconds = MIN_REMAINING_SECONDS
    
    if Path(COOKIE_FILE_PATH).is_file():
        logger.info(f"发现已缓存的 Cookie 文件: {COOKIE_FILE_PATH}")
        # 增加重试逻辑以处理文件并发读写导致的空文件或不完整JSON
        for i in range(3):
            try:
                with open(COOKIE_FILE_PATH, 'r') as f:
                    cookies = json.load(f)
                break
            except (json.JSONDecodeError, ValueError):
                if i < 2:
                    time.sleep(0.5)
                else:
                    logger.warning("Cookie 文件损坏或为空，将执行完整登录。")
                    cookies = None
            except Exception:
                cookies = None
                break
        
        if cookies:
            try:
                s = requests.Session()
                s.cookies.update(cookies)
                if check_session_validity(s, min_remaining_seconds=min_remaining_seconds):
                    logger.info("使用缓存的会话成功恢复登录状态。")
                    return s
                else:
                    logger.info("缓存的会话已失效，需要重新登录。")
            except Exception as e:
                 logger.warning(f"恢复会话失败: {e}")

    return perform_full_login()


def check_session_timeout(s):
    """
    Function checks session time out

    json_example = 
{
  "user": {
    "id": "XX12345"
  },
  "token": {
    "expiry": 23399.262573
  },
  "permissions": [
    "BEFORE_AND_AFTER_PERFORMANCE_V2",
    "BRAIN_LABS",
    "BRAIN_LABS_JUPYTER_LAB",
    "CONSULTANT",
    "MULTI_SIMULATION",
    "PROD_ALPHAS",
    "REFERRAL",
    "SUPER_ALPHA",
    "VISUALIZATION",
    "WORKDAY"
  ]
}
    """
    
    authentication_url = brain_api_url + "/authentication"
    try:
        response = s.get(authentication_url, timeout=15)
        response.raise_for_status()
        result = response.json().get("token", {}).get("expiry")
        return result
    except (requests.exceptions.RequestException, KeyError, json.JSONDecodeError) as e:
        logger.warning(f"检查会话超时时出错: {e}")
        return 0


def clear_credentials():
    """
    清除存储的凭证文件和会话cookie。
    """
    files_cleared = 0
    
    # 清除凭证文件
    credentials_file_path = os.path.join(os.path.expanduser("~"), "secrets", f"{USER_KEY}_platform-brain.json")
    credentials_file = Path(credentials_file_path)
    
    try:
        if credentials_file.exists():
            credentials_file.unlink()
            logger.info(f"凭证文件已清除: {credentials_file_path}")
            files_cleared += 1
        else:
            logger.info(f"未找到凭证文件: {credentials_file_path}")
    except (OSError, PermissionError) as e:
        logger.warning(f"清除凭证文件时出错: {e}")
    
    # 清除会话cookie文件
    cookie_file = Path(COOKIE_FILE_PATH)
    
    try:
        if cookie_file.exists():
            cookie_file.unlink()
            logger.info(f"会话cookie文件已清除: {COOKIE_FILE_PATH}")
            files_cleared += 1
        else:
            logger.info(f"未找到会话cookie文件: {COOKIE_FILE_PATH}")
    except (OSError, PermissionError) as e:
        logger.warning(f"清除会话cookie文件时出错: {e}")
    
    if files_cleared > 0:
        logger.info(f"总共清除了 {files_cleared} 个文件。下次使用时需要重新登录。")
    else:
        logger.info("没有找到需要清除的文件。")