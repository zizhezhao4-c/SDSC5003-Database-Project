#!/usr/bin/env python3
"""
Single Alpha Sync Tool
针对单个或列表中的Alpha进行精准爬取、智能合并更新与文件备份。

改进版本：
- 保存 JSON 到 final_data/ 目录（和主流程一致）
- 更新 efficient_state_manager 状态追踪（让增量检测能感知）
- 安全更新 MongoDB（智能合并，不毁库）
"""

import json
import hashlib
import os
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from pymongo import MongoClient

# 复用现有模块
import wq_login
from session_manager import SessionManager
from session_proxy import SessionProxy
from wq_logger import get_logger

# 尝试导入 efficient_state_manager
try:
    from efficient_state_manager import EnhancedStateManager, AlphaRecord
    STATE_MANAGER_AVAILABLE = True
except ImportError:
    STATE_MANAGER_AVAILABLE = False

# 尝试导入时区工具
try:
    from timezone_utils import get_file_date_from_utc_timestamp, convert_eastern_to_utc
    TIMEZONE_UTILS_AVAILABLE = True
except ImportError:
    TIMEZONE_UTILS_AVAILABLE = False

# 初始化日志
logger = get_logger('SingleAlphaSync')

# ============================================================================
# 核心工具函数：智能合并 (Deep Merge)
# ============================================================================

def deep_merge_dicts(old_dict: dict, new_dict: dict) -> dict:
    """深度递归合并两个字典。"""
    if not isinstance(old_dict, dict) or not isinstance(new_dict, dict):
        return new_dict
    
    result = old_dict.copy()
    
    for key, new_value in new_dict.items():
        if key not in result:
            result[key] = new_value
        else:
            old_value = result[key]
            if isinstance(old_value, dict) and isinstance(new_value, dict):
                result[key] = deep_merge_dicts(old_value, new_value)
            elif isinstance(old_value, list) and isinstance(new_value, list):
                result[key] = merge_arrays(old_value, new_value)
            else:
                result[key] = new_value
    return result


def merge_arrays(old_array: list, new_array: list) -> list:
    """智能合并两个数组。"""
    if not new_array: return old_array
    if not old_array: return new_array
    
    first_new = new_array[0]
    first_old = old_array[0]
    
    if not isinstance(first_new, dict) and not isinstance(first_old, dict):
        return new_array
    
    unique_key = None
    if isinstance(first_old, dict) and isinstance(first_new, dict):
        if 'id' in first_old and 'id' in first_new:
            unique_key = 'id'
        elif 'name' in first_old and 'name' in first_new:
            unique_key = 'name'
    
    if unique_key:
        items_dict = {}
        for item in old_array:
            if isinstance(item, dict) and unique_key in item:
                items_dict[item[unique_key]] = item.copy()
        
        for item in new_array:
            if isinstance(item, dict) and unique_key in item:
                item_key = item[unique_key]
                if item_key in items_dict:
                    items_dict[item_key] = deep_merge_dicts(items_dict[item_key], item)
                else:
                    items_dict[item_key] = item.copy()
        return list(items_dict.values())
    else:
        return new_array


# ============================================================================
# 数据库管理 (MongoDB)
# ============================================================================

class MongoManager:
    """MongoDB 管理器"""
    def __init__(self, host='localhost', port=27017):
        self.client = MongoClient(host, port)
        self.db_regular = self.client['regular_alphas']
        self.db_super = self.client['super_alphas']
        self.col_regular = self.db_regular[wq_login.USER_KEY]
        self.col_super = self.db_super[wq_login.USER_KEY]
        
    def get_alpha(self, alpha_id: str, alpha_type: str = 'REGULAR') -> Optional[dict]:
        collection = self.col_super if alpha_type == 'SUPER' else self.col_regular
        doc = collection.find_one({'id': alpha_id})
        if doc: 
            doc.pop('_id', None)
        return doc
        
    def save_alpha(self, alpha_id: str, data: dict, alpha_type: str = 'REGULAR'):
        collection = self.col_super if alpha_type == 'SUPER' else self.col_regular
        collection.update_one({'id': alpha_id}, {'$set': data}, upsert=True)


# ============================================================================
# 文件路径工具
# ============================================================================

def get_file_date_for_alpha(alpha: dict) -> str:
    """根据 Alpha 的时间戳确定文件日期（考虑时区转换）"""
    date_created = alpha.get('dateCreated')
    
    if not date_created:
        # 没有时间戳，使用当前日期
        return datetime.now().strftime('%Y-%m-%d')
    
    if TIMEZONE_UTILS_AVAILABLE:
        try:
            if date_created.endswith('Z'):
                return get_file_date_from_utc_timestamp(date_created)
            else:
                utc_dt = convert_eastern_to_utc(date_created)
                return get_file_date_from_utc_timestamp(utc_dt.isoformat() + 'Z')
        except Exception as e:
            logger.debug(f"时区转换失败: {e}")
    
    # 回退：直接解析日期部分
    try:
        date_part = date_created[:10]  # YYYY-MM-DD
        datetime.strptime(date_part, '%Y-%m-%d')  # 验证格式
        return date_part
    except:
        return datetime.now().strftime('%Y-%m-%d')


def build_final_data_path(alpha: dict, user_key: str, data_root: Path) -> Path:
    """
    构建 final_data 目录路径（和主流程一致）
    
    格式: final_data/{user_key}_{YYYY-MM}_all_{type}_alphas/{user_key}_{YYYY-MM-DD}_all_{type}_alphas/
    """
    file_date = get_file_date_for_alpha(alpha)
    file_dt = datetime.strptime(file_date, '%Y-%m-%d')
    
    alpha_type = alpha.get('type', 'REGULAR').lower()
    
    month_folder = f"{user_key}_{file_dt:%Y-%m}_all_{alpha_type}_alphas"
    day_folder = f"{user_key}_{file_dt:%Y-%m-%d}_all_{alpha_type}_alphas"
    
    target_dir = data_root / "final_data" / month_folder / day_folder
    target_dir.mkdir(parents=True, exist_ok=True)
    
    return target_dir


def generate_filename(alpha: dict, user_key: str) -> str:
    """
    生成文件名（和主流程一致）
    
    格式: {user_key}_{alpha_id}_{hash12}.json
    """
    alpha_id = alpha.get('id', 'unknown')
    
    # 生成 12 位哈希（基于 alpha 内容）
    content_str = json.dumps(alpha, sort_keys=True, ensure_ascii=False)
    content_hash = hashlib.md5(content_str.encode('utf-8')).hexdigest()[:12]
    
    return f"{user_key}_{alpha_id}_{content_hash}.json"


# ============================================================================
# 同步管理器
# ============================================================================

class SingleAlphaSyncer:
    def __init__(self, alpha_ids: List[str]):
        self.alpha_ids = alpha_ids
        self.session_manager = SessionManager()
        self.session_proxy = SessionProxy(self.session_manager)
        
        self.user_key = wq_login.USER_KEY
        self.data_root = wq_login.WQ_DATA_ROOT / self.user_key
        
        # MongoDB 管理器
        self.mongo_manager = MongoManager()
        
        # State Manager（和主流程共用）
        self.state_manager = None
        if STATE_MANAGER_AVAILABLE:
            state_dir = Path(wq_login.WQ_LOGS_ROOT) / self.user_key / "fetch_and_import_alphas"
            state_dir.mkdir(parents=True, exist_ok=True)
            self.state_manager = EnhancedStateManager(
                state_dir=state_dir,
                db_name="alpha_state_v2.db",
                enable_content_hash=False
            )
            logger.info(f"State Manager 已初始化: {state_dir}")
        else:
            logger.warning("State Manager 不可用，增量检测将无法感知同步的文件")
        
        # 记录已保存的文件路径（用于更新 state manager）
        self.saved_files: List[Tuple[str, str]] = []  # [(file_path, alpha_id), ...]
        
        logger.info(f"初始化同步器: 目标 {len(alpha_ids)} 个 Alphas")
        logger.info(f"数据目录: {self.data_root / 'final_data'}")

    def fetch_alpha_data(self, alpha_id: str) -> Optional[dict]:
        """从 API 获取 Alpha 数据"""
        url = f"https://api.worldquantbrain.com/alphas/{alpha_id}"
        try:
            response = self.session_proxy.get(url)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                logger.warning(f"Alpha {alpha_id} 未找到 (404)")
                return None
            else:
                logger.error(f"获取 Alpha {alpha_id} 失败: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"请求异常 {alpha_id}: {e}")
            return None

    def process_alpha(self, alpha_id: str) -> bool:
        """处理单个 Alpha"""
        try:
            # 1. 从 API 获取 Alpha 数据
            new_data = self.fetch_alpha_data(alpha_id)
            if not new_data:
                return False
            
            alpha_type = new_data.get('type', 'REGULAR')
            
            # 2. 保存到 final_data 目录（保存 API 原始数据，和主流程一致）
            file_path = self._save_to_final_data(alpha_id, new_data)
            if file_path:
                self.saved_files.append((file_path, alpha_id))
            
            # 3. 从 MongoDB 读取旧数据，智能合并
            old_data = self.mongo_manager.get_alpha(alpha_id, alpha_type)
            
            if old_data:
                merged_data = deep_merge_dicts(old_data, new_data)
                logger.info(f"[{alpha_id}] 智能合并完成 (基准: MongoDB)")
            else:
                merged_data = new_data
                logger.info(f"[{alpha_id}] 首次获取")
            
            # 4. 更新 MongoDB（智能合并后的数据，不毁库）
            self.mongo_manager.save_alpha(alpha_id, merged_data, alpha_type)
            
            return True
        except Exception as e:
            logger.error(f"处理 Alpha {alpha_id} 异常: {e}")
            return False

    def _save_to_final_data(self, alpha_id: str, data: dict) -> Optional[str]:
        """
        保存到 final_data 目录（和主流程一致的路径结构）
        
        Returns:
            保存的文件路径，失败返回 None
        """
        try:
            # 构建目标目录
            target_dir = build_final_data_path(data, self.user_key, self.data_root)
            
            # 生成文件名
            filename = generate_filename(data, self.user_key)
            file_path = target_dir / filename
            
            # 检查是否已存在同 alpha_id 的文件（可能哈希不同）
            # 如果存在，先删除旧文件
            existing_files = list(target_dir.glob(f"{self.user_key}_{alpha_id}_*.json"))
            for old_file in existing_files:
                if old_file != file_path:
                    old_file.unlink()
                    logger.debug(f"删除旧文件: {old_file.name}")
            
            # 原子写入
            temp_path = file_path.with_suffix('.tmp')
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            if file_path.exists():
                file_path.unlink()
            temp_path.rename(file_path)
            
            logger.info(f"[{alpha_id}] 已保存: {file_path.relative_to(self.data_root)}")
            return str(file_path)
            
        except Exception as e:
            logger.error(f"保存文件失败 {alpha_id}: {e}")
            return None

    def _update_state_manager(self):
        """更新 State Manager（让增量检测能感知）"""
        if not self.state_manager or not self.saved_files:
            return
        
        logger.info(f"更新 State Manager: {len(self.saved_files)} 个文件")
        
        records = []
        for file_path, alpha_id in self.saved_files:
            try:
                stat = Path(file_path).stat()
                record = AlphaRecord(
                    alpha_id=alpha_id,
                    file_path=file_path,
                    file_size=stat.st_size,
                    file_mtime=stat.st_mtime,
                    content_hash=None
                )
                records.append(record)
            except Exception as e:
                logger.warning(f"无法创建记录 {alpha_id}: {e}")
        
        if records:
            # 检查哪些是新增，哪些是更新
            existing_ids = set()
            alpha_ids = [r.alpha_id for r in records]
            
            import sqlite3
            try:
                with sqlite3.connect(self.state_manager.db_path) as conn:
                    batch_size = 900
                    for i in range(0, len(alpha_ids), batch_size):
                        batch = alpha_ids[i:i + batch_size]
                        placeholders = ','.join(['?'] * len(batch))
                        cursor = conn.execute(f"""
                            SELECT alpha_id FROM alpha_records 
                            WHERE alpha_id IN ({placeholders})
                        """, batch)
                        existing_ids.update(row[0] for row in cursor)
            except Exception as e:
                logger.warning(f"查询现有记录失败: {e}")
            
            new_records = [r for r in records if r.alpha_id not in existing_ids]
            update_records = [r for r in records if r.alpha_id in existing_ids]
            
            if new_records:
                self.state_manager.mark_processed(new_records, is_update=False)
                logger.info(f"State Manager: 新增 {len(new_records)} 条记录")
            if update_records:
                self.state_manager.mark_processed(update_records, is_update=True)
                logger.info(f"State Manager: 更新 {len(update_records)} 条记录")

    def run(self):
        """运行同步"""
        self.session_manager.get_session()
        success_count = 0
        failed_count = 0
        max_workers = min(5, len(self.alpha_ids))
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_id = {
                executor.submit(self.process_alpha, aid): aid 
                for aid in self.alpha_ids
            }
            for future in as_completed(future_to_id):
                try:
                    if future.result():
                        success_count += 1
                    else:
                        failed_count += 1
                except Exception:
                    failed_count += 1
        
        # 更新 State Manager
        self._update_state_manager()
        
        # 关闭 State Manager
        if self.state_manager:
            self.state_manager.close()
        
        logger.info(f"同步完成: 成功 {success_count}, 失败 {failed_count}")


def load_alpha_ids_from_file(file_path: str) -> List[str]:
    """从 JSON 文件加载 Alpha ID 列表"""
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 支持两种格式: 纯列表 ["id1", "id2"] 或 {"alpha_ids": ["id1", "id2"]}
    if isinstance(data, list):
        return data
    elif isinstance(data, dict) and 'alpha_ids' in data:
        return data['alpha_ids']
    else:
        raise ValueError(f"无效的配置文件格式: {file_path}")


if __name__ == "__main__":
    import argparse
    import sys
    
    # 默认配置文件路径
    SCRIPT_DIR = Path(__file__).parent
    DEFAULT_CONFIG = SCRIPT_DIR / "target_alpha_ids_to_sync.json"
    
    parser = argparse.ArgumentParser(
        description='Single Alpha Sync Tool - 精准同步指定的 Alpha',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 直接传入 Alpha ID
  python sync_single_alphas.py eaqag9g vAJZrqA EQgYVM1
  
  # 从配置文件读取
  python sync_single_alphas.py --file my_alphas.json
  
  # 使用默认配置文件 (target_alpha_ids_to_sync.json)
  python sync_single_alphas.py
  
配置文件格式 (JSON):
  ["id1", "id2", "id3"]
  或
  {"alpha_ids": ["id1", "id2", "id3"]}

数据保存位置:
  - JSON 文件: {WQ_DATA_ROOT}/{USER_KEY}/final_data/{月}/{日}/
  - MongoDB: regular_alphas / super_alphas 数据库
  - State: {WQ_LOGS_ROOT}/{USER_KEY}/fetch_and_import_alphas/alpha_state_v2.db
        """
    )
    parser.add_argument('alpha_ids', nargs='*', help='要同步的 Alpha ID 列表')
    parser.add_argument('--file', '-f', help=f'包含 Alpha ID 列表的 JSON 文件 (默认: {DEFAULT_CONFIG.name})')
    
    args = parser.parse_args()
    
    # 确定 Alpha ID 来源
    alpha_ids = []
    
    if args.alpha_ids:
        alpha_ids = args.alpha_ids
        logger.info(f"从命令行读取 {len(alpha_ids)} 个 Alpha ID")
    elif args.file:
        alpha_ids = load_alpha_ids_from_file(args.file)
        logger.info(f"从文件 {args.file} 读取 {len(alpha_ids)} 个 Alpha ID")
    elif DEFAULT_CONFIG.exists():
        alpha_ids = load_alpha_ids_from_file(str(DEFAULT_CONFIG))
        logger.info(f"从默认配置 {DEFAULT_CONFIG.name} 读取 {len(alpha_ids)} 个 Alpha ID")
    else:
        print("错误: 未指定 Alpha ID")
        print(f"用法: python {Path(__file__).name} <alpha_id1> <alpha_id2> ...")
        print(f"  或: python {Path(__file__).name} --file <config.json>")
        print(f"  或: 创建默认配置文件 {DEFAULT_CONFIG}")
        sys.exit(1)
    
    if not alpha_ids:
        print("错误: Alpha ID 列表为空")
        sys.exit(1)
    
    syncer = SingleAlphaSyncer(alpha_ids)
    syncer.run()
