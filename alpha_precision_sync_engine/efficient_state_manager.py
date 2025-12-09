#!/usr/bin/env python3
"""
Enhanced State Manager V2
智能状态管理器 - 基于Alpha ID的去重和变更检测

主要改进:
1. 使用 alpha_id 作为主键 (而非文件路径)
2. 检测文件变更 (通过 mtime, size, 可选的 content_hash)
3. 支持智能导入: INSERT (新alpha) 和 UPDATE (已修改alpha)

设计理念:
- Alpha ID 是真正的唯一标识符
- 文件路径可能变化 (重新分解、驱动器号等)
- 需要检测内容变更以支持增量更新
"""

import os
import sqlite3
import hashlib
import json
import time
from pathlib import Path
from typing import Set, List, Optional, Dict, Tuple
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class AlphaStatus(Enum):
    """Alpha处理状态"""
    NEW = "new"              # 新alpha，需要INSERT
    UNCHANGED = "unchanged"  # 未变化，跳过
    MODIFIED = "modified"    # 已修改，需要UPDATE


class AlphaRecord:
    """Alpha记录 - 用于变更检测"""
    
    def __init__(self, alpha_id: str, file_path: str, 
                 file_size: int = 0, file_mtime: float = 0, 
                 content_hash: Optional[str] = None):
        self.alpha_id = alpha_id
        self.file_path = file_path
        self.file_size = file_size
        self.file_mtime = file_mtime
        self.content_hash = content_hash
        self.processed_time = int(time.time())
    
    def has_changed(self, other: 'AlphaRecord', 
                    check_content_hash: bool = False,
                    mtime_tolerance: float = 1.0,
                    size_tolerance: int = 10) -> bool:
        """
        检测是否变更
        
        Args:
            other: 另一条记录（数据库中的旧记录）
            check_content_hash: 是否检查内容哈希（较慢但准确）
            mtime_tolerance: 修改时间容差（秒）
            size_tolerance: 文件大小容差（字节）
            
        Returns:
            True if changed, False if unchanged
        """
        # 优先级1: 内容哈希（如果可用且启用）
        if check_content_hash and self.content_hash and other.content_hash:
            if self.content_hash != other.content_hash:
                return True
            # 哈希相同，确认未变化
            return False
        
        # 优先级2: 文件大小（快速检测）
        # 注意：允许小幅容差，因为JSON格式化可能导致微小差异
        size_diff = abs(self.file_size - other.file_size)
        if size_diff > size_tolerance:
            return True
        
        # 优先级3: 修改时间
        # 注意：某些文件系统精度有限，允许小幅容差
        # 但如果时间变化且文件大小在容差范围内，仍视为可能的变更
        mtime_diff = abs(self.file_mtime - other.file_mtime)
        if mtime_diff > mtime_tolerance:
            # 时间变化了，即使size差异在容差内也认为是变更
            # 这是保守策略：宁可误判为changed，也不要漏掉真正的变更
            return True
        
        # 所有检查都通过，视为未变化
        return False


class EnhancedStateManager:
    """
    增强版状态管理器 - 基于Alpha ID
    
    核心改进:
    1. 使用 alpha_id 作为主键（真正的唯一标识符）
    2. 检测文件变更（mtime, size, content_hash）
    3. 支持 INSERT/UPDATE 决策
    
    数据库schema:
    - alpha_id (TEXT PRIMARY KEY): Alpha的唯一ID
    - file_path (TEXT): 文件路径（可变）
    - file_size (INTEGER): 文件大小
    - file_mtime (REAL): 文件修改时间
    - content_hash (TEXT): 内容MD5哈希（可选）
    - processed_time (INTEGER): 处理时间戳
    - update_count (INTEGER): 更新次数
    """
    
    def __init__(self, state_dir: Path, 
                 db_name: str = "alpha_state_v2.db",
                 enable_content_hash: bool = False):
        """
        Args:
            state_dir: 状态目录
            db_name: 数据库文件名
            enable_content_hash: 是否启用内容哈希（更准确但较慢）
        """
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.state_dir / db_name
        self.enable_content_hash = enable_content_hash
        self._alpha_cache = {}  # Cache for alpha records
        self._init_db()
    
    def _init_db(self):
        """初始化数据库schema"""
        conn = sqlite3.connect(self.db_path)
        try:
            # 主表: alpha记录
            conn.execute("""
                CREATE TABLE IF NOT EXISTS alpha_records (
                    alpha_id TEXT PRIMARY KEY,
                    file_path TEXT NOT NULL,
                    file_size INTEGER NOT NULL,
                    file_mtime REAL NOT NULL,
                    content_hash TEXT,
                    processed_time INTEGER NOT NULL,
                    update_count INTEGER DEFAULT 0
                )
            """)
            
            # 索引: 文件路径（用于路径查询）
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_file_path 
                ON alpha_records(file_path)
            """)
            
            # 索引: 处理时间（用于时间范围查询）
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_processed_time 
                ON alpha_records(processed_time)
            """)
            
            # 索引: 更新次数（用于查询频繁更新的alpha）
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_update_count 
                ON alpha_records(update_count)
            """)
            
            conn.commit()
            
            logger.info(f"Enhanced state manager initialized: {self.db_path}")
        finally:
            conn.close()
        
        # Compile regex pattern once for performance
        import re
        self.alpha_id_pattern = re.compile(r'_([A-Za-z0-9]{7})_[a-f0-9]{12}\.json$')
    
    def _extract_alpha_id_from_filename(self, file_path: str) -> Optional[str]:
        """
        从文件名中提取alpha_id (快速，无需读取文件)
        格式: ..._<alpha_id>_<hash>.json
        
        Args:
            file_path: JSON文件路径
            
        Returns:
            alpha_id 或 None (如果提取失败)
        """
        match = self.alpha_id_pattern.search(file_path)
        return match.group(1) if match else None
    
    def _extract_alpha_id_from_file(self, file_path: str) -> Optional[str]:
        """
        从JSON文件中提取alpha_id (慢速，需要读取文件内容)
        用作 _extract_alpha_id_from_filename 的后备方案
        
        Args:
            file_path: JSON文件路径
            
        Returns:
            alpha_id 或 None (如果提取失败)
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('id')
        except Exception as e:
            logger.warning(f"无法提取alpha_id from {file_path}: {e}")
            return None
    
    def _calculate_content_hash(self, file_path: str) -> Optional[str]:
        """
        计算文件内容的MD5哈希
        
        注意: 这会读取整个文件，对大文件较慢
        """
        if not self.enable_content_hash:
            return None
        
        try:
            with open(file_path, 'rb') as f:
                return hashlib.md5(f.read()).hexdigest()
        except Exception as e:
            logger.warning(f"无法计算content hash for {file_path}: {e}")
            return None
    
    def _create_record_from_file(self, file_path: str, 
                                  alpha_id: Optional[str] = None) -> Optional[AlphaRecord]:
        """
        从文件创建AlphaRecord
        
        Args:
            file_path: JSON文件路径
            alpha_id: Alpha ID (如果已知，避免重复读取)
            
        Returns:
            AlphaRecord 或 None (如果创建失败)
        """
        try:
            path = Path(file_path)
            
            # 获取文件信息
            stat = path.stat()
            file_size = stat.st_size
            file_mtime = stat.st_mtime
            
            # 提取 alpha_id (如果未提供)
            if alpha_id is None:
                alpha_id = self._extract_alpha_id_from_file(file_path)
                if not alpha_id:
                    logger.error(f"无法从文件提取alpha_id: {file_path}")
                    return None
            
            # 计算内容哈希（可选）
            content_hash = self._calculate_content_hash(file_path)
            
            return AlphaRecord(
                alpha_id=alpha_id,
                file_path=str(path),
                file_size=file_size,
                file_mtime=file_mtime,
                content_hash=content_hash
            )
            
        except Exception as e:
            logger.error(f"创建记录失败 {file_path}: {e}")
            return None
    
    def _get_record_from_db(self, alpha_id: str) -> Optional[AlphaRecord]:
        """
        从数据库获取alpha记录
        
        Args:
            alpha_id: Alpha ID
            
        Returns:
            AlphaRecord 或 None (如果不存在)
        """
        # Check cache first
        if alpha_id in self._alpha_cache:
            return self._alpha_cache[alpha_id]
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT alpha_id, file_path, file_size, file_mtime, content_hash
                FROM alpha_records
                WHERE alpha_id = ?
            """, (alpha_id,))
            
            row = cursor.fetchone()
            if row:
                record = AlphaRecord(
                    alpha_id=row[0],
                    file_path=row[1],
                    file_size=row[2],
                    file_mtime=row[3],
                    content_hash=row[4]
                )
                
                # Cache the result (limit cache size)
                if len(self._alpha_cache) < 100000:  # Max 100K entries
                    self._alpha_cache[alpha_id] = record
                
                return record
        
        return None
    
    def check_alpha_status(self, file_path: str, 
                          alpha_id: Optional[str] = None,
                          mtime_tolerance: float = 1.0,
                          size_tolerance: int = 10) -> Tuple[AlphaStatus, Optional[AlphaRecord]]:
        """
        检查alpha状态: NEW, UNCHANGED, 或 MODIFIED
        
        Args:
            file_path: JSON文件路径
            alpha_id: Alpha ID (如果已知)
            mtime_tolerance: 修改时间容差（秒）
            size_tolerance: 文件大小容差（字节）
            
        Returns:
            (status, current_record)
            - status: AlphaStatus枚举
            - current_record: 当前文件的记录（用于后续保存）
        """
        # 创建当前文件的记录
        current_record = self._create_record_from_file(file_path, alpha_id)
        if not current_record:
            # 无法创建记录，可能是文件格式错误
            return AlphaStatus.NEW, None
        
        # 从数据库查询旧记录
        old_record = self._get_record_from_db(current_record.alpha_id)
        
        if old_record is None:
            # 数据库中不存在 -> NEW
            return AlphaStatus.NEW, current_record
        
        # 检查是否变更
        has_changed = current_record.has_changed(
            old_record,
            check_content_hash=self.enable_content_hash,
            mtime_tolerance=mtime_tolerance,
            size_tolerance=size_tolerance
        )
        
        if has_changed:
            return AlphaStatus.MODIFIED, current_record
        else:
            return AlphaStatus.UNCHANGED, current_record
    
    def check_alpha_status_batch(self, file_paths: List[str],
                                 mtime_tolerance: float = 1.0,
                                 size_tolerance: int = 10,
                                 max_workers: int = 32,
                                 chunk_size: int = 50000) -> Dict[str, Tuple[AlphaStatus, Optional[AlphaRecord]]]:
        """
        批量检查alpha状态（优化版本，支持多线程，分块处理避免内存溢出）
        
        Args:
            file_paths: 文件路径列表
            mtime_tolerance: 修改时间容差
            size_tolerance: 文件大小容差
            max_workers: 最大并行线程数（用于文件I/O）
            chunk_size: 每次处理的文件数量，避免内存溢出
            
        Returns:
            {file_path: (status, record)}
        """
        # Process in chunks to avoid memory issues with large batches
        if len(file_paths) > chunk_size:
            logger.info(f"Processing {len(file_paths)} files in chunks of {chunk_size} to manage memory")
            results = {}
            for i in range(0, len(file_paths), chunk_size):
                chunk = file_paths[i:i + chunk_size]
                chunk_results = self._check_alpha_status_batch_internal(
                    chunk, mtime_tolerance, size_tolerance, max_workers
                )
                results.update(chunk_results)
                # Force garbage collection after each chunk
                import gc
                gc.collect()
            return results
        else:
            return self._check_alpha_status_batch_internal(
                file_paths, mtime_tolerance, size_tolerance, max_workers
            )
    
    def _get_file_metadata_helper(self, file_path: str) -> Tuple[str, Tuple[Optional[str], int, float]]:
        """
        Helper method to extract alpha_id and file metadata.
        Separated from nested function to avoid pickling issues with ThreadPoolExecutor.
        
        Returns:
            (file_path, (alpha_id, size, mtime))
        """
        try:
            from pathlib import Path
            # Try filename extraction first (fast, no file I/O)
            alpha_id = self._extract_alpha_id_from_filename(file_path)
            # Fallback to reading file if filename extraction fails
            if not alpha_id:
                alpha_id = self._extract_alpha_id_from_file(file_path)
            stat = Path(file_path).stat()
            return file_path, (alpha_id, stat.st_size, stat.st_mtime)
        except Exception as e:
            logger.warning(f"Failed to get metadata for {file_path}: {e}")
            return file_path, (None, 0, 0)
    
    def _check_alpha_status_batch_internal(self, file_paths: List[str],
                                           mtime_tolerance: float,
                                           size_tolerance: int,
                                           max_workers: int) -> Dict[str, Tuple[AlphaStatus, Optional[AlphaRecord]]]:
        """
        Memory-efficient implementation with smart metadata-first filtering.
        
        Key optimization: Only load old records from SQLite that we need to compare.
        This reduces memory usage by 70-80% for typical incremental runs.
        
        Strategy:
        1. Extract alpha_ids + get metadata for all files (cheap, parallel)
        2. Query SQLite in batches, comparing metadata immediately
        3. Only keep files needing hash calculation in memory
        4. Process hash calculations in parallel for filtered subset
        """
        results = {}
        files_needing_hash = []
        metadata_unchanged_count = 0
        
        # Phase 1: Extract metadata for all files (parallel for speed)
        logger.info(f"Phase 1: Extracting metadata from {len(file_paths)} files...")
        
        file_metadata = {}  # {file_path: (alpha_id, size, mtime)}
        
        # Parallel metadata extraction (I/O bound, fast)
        if len(file_paths) > 100 and max_workers > 1:
            from concurrent.futures import ThreadPoolExecutor, as_completed
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(self._get_file_metadata_helper, fp) for fp in file_paths]
                for future in as_completed(futures):
                    file_path, metadata = future.result()
                    file_metadata[file_path] = metadata
        else:
            for file_path in file_paths:
                file_path, metadata = self._get_file_metadata_helper(file_path)
                file_metadata[file_path] = metadata
        
        # Phase 2: Query SQLite and compare in batches (memory-efficient)
        logger.info(f"Phase 2: Querying SQLite and filtering by metadata...")
        
        # Group files by alpha_id for batch querying
        alpha_id_to_files = {}  # {alpha_id: [file_paths]}
        files_without_alphaid = []
        
        for file_path, (alpha_id, size, mtime) in file_metadata.items():
            if alpha_id:
                if alpha_id not in alpha_id_to_files:
                    alpha_id_to_files[alpha_id] = []
                alpha_id_to_files[alpha_id].append(file_path)
            else:
                files_without_alphaid.append(file_path)
        
        # Query SQLite in batches and process immediately (avoid loading all records)
        alpha_ids_list = list(alpha_id_to_files.keys())
        sqlite_batch_size = 900  # SQLite parameter limit
        
        with sqlite3.connect(self.db_path) as conn:
            for i in range(0, len(alpha_ids_list), sqlite_batch_size):
                batch_alpha_ids = alpha_ids_list[i:i + sqlite_batch_size]
                placeholders = ','.join(['?'] * len(batch_alpha_ids))
                
                cursor = conn.execute(f"""
                    SELECT alpha_id, file_path, file_size, file_mtime, content_hash
                    FROM alpha_records
                    WHERE alpha_id IN ({placeholders})
                """, batch_alpha_ids)
                
                try:
                    # Process each old record immediately
                    for row in cursor:
                        alpha_id = row[0]
                        old_record = AlphaRecord(
                            alpha_id=alpha_id,
                            file_path=row[1],
                            file_size=row[2],
                            file_mtime=row[3],
                            content_hash=row[4]
                        )
                        
                        # Check all files with this alpha_id
                        for file_path in alpha_id_to_files[alpha_id]:
                            _, current_size, current_mtime = file_metadata[file_path]
                            
                            # Quick metadata comparison
                            size_diff = abs(current_size - old_record.file_size)
                            mtime_diff = abs(current_mtime - old_record.file_mtime)
                            
                            if size_diff <= size_tolerance and mtime_diff <= mtime_tolerance:
                                # UNCHANGED - create record and mark immediately
                                quick_record = AlphaRecord(
                                    alpha_id=alpha_id,
                                    file_path=file_path,
                                    file_size=current_size,
                                    file_mtime=current_mtime,
                                    content_hash=old_record.content_hash
                                )
                                results[file_path] = (AlphaStatus.UNCHANGED, quick_record)
                                metadata_unchanged_count += 1
                            else:
                                # Metadata changed - needs hash verification
                                files_needing_hash.append(file_path)
                        
                        # Remove processed alpha_id from dict (memory cleanup)
                        del alpha_id_to_files[alpha_id]
                finally:
                    cursor.close()
        
        # Remaining alpha_ids in alpha_id_to_files are NEW (not in database)
        for alpha_id, file_paths_list in alpha_id_to_files.items():
            files_needing_hash.extend(file_paths_list)
        
        # Files without alpha_id in filename need full processing
        files_needing_hash.extend(files_without_alphaid)
        
        logger.info(f"Metadata filtering complete: {metadata_unchanged_count} unchanged, "
                   f"{len(files_needing_hash)} need hash calculation")
        
        # Clear large metadata dict to free memory
        file_metadata.clear()
        alpha_id_to_files.clear()
        
        # Phase 3: Calculate hashes only for files that need it (parallel)
        logger.info(f"Phase 3: Hash calculation for {len(files_needing_hash)} files...")
        
        if files_needing_hash:
            # Process in parallel for better performance
            if len(files_needing_hash) > 100 and max_workers > 1:
                from concurrent.futures import ThreadPoolExecutor, as_completed
                
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_path = {
                        executor.submit(self._create_record_from_file, fp): fp 
                        for fp in files_needing_hash
                    }
                    
                    # Process results immediately to avoid memory buildup
                    for future in as_completed(future_to_path):
                        file_path = future_to_path[future]
                        try:
                            record = future.result()
                            if record:
                                # Query old record only if needed for comparison
                                old_record = self._get_record_from_db(record.alpha_id)
                                
                                if old_record is None:
                                    status = AlphaStatus.NEW
                                else:
                                    has_changed = record.has_changed(
                                        old_record,
                                        check_content_hash=self.enable_content_hash,
                                        mtime_tolerance=mtime_tolerance,
                                        size_tolerance=size_tolerance
                                    )
                                    status = AlphaStatus.MODIFIED if has_changed else AlphaStatus.UNCHANGED
                                
                                results[file_path] = (status, record)
                        except Exception as e:
                            logger.warning(f"Failed to process {file_path}: {e}")
            else:
                # Single-threaded for small batches
                for file_path in files_needing_hash:
                    try:
                        record = self._create_record_from_file(file_path)
                        if record:
                            old_record = self._get_record_from_db(record.alpha_id)
                            
                            if old_record is None:
                                status = AlphaStatus.NEW
                            else:
                                has_changed = record.has_changed(
                                    old_record,
                                    check_content_hash=self.enable_content_hash,
                                    mtime_tolerance=mtime_tolerance,
                                    size_tolerance=size_tolerance
                                )
                                status = AlphaStatus.MODIFIED if has_changed else AlphaStatus.UNCHANGED
                            
                            results[file_path] = (status, record)
                    except Exception as e:
                        logger.warning(f"Failed to process {file_path}: {e}")
        
        # Clear remaining data structures
        files_needing_hash.clear()
        
        logger.info(f"Filtering complete: {len(results)} files total "
                   f"({metadata_unchanged_count} metadata-only, {len(results) - metadata_unchanged_count} hashed)")
        
        return results
    
    def mark_processed(self, records: List[AlphaRecord], 
                      is_update: bool = False) -> None:
        """
        标记alpha为已处理
        
        Args:
            records: AlphaRecord列表
            is_update: 是否为更新操作（会增加update_count）
        """
        if not records:
            return
        
        current_time = int(time.time())
        
        with sqlite3.connect(self.db_path) as conn:
            for record in records:
                if is_update:
                    # 更新现有记录，增加update_count
                    conn.execute("""
                        INSERT INTO alpha_records 
                        (alpha_id, file_path, file_size, file_mtime, content_hash, 
                         processed_time, update_count)
                        VALUES (?, ?, ?, ?, ?, ?, 1)
                        ON CONFLICT(alpha_id) DO UPDATE SET
                            file_path = excluded.file_path,
                            file_size = excluded.file_size,
                            file_mtime = excluded.file_mtime,
                            content_hash = excluded.content_hash,
                            processed_time = excluded.processed_time,
                            update_count = update_count + 1
                    """, (record.alpha_id, record.file_path, record.file_size,
                         record.file_mtime, record.content_hash, current_time))
                else:
                    # 新记录或保持update_count不变
                    conn.execute("""
                        INSERT OR REPLACE INTO alpha_records 
                        (alpha_id, file_path, file_size, file_mtime, content_hash, 
                         processed_time, update_count)
                        VALUES (?, ?, ?, ?, ?, ?, 0)
                    """, (record.alpha_id, record.file_path, record.file_size,
                         record.file_mtime, record.content_hash, current_time))
            
            conn.commit()
        
        # 更新缓存
        for record in records:
            if len(self._alpha_cache) < 100000:
                self._alpha_cache[record.alpha_id] = record
    
    def get_stats(self) -> dict:
        """获取统计信息"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total_alphas,
                    SUM(file_size) as total_size,
                    SUM(update_count) as total_updates,
                    COUNT(CASE WHEN update_count > 0 THEN 1 END) as updated_alphas,
                    MAX(processed_time) as last_processed
                FROM alpha_records
            """)
            
            row = cursor.fetchone()
            
            return {
                'total_alphas': row[0] or 0,
                'total_size_bytes': row[1] or 0,
                'total_updates': row[2] or 0,
                'updated_alphas': row[3] or 0,
                'last_processed_time': row[4] or 0,
                'db_size_bytes': self.db_path.stat().st_size if self.db_path.exists() else 0
            }
    
    def get_frequently_updated_alphas(self, min_updates: int = 5) -> List[Dict]:
        """
        获取频繁更新的alpha列表
        
        Args:
            min_updates: 最小更新次数阈值
            
        Returns:
            [{alpha_id, file_path, update_count}, ...]
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT alpha_id, file_path, update_count
                FROM alpha_records
                WHERE update_count >= ?
                ORDER BY update_count DESC
            """, (min_updates,))
            
            return [
                {
                    'alpha_id': row[0],
                    'file_path': row[1],
                    'update_count': row[2]
                }
                for row in cursor
            ]
    
    def clear_cache(self):
        """清除内存缓存"""
        self._alpha_cache.clear()
    
    def close(self):
        """关闭state manager并释放资源"""
        self.clear_cache()
        # 确保所有SQLite连接都已关闭
        # Python的sqlite3使用自动管理，但显式清理缓存
        import gc
        gc.collect()
    
    def cleanup_orphaned_records(self) -> int:
        """
        清理孤立记录（文件已不存在）
        
        Returns:
            删除的记录数
        """
        deleted_count = 0
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT alpha_id, file_path FROM alpha_records")
            
            orphaned_ids = []
            for row in cursor:
                alpha_id, file_path = row
                if not Path(file_path).exists():
                    orphaned_ids.append(alpha_id)
            
            if orphaned_ids:
                # 批量删除
                placeholders = ','.join(['?'] * len(orphaned_ids))
                conn.execute(f"""
                    DELETE FROM alpha_records 
                    WHERE alpha_id IN ({placeholders})
                """, orphaned_ids)
                
                deleted_count = len(orphaned_ids)
                conn.commit()
                
                logger.info(f"清理了 {deleted_count} 条孤立记录")
        
        return deleted_count


# ============================================================================
# Backward Compatibility Layer for Legacy Code
# ============================================================================

class SQLiteStateManager:
    """
    Backward-compatible wrapper for EnhancedStateManager
    Maintains old API while using new implementation
    
    This allows existing code (3_mongo_import_v4.py) to work without changes
    while benefiting from the new alpha_id-based tracking system.
    """
    
    def __init__(self, state_dir: Path, db_name: str = "processed_files.db", 
                 mode: str = "incremental_fast"):
        """
        Args:
            state_dir: State directory
            db_name: Database filename
            mode: "incremental_fast" (check alpha_id only) or 
                  "incremental_slow" (check alpha_id + changes)
        """
        self.state_dir = Path(state_dir)
        self.mode = mode
        
        # Use new EnhancedStateManager internally
        self.manager = EnhancedStateManager(
            state_dir=state_dir,
            db_name=db_name,
            enable_content_hash=(mode == "incremental_slow")
        )
        
        # Compile regex pattern for extracting alpha_id from filename
        # Pattern: ..._XX00000_aB1cD2e_6069f3308967.json
        import re
        self.alpha_id_pattern = re.compile(r'_([A-Za-z0-9]{7})_[a-f0-9]{12}\.json$')
    
    def _extract_alpha_id_from_filename(self, file_path: str) -> Optional[str]:
        """Extract alpha_id from filename without opening file"""
        match = self.alpha_id_pattern.search(file_path)
        return match.group(1) if match else None
    
    def is_processed(self, file_path: str) -> bool:
        """
        Check if file is processed (legacy API)
        
        incremental_fast: Only check if alpha_id exists in DB
        incremental_slow: Check if exists AND unchanged
        """
        # Try to extract alpha_id from filename (fast path)
        alpha_id = self._extract_alpha_id_from_filename(file_path)
        
        if alpha_id:
            # Fast path: alpha_id from filename
            if self.mode == "incremental_fast":
                # Only check existence
                old_record = self.manager._get_record_from_db(alpha_id)
                return old_record is not None
            else:
                # incremental_slow: check existence AND changes
                try:
                    status, _ = self.manager.check_alpha_status(file_path, alpha_id)
                    # If UNCHANGED, consider it processed (skip)
                    # If NEW or MODIFIED, not processed (import)
                    return status == AlphaStatus.UNCHANGED
                except:
                    # If error, assume not processed
                    return False
        else:
            # Slow path: need to open file to get alpha_id
            try:
                status, _ = self.manager.check_alpha_status(file_path)
                return status == AlphaStatus.UNCHANGED
            except:
                return False
    
    def is_processed_batch(self, file_paths: List[str], workers: int = 32) -> Set[str]:
        """
        Batch check if files are processed (legacy API)
        
        Args:
            file_paths: List of file paths to check
            workers: Number of parallel workers for I/O operations
        
        Returns:
            Set of file paths that are already processed (should skip)
        """
        if not file_paths:
            return set()
        
        processed_files = set()
        
        if self.mode == "incremental_fast":
            # Fast mode: only check alpha_id existence
            # Extract alpha_ids from filenames (parallel for large batches)
            path_to_alphaid = {}
            
            if len(file_paths) > 1000 and workers > 1:
                # Parallel extraction for large batches
                from concurrent.futures import ThreadPoolExecutor, as_completed
                
                logger.info(f"Parallel filtering (fast): Extracting alpha_id from {len(file_paths)} files with {workers} threads...")
                
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    future_to_path = {
                        executor.submit(self._extract_alpha_id_from_filename, fp): fp
                        for fp in file_paths
                    }
                    
                    for future in as_completed(future_to_path):
                        file_path = future_to_path[future]
                        try:
                            alpha_id = future.result()
                            if alpha_id:
                                path_to_alphaid[file_path] = alpha_id
                        except:
                            pass
            else:
                # Single-threaded for small batches
                for file_path in file_paths:
                    alpha_id = self._extract_alpha_id_from_filename(file_path)
                    if alpha_id:
                        path_to_alphaid[file_path] = alpha_id
            
            if not path_to_alphaid:
                return set()
            
            # Batch query database for these alpha_ids
            alpha_ids = list(path_to_alphaid.values())
            existing_ids = set()
            
            with sqlite3.connect(self.manager.db_path) as conn:
                # Query in batches
                batch_size = 900
                for i in range(0, len(alpha_ids), batch_size):
                    batch = alpha_ids[i:i + batch_size]
                    placeholders = ','.join(['?'] * len(batch))
                    cursor = conn.execute(f"""
                        SELECT alpha_id FROM alpha_records 
                        WHERE alpha_id IN ({placeholders})
                    """, batch)
                    try:
                        existing_ids.update(row[0] for row in cursor)
                    finally:
                        cursor.close()
            
            # Mark files with existing alpha_ids as processed
            for file_path, alpha_id in path_to_alphaid.items():
                if alpha_id in existing_ids:
                    processed_files.add(file_path)
            
            # Explicitly clear large data structures
            path_to_alphaid.clear()
            alpha_ids.clear()
            existing_ids.clear()
        
        else:
            # Slow mode: check changes (use parallel processing for I/O)
            try:
                # Use configured workers for file I/O operations (hash calculation)
                results = self.manager.check_alpha_status_batch(
                    file_paths, 
                    max_workers=workers
                )
                for file_path, (status, _) in results.items():
                    if status == AlphaStatus.UNCHANGED:
                        processed_files.add(file_path)
            except Exception as e:
                logger.warning(f"Batch check failed: {e}, falling back to individual checks")
                for file_path in file_paths:
                    if self.is_processed(file_path):
                        processed_files.add(file_path)
        
        return processed_files
    
    def mark_processed(self, file_paths: List[str]) -> None:
        """
        Mark files as processed (legacy API)
        
        This creates/updates records in the database
        """
        if not file_paths:
            return
        
        records = []
        for file_path in file_paths:
            # Extract alpha_id
            alpha_id = self._extract_alpha_id_from_filename(file_path)
            if not alpha_id:
                # Need to open file to get alpha_id
                try:
                    alpha_id = self.manager._extract_alpha_id_from_file(file_path)
                except:
                    logger.warning(f"Cannot extract alpha_id from {file_path}, skipping")
                    continue
            
            if not alpha_id:
                continue
            
            # Get file metadata
            try:
                stat = Path(file_path).stat()
                file_size = stat.st_size
                file_mtime = stat.st_mtime
            except:
                file_size = 0
                file_mtime = 0
            
            # Create record
            record = AlphaRecord(
                alpha_id=alpha_id,
                file_path=file_path,
                file_size=file_size,
                file_mtime=file_mtime,
                content_hash=None  # Don't calculate hash for marking
            )
            records.append(record)
        
        if records:
            # Check if any are updates (already exist in DB)
            existing_ids = set()
            alpha_ids = [r.alpha_id for r in records]
            
            with sqlite3.connect(self.manager.db_path) as conn:
                batch_size = 900
                for i in range(0, len(alpha_ids), batch_size):
                    batch = alpha_ids[i:i + batch_size]
                    placeholders = ','.join(['?'] * len(batch))
                    cursor = conn.execute(f"""
                        SELECT alpha_id FROM alpha_records 
                        WHERE alpha_id IN ({placeholders})
                    """, batch)
                    existing_ids.update(row[0] for row in cursor)
            
            # Separate new and update records
            new_records = [r for r in records if r.alpha_id not in existing_ids]
            update_records = [r for r in records if r.alpha_id in existing_ids]
            
            if new_records:
                self.manager.mark_processed(new_records, is_update=False)
            if update_records:
                self.manager.mark_processed(update_records, is_update=True)
    
    def get_stats(self, quick: bool = True) -> dict:
        """Get statistics (legacy API)"""
        return self.manager.get_stats()
    
    def cleanup(self) -> None:
        """Cleanup resources (legacy API)"""
        self.manager.close()
    
    def clear_hash_cache(self):
        """Clear cache (legacy API)"""
        self.manager.clear_cache()


def create_state_manager(state_dir: Path, manager_type: str = "sqlite", 
                        mode: str = "incremental_fast") -> SQLiteStateManager:
    """
    Factory function for creating state manager (legacy API)
    
    Args:
        state_dir: State directory
        manager_type: Type of manager ("sqlite" - only option now)
        mode: "incremental_fast" or "incremental_slow"
    
    Returns:
        SQLiteStateManager instance (wraps EnhancedStateManager)
    """
    if manager_type == "sqlite":
        return SQLiteStateManager(state_dir, mode=mode)
    else:
        raise ValueError(f"Unsupported manager type: {manager_type}")


# ============================================================================
# Test Code
# ============================================================================

# 示例用法
if __name__ == "__main__":
    import tempfile
    
    print("Testing Enhanced State Manager V2...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        state_dir = Path(temp_dir)
        
        # 创建测试JSON文件
        test_file = state_dir / "test_alpha.json"
        alpha_data = {
            "id": "aB1cD2e",
            "author": "XX00000",
            "dateCreated": "2025-02-04T13:28:53-05:00",
            "regular": {
                "code": "rank(close)"
            }
        }
        
        with open(test_file, 'w') as f:
            json.dump(alpha_data, f)
        
        # 创建state manager
        manager = EnhancedStateManager(state_dir, enable_content_hash=False)
        
        # 测试1: 首次检查 (应该是NEW)
        status, record = manager.check_alpha_status(str(test_file))
        print(f"Test 1 - First check: {status.value}")
        assert status == AlphaStatus.NEW, "Should be NEW"
        
        # 标记为已处理
        if record:
            manager.mark_processed([record], is_update=False)
        
        # 测试2: 再次检查相同文件 (应该是UNCHANGED)
        status, record = manager.check_alpha_status(str(test_file))
        print(f"Test 2 - Second check: {status.value}")
        assert status == AlphaStatus.UNCHANGED, "Should be UNCHANGED"
        
        # 测试3: 修改文件内容
        time.sleep(1.5)  # 确保mtime变化超过tolerance (1.0秒)
        alpha_data["regular"]["code"] = "rank(volume)"  # 修改内容
        with open(test_file, 'w') as f:
            json.dump(alpha_data, f)
        
        status, record = manager.check_alpha_status(str(test_file))
        print(f"Test 3 - After modification: {status.value}")
        assert status == AlphaStatus.MODIFIED, "Should be MODIFIED"
        
        # 标记更新
        if record:
            manager.mark_processed([record], is_update=True)
        
        # 测试4: 检查统计信息
        stats = manager.get_stats()
        print(f"Test 4 - Stats: {stats}")
        assert stats['total_alphas'] == 1, "Should have 1 alpha"
        assert stats['updated_alphas'] == 1, "Should have 1 updated alpha"
        
        # Close manager and cleanup
        manager.close()
        
        print("\nAll tests passed!")
        print("Enhanced State Manager V2 is working correctly!")
        
        # Ensure database file is closed
        import gc
        gc.collect()
        time.sleep(0.1)  # Give Windows time to release file handle

