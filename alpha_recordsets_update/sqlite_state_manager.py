import sqlite3
import json
import hashlib
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple, List, Dict

logger = logging.getLogger(__name__)

class SQLiteStateDB:
    def __init__(self, db_path: str = "recordsets_state.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initialize SQLite database and table."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS recordsets_state (
                alpha_id TEXT,
                recordset_name TEXT,
                file_hash TEXT,
                updated_at TEXT,
                PRIMARY KEY (alpha_id, recordset_name)
            )
        """)
        conn.commit()
        conn.close()

    def get_state(self, alpha_id: str, recordset_name: str) -> Optional[str]:
        """Get the last stored hash for a given alpha and recordset."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT file_hash FROM recordsets_state WHERE alpha_id = ? AND recordset_name = ?",
            (alpha_id, recordset_name)
        )
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else None

    def update_state(self, alpha_id: str, recordset_name: str, file_hash: str):
        """Update or insert the hash for a given alpha and recordset."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        updated_at = datetime.utcnow().isoformat() + "Z"
        cursor.execute("""
            INSERT OR REPLACE INTO recordsets_state (alpha_id, recordset_name, file_hash, updated_at)
            VALUES (?, ?, ?, ?)
        """, (alpha_id, recordset_name, file_hash, updated_at))
        conn.commit()
        conn.close()

    def close(self):
        pass # SQLite connection is per-call, nothing to close persistently

def compute_file_hash(file_path: Path) -> str:
    """Compute SHA256 hash of a file."""
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            # Read and update hash string value in blocks of 4K
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except Exception as e:
        logger.warning(f"Failed to compute hash for {file_path}: {e}")
        return ""

