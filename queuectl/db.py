from contextlib import contextmanager
from pathlib import Path
import sqlite3
from datetime import datetime

DB_PATH = Path("queuectl.db")  # file in current directory


def _get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def get_connection():
    conn = _get_connection()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    """Create jobs table if it doesn't exist."""
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT NOT NULL,
                payload TEXT NOT NULL,
                status TEXT NOT NULL,              -- pending | running | completed | failed | dlq
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 5,
                available_at TEXT NOT NULL,        -- ISO timestamp
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_status_available_at "
            "ON jobs(status, available_at);"
        )


def now_utc_iso() -> str:
    return datetime.utcnow().isoformat()
