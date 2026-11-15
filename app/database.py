"""SQLite 持久化层。

单进程应用，每次操作开一个连接，WAL 模式保证读写不互斥。
"""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    id          TEXT PRIMARY KEY,
    sample_id   TEXT NOT NULL,
    status      TEXT NOT NULL CHECK (status IN ('receiving','ready','failed','cancelled')),
    byte_limit  INTEGER NOT NULL,
    error       TEXT,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS files (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id    TEXT NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    role        TEXT NOT NULL CHECK (role IN ('R1','R2')),
    path        TEXT NOT NULL,
    size        INTEGER NOT NULL,
    uploaded_at TEXT NOT NULL,
    UNIQUE(batch_id, role)
);

-- 有租期的交接记录。一次领取写入一行，两端摘要、大小、到期时间同时落库，
-- 因此领取失败不会留下半份记录。completed / expired 记录保留，用于拒绝旧交接号的迟到确认。
CREATE TABLE IF NOT EXISTS handoffs (
    id            TEXT PRIMARY KEY,
    batch_id      TEXT NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    status        TEXT NOT NULL CHECK (status IN ('active','completed','expired')),
    lease_seconds INTEGER NOT NULL,
    expires_at    TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    completed_at  TEXT,
    r1_sha256     TEXT NOT NULL,
    r2_sha256     TEXT NOT NULL,
    r1_size       INTEGER NOT NULL,
    r2_size       INTEGER NOT NULL
);

-- 每个交接号每端只保留一条已吻合的回报；新交接号不会有旧交接的确认记录。
CREATE TABLE IF NOT EXISTS handoff_confirmations (
    handoff_id      TEXT NOT NULL REFERENCES handoffs(id) ON DELETE CASCADE,
    role            TEXT NOT NULL CHECK (role IN ('R1','R2')),
    received_sha256 TEXT NOT NULL,
    matched         INTEGER NOT NULL CHECK (matched IN (0,1)),
    confirmed_at    TEXT NOT NULL,
    PRIMARY KEY (handoff_id, role)
);

-- 同一批在任一时刻最多只有一个 active 交接；并发领取最终只会有一个 INSERT 成功。
CREATE UNIQUE INDEX IF NOT EXISTS idx_handoffs_active_per_batch
    ON handoffs(batch_id) WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_handoffs_batch
    ON handoffs(batch_id, created_at);
"""

_INIT_LOCK = threading.Lock()
_initialized: set[Path] = set()


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with _INIT_LOCK:
        if db_path in _initialized:
            return
        conn = _connect(db_path)
        try:
            conn.executescript(SCHEMA)
            conn.commit()
        finally:
            conn.close()
        _initialized.add(db_path)


@contextmanager
def get_conn(db_path: Path) -> Iterator[sqlite3.Connection]:
    conn = _connect(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
