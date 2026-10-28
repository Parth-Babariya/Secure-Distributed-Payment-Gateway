"""Small SQLite wrapper used for the durable bank ledger and coordinator log."""
import os
import sqlite3
import threading
from contextlib import contextmanager


class SQLiteStore:
    def __init__(self, db_path, schema):
        self.db = sqlite3.connect(str(db_path), check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        # FULL fsyncs every commit, so an acknowledged vote or decision
        # survives even a power failure. NORMAL (WAL) survives process crashes
        # but may lose the last commits on an OS crash.
        sync = os.environ.get("PG_SQLITE_SYNC", "FULL").upper()
        if sync not in ("FULL", "NORMAL"):
            raise ValueError("PG_SQLITE_SYNC must be FULL or NORMAL")
        self.db.execute(f"PRAGMA synchronous={sync}")
        self.db.executescript(schema)
        self._lock = threading.Lock()

    @contextmanager
    def transaction(self):
        """One serialised write transaction (BEGIN IMMEDIATE ... COMMIT)."""
        with self._lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                yield self.db
            except BaseException:
                self.db.execute("ROLLBACK")
                raise
            self.db.execute("COMMIT")

    def query(self, sql, params=()):
        with self._lock:
            return self.db.execute(sql, params).fetchall()

    def close(self):
        with self._lock:
            self.db.close()
