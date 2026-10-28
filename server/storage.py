"""SQLite wrapper with group commit, used for the bank ledger and coordinator log.

Every write is a function `fn(db)` handed to `write()`. A single writer thread
takes all writes that are waiting, runs each inside its own SAVEPOINT, and
commits the batch with one COMMIT, i.e. one fsync under synchronous=FULL.
`write()` returns only after that commit, so a caller never acts on state
that is not yet durable. While one batch is being flushed the next one
fills up, so under load many payments share each fsync; a lone writer pays
the same single fsync as before.

Writes run one after another on the writer's connection, so a check-then-
update inside one `fn` (e.g. a funds check) is still serialised against
every other write. Reads use a separate connection: in WAL mode they see the
last committed state and never wait for a flush.
"""
import os
import queue
import sqlite3
import threading


def _sync_mode():
    # FULL fsyncs every commit, so an acknowledged vote or decision survives
    # even a power failure. NORMAL (WAL) survives process crashes but may
    # lose the last commits on an OS crash.
    sync = os.environ.get("PG_SQLITE_SYNC", "FULL").upper()
    if sync not in ("FULL", "NORMAL"):
        raise ValueError("PG_SQLITE_SYNC must be FULL or NORMAL")
    return sync


def _max_batch():
    # PG_GROUP_COMMIT=0 commits every write on its own (for A/B benchmarks).
    return 256 if os.environ.get("PG_GROUP_COMMIT", "1") != "0" else 1


class _Write:
    __slots__ = ("fn", "done", "value", "error")

    def __init__(self, fn):
        self.fn = fn
        self.done = threading.Event()
        self.value = None
        self.error = None


_STOP = object()


class SQLiteStore:
    def __init__(self, db_path, schema, max_batch=None):
        sync = _sync_mode()
        self._writer_db = self._connect(db_path, sync)
        self._writer_db.executescript(schema)
        self._reader_db = self._connect(db_path, sync)
        self._read_lock = threading.Lock()
        self.max_batch = max_batch or _max_batch()
        self.batches = 0  # number of commits, for tests and benchmarks
        self._queue = queue.Queue()
        self._closed = False
        self._writer = threading.Thread(target=self._run_writer, daemon=True, name="sqlite-writer")
        self._writer.start()

    @staticmethod
    def _connect(db_path, sync):
        db = sqlite3.connect(str(db_path), check_same_thread=False, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute(f"PRAGMA synchronous={sync}")
        db.execute("PRAGMA busy_timeout=5000")
        return db

    # -- writes ------------------------------------------------------------
    def write(self, fn):
        """Run fn(db) in a write transaction and return its result once the
        transaction is durable. An exception from fn rolls back only fn's
        own changes and is re-raised here."""
        if self._closed:
            raise RuntimeError("store is closed")
        request = _Write(fn)
        self._queue.put(request)
        request.done.wait()
        if request.error is not None:
            raise request.error
        return request.value

    def _next_batch(self):
        first = self._queue.get()
        if first is _STOP:
            return None
        batch = [first]
        while len(batch) < self.max_batch:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break
            if item is _STOP:
                self._queue.put(_STOP)  # finish this batch, then stop
                break
            batch.append(item)
        return batch

    def _run_writer(self):
        db = self._writer_db
        while True:
            batch = self._next_batch()
            if batch is None:
                self._fail_pending()
                return
            try:
                db.execute("BEGIN IMMEDIATE")
                for request in batch:
                    db.execute("SAVEPOINT w")
                    try:
                        request.value = request.fn(db)
                        db.execute("RELEASE w")
                    except Exception as e:
                        db.execute("ROLLBACK TO w")
                        db.execute("RELEASE w")
                        request.error = e
                db.execute("COMMIT")
                self.batches += 1
            except Exception as e:
                # The commit itself failed: nothing in the batch is durable.
                if db.in_transaction:
                    db.execute("ROLLBACK")
                for request in batch:
                    request.value, request.error = None, request.error or e
            for request in batch:
                request.done.set()

    def _fail_pending(self):
        """Writes queued behind the stop marker are refused, not left hanging."""
        while True:
            try:
                request = self._queue.get_nowait()
            except queue.Empty:
                return
            if request is not _STOP:
                request.error = RuntimeError("store is closed")
                request.done.set()

    # -- reads -------------------------------------------------------------
    def read(self, fn):
        """Run fn(db) against one consistent snapshot of committed data."""
        with self._read_lock:
            self._reader_db.execute("BEGIN")
            try:
                return fn(self._reader_db)
            finally:
                self._reader_db.execute("COMMIT")

    def query(self, sql, params=()):
        return self.read(lambda db: db.execute(sql, params).fetchall())

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._queue.put(_STOP)
        self._writer.join()
        self._writer_db.close()
        with self._read_lock:
            self._reader_db.close()
