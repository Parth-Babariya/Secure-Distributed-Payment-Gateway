"""Group commit in the SQLite store."""
import sqlite3
import threading
import time

import pytest

from storage import SQLiteStore

SCHEMA = "CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, value INTEGER NOT NULL CHECK (value >= 0));"


def make_store(tmp_path, **kwargs):
    return SQLiteStore(tmp_path / "store.db", SCHEMA, **kwargs)


def insert(item_id, value):
    return lambda db: db.execute("INSERT INTO items VALUES (?, ?)", (item_id, value)).rowcount


def test_concurrent_writes_share_commits(tmp_path):
    store = make_store(tmp_path)
    threads = [threading.Thread(target=store.write, args=(insert(i, i),)) for i in range(200)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert store.query("SELECT COUNT(*) FROM items")[0][0] == 200
    # 200 writers raced; far fewer commits (fsyncs) were needed.
    assert store.batches < 200
    store.close()


def test_a_failing_write_does_not_undo_its_batch(tmp_path):
    store = make_store(tmp_path)
    store.write(insert(1, 1))
    gate = threading.Event()
    # Hold the writer so that the next three writes land in one batch.
    blocker = threading.Thread(target=store.write, args=(lambda db: gate.wait(),))
    blocker.start()
    results = {}

    def run(name, fn):
        try:
            results[name] = store.write(fn)
        except Exception as e:
            results[name] = e

    writers = [threading.Thread(target=run, args=args) for args in
               (("ok-a", insert(2, 2)), ("dup", insert(1, 9)), ("ok-b", insert(3, 3)))]
    for t in writers:
        t.start()
    while store._queue.qsize() < 3:
        time.sleep(0.001)
    batches_before = store.batches
    gate.set()
    for t in writers + [blocker]:
        t.join()
    assert results["ok-a"] == 1 and results["ok-b"] == 1
    assert isinstance(results["dup"], sqlite3.IntegrityError)
    assert [r[0] for r in store.query("SELECT value FROM items ORDER BY id")] == [1, 2, 3]
    assert store.batches - batches_before <= 2
    store.close()


def test_writes_are_durable_when_write_returns(tmp_path):
    store = make_store(tmp_path)
    store.write(insert(7, 70))
    # A separate connection (another process, in effect) sees it immediately.
    other = sqlite3.connect(tmp_path / "store.db")
    assert other.execute("SELECT value FROM items WHERE id = 7").fetchone() == (70,)
    other.close()
    store.close()


def test_group_commit_can_be_disabled(tmp_path, monkeypatch):
    monkeypatch.setenv("PG_GROUP_COMMIT", "0")
    store = make_store(tmp_path)
    assert store.max_batch == 1
    store.close()


def test_writes_after_close_are_refused(tmp_path):
    store = make_store(tmp_path)
    store.close()
    with pytest.raises(RuntimeError):
        store.write(insert(1, 1))
