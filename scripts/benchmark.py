"""Throughput and latency of end-to-end payments (gateway + 3 banks as
separate processes, mTLS everywhere, every 2PC step fsynced).

Usage: python scripts/benchmark.py [--concurrency 1 4 16 32] [--seconds 10]
"""
import argparse
import random
import shutil
import statistics
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path

from cluster import Cluster
import payment_gateway_pb2 as pg

ACCOUNTS = [("alice", "HDFC"), ("alice", "SBI"), ("bob", "ICICI"), ("jack", "ICICI"), ("carol", "HDFC")]
PASSWORDS = {"alice": "alice123", "bob": "bob123", "jack": "jack123", "carol": "carol123"}
# Large balances so that no payment aborts for lack of funds.
SEED = {"HDFC": {"1001": 10 ** 9, "1002": 10 ** 9}, "ICICI": {"2001": 10 ** 9, "2002": 10 ** 9},
        "SBI": {"3001": 10 ** 9}}


def run_level(cluster, sessions, concurrency, seconds):
    latencies, failures = [], [0]
    lock = threading.Lock()
    stop = threading.Event()

    def worker(seed):
        rng = random.Random(seed)
        local = []
        while not stop.is_set():
            (sender, sender_bank), (receiver, receiver_bank) = rng.sample(ACCOUNTS, 2)
            stub, md = sessions[sender]
            start = time.perf_counter()
            response = stub.ProcessPayment(pg.PaymentRequest(
                transaction_id=uuid.uuid4().hex, sender_bank_name=sender_bank, receiver_username=receiver,
                receiver_bank_name=receiver_bank, amount_paise=100), metadata=md, timeout=30)
            elapsed = time.perf_counter() - start
            if response.status == "COMMITTED":
                local.append(elapsed)
            else:
                with lock:
                    failures[0] += 1
        with lock:
            latencies.extend(local)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(concurrency)]
    started = time.perf_counter()
    for t in threads:
        t.start()
    time.sleep(seconds)
    stop.set()
    for t in threads:
        t.join()
    duration = time.perf_counter() - started

    latencies.sort()
    pct = lambda p: latencies[min(len(latencies) - 1, int(p * len(latencies)))] * 1000  # noqa: E731
    return {
        "concurrency": concurrency,
        "payments": len(latencies),
        "failed": failures[0],
        "tps": len(latencies) / duration,
        "p50_ms": pct(0.50),
        "p95_ms": pct(0.95),
        "p99_ms": pct(0.99),
        "mean_ms": statistics.mean(latencies) * 1000,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 4, 16, 32])
    parser.add_argument("--seconds", type=float, default=10)
    args = parser.parse_args(argv)

    work_dir = Path(tempfile.mkdtemp(prefix="pg-bench-"))
    cluster = Cluster(work_dir, {b: {a: p / 100 for a, p in accts.items()} for b, accts in SEED.items()})
    try:
        cluster.start_all()
        cluster.wait_until_banks_registered()
        sessions = {user: cluster.login(user, pw) for user, pw in PASSWORDS.items()}
        run_level(cluster, sessions, 4, 2)  # warm-up: channels, SQLite caches
        print(f"{'clients':>7} {'payments':>9} {'failed':>6} {'TPS':>7} {'p50 ms':>7} {'p95 ms':>7} {'p99 ms':>7}")
        for level in args.concurrency:
            r = run_level(cluster, sessions, level, args.seconds)
            print(f"{r['concurrency']:>7} {r['payments']:>9} {r['failed']:>6} {r['tps']:>7.1f} "
                  f"{r['p50_ms']:>7.1f} {r['p95_ms']:>7.1f} {r['p99_ms']:>7.1f}")
    finally:
        cluster.stop_all()
        shutil.rmtree(work_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
