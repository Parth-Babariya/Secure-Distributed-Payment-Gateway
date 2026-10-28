"""Chaos demo: random payments while the gateway and banks are repeatedly
hard-killed and restarted, then a full consistency audit.

Checks, after recovery:
  * no payment is left in doubt and no bank holds a PREPARED reservation;
  * the total money across all banks is unchanged and no balance is negative;
  * atomicity: for every payment both banks agree with the gateway's outcome
    (COMMITTED at both, or committed at neither).

With --replicated the coordinator log is a three-node etcd (Raft) cluster
shared by three gateways, and etcd nodes are killed too. One component is
down at a time, so etcd always keeps its majority.

Usage: python scripts/chaos_demo.py [--seconds 60] [--workers 8] [--seed 1] [--replicated]
"""
import argparse
import random
import shutil
import sys
import tempfile
import threading
import time
import uuid
from collections import Counter
from pathlib import Path

import grpc

from cluster import BANK_PORTS, IN_DOUBT, Cluster
import payment_gateway_pb2 as pg

ACCOUNTS = [("alice", "HDFC"), ("alice", "SBI"), ("bob", "ICICI"), ("jack", "ICICI"), ("carol", "HDFC")]
PASSWORDS = {"alice": "alice123", "bob": "bob123", "jack": "jack123", "carol": "carol123"}
SEED = {"HDFC": {"1001": 1500, "1002": 3000}, "ICICI": {"2001": 4000, "2002": 8000}, "SBI": {"3001": 6500}}


def load_worker(cluster, stop, results, rng_seed):
    rng = random.Random(rng_seed)
    sessions = {}
    while not stop.is_set():
        (sender, sender_bank), (receiver, receiver_bank) = rng.sample(ACCOUNTS, 2)
        try:
            if sender not in sessions:
                sessions[sender] = cluster.login(sender, PASSWORDS[sender])
            stub, md = sessions[sender]
            response = stub.ProcessPayment(pg.PaymentRequest(
                transaction_id=uuid.uuid4().hex, sender_bank_name=sender_bank, receiver_username=receiver,
                receiver_bank_name=receiver_bank, amount_paise=rng.randint(100, 200_000)), metadata=md, timeout=10)
            results[response.status] += 1
        except grpc.RpcError as e:
            results[f"rpc:{e.code().name}"] += 1
            time.sleep(0.1)
        except Exception as e:  # login timed out while the gateway was down
            results[f"error:{type(e).__name__}"] += 1


def audit(cluster, opening_total):
    problems = []
    payments = cluster.payments()
    ledgers = {bank: cluster.bank_ledger(bank) for bank in BANK_PORTS}

    total = sum(sum(balances.values()) for balances, _ in ledgers.values())
    if total != opening_total:
        problems.append(f"money not conserved: {opening_total} -> {total}")
    negative = [(b, a) for b, (bal, _) in ledgers.items() for a, v in bal.items() if v < 0]
    if negative:
        problems.append(f"negative balances: {negative}")
    held = [(b, k) for b, (_, entries) in ledgers.items() for k, s in entries.items() if s == "PREPARED"]
    if held:
        problems.append(f"{len(held)} reservations still PREPARED")

    for txn_id, sender_bank, receiver_bank, _, status in payments:
        if status in IN_DOUBT:
            problems.append(f"{txn_id} still {status}")
            continue
        debit = ledgers[sender_bank][1].get((txn_id, "DEBIT"))
        credit = ledgers[receiver_bank][1].get((txn_id, "CREDIT"))
        if status == "COMMITTED" and not (debit == credit == "COMMITTED"):
            problems.append(f"{txn_id} COMMITTED but banks have debit={debit} credit={credit}")
        if status == "ABORTED" and "COMMITTED" in (debit, credit):
            problems.append(f"{txn_id} ABORTED but banks have debit={debit} credit={credit}")
    return payments, total, problems


def main(argv=None):
    parser = argparse.ArgumentParser(description="Kill components mid-payment and audit consistency")
    parser.add_argument("--seconds", type=float, default=60)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--keep", action="store_true", help="keep the working directory")
    parser.add_argument("--replicated", action="store_true",
                        help="three gateways sharing a three-node etcd log (needs scripts/get_etcd.py)")
    args = parser.parse_args(argv)

    work_dir = Path(tempfile.mkdtemp(prefix="pg-chaos-"))
    seed_paise = {b: {a: r * 100 for a, r in accts.items()} for b, accts in SEED.items()}
    opening_total = sum(sum(a.values()) for a in seed_paise.values())
    cluster = Cluster(work_dir, SEED, replicated=args.replicated)
    rng = random.Random(args.seed)
    results = Counter()
    kills = Counter()
    stop = threading.Event()

    try:
        cluster.start_all()
        cluster.wait_until_banks_registered()
        workers = [threading.Thread(target=load_worker, args=(cluster, stop, results, args.seed + i), daemon=True)
                   for i in range(args.workers)]
        for w in workers:
            w.start()

        print(f"Running payments for {args.seconds:.0f}s while killing components...")
        deadline = time.time() + args.seconds
        while time.time() < deadline:
            time.sleep(rng.uniform(1.0, 3.0))
            victim = rng.choice(cluster.gateways + cluster.banks + cluster.etcd_nodes)
            cluster.kill(victim)
            kills[victim] += 1
            time.sleep(rng.uniform(0.2, 1.5))
            cluster.start(victim)

        stop.set()
        for w in workers:
            w.join(timeout=15)

        print("Load stopped; waiting for recovery to settle every in-doubt payment...")
        settle_deadline = time.time() + 60
        while time.time() < settle_deadline:
            if not any(p[4] in IN_DOUBT for p in cluster.payments()):
                break
            time.sleep(0.5)
        payments, total, problems = audit(cluster, opening_total)
        cluster.stop_all()
        statuses = Counter(p[4] for p in payments)
        print()
        print(f"Kills:            {dict(kills)} ({sum(kills.values())} total)")
        print(f"Client outcomes:  {dict(results)}")
        print(f"Logged payments:  {len(payments)} -> {dict(statuses)}")
        print(f"Money:            opening {opening_total} paise, closing {total} paise")
        if problems:
            print(f"FAILED: {len(problems)} consistency violations")
            for problem in problems[:20]:
                print("  ", problem)
            return 1
        print("PASSED: every payment is atomic, nothing in doubt, money conserved")
        return 0
    finally:
        stop.set()
        cluster.stop_all()
        if args.keep:
            print(f"Working directory kept at {work_dir}")
        else:
            shutil.rmtree(work_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
