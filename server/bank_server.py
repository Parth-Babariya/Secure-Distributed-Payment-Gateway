"""Bank server: a two-phase-commit participant backed by a SQLite ledger.

Every vote and decision is written to SQLite before the RPC returns, so a bank
that crashes after voting yes still honours the coordinator's decision when it
restarts.

Usage: python bank_server.py <BANK_NAME> <PORT>
"""
import json
import logging
import os
import sys
import threading
import time
from concurrent import futures

import grpc

import config
import bank_pb2
import bank_pb2_grpc
import payment_gateway_pb2
import payment_gateway_pb2_grpc
from interceptor import LoggingInterceptor, PeerIdentityInterceptor
from storage import SQLiteStore
from tls import secure_channel, server_credentials

log = logging.getLogger("bank")

DEBIT, CREDIT = "DEBIT", "CREDIT"
OPERATIONS = {bank_pb2.DEBIT: DEBIT, bank_pb2.CREDIT: CREDIT}

SCHEMA = """
CREATE TABLE IF NOT EXISTS accounts (
    account_number TEXT PRIMARY KEY,
    balance_paise INTEGER NOT NULL CHECK (balance_paise >= 0)
);
CREATE TABLE IF NOT EXISTS ledger (
    transaction_id TEXT NOT NULL,
    operation TEXT NOT NULL CHECK (operation IN ('DEBIT', 'CREDIT')),
    account_number TEXT,
    amount_paise INTEGER,
    state TEXT NOT NULL CHECK (state IN ('PREPARED', 'COMMITTED', 'ABORTED')),
    updated_at REAL NOT NULL,
    PRIMARY KEY (transaction_id, operation)
);
"""


class BankLedger(SQLiteStore):
    """Durable account balances and per-transaction 2PC state.

    Each method runs in one SQLite write transaction (BEGIN IMMEDIATE), which
    serialises the check-then-update steps across threads, so two concurrent
    debits can never both pass the funds check.
    """

    def __init__(self, db_path, seed_accounts=None):
        super().__init__(db_path, SCHEMA)
        if seed_accounts and not self.query("SELECT 1 FROM accounts LIMIT 1"):
            with self.transaction() as db:
                db.executemany(
                    "INSERT INTO accounts VALUES (?, ?)",
                    [(acct, int(paise)) for acct, paise in seed_accounts.items()],
                )

    @staticmethod
    def _reserved(db, account):
        row = db.execute(
            "SELECT COALESCE(SUM(amount_paise), 0) FROM ledger "
            "WHERE account_number = ? AND operation = 'DEBIT' AND state = 'PREPARED'",
            (account,),
        ).fetchone()
        return row[0]

    def balance(self, account):
        """Return (balance, available) or None if the account does not exist."""
        with self.transaction() as db:
            row = db.execute("SELECT balance_paise FROM accounts WHERE account_number = ?", (account,)).fetchone()
            if row is None:
                return None
            return row[0], row[0] - self._reserved(db, account)

    def prepare(self, txn_id, operation, account, amount):
        """Phase 1. Returns (vote_commit, message)."""
        if amount <= 0:
            return False, "Amount must be positive"
        with self.transaction() as db:
            existing = db.execute(
                "SELECT account_number, amount_paise, state FROM ledger WHERE transaction_id = ? AND operation = ?",
                (txn_id, operation),
            ).fetchone()
            if existing is not None:
                # A retried prepare: repeat the original vote.
                same = existing[0] == account and existing[1] == amount
                if existing[2] in ("PREPARED", "COMMITTED") and same:
                    return True, "Already prepared"
                return False, f"Transaction already {existing[2].lower()}"

            row = db.execute("SELECT balance_paise FROM accounts WHERE account_number = ?", (account,)).fetchone()
            if row is None:
                return False, f"Unknown account {account}"
            if operation == DEBIT and row[0] - self._reserved(db, account) < amount:
                return False, "Insufficient funds"

            db.execute(
                "INSERT INTO ledger VALUES (?, ?, ?, ?, 'PREPARED', ?)",
                (txn_id, operation, account, amount, time.time()),
            )
            return True, "Funds reserved" if operation == DEBIT else "Ready to receive funds"

    def commit(self, txn_id, operation):
        """Phase 2 commit. Idempotent. Returns (success, message)."""
        with self.transaction() as db:
            row = db.execute(
                "SELECT account_number, amount_paise, state FROM ledger WHERE transaction_id = ? AND operation = ?",
                (txn_id, operation),
            ).fetchone()
            if row is None:
                return False, "Unknown transaction"
            account, amount, state = row
            if state == "COMMITTED":
                return True, "Already committed"
            if state == "ABORTED":
                return False, "Transaction was aborted"
            delta = -amount if operation == DEBIT else amount
            db.execute(
                "UPDATE accounts SET balance_paise = balance_paise + ? WHERE account_number = ?",
                (delta, account),
            )
            db.execute(
                "UPDATE ledger SET state = 'COMMITTED', updated_at = ? WHERE transaction_id = ? AND operation = ?",
                (time.time(), txn_id, operation),
            )
            return True, "Committed"

    def abort(self, txn_id, operation):
        """Phase 2 abort. Idempotent. Returns (success, message)."""
        with self.transaction() as db:
            row = db.execute(
                "SELECT state FROM ledger WHERE transaction_id = ? AND operation = ?",
                (txn_id, operation),
            ).fetchone()
            if row is None:
                # Leave a tombstone so a delayed prepare for this transaction
                # arriving after the abort is refused.
                db.execute(
                    "INSERT INTO ledger VALUES (?, ?, NULL, NULL, 'ABORTED', ?)",
                    (txn_id, operation, time.time()),
                )
                return True, "Aborted (never prepared)"
            if row[0] == "COMMITTED":
                return False, "Transaction already committed"
            db.execute(
                "UPDATE ledger SET state = 'ABORTED', updated_at = ? WHERE transaction_id = ? AND operation = ?",
                (time.time(), txn_id, operation),
            )
            return True, "Aborted"

    def total_balance(self):
        return self.query("SELECT COALESCE(SUM(balance_paise), 0) FROM accounts")[0][0]

    def prepared_count(self):
        return self.query("SELECT COUNT(*) FROM ledger WHERE state = 'PREPARED'")[0][0]


class BankService(bank_pb2_grpc.BankServicer):
    def __init__(self, bank_name, ledger):
        self.bank_name = bank_name
        self.ledger = ledger
        # Fault injection for tests and the chaos demo: method name -> number
        # of upcoming calls to fail with UNAVAILABLE.
        self.faults = {}
        self._faults_lock = threading.Lock()

    def _maybe_fail(self, method, context):
        with self._faults_lock:
            remaining = self.faults.get(method, 0)
            if remaining <= 0:
                return
            self.faults[method] = remaining - 1
        context.abort(grpc.StatusCode.UNAVAILABLE, f"Injected failure in {method}")

    @staticmethod
    def _operation(request, context):
        operation = OPERATIONS.get(request.operation)
        if operation is None:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Operation must be DEBIT or CREDIT")
        return operation

    def GetBalance(self, request, context):
        result = self.ledger.balance(request.account_number)
        if result is None:
            context.abort(grpc.StatusCode.NOT_FOUND, "Unknown account")
        balance, available = result
        return bank_pb2.BalanceResponse(balance_paise=balance, available_paise=available)

    def PrepareTransaction(self, request, context):
        self._maybe_fail("prepare", context)
        operation = self._operation(request, context)
        vote, message = self.ledger.prepare(
            request.transaction_id, operation, request.account_number, request.amount_paise
        )
        log.info("[%s] prepare %s %s -> %s (%s)", self.bank_name, request.transaction_id, operation,
                 "YES" if vote else "NO", message)
        return bank_pb2.PrepareResponse(vote_commit=vote, message=message)

    def CommitTransaction(self, request, context):
        self._maybe_fail("commit", context)
        operation = self._operation(request, context)
        ok, message = self.ledger.commit(request.transaction_id, operation)
        log.info("[%s] commit %s %s -> %s", self.bank_name, request.transaction_id, operation, message)
        return bank_pb2.DecisionResponse(success=ok, message=message)

    def AbortTransaction(self, request, context):
        self._maybe_fail("abort", context)
        operation = self._operation(request, context)
        ok, message = self.ledger.abort(request.transaction_id, operation)
        log.info("[%s] abort %s %s -> %s", self.bank_name, request.transaction_id, operation, message)
        return bank_pb2.DecisionResponse(success=ok, message=message)


def register_with_gateway(bank_name, host, port, gateway_address, cert_dir, stop_event,
                          retry_delay=2.0):
    """Register this bank with the gateway, retrying until it succeeds."""
    channel = secure_channel(gateway_address, cert_dir, bank_name.lower())
    stub = payment_gateway_pb2_grpc.PaymentGatewayStub(channel)
    request = payment_gateway_pb2.RegisterBankRequest(bank_name=bank_name, bank_port=port, bank_host=host)
    try:
        while not stop_event.is_set():
            try:
                response = stub.RegisterBank(request, timeout=5)
                if response.success:
                    log.info("Registered %s at %s:%s with the gateway", bank_name, host, port)
                    return True
                log.warning("Registration refused: %s", response.message)
            except grpc.RpcError as e:
                log.warning("Gateway unreachable (%s); retrying in %.0fs", e.code().name, retry_delay)
            stop_event.wait(retry_delay)
        return False
    finally:
        channel.close()


def load_seed_accounts(bank_name, seed_file=None):
    """Opening balances (rupees in the JSON file) converted to paise."""
    with open(seed_file or config.BANK_SEED_FILE, "r", encoding="utf-8") as f:
        accounts = json.load(f).get(bank_name, {})
    return {acct: round(float(rupees) * 100) for acct, rupees in accounts.items()}


def build_server(bank_name, port, *, cert_dir=None, data_dir=None, seed_accounts=None,
                 bind_host="[::]"):
    """Create (server, service, bound_port) for a bank without starting it."""
    cert_dir = cert_dir or config.CERT_DIR
    data_dir = data_dir or config.DATA_DIR
    data_dir.mkdir(parents=True, exist_ok=True)
    if seed_accounts is None:
        seed_accounts = load_seed_accounts(bank_name)

    ledger = BankLedger(data_dir / f"{bank_name.lower()}.db", seed_accounts)
    service = BankService(bank_name, ledger)
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=16),
        interceptors=[LoggingInterceptor(), PeerIdentityInterceptor({config.GATEWAY_CN})],
    )
    bank_pb2_grpc.add_BankServicer_to_server(service, server)
    bound_port = server.add_secure_port(f"{bind_host}:{port}", server_credentials(cert_dir, bank_name.lower()))
    return server, service, bound_port


def serve(bank_name, port):
    server, service, bound_port = build_server(bank_name, port)
    server.start()
    log.info("Bank %s listening on port %s (mTLS, gateway-only)", bank_name, bound_port)

    stop = threading.Event()
    host = os.environ.get("PG_BANK_HOST", "localhost")
    threading.Thread(
        target=register_with_gateway,
        args=(bank_name, host, bound_port, config.GATEWAY_ADDRESS, config.CERT_DIR, stop),
        daemon=True,
    ).start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        server.stop(grace=2)
        service.ledger.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python bank_server.py <BANK_NAME> <PORT>")
        sys.exit(1)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    serve(sys.argv[1].upper(), int(sys.argv[2]))
