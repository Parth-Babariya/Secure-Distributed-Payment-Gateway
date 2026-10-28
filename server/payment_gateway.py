"""Payment gateway: authenticates users and coordinates two-phase commit.

The coordinator writes every state change to a SQLite log before acting on
it. In particular the commit/abort decision is durable before phase 2 starts,
and a recovery worker drives any in-doubt transaction to completion, including
after a gateway restart.

    PREPARING --both vote yes--> COMMITTING --both ack--> COMMITTED
        |
        +--a no vote, error, or prepare timeout--> ABORTING --both ack--> ABORTED

Usage: python payment_gateway.py
"""
import datetime
import logging
import re
import threading
import time
import uuid
from concurrent import futures

import grpc

import config
import etcd_log
import bank_pb2
import bank_pb2_grpc
import payment_gateway_pb2
import payment_gateway_pb2_grpc
from auth import LoginThrottle, TokenError, TokenManager, _DUMMY_HASH, load_users, verify_password
from interceptor import AuthorizationInterceptor, LoggingInterceptor, PeerIdentityInterceptor
from jwt_keys import KeyRing
from storage import SQLiteStore
from tls import peer_common_name, peer_host, secure_channel, server_credentials

log = logging.getLogger("gateway")

PREPARING, COMMITTING, COMMITTED = "PREPARING", "COMMITTING", "COMMITTED"
ABORTING, ABORTED = "ABORTING", "ABORTED"

TXN_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
BANK_NAME_PATTERN = re.compile(r"^[A-Z][A-Z0-9]{1,15}$")

SCHEMA = """
CREATE TABLE IF NOT EXISTS payments (
    transaction_id TEXT PRIMARY KEY,
    sender_username TEXT NOT NULL,
    sender_bank TEXT NOT NULL,
    sender_account TEXT NOT NULL,
    receiver_username TEXT NOT NULL,
    receiver_bank TEXT NOT NULL,
    receiver_account TEXT NOT NULL,
    amount_paise INTEGER NOT NULL CHECK (amount_paise > 0),
    status TEXT NOT NULL,
    message TEXT NOT NULL DEFAULT '',
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS payments_status ON payments (status);
CREATE TABLE IF NOT EXISTS banks (
    name TEXT PRIMARY KEY,
    host TEXT NOT NULL,
    port INTEGER NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS revoked_tokens (
    jti TEXT PRIMARY KEY,
    expires_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS login_failures (
    key TEXT NOT NULL,
    at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS login_failures_key ON login_failures (key, at);
"""


class CoordinatorLog(SQLiteStore):
    """Durable 2PC state, the bank directory and revoked session tokens."""

    def __init__(self, db_path):
        super().__init__(db_path, SCHEMA)

    # -- payments --------------------------------------------------------
    def create_payment(self, payment):
        """Insert a new PREPARING payment. Returns the existing row instead if
        the transaction id is already in use."""
        now = time.time()

        def insert(db):
            existing = db.execute("SELECT * FROM payments WHERE transaction_id = ?",
                                  (payment["transaction_id"],)).fetchone()
            if existing is not None:
                return False, existing
            db.execute(
                "INSERT INTO payments VALUES (:transaction_id, :sender_username, :sender_bank, :sender_account,"
                " :receiver_username, :receiver_bank, :receiver_account, :amount_paise, :status, '', :now, :now)",
                {**payment, "status": PREPARING, "now": now},
            )
            return True, db.execute("SELECT * FROM payments WHERE transaction_id = ?",
                                    (payment["transaction_id"],)).fetchone()
        return self.write(insert)

    def get_payment(self, txn_id):
        rows = self.query("SELECT * FROM payments WHERE transaction_id = ?", (txn_id,))
        return rows[0] if rows else None

    def transition(self, txn_id, from_states, to_state, message=None):
        """Compare-and-set the status. Returns True if this call made the change."""
        placeholders = ",".join("?" * len(from_states))
        return self.write(lambda db: db.execute(
            f"UPDATE payments SET status = ?, message = COALESCE(?, message), updated_at = ? "
            f"WHERE transaction_id = ? AND status IN ({placeholders})",
            (to_state, message, time.time(), txn_id, *from_states),
        ).rowcount == 1)

    def in_doubt(self, prepare_timeout, grace=0.0):
        """Payments the recovery worker must drive forward: decided ones not
        touched for `grace` seconds, and undecided ones past the timeout."""
        now = time.time()
        return self.query(
            "SELECT * FROM payments WHERE (status IN (?, ?) AND updated_at <= ?) "
            "OR (status = ? AND updated_at < ?) ORDER BY created_at",
            (COMMITTING, ABORTING, now - grace, PREPARING, now - prepare_timeout),
        )

    def history(self, username):
        return self.query(
            "SELECT * FROM payments WHERE sender_username = ? OR receiver_username = ? ORDER BY created_at",
            (username, username),
        )

    # -- bank directory ----------------------------------------------------
    def upsert_bank(self, name, host, port):
        self.write(lambda db: db.execute(
            "INSERT INTO banks VALUES (?, ?, ?, ?) ON CONFLICT(name) DO UPDATE "
            "SET host = excluded.host, port = excluded.port, updated_at = excluded.updated_at",
            (name, host, port, time.time()),
        ))

    def get_bank(self, name):
        rows = self.query("SELECT host, port FROM banks WHERE name = ?", (name,))
        return (rows[0]["host"], rows[0]["port"]) if rows else None

    # -- token revocation (used by auth.TokenManager) ----------------------
    def revoke(self, jti, expires_at):
        def insert(db):
            db.execute("DELETE FROM revoked_tokens WHERE expires_at < ?", (time.time(),))
            db.execute("INSERT OR IGNORE INTO revoked_tokens VALUES (?, ?)", (jti, expires_at))
        self.write(insert)

    def is_revoked(self, jti):
        return bool(self.query("SELECT 1 FROM revoked_tokens WHERE jti = ?", (jti,)))

    # -- failed logins (used by auth.LoginThrottle) ------------------------
    def record_login_failure(self, keys, at, window):
        def insert(db):
            db.execute("DELETE FROM login_failures WHERE at < ?", (at - window,))
            db.executemany("INSERT INTO login_failures VALUES (?, ?)", [(key, at) for key in keys])
        self.write(insert)

    def login_failures(self, key, since):
        """Return (count, latest timestamp) of failures for key since `since`."""
        row = self.query("SELECT COUNT(*), MAX(at) FROM login_failures WHERE key = ? AND at >= ?", (key, since))[0]
        return row[0], row[1] or 0.0

    def clear_login_failures(self, key):
        self.write(lambda db: db.execute("DELETE FROM login_failures WHERE key = ?", (key,)))


class BankUnavailable(Exception):
    pass


class BankDirectory:
    """Maps bank names to gRPC stubs, rebuilding a channel when a bank
    re-registers at a different address."""

    def __init__(self, coordinator_log, cert_dir, cache_seconds=2.0):
        self.log = coordinator_log
        self.cert_dir = cert_dir
        self.cache_seconds = cache_seconds
        self._channels = {}  # name -> (address, channel, stub)
        self._locations = {}  # name -> (looked up at, (host, port) or None)
        self._lock = threading.Lock()

    def _location(self, bank_name):
        """The bank's registered address, re-read at most every few seconds
        (each lookup is a round trip when the log is in etcd)."""
        now = time.monotonic()
        cached = self._locations.get(bank_name)
        if cached and cached[1] is not None and now - cached[0] < self.cache_seconds:
            return cached[1]
        location = self.log.get_bank(bank_name)
        self._locations[bank_name] = (now, location)
        return location

    def stub(self, bank_name):
        location = self._location(bank_name)
        if location is None:
            raise BankUnavailable(f"Bank {bank_name} is not registered")
        address = f"{location[0]}:{location[1]}"
        with self._lock:
            cached = self._channels.get(bank_name)
            if cached and cached[0] == address:
                return cached[2]
            if cached:
                cached[1].close()
            channel = secure_channel(address, self.cert_dir, config.GATEWAY_CN)
            stub = bank_pb2_grpc.BankStub(channel)
            self._channels[bank_name] = (address, channel, stub)
            return stub

    def forget(self, bank_name):
        """Drop the cached address after a failed call: the bank may have
        restarted elsewhere."""
        self._locations.pop(bank_name, None)

    def close(self):
        with self._lock:
            for _, channel, _ in self._channels.values():
                channel.close()
            self._channels.clear()


class Coordinator:
    """Runs the two-phase commit protocol over the participant banks."""

    def __init__(self, coordinator_log, banks, rpc_timeout=config.BANK_RPC_TIMEOUT_SECONDS,
                 prepare_timeout=config.PREPARE_TIMEOUT_SECONDS, recovery_grace=0.0):
        self.log = coordinator_log
        # With several gateways sharing one log, a decided payment is left to
        # the gateway running it for this long before recovery joins in.
        self.recovery_grace = recovery_grace
        self.banks = banks
        self.rpc_timeout = rpc_timeout
        self.prepare_timeout = prepare_timeout
        # Both participants are asked to prepare at the same time.
        self._prepare_pool = futures.ThreadPoolExecutor(max_workers=64, thread_name_prefix="prepare")

    def close(self):
        self._prepare_pool.shutdown(wait=False)

    @staticmethod
    def _participants(row):
        return [
            (row["sender_bank"], bank_pb2.DEBIT, row["sender_account"]),
            (row["receiver_bank"], bank_pb2.CREDIT, row["receiver_account"]),
        ]

    def _prepare(self, row, bank, operation, account):
        try:
            response = self.banks.stub(bank).PrepareTransaction(
                bank_pb2.PrepareRequest(transaction_id=row["transaction_id"], account_number=account,
                                        amount_paise=row["amount_paise"], operation=operation),
                timeout=self.rpc_timeout,
            )
            return response.vote_commit, response.message
        except (grpc.RpcError, BankUnavailable) as e:
            self.banks.forget(bank)
            detail = e.details() if isinstance(e, grpc.RpcError) else str(e)
            return False, f"{bank} unavailable during prepare: {detail}"

    def _send_decision(self, row, commit):
        """Deliver the decision to both participants. Returns True if both acked."""
        txn_id = row["transaction_id"]
        all_acked = True
        for bank, operation, _ in self._participants(row):
            request = bank_pb2.DecisionRequest(transaction_id=txn_id, operation=operation)
            try:
                stub = self.banks.stub(bank)
                call = stub.CommitTransaction if commit else stub.AbortTransaction
                response = call(request, timeout=self.rpc_timeout)
                if not response.success:
                    # Should be impossible: the participant disagrees with a
                    # durable decision. Keep the transaction in doubt and shout.
                    log.error("[%s] %s refused %s: %s", txn_id, bank,
                              "commit" if commit else "abort", response.message)
                    all_acked = False
            except (grpc.RpcError, BankUnavailable) as e:
                self.banks.forget(bank)
                detail = e.code().name if isinstance(e, grpc.RpcError) else str(e)
                log.warning("[%s] %s not reached for %s (%s); will retry", txn_id, bank,
                            "commit" if commit else "abort", detail)
                all_acked = False
        return all_acked

    def _complete(self, txn_id):
        """Run phase 2 for whatever decision is logged."""
        row = self.log.get_payment(txn_id)
        if row["status"] == COMMITTING and self._send_decision(row, commit=True):
            self.log.transition(txn_id, [COMMITTING], COMMITTED)
        elif row["status"] == ABORTING and self._send_decision(row, commit=False):
            self.log.transition(txn_id, [ABORTING], ABORTED)
        return self.log.get_payment(txn_id)

    def execute(self, row):
        """Run both phases for a freshly logged PREPARING payment."""
        txn_id = row["transaction_id"]
        participants = self._participants(row)
        pending = [self._prepare_pool.submit(self._prepare, row, bank, operation, account)
                   for bank, operation, account in participants]
        votes = [future.result() for future in pending]
        # Report the sender's reason first (e.g. insufficient funds).
        for (bank, _, _), (vote, message) in zip(participants, votes):
            if not vote:
                log.info("[%s] %s voted NO: %s", txn_id, bank, message)
                self.log.transition(txn_id, [PREPARING], ABORTING, message)
                return self._complete(txn_id)

        # The decision is durable before any participant is told to commit.
        # The compare-and-set loses only if recovery already presumed this
        # transaction aborted, in which case we follow that decision.
        if self.log.transition(txn_id, [PREPARING], COMMITTING, "Both banks voted to commit"):
            log.info("[%s] decision COMMIT", txn_id)
        return self._complete(txn_id)

    def recover_once(self):
        """Drive every in-doubt transaction one step forward."""
        for row in self.log.in_doubt(self.prepare_timeout, self.recovery_grace):
            txn_id = row["transaction_id"]
            if row["status"] == PREPARING:
                # Presumed abort: the coordinator never logged a decision.
                if self.log.transition(txn_id, [PREPARING], ABORTING, "Timed out before a decision"):
                    log.warning("[%s] prepare timed out; aborting", txn_id)
            final = self._complete(txn_id)
            if final["status"] in (COMMITTED, ABORTED):
                log.info("[%s] recovered -> %s", txn_id, final["status"])


class RecoveryWorker(threading.Thread):
    def __init__(self, coordinator, interval=config.RECOVERY_INTERVAL_SECONDS):
        super().__init__(daemon=True, name="recovery")
        self.coordinator = coordinator
        self.interval = interval
        self.stop_event = threading.Event()

    def run(self):
        while not self.stop_event.is_set():
            try:
                self.coordinator.recover_once()
            except Exception:
                log.exception("Recovery pass failed")
            self.stop_event.wait(self.interval)

    def stop(self):
        self.stop_event.set()


def payment_response(row):
    status = row["status"]
    messages = {
        COMMITTED: (True, "Payment completed"),
        COMMITTING: (True, "Payment committed; settlement with a bank is being retried"),
        PREPARING: (False, "Payment is in progress"),
    }
    success, message = messages.get(status, (False, row["message"] or "Payment aborted"))
    return payment_gateway_pb2.PaymentResponse(
        success=success, message=message, transaction_id=row["transaction_id"], status=status
    )


class PaymentGatewayService(payment_gateway_pb2_grpc.PaymentGatewayServicer):
    def __init__(self, users, coordinator_log, banks, coordinator, tokens, throttle):
        self.users = users
        self.log = coordinator_log
        self.banks = banks
        self.coordinator = coordinator
        self.tokens = tokens
        self.throttle = throttle

    def _claims(self, context):
        token = dict(context.invocation_metadata()).get("authorization")
        try:
            claims = self.tokens.verify(token)
        except TokenError as e:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, str(e))
        if claims["sub"] not in self.users:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Unknown user")
        return claims

    def _account(self, username, bank_name, context):
        account = self.users[username]["accounts"].get(bank_name)
        if account is None:
            context.abort(grpc.StatusCode.PERMISSION_DENIED, f"You have no account at {bank_name}")
        return account

    def RegisterBank(self, request, context):
        bank_name = request.bank_name.upper()
        if not BANK_NAME_PATTERN.match(bank_name) or not 0 < request.bank_port < 65536:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid bank name or port")
        # A bank may only register itself: its certificate must be issued for it.
        peer = peer_common_name(context)
        if peer != config.bank_cn(bank_name):
            log.warning("Rejected registration of %s from certificate %r", bank_name, peer)
            context.abort(grpc.StatusCode.PERMISSION_DENIED, "Certificate does not belong to this bank")
        host = request.bank_host or "localhost"
        self.log.upsert_bank(bank_name, host, request.bank_port)
        log.info("Registered bank %s at %s:%s", bank_name, host, request.bank_port)
        return payment_gateway_pb2.RegisterBankResponse(success=True, message="Bank registered")

    def AuthenticateClient(self, request, context):
        username = request.username
        client = peer_host(context)
        locked_for = self.throttle.locked_for(username, client)
        if locked_for > 0:
            context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED,
                          f"Too many failed attempts; try again in {locked_for:.0f}s")
        user = self.users.get(username)
        ok = verify_password(request.password, user["password_hash"] if user else _DUMMY_HASH)
        if not (ok and user):
            self.throttle.record_failure(username, client)
            log.warning("Failed login for %r from %s", username, client)
            return payment_gateway_pb2.AuthResponse(authenticated=False, message="Invalid username or password")
        self.throttle.record_success(username, client)
        log.info("%s logged in", username)
        return payment_gateway_pb2.AuthResponse(authenticated=True, token=self.tokens.issue(username),
                                                message="Login successful")

    def LogoutClient(self, request, context):
        claims = self._claims(context)
        self.tokens.revoke(claims)
        log.info("%s logged out", claims["sub"])
        return payment_gateway_pb2.LogoutResponse(success=True, message="Logout successful")

    def CheckBalance(self, request, context):
        username = self._claims(context)["sub"]
        account = self._account(username, request.bank_name, context)
        try:
            response = self.banks.stub(request.bank_name).GetBalance(
                bank_pb2.BalanceRequest(account_number=account), timeout=self.coordinator.rpc_timeout
            )
        except (grpc.RpcError, BankUnavailable):
            context.abort(grpc.StatusCode.UNAVAILABLE, f"{request.bank_name} is unavailable")
        return payment_gateway_pb2.BalanceResponse(balance_paise=response.balance_paise,
                                                   available_paise=response.available_paise)

    def ProcessPayment(self, request, context):
        username = self._claims(context)["sub"]
        txn_id = request.transaction_id or uuid.uuid4().hex
        if not TXN_ID_PATTERN.match(txn_id):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid transaction id")
        amount = request.amount_paise
        if not 0 < amount <= config.MAX_PAYMENT_PAISE:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          f"Amount must be between 1 paisa and {config.MAX_PAYMENT_PAISE} paise")

        sender_account = self._account(username, request.sender_bank_name, context)
        receiver = self.users.get(request.receiver_username)
        receiver_account = receiver["accounts"].get(request.receiver_bank_name) if receiver else None
        if receiver_account is None:
            context.abort(grpc.StatusCode.NOT_FOUND, "Receiver account not found")
        if request.sender_bank_name == request.receiver_bank_name and sender_account == receiver_account:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Cannot transfer to the same account")

        payment = {
            "transaction_id": txn_id,
            "sender_username": username,
            "sender_bank": request.sender_bank_name,
            "sender_account": sender_account,
            "receiver_username": request.receiver_username,
            "receiver_bank": request.receiver_bank_name,
            "receiver_account": receiver_account,
            "amount_paise": amount,
        }
        created, row = self.log.create_payment(payment)
        if not created:
            # Idempotent retry: same user and same parameters get the stored
            # outcome; anything else is a conflicting reuse of the id.
            if any(row[key] != value for key, value in payment.items()):
                context.abort(grpc.StatusCode.ALREADY_EXISTS, "Transaction id already used for a different payment")
            log.info("[%s] duplicate request; returning stored status %s", txn_id, row["status"])
            return payment_response(row)

        log.info("[%s] %s pays %d paise to %s@%s", txn_id, username, amount,
                 request.receiver_username, request.receiver_bank_name)
        return payment_response(self.coordinator.execute(row))

    def GetPaymentStatus(self, request, context):
        username = self._claims(context)["sub"]
        row = self.log.get_payment(request.transaction_id)
        if row is None or username not in (row["sender_username"], row["receiver_username"]):
            context.abort(grpc.StatusCode.NOT_FOUND, "Unknown transaction")
        return payment_response(row)

    def ViewTransactionHistory(self, request, context):
        username = self._claims(context)["sub"]
        history = []
        for row in self.log.history(username):
            txn = payment_gateway_pb2.Transaction(
                transaction_id=row["transaction_id"],
                from_bank=row["sender_bank"],
                to_bank=row["receiver_bank"],
                amount_paise=row["amount_paise"],
                timestamp=datetime.datetime.fromtimestamp(row["created_at"], datetime.timezone.utc).isoformat(),
                status=row["status"],
                message=row["message"],
            )
            if row["sender_username"] == username:
                txn.from_account_no = row["sender_account"]
                txn.to_user_id = row["receiver_username"]
            else:
                txn.from_user_id = row["sender_username"]
            history.append(txn)
        return payment_gateway_pb2.TransactionHistoryResponse(transactions=history)


class Gateway:
    """A running gateway: gRPC server plus recovery worker."""

    def __init__(self, server, service, recovery, port):
        self.server = server
        self.service = service
        self.recovery = recovery
        self.port = port

    def start(self):
        self.server.start()
        self.recovery.start()
        return self

    def stop(self, grace=None):
        self.recovery.stop()
        self.server.stop(grace).wait()
        self.recovery.join(timeout=5)
        self.service.coordinator.close()
        self.service.banks.close()
        self.service.log.close()


def build_gateway(port=config.GATEWAY_PORT, *, cert_dir=None, data_dir=None, users=None,
                  jwt_key_dir=None, prepare_timeout=config.PREPARE_TIMEOUT_SECONDS,
                  recovery_interval=config.RECOVERY_INTERVAL_SECONDS,
                  rpc_timeout=config.BANK_RPC_TIMEOUT_SECONDS, bind_host="[::]",
                  etcd_endpoints=None, etcd_prefix=config.ETCD_PREFIX):
    """Create a Gateway (not yet started).

    With etcd endpoints (argument or PG_ETCD_ENDPOINTS) the coordinator log
    is the shared, replicated etcd store and any number of gateways can run
    against it; otherwise it is data_dir/gateway.db.
    """
    cert_dir = cert_dir or config.CERT_DIR
    data_dir = data_dir or config.DATA_DIR
    data_dir.mkdir(parents=True, exist_ok=True)

    etcd_endpoints = etcd_endpoints or config.ETCD_ENDPOINTS
    if etcd_endpoints:
        coordinator_log = etcd_log.connect(etcd_endpoints, config.ETCD_CERT_DIR or cert_dir / "etcd", etcd_prefix)
        recovery_grace = rpc_timeout
        log.info("Coordinator log: etcd at %s", ", ".join(etcd_endpoints))
    else:
        coordinator_log = CoordinatorLog(data_dir / "gateway.db")
        recovery_grace = 0.0
    banks = BankDirectory(coordinator_log, cert_dir)
    coordinator = Coordinator(coordinator_log, banks, rpc_timeout=rpc_timeout, prepare_timeout=prepare_timeout,
                              recovery_grace=recovery_grace)
    tokens = TokenManager(coordinator_log, KeyRing(jwt_key_dir or config.JWT_KEY_DIR or cert_dir / "jwt"))
    service = PaymentGatewayService(users if users is not None else load_users(), coordinator_log, banks,
                                    coordinator, tokens, LoginThrottle(coordinator_log))

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=32),
        interceptors=[
            LoggingInterceptor(),
            # User RPCs only from client applications; RegisterBank checks
            # for the registering bank's own certificate itself.
            PeerIdentityInterceptor({config.CLIENT_CN}, exempt_methods={"RegisterBank"}),
            AuthorizationInterceptor(tokens, exempt_methods={"AuthenticateClient", "RegisterBank"}),
        ],
    )
    payment_gateway_pb2_grpc.add_PaymentGatewayServicer_to_server(service, server)
    bound_port = server.add_secure_port(f"{bind_host}:{port}", server_credentials(cert_dir, config.GATEWAY_CN))
    return Gateway(server, service, RecoveryWorker(coordinator, recovery_interval), bound_port)


def serve():
    gateway = build_gateway().start()
    log.info("Payment gateway listening on port %s (mTLS)", gateway.port)
    try:
        gateway.server.wait_for_termination()
    except KeyboardInterrupt:
        pass
    finally:
        gateway.stop(grace=2)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    serve()
