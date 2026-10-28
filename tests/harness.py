"""Runs a gateway and banks in-process on free ports, for tests and benchmarks."""
import sqlite3
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
for path in (ROOT / "server", ROOT / "scripts"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import grpc  # noqa: E402

import config  # noqa: E402
import bank_pb2_grpc  # noqa: E402
import payment_gateway_pb2  # noqa: E402
import payment_gateway_pb2_grpc  # noqa: E402
from bank_server import build_server as build_bank, register_with_gateway  # noqa: E402
from gen_certs import generate as generate_certs  # noqa: E402
from payment_gateway import build_gateway  # noqa: E402
from tls import secure_channel  # noqa: E402

# Opening balances in paise.
SEED = {
    "HDFC": {"1001": 150_000, "1002": 300_000},
    "ICICI": {"2001": 400_000, "2002": 800_000},
    "SBI": {"3001": 650_000},
}
PASSWORDS = {"alice": "alice123", "bob": "bob123", "jack": "jack123", "carol": "carol123"}


class System:
    def __init__(self, cert_dir, data_dir, seed=SEED, **gateway_options):
        self.cert_dir = Path(cert_dir)
        self.data_dir = Path(data_dir)
        self.seed = seed
        self.gateway_options = {"recovery_interval": 0.2, **gateway_options}
        self.gateway = None
        self.gateway_port = 0
        self.banks = {}  # name -> (server, service, port, stop_event)
        self._channels = []

    # -- lifecycle ---------------------------------------------------------
    def start_gateway(self, **overrides):
        options = {**self.gateway_options, **overrides}
        self.gateway = build_gateway(self.gateway_port, cert_dir=self.cert_dir, data_dir=self.data_dir,
                                     bind_host="localhost", **options).start()
        self.gateway_port = self.gateway.port
        return self.gateway

    def stop_gateway(self):
        self.gateway.stop(grace=0)
        self.gateway = None

    def start_bank(self, name):
        server, service, port = build_bank(name, 0, cert_dir=self.cert_dir, data_dir=self.data_dir,
                                           seed_accounts=self.seed[name], bind_host="localhost")
        server.start()
        stop = threading.Event()
        registered = register_with_gateway(name, "localhost", port, f"localhost:{self.gateway_port}",
                                           self.cert_dir, stop, retry_delay=0.2)
        assert registered, f"{name} failed to register"
        self.banks[name] = (server, service, port, stop)
        return service

    def stop_bank(self, name):
        server, service, _, stop = self.banks.pop(name)
        stop.set()
        server.stop(0).wait()
        service.ledger.close()

    def start(self):
        self.start_gateway()
        for name in self.seed:
            self.start_bank(name)
        return self

    def stop(self):
        for channel in self._channels:
            channel.close()
        for name in list(self.banks):
            self.stop_bank(name)
        if self.gateway:
            self.stop_gateway()

    # -- clients -----------------------------------------------------------
    def channel(self, identity=config.CLIENT_CN, target=None):
        channel = secure_channel(target or f"localhost:{self.gateway_port}", self.cert_dir, identity)
        self._channels.append(channel)
        return channel

    def stub(self, identity=config.CLIENT_CN):
        return payment_gateway_pb2_grpc.PaymentGatewayStub(self.channel(identity))

    def bank_stub(self, bank_name, identity=config.CLIENT_CN):
        port = self.banks[bank_name][2]
        return bank_pb2_grpc.BankStub(self.channel(identity, f"localhost:{port}"))

    def login(self, username):
        """Return (stub, metadata) for a logged-in user."""
        stub = self.stub()
        response = stub.AuthenticateClient(
            payment_gateway_pb2.AuthRequest(username=username, password=PASSWORDS[username]), timeout=10)
        assert response.authenticated, response.message
        return stub, [("authorization", response.token)]

    # -- inspection --------------------------------------------------------
    def _bank_db(self, name):
        return sqlite3.connect(str(self.data_dir / f"{name.lower()}.db"))

    def balances(self):
        result = {}
        for name in self.seed:
            with self._bank_db(name) as db:
                result[name] = dict(db.execute("SELECT account_number, balance_paise FROM accounts"))
        return result

    def total_money(self):
        return sum(sum(accounts.values()) for accounts in self.balances().values())

    def prepared_holds(self):
        total = 0
        for name in self.seed:
            with self._bank_db(name) as db:
                total += db.execute("SELECT COUNT(*) FROM ledger WHERE state = 'PREPARED'").fetchone()[0]
        return total

    def payment_status(self, txn_id):
        with sqlite3.connect(str(self.data_dir / "gateway.db")) as db:
            row = db.execute("SELECT status FROM payments WHERE transaction_id = ?", (txn_id,)).fetchone()
        return row[0] if row else None

    def wait_for(self, predicate, timeout=10.0, interval=0.05):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return True
            time.sleep(interval)
        return predicate()


def ensure_certs(cert_dir):
    generate_certs(cert_dir, list(SEED))
    return Path(cert_dir)


def expect_code(code, fn, *args, **kwargs):
    """Call fn and assert it fails with the given gRPC status code."""
    try:
        fn(*args, **kwargs)
    except grpc.RpcError as e:
        assert e.code() == code, f"expected {code.name}, got {e.code().name}: {e.details()}"
        return e
    raise AssertionError(f"expected {code.name}, call succeeded")
