"""Launch the gateway and banks as real OS processes (for demos and benchmarks)."""
import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "server"))
sys.path.insert(0, str(ROOT / "scripts"))

import grpc  # noqa: E402

import config  # noqa: E402
import payment_gateway_pb2 as pg  # noqa: E402
import payment_gateway_pb2_grpc  # noqa: E402
from gen_certs import generate as generate_certs  # noqa: E402
from tls import secure_channel  # noqa: E402

BANK_PORTS = {"HDFC": 61051, "ICICI": 61052, "SBI": 61053}
GATEWAY_PORT = 51052
JWT_SECRET = "cluster-demo-secret-not-for-production-use"
IN_DOUBT = ("PREPARING", "COMMITTING", "ABORTING")


class Cluster:
    def __init__(self, work_dir, seed, prepare_timeout=3.0, recovery_interval=0.5, extra_env=None):
        self.work_dir = Path(work_dir)
        self.cert_dir = self.work_dir / "certs"
        self.data_dir = self.work_dir / "data"
        self.log_dir = self.work_dir / "logs"
        for d in (self.data_dir, self.log_dir):
            d.mkdir(parents=True, exist_ok=True)
        generate_certs(self.cert_dir, list(BANK_PORTS))
        seed_file = self.work_dir / "bank_seed.json"
        seed_file.write_text(json.dumps(seed), encoding="utf-8")
        self.env = {
            **os.environ,
            "PG_CERT_DIR": str(self.cert_dir),
            "PG_DATA_DIR": str(self.data_dir),
            "PG_BANK_SEED_FILE": str(seed_file),
            "PG_GATEWAY_PORT": str(GATEWAY_PORT),
            "PG_GATEWAY_ADDRESS": f"localhost:{GATEWAY_PORT}",
            "PG_JWT_SECRET": JWT_SECRET,
            "PG_PREPARE_TIMEOUT": str(prepare_timeout),
            "PG_RECOVERY_INTERVAL": str(recovery_interval),
            "PYTHONUNBUFFERED": "1",
            **(extra_env or {}),
        }
        self.processes = {}

    def _spawn(self, name, args):
        log = open(self.log_dir / f"{name.lower()}.log", "ab")
        self.processes[name] = subprocess.Popen(
            [sys.executable, *args], cwd=ROOT / "server", env=self.env, stdout=log, stderr=subprocess.STDOUT
        )

    def start(self, name):
        if name == "GATEWAY":
            self._spawn(name, ["payment_gateway.py"])
        else:
            self._spawn(name, ["bank_server.py", name, str(BANK_PORTS[name])])

    def kill(self, name):
        """Hard kill (SIGKILL / TerminateProcess): no cleanup runs."""
        process = self.processes.pop(name)
        process.kill()
        process.wait()

    def start_all(self):
        self.start("GATEWAY")
        for bank in BANK_PORTS:
            self.start(bank)

    def stop_all(self):
        for name in list(self.processes):
            self.kill(name)

    def stub(self):
        channel = secure_channel(f"localhost:{GATEWAY_PORT}", self.cert_dir, config.CLIENT_CN)
        return payment_gateway_pb2_grpc.PaymentGatewayStub(channel)

    def login(self, username, password, timeout=30):
        """Log in, waiting for the gateway to come up. Returns (stub, metadata)."""
        stub = self.stub()
        deadline = time.time() + timeout
        while True:
            try:
                response = stub.AuthenticateClient(pg.AuthRequest(username=username, password=password), timeout=5)
                assert response.authenticated, response.message
                return stub, [("authorization", response.token)]
            except grpc.RpcError:
                if time.time() > deadline:
                    raise
                time.sleep(0.3)

    def wait_until_banks_registered(self, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                with sqlite3.connect(self.data_dir / "gateway.db") as db:
                    if db.execute("SELECT COUNT(*) FROM banks").fetchone()[0] == len(BANK_PORTS):
                        return
            except sqlite3.OperationalError:
                pass
            time.sleep(0.3)
        raise TimeoutError("banks did not register")

    # -- inspection of the durable state ----------------------------------
    def payments(self):
        with sqlite3.connect(self.data_dir / "gateway.db") as db:
            return db.execute("SELECT transaction_id, sender_bank, receiver_bank, amount_paise, status "
                              "FROM payments").fetchall()

    def bank_ledger(self, bank):
        with sqlite3.connect(self.data_dir / f"{bank.lower()}.db") as db:
            balances = dict(db.execute("SELECT account_number, balance_paise FROM accounts"))
            entries = {(t, op): state for t, op, state in
                       db.execute("SELECT transaction_id, operation, state FROM ledger")}
        return balances, entries
