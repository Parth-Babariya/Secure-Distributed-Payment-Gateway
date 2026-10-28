"""Launch the gateway and banks as real OS processes (for demos and benchmarks).

Two layouts:
  * single:     one gateway with its SQLite log, three banks;
  * replicated: a three-node etcd (Raft) cluster holding the coordinator log,
                three gateways sharing it, and three banks. Needs etcd
                (python scripts/get_etcd.py).
"""
import json
import os
import shutil
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
import etcd_log  # noqa: E402
import payment_gateway_pb2 as pg  # noqa: E402
import payment_gateway_pb2_grpc  # noqa: E402
from gen_certs import generate as generate_certs  # noqa: E402
from tls import gateway_channel  # noqa: E402

BANK_PORTS = {"HDFC": 61051, "ICICI": 61052, "SBI": 61053}
GATEWAY_PORT = 51052
# Replicated layout: gateway name -> port, etcd name -> (client port, peer port).
GATEWAY_PORTS = {"GATEWAY-1": 51052, "GATEWAY-2": 51053, "GATEWAY-3": 51054}
ETCD_PORTS = {"ETCD-1": (23791, 23801), "ETCD-2": (23792, 23802), "ETCD-3": (23793, 23803)}
IN_DOUBT = ("PREPARING", "COMMITTING", "ABORTING")


def find_etcd():
    """Path to the etcd binary: PG_ETCD_BIN, .tools/etcd (get_etcd.py), or PATH."""
    candidates = [os.environ.get("PG_ETCD_BIN"),
                  ROOT / ".tools" / "etcd" / ("etcd.exe" if os.name == "nt" else "etcd"),
                  shutil.which("etcd")]
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return Path(candidate)
    return None


class Cluster:
    def __init__(self, work_dir, seed, prepare_timeout=3.0, recovery_interval=0.5, extra_env=None,
                 replicated=False, cert_dir=None):
        self.work_dir = Path(work_dir)
        self.cert_dir = Path(cert_dir) if cert_dir else self.work_dir / "certs"
        self.data_dir = self.work_dir / "data"
        self.log_dir = self.work_dir / "logs"
        for d in (self.data_dir, self.log_dir):
            d.mkdir(parents=True, exist_ok=True)
        generate_certs(self.cert_dir, list(BANK_PORTS))
        seed_file = self.work_dir / "bank_seed.json"
        seed_file.write_text(json.dumps(seed), encoding="utf-8")

        self.replicated = replicated
        if replicated:
            self.etcd_bin = find_etcd()
            if self.etcd_bin is None:
                raise RuntimeError("etcd not found; run python scripts/get_etcd.py")
            self.gateway_ports = dict(GATEWAY_PORTS)
            self.etcd_endpoints = [f"https://127.0.0.1:{client}" for client, _ in ETCD_PORTS.values()]
        else:
            self.gateway_ports = {"GATEWAY": GATEWAY_PORT}
            self.etcd_endpoints = []
        self.gateway_address = ",".join(f"127.0.0.1:{port}" for port in self.gateway_ports.values())

        self.env = {
            **os.environ,
            "PG_CERT_DIR": str(self.cert_dir),
            "PG_DATA_DIR": str(self.data_dir),
            "PG_BANK_SEED_FILE": str(seed_file),
            "PG_GATEWAY_ADDRESS": self.gateway_address,
            "PG_ETCD_ENDPOINTS": ",".join(self.etcd_endpoints),
            "PG_PREPARE_TIMEOUT": str(prepare_timeout),
            "PG_RECOVERY_INTERVAL": str(recovery_interval),
            "PYTHONUNBUFFERED": "1",
            **(extra_env or {}),
        }
        self.processes = {}
        self._store = None

    @property
    def gateways(self):
        return list(self.gateway_ports)

    @property
    def etcd_nodes(self):
        return list(ETCD_PORTS) if self.replicated else []

    @property
    def banks(self):
        return list(BANK_PORTS)

    def _spawn(self, name, command, cwd=ROOT / "server", env=None):
        log = open(self.log_dir / f"{name.lower()}.log", "ab")
        self.processes[name] = subprocess.Popen(
            command, cwd=cwd, env={**self.env, **(env or {})}, stdout=log, stderr=subprocess.STDOUT
        )

    def _etcd_command(self, name):
        client, peer = ETCD_PORTS[name]
        tls = self.cert_dir / "etcd"
        cluster = ",".join(f"{n}=https://127.0.0.1:{p}" for n, (_, p) in ETCD_PORTS.items())
        return [
            str(self.etcd_bin), "--name", name, "--data-dir", str(self.data_dir / name.lower()),
            "--listen-client-urls", f"https://127.0.0.1:{client}",
            "--advertise-client-urls", f"https://127.0.0.1:{client}",
            "--listen-peer-urls", f"https://127.0.0.1:{peer}",
            "--initial-advertise-peer-urls", f"https://127.0.0.1:{peer}",
            "--initial-cluster", cluster, "--initial-cluster-token", "payment-gateway",
            # Mutual TLS for clients (gateways) and between the Raft peers.
            "--cert-file", str(tls / "etcd.crt"), "--key-file", str(tls / "etcd.key"),
            "--trusted-ca-file", str(tls / "ca.crt"), "--client-cert-auth",
            "--peer-cert-file", str(tls / "etcd.crt"), "--peer-key-file", str(tls / "etcd.key"),
            "--peer-trusted-ca-file", str(tls / "ca.crt"), "--peer-client-cert-auth",
            "--log-level", "warn",
        ]

    def start(self, name):
        if name in ETCD_PORTS:
            self._spawn(name, self._etcd_command(name), cwd=self.work_dir)
        elif name in self.gateway_ports:
            self._spawn(name, [sys.executable, "payment_gateway.py"],
                        env={"PG_GATEWAY_PORT": str(self.gateway_ports[name])})
        else:
            self._spawn(name, [sys.executable, "bank_server.py", name, str(BANK_PORTS[name])])

    def kill(self, name):
        """Hard kill (SIGKILL / TerminateProcess): no cleanup runs."""
        process = self.processes.pop(name)
        process.kill()
        process.wait()

    def start_all(self):
        for name in self.etcd_nodes:
            self.start(name)
        if self.replicated:
            self.wait_until_etcd_ready()
        for name in self.gateways + self.banks:
            self.start(name)

    def stop_all(self):
        for name in list(self.processes):
            self.kill(name)
        if self._store is not None:
            self._store.close()
            self._store = None

    # -- the coordinator log, wherever it lives ------------------------------
    def store(self):
        if self._store is None:
            self._store = etcd_log.connect(self.etcd_endpoints, self.cert_dir / "etcd")
        return self._store

    def wait_until_etcd_ready(self, timeout=30):
        deadline = time.time() + timeout
        while True:
            try:
                self.store().etcd.call("/v3/kv/range", {"key": "AA=="})
                return
            except etcd_log.EtcdError:
                if time.time() > deadline:
                    raise
                time.sleep(0.3)

    def registered_banks(self):
        if self.replicated:
            return sum(self.store().get_bank(b) is not None for b in BANK_PORTS)
        try:
            with sqlite3.connect(self.data_dir / "gateway.db") as db:
                return db.execute("SELECT COUNT(*) FROM banks").fetchone()[0]
        except sqlite3.OperationalError:
            return 0

    def wait_until_banks_registered(self, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.registered_banks() == len(BANK_PORTS):
                return
            time.sleep(0.3)
        raise TimeoutError("banks did not register")

    # -- clients -------------------------------------------------------------
    def stub(self):
        channel = gateway_channel(self.gateway_address, self.cert_dir, config.CLIENT_CN)
        return payment_gateway_pb2_grpc.PaymentGatewayStub(channel)

    def login(self, username, password, timeout=30):
        """Log in, waiting for a gateway to come up. Returns (stub, metadata)."""
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

    # -- inspection of the durable state ----------------------------------
    def payments(self):
        """[(transaction_id, sender_bank, receiver_bank, amount_paise, status)]"""
        if self.replicated:
            prefix = self.store().prefix + "payments/"
            rows = [json.loads(v) for _, v in self.store().etcd.get_prefix(prefix)]
            return [(r["transaction_id"], r["sender_bank"], r["receiver_bank"], r["amount_paise"], r["status"])
                    for r in rows]
        with sqlite3.connect(self.data_dir / "gateway.db") as db:
            return db.execute("SELECT transaction_id, sender_bank, receiver_bank, amount_paise, status "
                              "FROM payments").fetchall()

    def bank_ledger(self, bank):
        with sqlite3.connect(self.data_dir / f"{bank.lower()}.db") as db:
            balances = dict(db.execute("SELECT account_number, balance_paise FROM accounts"))
            entries = {(t, op): state for t, op, state in
                       db.execute("SELECT transaction_id, operation, state FROM ledger")}
        return balances, entries
