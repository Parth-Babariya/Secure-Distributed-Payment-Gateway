"""Replicated coordinator: several gateways sharing a Raft-replicated log in
a three-node etcd cluster. Skipped when etcd is not installed
(python scripts/get_etcd.py)."""
import threading
import uuid

import grpc
import pytest

from cluster import Cluster, find_etcd
from harness import PASSWORDS, SEED, System, expect_code
import bank_pb2
import bank_pb2_grpc
import config
import etcd_log
import payment_gateway_pb2 as pg
import payment_gateway_pb2_grpc
from payment_gateway import build_gateway
from test_payments import pay
from tls import secure_channel

pytestmark = pytest.mark.skipif(find_etcd() is None, reason="etcd not installed (scripts/get_etcd.py)")

TOTAL = sum(sum(accounts.values()) for accounts in SEED.values())
PAYMENT = {"sender_username": "alice", "sender_bank": "HDFC", "sender_account": "1001",
           "receiver_username": "bob", "receiver_bank": "ICICI", "receiver_account": "2001"}


@pytest.fixture(scope="module")
def etcd(cert_dir, tmp_path_factory):
    cluster = Cluster(tmp_path_factory.mktemp("etcd"), {}, replicated=True, cert_dir=cert_dir)
    for name in cluster.etcd_nodes:
        cluster.start(name)
    cluster.wait_until_etcd_ready()
    yield cluster
    cluster.stop_all()


class Replicated:
    """Gateway A (the banks register through it) and gateway B, both using
    the same etcd log under a prefix unique to the test."""

    def __init__(self, etcd, cert_dir, tmp_path, **options):
        self.options = {"etcd_endpoints": etcd.etcd_endpoints, "etcd_prefix": f"/test-{uuid.uuid4().hex}/",
                        "rpc_timeout": 1.0, **options}
        self.cert_dir = cert_dir
        self.tmp_path = tmp_path
        self.channels = []
        self.system = System(cert_dir, tmp_path / "a", **self.options).start()
        self.b = build_gateway(0, cert_dir=cert_dir, data_dir=tmp_path / "b", bind_host="localhost",
                               recovery_interval=0.2, **self.options).start()

    def stub_b(self):
        channel = secure_channel(f"localhost:{self.b.port}", self.cert_dir, config.CLIENT_CN)
        self.channels.append(channel)
        return payment_gateway_pb2_grpc.PaymentGatewayStub(channel)

    def status(self, txn_id):
        row = self.b.service.log.get_payment(txn_id)
        return row["status"] if row else None

    def stop(self):
        for channel in self.channels:
            channel.close()
        self.b.stop(grace=0)
        self.system.stop()


@pytest.fixture
def replicated(etcd, cert_dir, tmp_path):
    made = []

    def make(**options):
        made.append(Replicated(etcd, cert_dir, tmp_path, **options))
        return made[-1]

    yield make
    for r in made:
        r.stop()


def login(stub, user):
    response = stub.AuthenticateClient(pg.AuthRequest(username=user, password=PASSWORDS[user]))
    assert response.authenticated
    return [("authorization", response.token)]


def test_any_gateway_serves_any_session_and_payment(replicated):
    r = replicated()
    a, b = r.system.stub(), r.stub_b()
    md = login(a, "alice")
    # A token from gateway A works at gateway B (shared signing keys), and B
    # reaches the banks that registered through A (shared bank directory).
    assert b.CheckBalance(pg.BalanceRequest(bank_name="HDFC"), metadata=md).balance_paise == 150_000

    response = pay(a, md, "HDFC", "bob", "ICICI", 10_000)
    assert response.status == "COMMITTED"
    assert b.GetPaymentStatus(pg.PaymentStatusRequest(transaction_id=response.transaction_id),
                              metadata=md).status == "COMMITTED"
    history = b.ViewTransactionHistory(pg.TransactionHistoryRequest(), metadata=md).transactions
    assert [t.transaction_id for t in history] == [response.transaction_id]

    # Retrying the same payment at the other gateway is idempotent.
    retry = pay(b, md, "HDFC", "bob", "ICICI", 10_000, txn_id=response.transaction_id)
    assert retry.status == "COMMITTED"
    assert r.system.balances()["HDFC"]["1001"] == 140_000

    # Logging out at B revokes the session at A too.
    b.LogoutClient(pg.LogoutRequest(), metadata=md)
    expect_code(grpc.StatusCode.UNAUTHENTICATED, a.CheckBalance, pg.BalanceRequest(bank_name="HDFC"), metadata=md)


def test_another_gateway_finishes_a_crashed_gateways_payment(replicated):
    r = replicated()
    r.system.banks["ICICI"][1].faults["commit"] = 10 ** 6
    stub, md = r.system.login("alice")
    response = pay(stub, md, "HDFC", "bob", "ICICI", 10_000)
    assert response.status == "COMMITTING"
    r.system.stop_gateway()  # gateway A dies with the payment half settled

    r.system.banks["ICICI"][1].faults.clear()
    # Gateway B never saw this payment, but its recovery worker reads the shared log.
    assert r.system.wait_for(lambda: r.status(response.transaction_id) == "COMMITTED")
    assert r.system.balances()["ICICI"]["2001"] == 410_000
    assert r.system.total_money() == TOTAL
    assert r.system.prepared_holds() == 0
    r.system.start_gateway()  # teardown expects it running


def test_another_gateway_presumes_abort_for_an_undecided_payment(replicated):
    r = replicated(prepare_timeout=1.0)
    txn_id = uuid.uuid4().hex
    created, _ = r.system.gateway.service.log.create_payment(
        {"transaction_id": txn_id, "amount_paise": 50_000, **PAYMENT})
    assert created
    hdfc = bank_pb2_grpc.BankStub(r.system.channel(config.GATEWAY_CN, f"localhost:{r.system.banks['HDFC'][2]}"))
    assert hdfc.PrepareTransaction(bank_pb2.PrepareRequest(
        transaction_id=txn_id, account_number="1001", amount_paise=50_000, operation=bank_pb2.DEBIT)).vote_commit
    r.system.stop_gateway()  # A dies before deciding

    assert r.system.wait_for(lambda: r.status(txn_id) == "ABORTED")
    assert r.system.prepared_holds() == 0
    assert r.system.balances()["HDFC"]["1001"] == 150_000
    r.system.start_gateway()


def test_payments_continue_while_an_etcd_node_is_down(replicated, etcd):
    r = replicated()
    stub, md = r.system.login("alice")
    etcd.kill("ETCD-1")
    try:
        for _ in range(3):
            assert pay(stub, md, "HDFC", "bob", "ICICI", 1_000).status == "COMMITTED"
    finally:
        etcd.start("ETCD-1")
    assert r.system.total_money() == TOTAL


def test_login_lockout_is_shared_by_all_gateways(replicated):
    r = replicated()
    a, b = r.system.stub(), r.stub_b()
    for _ in range(config.MAX_LOGIN_FAILURES):
        assert not a.AuthenticateClient(pg.AuthRequest(username="alice", password="guess")).authenticated
    expect_code(grpc.StatusCode.RESOURCE_EXHAUSTED, b.AuthenticateClient,
                pg.AuthRequest(username="alice", password=PASSWORDS["alice"]))


def test_compare_and_set_has_exactly_one_winner(etcd, cert_dir):
    prefix = f"/cas-{uuid.uuid4().hex}/"
    logs = [etcd_log.connect(etcd.etcd_endpoints, cert_dir / "etcd", prefix) for _ in range(2)]
    txn_id = uuid.uuid4().hex
    logs[0].create_payment({"transaction_id": txn_id, "amount_paise": 1, **PAYMENT})
    wins = []

    def race(log, target):
        if log.transition(txn_id, ["PREPARING"], target):
            wins.append(target)

    threads = [threading.Thread(target=race, args=(logs[i % 2], ("COMMITTING", "ABORTING")[i % 2]))
               for i in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(wins) == 1
    assert logs[1].get_payment(txn_id)["status"] == wins[0]
    assert [row["transaction_id"] for row in logs[0].in_doubt(0)] == [txn_id]
    logs[1].transition(txn_id, [wins[0]], "COMMITTED" if wins[0] == "COMMITTING" else "ABORTED")
    assert logs[0].in_doubt(0) == []
    for log in logs:
        log.close()
