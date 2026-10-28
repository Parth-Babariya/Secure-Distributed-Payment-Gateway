"""Failure injection: crashes and outages at every step of two-phase commit."""
import uuid

import grpc

from harness import SEED
import bank_pb2
import payment_gateway_pb2 as pg
from test_payments import pay

TOTAL = sum(sum(accounts.values()) for accounts in SEED.values())


def test_receiver_commit_failure_is_retried_until_settled(system):
    system.banks["ICICI"][1].faults["commit"] = 3
    stub, md = system.login("alice")
    response = pay(stub, md, "HDFC", "bob", "ICICI", 10_000)
    # The decision is COMMIT and durable, so the client is told it succeeded.
    assert response.success and response.status == "COMMITTING"

    assert system.wait_for(lambda: system.payment_status(response.transaction_id) == "COMMITTED")
    assert system.balances()["ICICI"]["2001"] == 410_000
    assert system.total_money() == TOTAL
    assert system.prepared_holds() == 0


def test_bank_down_during_prepare_aborts_and_releases_funds(system):
    system.stop_bank("ICICI")
    stub, md = system.login("alice")
    response = pay(stub, md, "HDFC", "bob", "ICICI", 10_000)
    assert response.status in ("ABORTING", "ABORTED") and not response.success

    # HDFC's reservation is released straight away; ICICI's abort is retried
    # once it is back.
    balance = stub.CheckBalance(pg.BalanceRequest(bank_name="HDFC"), metadata=md)
    assert balance.available_paise == balance.balance_paise == 150_000
    system.start_bank("ICICI")
    assert system.wait_for(lambda: system.payment_status(response.transaction_id) == "ABORTED")
    assert system.total_money() == TOTAL
    assert system.prepared_holds() == 0


def test_gateway_crash_after_commit_decision_is_completed_on_restart(system):
    system.banks["ICICI"][1].faults["commit"] = 10 ** 6
    stub, md = system.login("alice")
    response = pay(stub, md, "HDFC", "bob", "ICICI", 10_000)
    assert response.status == "COMMITTING"
    system.stop_gateway()  # crash with the payment half settled

    system.banks["ICICI"][1].faults.clear()
    system.start_gateway()
    assert system.wait_for(lambda: system.payment_status(response.transaction_id) == "COMMITTED")
    assert system.balances()["ICICI"]["2001"] == 410_000
    assert system.total_money() == TOTAL


def test_gateway_crash_before_decision_is_presumed_aborted(system):
    """Coordinator logs PREPARING and the sender reserves funds, then the
    coordinator dies before deciding. On restart the payment must abort."""
    txn_id = uuid.uuid4().hex
    coordinator = system.gateway.service.coordinator
    _, row = system.gateway.service.log.create_payment({
        "transaction_id": txn_id, "sender_username": "alice", "sender_bank": "HDFC", "sender_account": "1001",
        "receiver_username": "bob", "receiver_bank": "ICICI", "receiver_account": "2001",
        "amount_paise": 10_000,
    })
    assert coordinator._prepare(row, "HDFC", bank_pb2.DEBIT, "1001")[0]
    system.stop_gateway()
    assert system.prepared_holds() == 1

    system.start_gateway(prepare_timeout=0.3)
    assert system.wait_for(lambda: system.payment_status(txn_id) == "ABORTED")
    assert system.prepared_holds() == 0
    assert system.balances()["HDFC"]["1001"] == 150_000


def test_participant_crash_after_yes_vote_still_commits(system):
    """A bank that voted yes and then crashed must apply the commit once it
    is back: its vote is durable in its own SQLite ledger."""
    system.banks["ICICI"][1].faults["commit"] = 10 ** 6
    stub, md = system.login("alice")
    response = pay(stub, md, "HDFC", "bob", "ICICI", 10_000)
    assert response.status == "COMMITTING"

    system.stop_bank("ICICI")
    assert system.prepared_holds() == 1  # ICICI's yes vote is on disk
    system.start_bank("ICICI")           # new process, new port, re-registers
    assert system.wait_for(lambda: system.payment_status(response.transaction_id) == "COMMITTED")
    assert system.balances()["ICICI"]["2001"] == 410_000
    assert system.total_money() == TOTAL


def test_prepare_after_abort_is_refused(system):
    """A delayed prepare that arrives after the abort must not reserve funds."""
    ledger = system.banks["HDFC"][1].ledger
    assert ledger.abort("late-txn", "DEBIT")[0]
    vote, message = ledger.prepare("late-txn", "DEBIT", "1001", 100)
    assert not vote and "aborted" in message
    assert ledger.balance("1001") == (150_000, 150_000)


def test_bank_rejects_unknown_operation(system):
    stub = system.bank_stub("HDFC", identity="gateway")
    try:
        stub.PrepareTransaction(bank_pb2.PrepareRequest(transaction_id="t", account_number="1001",
                                                        amount_paise=1))
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.INVALID_ARGUMENT
    else:
        raise AssertionError("expected INVALID_ARGUMENT")
