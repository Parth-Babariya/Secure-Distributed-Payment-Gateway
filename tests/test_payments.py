import random
import uuid
from concurrent.futures import ThreadPoolExecutor

import grpc

from harness import SEED, expect_code
import payment_gateway_pb2 as pg

TOTAL = sum(sum(accounts.values()) for accounts in SEED.values())


def pay(stub, metadata, sender_bank, receiver, receiver_bank, amount, txn_id=None):
    request = pg.PaymentRequest(transaction_id=txn_id or uuid.uuid4().hex, sender_bank_name=sender_bank,
                                receiver_username=receiver, receiver_bank_name=receiver_bank,
                                amount_paise=amount)
    return stub.ProcessPayment(request, metadata=metadata, timeout=20)


def test_payment_moves_money_across_banks(system):
    stub, md = system.login("alice")
    response = pay(stub, md, "HDFC", "bob", "ICICI", 10_000)
    assert response.success and response.status == "COMMITTED"

    balances = system.balances()
    assert balances["HDFC"]["1001"] == 150_000 - 10_000
    assert balances["ICICI"]["2001"] == 400_000 + 10_000
    assert system.total_money() == TOTAL
    assert system.prepared_holds() == 0

    balance = stub.CheckBalance(pg.BalanceRequest(bank_name="HDFC"), metadata=md)
    assert balance.balance_paise == balance.available_paise == 140_000

    bob_stub, bob_md = system.login("bob")
    [received] = bob_stub.ViewTransactionHistory(pg.TransactionHistoryRequest(), metadata=bob_md).transactions
    assert received.from_user_id == "alice" and received.from_account_no == ""
    assert received.amount_paise == 10_000 and received.status == "COMMITTED"


def test_insufficient_funds_aborts_without_side_effects(system):
    stub, md = system.login("alice")
    response = pay(stub, md, "HDFC", "bob", "ICICI", 150_001)
    assert not response.success
    assert response.status == "ABORTED"
    assert response.message == "Insufficient funds"
    assert system.total_money() == TOTAL
    assert system.balances()["HDFC"]["1001"] == 150_000
    assert system.prepared_holds() == 0


def test_invalid_amounts_are_rejected_by_the_server(system):
    stub, md = system.login("alice")
    for amount in (0, -10_000, 10 ** 12):
        expect_code(grpc.StatusCode.INVALID_ARGUMENT, pay, stub, md, "HDFC", "bob", "ICICI", amount)
    assert system.total_money() == TOTAL
    assert system.balances()["ICICI"]["2001"] == 400_000


def test_validation_errors_use_specific_status_codes(system):
    stub, md = system.login("alice")
    expect_code(grpc.StatusCode.NOT_FOUND, pay, stub, md, "HDFC", "nobody", "ICICI", 100)
    expect_code(grpc.StatusCode.NOT_FOUND, pay, stub, md, "HDFC", "bob", "HDFC", 100)
    expect_code(grpc.StatusCode.PERMISSION_DENIED, pay, stub, md, "ICICI", "bob", "ICICI", 100)
    expect_code(grpc.StatusCode.INVALID_ARGUMENT, pay, stub, md, "HDFC", "alice", "HDFC", 100)
    expect_code(grpc.StatusCode.INVALID_ARGUMENT, pay, stub, md, "HDFC", "bob", "ICICI", 100, txn_id="bad id!")
    # Moving money between your own accounts at different banks is allowed.
    assert pay(stub, md, "HDFC", "alice", "SBI", 100).success


def test_retry_with_same_transaction_id_is_idempotent(system):
    stub, md = system.login("alice")
    txn_id = uuid.uuid4().hex
    first = pay(stub, md, "HDFC", "bob", "ICICI", 5_000, txn_id=txn_id)
    second = pay(stub, md, "HDFC", "bob", "ICICI", 5_000, txn_id=txn_id)
    assert first.status == second.status == "COMMITTED"
    assert system.balances()["HDFC"]["1001"] == 145_000  # charged once

    # Reusing the id for a different payment, or as another user, is refused.
    expect_code(grpc.StatusCode.ALREADY_EXISTS, pay, stub, md, "HDFC", "bob", "ICICI", 6_000, txn_id=txn_id)
    jack_stub, jack_md = system.login("jack")
    expect_code(grpc.StatusCode.ALREADY_EXISTS, pay, jack_stub, jack_md, "ICICI", "bob", "ICICI", 5_000,
                txn_id=txn_id)

    status = stub.GetPaymentStatus(pg.PaymentStatusRequest(transaction_id=txn_id), metadata=md)
    assert status.status == "COMMITTED"
    expect_code(grpc.StatusCode.NOT_FOUND, jack_stub.GetPaymentStatus,
                pg.PaymentStatusRequest(transaction_id=txn_id), metadata=jack_md)


def test_concurrent_payments_cannot_double_spend(system):
    stub, md = system.login("alice")
    # Rs 1500 available; 40 concurrent Rs 100 payments: exactly 15 may succeed.
    with ThreadPoolExecutor(max_workers=20) as pool:
        responses = list(pool.map(lambda _: pay(stub, md, "HDFC", "bob", "ICICI", 10_000), range(40)))
    committed = [r for r in responses if r.status == "COMMITTED"]
    assert len(committed) == 15
    assert all(r.status == "ABORTED" for r in responses if r.status != "COMMITTED")
    assert system.balances()["HDFC"]["1001"] == 0
    assert system.balances()["ICICI"]["2001"] == 400_000 + 150_000
    assert system.total_money() == TOTAL
    assert system.prepared_holds() == 0


def test_random_concurrent_transfers_conserve_money(system):
    accounts = [("alice", "HDFC"), ("alice", "SBI"), ("bob", "ICICI"), ("jack", "ICICI"), ("carol", "HDFC")]
    sessions = {user: system.login(user) for user in {u for u, _ in accounts}}
    rng = random.Random(7)
    transfers = []
    for _ in range(120):
        (sender, sender_bank), (receiver, receiver_bank) = rng.sample(accounts, 2)
        transfers.append((sender, sender_bank, receiver, receiver_bank, rng.randint(1, 300_000)))

    def run(transfer):
        sender, sender_bank, receiver, receiver_bank, amount = transfer
        stub, md = sessions[sender]
        return pay(stub, md, sender_bank, receiver, receiver_bank, amount).status

    with ThreadPoolExecutor(max_workers=16) as pool:
        statuses = list(pool.map(run, transfers))
    assert set(statuses) <= {"COMMITTED", "ABORTED"}
    assert "COMMITTED" in statuses
    assert system.total_money() == TOTAL
    assert all(balance >= 0 for bank in system.balances().values() for balance in bank.values())
    assert system.prepared_holds() == 0
