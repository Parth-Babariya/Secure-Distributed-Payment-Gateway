import time

import grpc
import jwt

from harness import TEST_JWT_SECRET, expect_code
import bank_pb2
import payment_gateway_pb2 as pg


def test_calls_without_a_valid_token_are_unauthenticated(system):
    stub = system.stub()
    request = pg.BalanceRequest(bank_name="HDFC")
    expect_code(grpc.StatusCode.UNAUTHENTICATED, stub.CheckBalance, request)
    expect_code(grpc.StatusCode.UNAUTHENTICATED, stub.CheckBalance, request,
                metadata=[("authorization", "not-a-jwt")])

    forged = jwt.encode({"sub": "alice", "jti": "x", "exp": int(time.time()) + 60}, "wrong-secret-that-is-also-long-enough",
                        algorithm="HS256")
    expect_code(grpc.StatusCode.UNAUTHENTICATED, stub.CheckBalance, request,
                metadata=[("authorization", forged)])

    expired = jwt.encode({"sub": "alice", "jti": "y", "exp": int(time.time()) - 5}, TEST_JWT_SECRET,
                         algorithm="HS256")
    error = expect_code(grpc.StatusCode.UNAUTHENTICATED, stub.CheckBalance, request,
                        metadata=[("authorization", expired)])
    assert "expired" in error.details().lower()


def test_logout_revokes_the_token_and_allows_a_fresh_login(system):
    stub, md = system.login("alice")
    stub.LogoutClient(pg.LogoutRequest(), metadata=md)
    error = expect_code(grpc.StatusCode.UNAUTHENTICATED, stub.CheckBalance,
                        pg.BalanceRequest(bank_name="HDFC"), metadata=md)
    assert "revoked" in error.details()
    # Logging straight back in works (no "already logged in" lock-out).
    stub, md = system.login("alice")
    assert stub.CheckBalance(pg.BalanceRequest(bank_name="HDFC"), metadata=md).balance_paise == 150_000


def test_revocation_survives_a_gateway_restart(system):
    stub, md = system.login("alice")
    stub.LogoutClient(pg.LogoutRequest(), metadata=md)
    system.stop_gateway()
    system.start_gateway()
    expect_code(grpc.StatusCode.UNAUTHENTICATED, system.stub().CheckBalance,
                pg.BalanceRequest(bank_name="HDFC"), metadata=md)


def test_repeated_failed_logins_lock_the_account(system):
    stub = system.stub()
    for _ in range(5):
        response = stub.AuthenticateClient(pg.AuthRequest(username="alice", password="guess"))
        assert not response.authenticated
    expect_code(grpc.StatusCode.RESOURCE_EXHAUSTED, stub.AuthenticateClient,
                pg.AuthRequest(username="alice", password="alice123"))
    # Unknown users get the same answer as a wrong password.
    response = stub.AuthenticateClient(pg.AuthRequest(username="mallory", password="x"))
    assert not response.authenticated and response.message == "Invalid username or password"


def test_users_cannot_read_other_users_accounts(system):
    stub, md = system.login("bob")
    expect_code(grpc.StatusCode.PERMISSION_DENIED, stub.CheckBalance, pg.BalanceRequest(bank_name="HDFC"),
                metadata=md)


def test_only_a_banks_own_certificate_can_register_it(system):
    request = pg.RegisterBankRequest(bank_name="HDFC", bank_port=1, bank_host="attacker.example")
    expect_code(grpc.StatusCode.PERMISSION_DENIED, system.stub("client").RegisterBank, request)
    expect_code(grpc.StatusCode.PERMISSION_DENIED, system.stub("icici").RegisterBank, request)

    # HDFC is still reachable at its real address.
    stub, md = system.login("alice")
    assert stub.CheckBalance(pg.BalanceRequest(bank_name="HDFC"), metadata=md).balance_paise == 150_000


def test_banks_only_accept_calls_from_the_gateway(system):
    bank = system.bank_stub("HDFC", identity="client")
    expect_code(grpc.StatusCode.PERMISSION_DENIED, bank.GetBalance, bank_pb2.BalanceRequest(account_number="1001"))
    expect_code(grpc.StatusCode.PERMISSION_DENIED, bank.PrepareTransaction,
                bank_pb2.PrepareRequest(transaction_id="t", account_number="1001", amount_paise=100,
                                        operation=bank_pb2.CREDIT))
    # Another bank cannot drive this bank's ledger either.
    other_bank = system.bank_stub("HDFC", identity="sbi")
    expect_code(grpc.StatusCode.PERMISSION_DENIED, other_bank.CommitTransaction,
                bank_pb2.DecisionRequest(transaction_id="t", operation=bank_pb2.CREDIT))
