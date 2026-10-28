"""Login throttling, certificate roles and per-component key isolation."""
import grpc

from harness import PASSWORDS, expect_code
import config
import payment_gateway_pb2 as pg
from auth import LoginThrottle
from gen_certs import generate as generate_certs
from payment_gateway import CoordinatorLog
from tls import server_credentials


def make_throttle(tmp_path, **limits):
    return LoginThrottle(CoordinatorLog(tmp_path / "throttle.db"), **limits)


def test_failures_from_one_client_do_not_lock_the_user_out_elsewhere(tmp_path):
    throttle = make_throttle(tmp_path)
    for _ in range(5):
        throttle.record_failure("alice", "10.0.0.66")
    assert throttle.locked_for("alice", "10.0.0.66") > 0
    # The real alice, connecting from her own machine, is unaffected.
    assert throttle.locked_for("alice", "10.0.0.7") == 0


def test_a_client_guessing_across_many_usernames_is_locked(tmp_path):
    throttle = make_throttle(tmp_path, max_client_failures=20)
    for i in range(20):
        throttle.record_failure(f"user{i}", "10.0.0.66")
    assert throttle.locked_for("someone-new", "10.0.0.66") > 0
    assert throttle.locked_for("someone-new", "10.0.0.7") == 0


def test_success_clears_the_account_counter_but_not_the_client_counter(tmp_path):
    throttle = make_throttle(tmp_path, max_failures=3, max_client_failures=4)
    for _ in range(2):
        throttle.record_failure("alice", "10.0.0.66")
    throttle.record_success("alice", "10.0.0.66")
    throttle.record_failure("alice", "10.0.0.66")
    assert throttle.locked_for("alice", "10.0.0.66") == 0
    # Logging into an account you own does not reset the spraying budget.
    throttle.record_failure("bob", "10.0.0.66")
    assert throttle.locked_for("carol", "10.0.0.66") > 0


def test_lockouts_survive_a_gateway_restart(system):
    stub = system.stub()
    for _ in range(5):
        assert not stub.AuthenticateClient(pg.AuthRequest(username="alice", password="guess")).authenticated
    system.stop_gateway()
    system.start_gateway()
    expect_code(grpc.StatusCode.RESOURCE_EXHAUSTED, system.stub().AuthenticateClient,
                pg.AuthRequest(username="alice", password=PASSWORDS["alice"]))


def test_user_calls_require_the_client_certificate(system):
    _, metadata = system.login("alice")
    # A valid user token presented over a bank's or the gateway's certificate
    # is refused: user RPCs are only for client applications.
    for identity in ("hdfc", "gateway"):
        expect_code(grpc.StatusCode.PERMISSION_DENIED, system.stub(identity).CheckBalance,
                    pg.BalanceRequest(bank_name="HDFC"), metadata=metadata)
        expect_code(grpc.StatusCode.PERMISSION_DENIED, system.stub(identity).AuthenticateClient,
                    pg.AuthRequest(username="alice", password=PASSWORDS["alice"]))


def test_split_layout_gives_each_component_only_its_own_key(tmp_path):
    generate_certs(tmp_path, ["HDFC", "ICICI"], split=True)
    assert sorted(p.name for p in (tmp_path / "ca").iterdir()) == ["ca.crt", "ca.key"]
    for identity in ("gateway", "client", "hdfc", "icici"):
        files = sorted(p.name for p in (tmp_path / identity).iterdir())
        assert files == sorted(["ca.crt", f"{identity}.crt", f"{identity}.key"])
        server_credentials(tmp_path / identity, identity)  # loads cleanly
    # Every component trusts the same CA.
    ca = (tmp_path / "ca" / "ca.crt").read_bytes()
    assert all((tmp_path / i / "ca.crt").read_bytes() == ca for i in ("gateway", "hdfc", "client"))
    assert config.bank_cn("HDFC") == "hdfc.bank"
