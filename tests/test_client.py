import sys

import pytest

from harness import ROOT, PASSWORDS

sys.path.insert(0, str(ROOT / "client"))
import client as client_module  # noqa: E402


@pytest.fixture
def make_client(system, tmp_path, monkeypatch):
    monkeypatch.setattr(client_module, "OFFLINE_QUEUE_FILE", tmp_path / "queue.json")
    monkeypatch.setattr(client_module, "RETRY_DELAY", 0.05)
    monkeypatch.setattr(client_module, "RPC_TIMEOUT", 2)

    def make():
        return client_module.GatewayClient(f"localhost:{system.gateway_port}", system.cert_dir)
    return make


@pytest.mark.parametrize("text, paise", [("100", 10_000), ("0.01", 1), ("12.5", 1_250), (" 7.25 ", 725)])
def test_parse_amount_accepts_rupees(text, paise):
    assert client_module.parse_amount(text) == paise


@pytest.mark.parametrize("text", ["0", "-5", "1.234", "abc", "nan", "inf", ""])
def test_parse_amount_rejects_invalid_input(text):
    assert client_module.parse_amount(text) is None


def test_payments_made_offline_are_sent_once_when_the_gateway_returns(system, make_client):
    client = make_client()
    assert client.login("alice", PASSWORDS["alice"])[0]

    system.stop_gateway()
    assert client.pay_or_queue("HDFC", "bob", "ICICI", 10_000) is None
    assert len(client.offline_queue) == 1
    # The queue is on disk, so it survives a client restart.
    assert len(make_client().offline_queue) == 1

    system.start_gateway()
    # The background monitor flushes once the channel reconnects.
    assert system.wait_for(client.is_online)
    client.flush_offline_queue()
    assert client.offline_queue == []
    assert system.balances()["ICICI"]["2001"] == 410_000

    # Flushing the same payment again (e.g. the ack was lost) is harmless.
    queued_id = system.gateway.service.log.query("SELECT transaction_id FROM payments")[0][0]
    client.offline_queue = [{"transaction_id": queued_id, "sender_bank_name": "HDFC", "receiver_username": "bob",
                             "receiver_bank_name": "ICICI", "amount_paise": 10_000}]
    client.flush_offline_queue()
    assert system.balances()["ICICI"]["2001"] == 410_000
