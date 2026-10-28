"""Interactive client for the payment gateway.

Payments made while the gateway is unreachable are queued on disk and sent
automatically once it is back. Each queued payment keeps its transaction id,
so a payment that did reach the gateway before the connection dropped is never
applied twice.
"""
import json
import logging
import os
import sys
import threading
import uuid
from decimal import Decimal, InvalidOperation
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT / "server"))

import grpc  # noqa: E402

import config  # noqa: E402  (also puts protofiles/ on sys.path)
import payment_gateway_pb2  # noqa: E402
import payment_gateway_pb2_grpc  # noqa: E402
from tls import secure_channel  # noqa: E402

MAX_RETRIES = 3
RETRY_DELAY = 2
RPC_TIMEOUT = 10
RECONNECT_INTERVAL = 10
OFFLINE_QUEUE_FILE = Path(os.environ.get("PG_OFFLINE_QUEUE", Path(__file__).resolve().parent / "offline_payments.json"))

logging.basicConfig(filename=str(Path(__file__).resolve().parent / "client.log"), level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("client")


def rupees(paise):
    return f"Rs {paise / 100:,.2f}"


def parse_amount(text):
    """Convert a rupee amount such as '12.50' to integer paise (or None)."""
    try:
        value = Decimal(text.strip())
    except InvalidOperation:
        return None
    if not value.is_finite() or value <= 0 or value != value.quantize(Decimal("0.01")):
        return None
    return int(value * 100)


class GatewayClient:
    def __init__(self, address=config.GATEWAY_ADDRESS, cert_dir=config.CERT_DIR):
        self.channel = secure_channel(address, cert_dir, config.CLIENT_CN)
        self.stub = payment_gateway_pb2_grpc.PaymentGatewayStub(self.channel)
        self.token = None
        self.queue_lock = threading.Lock()
        self.offline_queue = self._load_queue()

    # -- plumbing ----------------------------------------------------------
    def _metadata(self):
        return [("authorization", self.token)] if self.token else []

    def _call(self, method, request):
        """Call an RPC, retrying while the gateway is unavailable."""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                return method(request, metadata=self._metadata(), timeout=RPC_TIMEOUT)
            except grpc.RpcError as e:
                if e.code() not in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED) \
                        or attempt == MAX_RETRIES:
                    raise
                print(f"Gateway unavailable, retrying ({attempt}/{MAX_RETRIES})...")
                threading.Event().wait(RETRY_DELAY)

    def is_online(self):
        try:
            grpc.channel_ready_future(self.channel).result(timeout=2)
            return True
        except grpc.FutureTimeoutError:
            return False

    def _load_queue(self):
        try:
            return json.loads(OFFLINE_QUEUE_FILE.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            return []

    def _save_queue(self):
        OFFLINE_QUEUE_FILE.write_text(json.dumps(self.offline_queue, indent=2), encoding="utf-8")

    # -- operations --------------------------------------------------------
    def login(self, username, password):
        response = self._call(self.stub.AuthenticateClient,
                              payment_gateway_pb2.AuthRequest(username=username, password=password))
        if response.authenticated:
            self.token = response.token
            log.info("Logged in as %s", username)
        return response.authenticated, response.message

    def logout(self):
        response = self._call(self.stub.LogoutClient, payment_gateway_pb2.LogoutRequest())
        self.token = None
        return response.message

    def balance(self, bank_name):
        return self._call(self.stub.CheckBalance, payment_gateway_pb2.BalanceRequest(bank_name=bank_name))

    def history(self):
        return self._call(self.stub.ViewTransactionHistory, payment_gateway_pb2.TransactionHistoryRequest()).transactions

    def pay(self, payment):
        request = payment_gateway_pb2.PaymentRequest(**payment)
        return self._call(self.stub.ProcessPayment, request)

    def pay_or_queue(self, sender_bank, receiver, receiver_bank, amount_paise):
        payment = {
            "transaction_id": uuid.uuid4().hex,
            "sender_bank_name": sender_bank,
            "receiver_username": receiver,
            "receiver_bank_name": receiver_bank,
            "amount_paise": amount_paise,
        }
        try:
            return self.pay(payment)
        except grpc.RpcError as e:
            if e.code() not in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                raise
        with self.queue_lock:
            self.offline_queue.append(payment)
            self._save_queue()
        log.info("Queued payment %s while offline", payment["transaction_id"])
        return None

    def flush_offline_queue(self):
        """Send queued payments. Retrying with the same id is safe."""
        if not self.token:
            return
        with self.queue_lock:
            pending = list(self.offline_queue)
        done = []
        for payment in pending:
            try:
                response = self.pay(payment)
                print(f"\n[offline queue] {payment['transaction_id']}: {response.status} - {response.message}")
                done.append(payment["transaction_id"])
            except grpc.RpcError as e:
                if e.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED,
                                grpc.StatusCode.UNAUTHENTICATED):
                    break  # try again later
                print(f"\n[offline queue] {payment['transaction_id']} rejected: {e.details()}")
                done.append(payment["transaction_id"])
        if done:
            with self.queue_lock:
                self.offline_queue = [p for p in self.offline_queue if p["transaction_id"] not in done]
                self._save_queue()

    def monitor(self, stop):
        """Background loop that flushes the offline queue when the gateway is reachable."""
        while not stop.wait(RECONNECT_INTERVAL):
            if self.offline_queue and self.is_online():
                try:
                    self.flush_offline_queue()
                except Exception:
                    log.exception("Flushing offline queue failed")


def print_history(transactions):
    if not transactions:
        print("No transactions yet.")
        return
    for txn in transactions:
        direction = f"to {txn.to_user_id}" if txn.to_user_id else f"from {txn.from_user_id}"
        print(f"{txn.timestamp}  {txn.transaction_id}  {rupees(txn.amount_paise)} {direction} "
              f"({txn.from_bank} -> {txn.to_bank})  {txn.status}"
              + (f"  [{txn.message}]" if txn.status.startswith("ABORT") and txn.message else ""))


MENU = """
1. Log in
2. Make a payment
3. Check balance
4. Transaction history
5. Log out
6. Exit"""


def main():
    client = GatewayClient()
    stop = threading.Event()
    threading.Thread(target=client.monitor, args=(stop,), daemon=True).start()
    if client.offline_queue:
        print(f"{len(client.offline_queue)} queued payment(s) will be sent after you log in.")

    while True:
        print(MENU)
        try:
            option = input("Select an option: ").strip()
            if option == "1":
                ok, message = client.login(input("Username: ").strip(), input("Password: "))
                print(message)
                if ok:
                    client.flush_offline_queue()
            elif option == "6":
                break
            elif client.token is None and option in {"2", "3", "4", "5"}:
                print("Please log in first.")
            elif option == "2":
                sender_bank = input("Your bank: ").strip().upper()
                receiver = input("Recipient username: ").strip()
                receiver_bank = input("Recipient bank: ").strip().upper()
                amount = parse_amount(input("Amount (Rs): "))
                if amount is None:
                    print("Enter a positive amount with at most two decimal places.")
                    continue
                response = client.pay_or_queue(sender_bank, receiver, receiver_bank, amount)
                if response is None:
                    print("Gateway unreachable: payment queued and will be sent automatically.")
                else:
                    print(f"{response.status}: {response.message} (id {response.transaction_id})")
            elif option == "3":
                response = client.balance(input("Bank: ").strip().upper())
                print(f"Balance {rupees(response.balance_paise)}, available {rupees(response.available_paise)}")
            elif option == "4":
                print_history(client.history())
            elif option == "5":
                print(client.logout())
            else:
                print("Invalid option.")
        except EOFError:
            break
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAUTHENTICATED:
                client.token = None
                print(f"Session ended ({e.details()}). Please log in again.")
            else:
                print(f"Error: {e.details()} [{e.code().name}]")
    stop.set()


if __name__ == "__main__":
    main()
