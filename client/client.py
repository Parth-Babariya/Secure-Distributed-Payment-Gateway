import grpc
import sys
import os
import json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'protofiles')))
import payment_gateway_pb2
import payment_gateway_pb2_grpc
import time
import uuid
import logging
from datetime import datetime
from threading import Thread, Lock

# Constants
MAX_RETRIES = 3
RETRY_DELAY = 3
OFFLINE_QUEUE_FILE = "offline_payments.json"
RECONNECT_INTERVAL = 30  # Check connection every 30 seconds

#Global variables
jwt_token = None
stub = None # ✅ Global stub for reuse
offline_queue = []
queue_lock = Lock()
is_online = False


# Configure logging
logging.basicConfig(
    filename="client.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def load_offline_queue():
    """Load pending offline payments from disk."""
    global offline_queue
    try:
        if os.path.exists(OFFLINE_QUEUE_FILE):
            with open(OFFLINE_QUEUE_FILE, 'r') as file:
                offline_queue = json.load(file)
                if offline_queue:
                    logging.info(f"Loaded {len(offline_queue)} pending offline payments")
                    # print(f"📋 Loaded {len(offline_queue)} pending offline payments")
    except Exception as e:
        logging.error(f"Failed to load offline queue: {str(e)}")
        print(f"❌ Failed to load offline queue: {str(e)}")
        offline_queue = []


def save_offline_queue():
    """Save pending offline payments to disk."""
    try:
        with open(OFFLINE_QUEUE_FILE, 'w') as file:
            json.dump(offline_queue, file)
            logging.info(f"Saved {len(offline_queue)} pending offline payments")
    except Exception as e:
        logging.error(f"Failed to save offline queue: {str(e)}")
        print(f"❌ Failed to save offline queue: {str(e)}")       

def authenticate_client(stub, username, password):
    """Authenticates a client using JWT and retries if the Payment Gateway is down."""
    global jwt_token, is_online
   
   
        
    metadata = [("authorization", jwt_token)] if jwt_token else []  # Attach token if available
    # request = payment_gateway_pb2.AuthRequest(username=username, password=password,metadata=metadata)
    
    request = payment_gateway_pb2.AuthRequest(username=username, password=password)
    print("🔄 Request sent")

    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            response = stub.AuthenticateClient(request,metadata=metadata)
            print("✅ Response received")
            is_online = True  # Mark system as online

            if response.authenticated:
                print(f"✅ Login successful!")
                jwt_token = response.token
                logging.info(f"✅ Authentication successful for {username}.")
                return True
            else:
                if response.message == "Already logged in. Please log out first or wait for token expiration.":
                    print("❌ Login failed! You are already logged in with an active session.")
                    return False
                else:
                    print("❌ Login failed! Invalid username or password.")
                    return False

        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                attempt += 1
                print(f"❌ Payment Gateway is down. Retrying in {RETRY_DELAY} seconds... (Attempt {attempt}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY)
                initialize_stub()
                print("🔄 Retrying authentication...")
                is_online = False  # Mark system as offline
            elif e.code() == grpc.StatusCode.UNAUTHENTICATED:
                print("❌ Authentication failed: Invalid token or credentials.")
                return False
            elif e.code() == grpc.StatusCode.INVALID_ARGUMENT:
                print("❌ Authentication failed: Invalid request format.")
                return False
            else:
                print(f"❌ Authentication failed: {e.code()} - {e.details()}")
                return False

    print("❌ Authentication failed after multiple attempts. Please check the server.")
    is_online = False  # Mark system as offline
    return False         



def check_balance(stub,bank_name):
    """Checks balance with retries if Payment Gateway is down."""
    global is_online
    
    if jwt_token is None:
        print("❌ Please authenticate first.")
        return
    print("bank_name",bank_name)
    metadata = [("authorization", jwt_token)]
    request = payment_gateway_pb2.BalanceRequest(bank_name=bank_name)
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            response = stub.CheckBalance(request, metadata=metadata)
            print(f"💰 Your Balance: {response.balance}")
            logging.info(f"✅ Balance check successful.")
            is_online = True  # Mark system as online
            return
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                attempt += 1
                logging.warning(f"🔄 Retrying balance check... Attempt {attempt}/{MAX_RETRIES}")
                print(f"🔄 Retrying balance check... Attempt {attempt}/{MAX_RETRIES}")
                time.sleep(RETRY_DELAY)
                initialize_stub()
                is_online = False  # Mark system as offline
            else:
                print(f"❌ Balance check failed: {e.code()} - {e.details()}")
                logging.error(f"❌ Balance check failed: {e.code()} - {e.details()}")
                return

    logging.error(f"❌ Balance check failed after {MAX_RETRIES} attempts.")
    print("❌ Balance check failed after multiple attempts. Please try again later.")
    is_online = False  # Mark system as offline





def process_payment(stub, sender_bank_name, receiver_username, receiver_bank_name, amount):
    """Processes a payment using JWT authentication with automatic offline queuing."""
    global is_online, offline_queue

    if jwt_token is None:
        print("❌ Please authenticate first.")
        return

    # Generate a unique transaction ID for idempotency
    transaction_id = str(uuid.uuid4())
    timestamp = datetime.now().isoformat()

    # Create payment data structure for offline queueing
    payment_data = {
        "transaction_id": transaction_id,
        "sender_bank_name": sender_bank_name,
        "receiver_username": receiver_username,
        "receiver_bank_name": receiver_bank_name,
        "amount": amount,
        "timestamp": timestamp,
    }

    # If offline, queue the payment automatically
    if not is_online:
        with queue_lock:
            offline_queue.append(payment_data)
            save_offline_queue()
        print(f"📝 Payment queued for offline processing (Transaction ID: {transaction_id})")
        logging.info(f"💳 Payment of ₹{amount} to {receiver_username}@{receiver_bank_name} queued for offline processing.")
        return

    metadata = [("authorization", jwt_token)]
    request = payment_gateway_pb2.PaymentRequest(
        sender_bank_name=sender_bank_name,  # ✅ Only sender_bank_name (no account number)
        receiver_username=receiver_username,
        receiver_bank_name=receiver_bank_name,
        amount=amount,
        transaction_id=transaction_id
    )

    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            response = stub.ProcessPayment(request, metadata=metadata)
            print(f"📌 Payment Status: {response.message}")
            logging.info(f"✅ Payment of ₹{amount} from {sender_bank_name} to {receiver_username}@{receiver_bank_name} succeeded.")
            is_online = True  # Mark system as online
            return
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                attempt += 1
                logging.warning(f"🔄 Retrying payment... Attempt {attempt}/{MAX_RETRIES}")
                print(f"🔄 Retrying payment... Attempt {attempt}/{MAX_RETRIES}")
                time.sleep(RETRY_DELAY)
                initialize_stub()
                if attempt >= MAX_RETRIES:
                    with queue_lock:
                        offline_queue.append(payment_data)
                        save_offline_queue()
                    print(f"📝 Payment queued for offline processing after failed retries (Transaction ID: {transaction_id})")
                    logging.info(f"💳 Payment of ₹{amount} to {receiver_username}@{receiver_bank_name} queued for offline processing after failed retries.")
                    is_online = False  # Mark system as offline
                    return
            elif e.code() == grpc.StatusCode.FAILED_PRECONDITION:
                print("❌ Transaction failed: Insufficient funds.")
                logging.warning(f"❌ Payment of ₹{amount} from {sender_bank_name} to {receiver_username}@{receiver_bank_name} failed due to insufficient funds.")
                return
            elif e.code() == grpc.StatusCode.ALREADY_EXISTS:
                print("❌ Transaction with this ID already processed.")
                logging.warning(f"❌ Payment with ID {transaction_id} already processed.")
                return
            else:
                print(f"❌ Payment failed: {e.code()} - {e.details()}")
                logging.error(f"❌ Payment failed: {e.code()} - {e.details()}")
                return

    logging.error(f"❌ Payment permanently failed after {MAX_RETRIES} attempts.")
    print("❌ Payment failed after multiple attempts. Please try again later.")
    is_online = False  # Mark system as offline




def logout_client(stub):
    """Logs out the user by sending a logout request with the current token."""
    global jwt_token  # Ensure token is globally updated

    if not jwt_token:
        print("❌ You are not logged in.")
        return

    metadata = [("authorization", jwt_token)]
    request = payment_gateway_pb2.LogoutRequest()
    attempt = 0
    while attempt < MAX_RETRIES:

        try:
            response = stub.LogoutClient(request, metadata=metadata)
            if response.success:
                jwt_token = None  # Invalidate locally
                print("✅ Logout successful!")
                is_online = True  # Mark system as online
                return
            else:
                print(f"❌ Logout failed: {response.message}")
                is_online = True  # Mark system as online
                return

        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                attempt += 1
                logging.warning(f"🔄 Retrying logout... Attempt {attempt}/{MAX_RETRIES}")
                print(f"🔄 Retrying logout... Attempt {attempt}/{MAX_RETRIES}")
                time.sleep(RETRY_DELAY)
                initialize_stub()
                is_online = False  # Mark system as offline
            else:    
                print(f"❌ Logout error: {e.code()} - {e.details()}")
                return     



def process_offline_queue():
    """Process all pending offline payments automatically."""
    global offline_queue
    
    if not offline_queue:
        # print("✅ No pending offline payments to process.")
        return
    
    if jwt_token is None:
        print("❌ Please authenticate first before processing offline payments.")
        return
    
    # print(f"🔄 Processing {len(offline_queue)} offline payments...")
    
    with queue_lock:
        current_queue = offline_queue.copy()
    
    successful_payments = []
    
    for payment in current_queue:
        # print(f"🔄 Processing offline payment to {payment['to_account']}, amount: ₹{payment['amount']}")
        
        metadata = [("authorization", jwt_token)]
        request = payment_gateway_pb2.PaymentRequest(
             sender_bank_name=payment["sender_bank_name"],  # ✅ Only sender_bank_name (no account number)
            receiver_username=payment["receiver_username"],
            receiver_bank_name=payment["receiver_bank_name"],
            amount=payment["amount"],
            transaction_id=payment["transaction_id"]
            
        )
        
        try:
            response = stub.ProcessPayment(request, metadata=metadata)
            print(f"✅ Offline payment processed: {response.message}")
            logging.info(f"✅ Offline payment (ID: {payment['transaction_id']}) of ₹{payment['amount']} to {payment['to_account']} processed successfully.")
            successful_payments.append(payment)
            
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.ALREADY_EXISTS:
                # print(f"✅ Payment already processed (Transaction ID: {payment['transaction_id']})")
                logging.info(f"Payment already processed (Transaction ID: {payment['transaction_id']})")
                successful_payments.append(payment)
            elif e.code() == grpc.StatusCode.FAILED_PRECONDITION:
                print(f"❌ Offline payment failed: Insufficient funds (Transaction ID: {payment['transaction_id']})")
                logging.warning(f"❌ Offline payment failed: Insufficient funds (Transaction ID: {payment['transaction_id']})")
                successful_payments.append(payment)
            elif e.code() == grpc.StatusCode.INVALID_ARGUMENT:  
                print("Cannot transfer money to your own account within the same bank.")
                logging.error("Cannot transfer money to your own account within the same bank.")  
            else:
                print(f"❌ Failed to process offline payment: {e.code()} - {e.details()}")
                logging.error(f"❌ Failed to process offline payment (ID: {payment['transaction_id']}): {e.code()} - {e.details()}")
        except Exception as ex:
            print(f"❌ Unexpected error processing offline payment: {str(ex)}")
            logging.error(f"❌ Unexpected error processing offline payment (ID: {payment['transaction_id']}): {str(ex)}")
    
    with queue_lock:
        offline_queue = [p for p in offline_queue if p not in successful_payments]
        save_offline_queue()
    logging.info(f"Processed {len(successful_payments)} offline payments. {len(offline_queue)} remaining.")
    print(f"✅ Processed {len(successful_payments)} offline payments. {len(offline_queue)} remaining.")

def check_connection():
    """Check if the payment gateway is reachable."""
    global is_online
    
    try:
        if stub is None:
            initialize_stub()
        # logging.info("🔄 Checking connection...")
        dummy_request = payment_gateway_pb2.AuthRequest(username="ping", password="ping")
        stub.AuthenticateClient(dummy_request, timeout=2)
        
        is_online = True
        return True
    except Exception:
        is_online = False
        return False

def connection_monitor():
    """Background thread to monitor connection and automatically process offline payments when online."""
    # global is_online
    
    logging.info("Connection monitor started")
    
    while True:
        try:
            current_status = is_online
            new_status = check_connection()
            
            # If connection was restored and we have authentication
            if new_status and jwt_token is not None and offline_queue:
                print(f"DEBUG: offline_queue contents: {offline_queue} (length: {len(offline_queue)})")

                # Only log if status changed or we have payments to process
                if not current_status:
                    logging.info("🌐 Connection restored, processing offline queue...")
                    print("🌐 Connection restored, processing offline queue...")
                
                # Process the offline queue automatically
                process_offline_queue()
            
            # Sleep for a bit before checking again
            time.sleep(RECONNECT_INTERVAL)
        except Exception as e:
            logging.error(f"Error in connection monitor: {str(e)}")
            time.sleep(RECONNECT_INTERVAL)


def initialize_stub():
    """Creates a secure gRPC connection with TLS."""
    global stub

    try:
        with open("../certs/client/client.key", "rb") as f:
            private_key = f.read()
        with open("../certs/client/client.crt", "rb") as f:
            certificate_chain = f.read()
        with open("../certs/ca/ca.crt", "rb") as f:
            root_certificates = f.read()
        
        credentials = grpc.ssl_channel_credentials(
            root_certificates=root_certificates,
            private_key=private_key,
            certificate_chain=certificate_chain
        )
        
        options = [
            ('grpc.max_receive_message_length', 10 * 1024 * 1024),
            ('grpc.max_send_message_length', 10 * 1024 * 1024),
            ('grpc.keepalive_time_ms', 30000),
            ('grpc.keepalive_timeout_ms', 10000)
        ]
        
        channel = grpc.secure_channel("localhost:50052", credentials, options=options)
        stub = payment_gateway_pb2_grpc.PaymentGatewayStub(channel)
          # ✅ Verify the connection by making a lightweight test call
        try:
            grpc.channel_ready_future(channel).result(timeout=3)  # Wait for connection
            print("🔄 Secure gRPC connection established.")
            logging.info("Secure gRPC connection established.")
        except grpc.FutureTimeoutError:
            print("❌ Failed to connect: Payment Gateway is down.")
            logging.error("Failed to connect: Payment Gateway is down.")
    except Exception as e:
        print(f"❌ Failed to initialize connection: {str(e)}")
        logging.error(f"Failed to initialize connection: {str(e)}")


def view_transaction_history(stub):
    """Request and display the user's transaction history with updated fields."""
    if jwt_token is None:
        print("❌ Please authenticate first.")
        return

    metadata = [("authorization", jwt_token)]
    request = payment_gateway_pb2.TransactionHistoryRequest()
    print("🔄 Request sent")

    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            response = stub.ViewTransactionHistory(request, metadata=metadata)
            print("✅ Response received")
            transactions = response.transactions  # ✅ Fetch transactions from gRPC response
            
            if not transactions:
                print("📜 No transaction history available.")
                is_online = True  # Mark system as online
                return

            print("\n📜 Transaction History:")
            print("🆔 Transaction ID | 🏦 From Bank | 🏦 To Bank | 💰 Amount | 📌 Status | 📅 Timestamp")
            print("-" * 120)
            
            for txn in transactions:
                from_account_display = txn.from_account_no if txn.from_account_no else "N/A"
                to_user_display = txn.to_user_id if txn.to_user_id else "N/A"
                from_user_display = txn.from_user_id if txn.from_user_id else "N/A"
                
                print(f"🆔 {txn.transaction_id} | 🏦 {txn.from_bank} | 🏦 {txn.to_bank} | ₹{txn.amount} | {txn.status} | 📅 {txn.timestamp}")

                if from_account_display != "N/A":
                    print(f"   🔹 From Account No: {from_account_display}")
                if to_user_display != "N/A":
                    print(f"   🔹 To User ID: {to_user_display}")
                if from_user_display != "N/A":
                    print(f"   🔹 From User ID: {from_user_display}")
                
                print("-" * 120)

            return     
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                attempt += 1
                logging.warning(f"🔄 Retrying Transaction History... Attempt {attempt}/{MAX_RETRIES}")
                print(f"🔄 Retrying Transaction History... Attempt {attempt}/{MAX_RETRIES}")
                time.sleep(RETRY_DELAY)
                initialize_stub()
                is_online = False  # Mark system as offline
            else:       
                print(f"❌ Error fetching transaction history: {e.code()} - {e.details()}")
                logging.error(f"❌ Error fetching transaction history: {e.code()} - {e.details()}")
                return

def main():
    initialize_stub()  # ✅ Initialize stub at startup# Load any pending offline payments
    load_offline_queue() # Load any pending offline payments
    
    # Start the background connection monitor thread
    monitor_thread = Thread(target=connection_monitor, daemon=True)
    monitor_thread.start()
    logging.info("Started background connection monitor")
    print("🔄 Started background connection monitor")

    while True:
        print("\nOptions:")
        # print("1. Register Client")
        print("1. Authenticate")
        print("2. Process Payment")
        print("3. Check Balance")
        print("4. View Transaction History")
        print("5. Logout")
        print("6. Exit")
        option = input("Select an option: ")

        if option == "1":
            username = input("Enter username: ")
            password = input("Enter password: ")
            authenticate_client( stub , username, password)

        elif option == "2":
            sender_bank_name= input("Enter sender bank name: ")
            receiver_username = input("Enter recipient username: ")
            receiver_bank_name = input("Enter recipient bank name: ")
             # ✅ Validate amount input

            try:
                amount = float(input("Enter amount: "))
                if amount <= 0:
                    print("❌ Amount must be greater than zero. Please enter a valid amount.")
                    continue  # Go back to the menu without calling process_payment
            except ValueError:
                print("❌ Invalid amount! Please enter a numeric value.")
                continue  # Go back to the menu without calling process_payment
            process_payment(stub, sender_bank_name,receiver_username, receiver_bank_name, amount)

        elif option == "3":
            bank_name = input("Enter your bank name: ") 
            check_balance(stub,bank_name)

        elif option == "4":
            view_transaction_history(stub) 

        elif option == "5":
            logout_client(stub)   

        elif option == "6":
            print("🚀 Exiting...")
            break

        else:
            print("❌ Invalid option. Please try again.")

if __name__ == "__main__":
    main()  # ✅ Now only one function handles everything












