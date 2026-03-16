
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'protofiles')))
import grpc
from concurrent import futures
import json
import bank_pb2
import bank_pb2_grpc
import time
import logging
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'protofiles')))
import payment_gateway_pb2
import payment_gateway_pb2_grpc

# Configure logging
logging.basicConfig(
    filename="banktransactions.log",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


PAYMENT_GATEWAY_ADDRESS = "localhost:50052"

def register_bank(bank_name, port):
    """Registers the bank with the Payment Gateway with retries until successful."""
    RETRY_DELAY = 5  # Delay between retry attempts in seconds

    # Load SSL/TLS certificates (done once, outside the retry loop)
    try:
        bank_key_path = f"../certs/bank/{bank_name.lower()}.key"  # Example: hdfc.key
        bank_cert_path = f"../certs/bank/{bank_name.lower()}.crt"  # Example: hdfc.crt
        ca_cert_path = "../certs/ca/ca.crt"

        with open(bank_key_path, "rb") as f:
            private_key = f.read()
        with open(bank_cert_path, "rb") as f:
            certificate_chain = f.read()
        with open(ca_cert_path, "rb") as f:
            root_certificates = f.read()
    except Exception as e:
        print(f"❌ Failed to load certificates: {str(e)}")
        logging.error(f"Failed to load certificates: {str(e)}")
        raise  # Exit if certificates cannot be loaded, as this is a fatal error

    bank_port = int(port)  # Ensure it's stored as an integer

    # Establish credentials (done once, outside the retry loop)
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

    # Retry loop for gRPC connection and registration
    while True:
        try:
            # Create a new channel for each attempt
            channel = grpc.secure_channel("localhost:50052", credentials, options=options)
            print("channel created")
            stub = payment_gateway_pb2_grpc.PaymentGatewayStub(channel)

            try:
                grpc.channel_ready_future(channel).result(timeout=3)  # Wait for connection
                print("🔄 Secure gRPC connection established.")
                logging.info("Secure gRPC connection established.")

                # Send registration request
                request = payment_gateway_pb2.RegisterBankRequest(bank_name=bank_name, bank_port=bank_port)
                print("🔄 Request sent")
                response = stub.RegisterBank(request)
                print("✅ Response received")

                if response.success:
                    print(f"✅ Successfully registered {bank_name} on port {bank_port} with Payment Gateway.")
                    logging.info(f"Successfully registered {bank_name} on port {bank_port} with Payment Gateway.")
                    channel.close()  # Clean up the channel
                    return  # Exit on successful registration
                else:
                    print(f"⚠️ {bank_name} registration failed: {response.message}")
                    logging.warning(f"{bank_name} registration failed: {response.message}")

            except grpc.FutureTimeoutError:
                print("❌ Failed to connect: Payment Gateway is down.")
                logging.error("Failed to connect: Payment Gateway is down.")
            
            channel.close()  # Clean up the channel before retrying
            print(f"🔄 Retrying registration for {bank_name} in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)  # Wait before retrying

        except Exception as e:
            print(f"❌ Registration attempt failed: {str(e)}")
            logging.error(f"Registration attempt failed: {str(e)}")
            time.sleep(RETRY_DELAY)  # Wait before retrying
        



class BankService(bank_pb2_grpc.BankServicer):
    def __init__(self, bank_name):
        self.bank_name = bank_name
        self.accounts = self.load_bank_data()
        # ✅ Register the bank with Payment Gateway at startup
        print("before register_bank")
        register_bank(bank_name, port)
        # Dictionary to store pending transactions
        self.pending_transactions = {}
        # Track account locks
        self.account_locks = {}

    def load_bank_data(self):
        """Load account balances for the given bank from bank_data.json."""
        with open("bank_data.json", "r") as f:
            data = json.load(f)
        return data.get(self.bank_name, {})
    
    def save_bank_data(self):
        """Save the current account balances to bank_data.json."""
        with open("bank_data.json", "r") as f:
            data = json.load(f)
        
        data[self.bank_name] = self.accounts
        
        with open("bank_data.json", "w") as f:
            json.dump(data, f, indent=4)


    def GetBalance(self, request, context):
        """Returns the balance of an account."""
        account = request.account_number
        print("account", account)
        balance = self.accounts.get(account, 0.0)
        return bank_pb2.BalanceResponse(balance=balance)
    
    def ProcessTransaction(self, request, context):
        """Legacy method - still supported but 2PC is recommended."""
        logging.warning(f"⚠️ Legacy transaction processing for {request.from_account} -> {request.to_account}")

        # ✅ Step 1: Check if this is a credit transaction (funds coming from outside)
        if request.from_account == "SYSTEM":
            if request.to_account not in self.accounts:
                return bank_pb2.TransactionResponse(success=False, message="Invalid Receiver Account")

            self.accounts[request.to_account] += request.amount  # Credit the amount
            self.save_bank_data()
            return bank_pb2.TransactionResponse(success=True, message="Funds credited successfully")

        # ✅ Step 2: Deduct funds from sender's account (existing logic)
        if request.from_account not in self.accounts:
            return bank_pb2.TransactionResponse(success=False, message="Invalid Sender Account")

        if self.accounts[request.from_account] < request.amount:
            return bank_pb2.TransactionResponse(success=False, message="Insufficient funds")

        self.accounts[request.from_account] -= request.amount  # Deduct funds
        self.save_bank_data()

        return bank_pb2.TransactionResponse(success=True, message="Amount deducted, waiting for receiver bank")

    def PrepareTransaction(self, request, context):
        """Phase 1: Prepare for transaction by checking and locking funds."""
        transaction_id = request.transaction_id
        account_number = request.account_number
        amount = request.amount
        operation = request.operation  # "debit" or "credit"
        
        is_debit = (operation == "debit")
        
        logging.info(f"🔒 Preparing transaction {transaction_id}: {'debit from' if is_debit else 'credit to'} {account_number} for ${amount}")
        
        # Check if this is a credit transaction (receiver bank)
        if not is_debit:
            # For receiving bank, check if account exists
            if account_number not in self.accounts:
                return bank_pb2.PrepareResponse(
                    success=False,
                    message="Invalid receiver account"
                )
                
            # Store the pending transaction
            self.pending_transactions[(transaction_id, operation)] = {
                'type': 'credit',
                'account': account_number,
                'amount': amount,
                'timestamp': time.time()
            }
            
            return bank_pb2.PrepareResponse(
                success=True,
                message="Ready to receive funds"
            )
            
        # For sending bank (debit operation), check if there are sufficient funds
        if account_number not in self.accounts:
            return bank_pb2.PrepareResponse(
                success=False,
                message="Invalid sender account"
            )
            
        if self.accounts[account_number] < amount:
            return bank_pb2.PrepareResponse(
                success=False,
                message="Insufficient funds"
            )
            
        # Check if the account is already locked by another transaction
        if account_number in self.account_locks and self.account_locks[account_number] != transaction_id:
            return bank_pb2.PrepareResponse(
                success=False,
                message="Account locked by another transaction"
            )
            
        # Lock the funds
        self.account_locks[account_number] = transaction_id
        
        # Store the pending transaction
        self.pending_transactions[(transaction_id, operation)] = {
            'type': 'debit',
            'account': account_number,
            'amount': amount,
            'timestamp': time.time()
        }
        
        return bank_pb2.PrepareResponse(
            success=True,
            message="Funds locked successfully"
        )
        
    def CommitTransaction(self, request, context):
        """Phase 2: Commit the transaction after all participants are prepared."""
        transaction_id = request.transaction_id
        account_number = request.account_number
        amount = request.amount
        operation = request.operation  # "debit" or "credit"
        
        logging.info(f"✅ Committing transaction {transaction_id} for {operation} operation")
        
        # Check if this transaction exists in our pending transactions
        if (transaction_id, operation) not in self.pending_transactions:
            return bank_pb2.CommitResponse(
                success=False,
                message=f"Unknown transaction: {transaction_id}"
            )
            
        transaction = self.pending_transactions[(transaction_id, operation)]
        
        # Verify that the account number matches the one in pending transaction
        if transaction['account'] != account_number:
            return bank_pb2.CommitResponse(
                success=False,
                message=f"Account mismatch: {account_number} vs {transaction['account']}"
            )
            
        # Process based on operation type
        if operation == "debit":
            # Actually deduct the funds
            self.accounts[account_number] -= amount
            
            # Release the lock
            if account_number in self.account_locks:
                del self.account_locks[account_number]
                
        elif operation == "credit":
            # Credit the funds
            self.accounts[account_number] += amount
            
        # Save the updated account data
        self.save_bank_data()
            
        # Remove from pending transactions
        del self.pending_transactions[(transaction_id, operation)]
        
        return bank_pb2.CommitResponse(
            success=True,
            message="Transaction committed successfully"
        )
    
    def AbortTransaction(self, request, context):
        """Phase 2 (Abort): Release locks and cancel the transaction."""
        transaction_id = request.transaction_id
        account_number = request.account_number
        operation = request.operation  # "debit" or "credit"
        
        logging.info(f"❌ Aborting transaction {transaction_id} for {operation} operation")
        
        # Check if this transaction exists in our pending transactions
        if (transaction_id, operation) not in self.pending_transactions:
            return bank_pb2.AbortResponse(
                success=True,
                message=f"Unknown transaction: {transaction_id}, nothing to abort"
            )
            
        transaction = self.pending_transactions[(transaction_id, operation)]
        
        # If it's a debit transaction, release the account lock
        if operation == "debit" and account_number in self.account_locks:
            del self.account_locks[account_number]
        
        # Remove from pending transactions
        del self.pending_transactions[(transaction_id, operation)]
        
        return bank_pb2.AbortResponse(
            success=True,
            message="Transaction aborted successfully"
        )

def serve(bank_name, port):
    """Start a secure bank server with TLS."""
    cert_path = f'../certs/bank/{bank_name.lower()}.crt'
    key_path = f'../certs/bank/{bank_name.lower()}.key'

    with open(key_path, 'rb') as f:
        private_key = f.read()
    with open(cert_path, 'rb') as f:
        certificate_chain = f.read()
    with open('../certs/ca/ca.crt', 'rb') as f:
        root_certificates = f.read()

    server_credentials = grpc.ssl_server_credentials(
        [(private_key, certificate_chain)],
        root_certificates=root_certificates,
        require_client_auth=True
    )


    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    bank_pb2_grpc.add_BankServicer_to_server(BankService(bank_name), server)

    # Start the secure server
    server.add_secure_port(f"[::]:{port}", server_credentials)
    logging.info(f"port={port}")
    logging.info(f"🚀 Secure Bank Server '{bank_name}' started on port {port} with 2PC support")

    print("port=" + port)
    print(f"🚀 Secure Bank Server '{bank_name}' started on port {port}")

    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python bank_server.py <BANK_NAME> <PORT>")
        sys.exit(1)

    bank_name = sys.argv[1]
    port = sys.argv[2]
    # ✅ Register the bank before starting the server
    # register_bank(bank_name,port)
    serve(bank_name, port)
