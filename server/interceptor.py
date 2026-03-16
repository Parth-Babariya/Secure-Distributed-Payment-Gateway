import logging
import grpc
import jwt
from grpc import StatusCode

# Configure logging
logging.basicConfig(filename="interceptortransactions.log", level=logging.INFO, format="%(asctime)s - %(message)s")

# Secret key for JWT verification (must match the one used in `payment_gateway.py`)
JWT_SECRET = "supersecretkey"


class RegisterBankInterceptor(grpc.ServerInterceptor):
    def intercept_service(self, continuation, handler_call_details):
        """Interceptor specifically for bank registration requests."""
        method = handler_call_details.method
        
        # Only apply logic to RegisterBank method
        if "/PaymentGateway/RegisterBank" in method:
            print(f"Processing bank registration request for method: {method}")
            
            # Extract metadata
            metadata = dict(handler_call_details.invocation_metadata)
            bank_name = metadata.get("bank_name", "Unknown Bank")
            
            # Log the bank registration attempt
            logging.info(f"🏦 Bank registration attempt: {bank_name}")
            
            try:
                # Execute the original method
                response = continuation(handler_call_details)
                
                # Log successful registration
                logging.info(f"✅ Successfully registered bank: {bank_name}")
                return response
            except grpc.RpcError as e:
                # Log registration failure
                logging.error(f"❌ Bank registration failed for {bank_name}: {e.code()} - {e.details()}")
                raise e
        else:
            # For non-bank registration methods, just pass through
            return continuation(handler_call_details)

class AuthorizationInterceptor(grpc.ServerInterceptor):
    def intercept_service(self, continuation, handler_call_details):
        """Intercepts gRPC requests to enforce authorization checks."""
        method = handler_call_details.method

        # Skip auth check for the exempt methods
        exempt_methods = ["/PaymentGateway/AuthenticateClient", "/PaymentGateway/RegisterBank"]
        if any(exempt_method in method for exempt_method in exempt_methods):
            return continuation(handler_call_details)

        # method = handler_call_details.method

        # Extract JWT from metadata
        metadata = dict(handler_call_details.invocation_metadata)
        token = metadata.get("authorization")
        print(f"Token: {token}")
        # Skip auth check for the authentication method itself
        if "/PaymentGateway/AuthenticateClient" in method:
            return continuation(handler_call_details)

        if not token:
            logging.warning(f"❌ Unauthorized request to {method} - No token provided.")
            context = self._create_abortion_context(StatusCode.UNAUTHENTICATED, "Missing authentication token.")
            return context

        # Decode JWT Token
        try:
            decoded_token = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
            username = decoded_token.get("username")
        except jwt.ExpiredSignatureError:
            logging.warning(f"❌ Unauthorized request to {method} - Token expired.")
            context = self._create_abortion_context(StatusCode.UNAUTHENTICATED, "Token expired.")
            return context
        except jwt.InvalidTokenError:
            logging.warning(f"❌ Unauthorized request to {method} - Invalid token.")
            context = self._create_abortion_context(StatusCode.UNAUTHENTICATED, "Invalid token.")
            return context

        logging.info(f"✅ Authorized request from {username} - Method: {method}")

        # Additional Authorization: Restrict Access Based on Method
        if "/PaymentGateway/CheckBalance" in method:
            requested_user = metadata.get("username")
            if requested_user and requested_user != username:
                logging.warning(f"❌ Unauthorized access attempt by {username} to check balance of {requested_user}.")
                print("mdnkmn,mdndkndmndm,ndlmndm")
                context = self._create_abortion_context(StatusCode.PERMISSION_DENIED, "Unauthorized balance access.")
                return context

        if "/PaymentGateway/ProcessPayment" in method:
            logging.info(f"✅ {username} initiated a payment.")
        
        # Call the original RPC method if authorization is successful
        return continuation(handler_call_details)
    
    def _create_abortion_context(self, status_code, details):
        """Helper method to create a context that aborts the call with given status code and details."""
        class AbortionContext(grpc.ServicerContext):
            def abort(self, code, details):
                raise grpc.RpcError(code, details)
            
            def abort_with_status(self, status):
                raise grpc.RpcError(status.code(), status.details())
            
            def is_active(self):
                return False
            
            def time_remaining(self):
                return None
            
            def cancel(self):
                return False
            
            def add_callback(self, callback):
                return False
            
            def invocation_metadata(self):
                return None
            
            def peer(self):
                return None
            
            def peer_identities(self):
                return None
            
            def peer_identity_key(self):
                return None
            
            def auth_context(self):
                return None
            
            def set_compression(self, compression):
                pass
            
            def send_initial_metadata(self, initial_metadata):
                pass
            
            def set_trailing_metadata(self, trailing_metadata):
                pass
            
            def disable_next_message_compression(self):
                pass
            
            def get_code(self):
                return status_code
            
            def get_details(self):
                return details
        
        context = AbortionContext()
        context.abort(status_code, details)
        return context

class LoggingInterceptor(grpc.ServerInterceptor):
    def intercept_service(self, continuation, handler_call_details):
        """Intercepts and logs all gRPC requests and responses."""
        method = handler_call_details.method
        metadata = dict(handler_call_details.invocation_metadata)
        client_ip = metadata.get("x-real-ip", "Unknown")
        
        # Log the request
        logging.info(f"✅ Request from {client_ip} - Method: {method}")
        
        try:
            # Execute the original method
            response = continuation(handler_call_details)
            
            # Log successful response
            logging.info(f"✅ Success: {method} completed")
            return response
        except grpc.RpcError as e:
            # Log error response
            logging.error(f"❌ Error in {method}: {e.code()} - {e.details()}")
            raise e

# Add detailed logging for transactions
class TransactionLoggingInterceptor(grpc.ServerInterceptor):
    def intercept_service(self, continuation, handler_call_details):
        """Specialized interceptor for detailed transaction logging."""
        method = handler_call_details.method
        
        if "ProcessPayment" in method:
            metadata = dict(handler_call_details.invocation_metadata)
            token = metadata.get("authorization")
            username = "Unknown"
            
            # Try to extract username from JWT for better logs
            if token:
                try:
                    decoded_token = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
                    username = decoded_token.get("username", "Unknown")
                except:
                    pass
            
            try:
                # Execute the original method
                response = continuation(handler_call_details)
                
                # Log successful transaction
                logging.info(f"✅ Transaction processed successfully by {username} in {method}")
                return response
            except grpc.RpcError as e:
                # Handle network errors with retry logging
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    logging.warning(f"🔄 Retrying {method} for {username} due to network failure")
                
                # Log transaction failure with details
                logging.error(f"❌ Transaction failed for {username} in {method}: {e.code()} - {e.details()}")
                raise e
        else:
            # For non-transaction methods, just pass through
            return continuation(handler_call_details)