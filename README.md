# 🏦 Secure Distributed Payment Gateway

This project implements a **secure and distributed** payment gateway system using **gRPC**. It supports multiple **bank servers**, **clients**, **secure transactions**, and **two-phase commit (2PC)** for **atomic transactions**.

## 🚀 Features

### ✅ **Authentication & Authorization**
- Clients authenticate using **JWT tokens**.
- Role-based access control enforced via **gRPC Interceptors**.
- Secure **SSL/TLS encryption** for all communication.

### ✅ **Bank Registration**
- **Dynamic Bank Registration**: Banks **self-register** with the Payment Gateway at startup.
- Secure registration using **bank-specific certificates**.

### ✅ **Transactions**
- **Process Payments** securely via **Two-Phase Commit (2PC)**.
- **Idempotent Transactions** prevent duplicate payments.
- **Transaction History** is stored and accessible to users.

### ✅ **Offline Payments & Retry Mechanism**
- **Offline Mode**: If the Payment Gateway is down, transactions are **queued** and retried when online.
- **Automatic Retries** for failed transactions.

### ✅ **Logging & Monitoring**
- **All requests and responses** are logged.
- Failed transactions are **recorded in transactions.json**.
- Logging is handled via **gRPC Interceptors**.

---

## **1️⃣ Assumptions**
1. **Predefined Users**  
   - The system **does not allow new user registrations**.  
   - A predefined list of users is stored in `users.json`.  
   - Users must authenticate using their registered username and password. 
2. **Single Account per Bank**  
   - A user **cannot have multiple accounts in the same bank**.  
   - However, a user **can have accounts in different banks**. 
3. **Bank Registration Requires Certificates**  
   - Before a bank can register with the Payment Gateway,  
     it **must have its SSL key and certificate** (`{bank_name}.key` and `{bank_name}.crt`).  
   - If these files are missing, the bank **cannot establish a secure connection**.  

4. **Client & Bank Servers Know Payment Gateway Address**  
   - The Payment Gateway runs on a **fixed, known address** (`localhost:50052`).  
   - Clients and bank servers must **already know this address** to communicate securely.   
5. **Dynamic Bank Registration** – Banks **register automatically** with the Payment Gateway when started.  
6. **Client-Generated Transaction IDs** – Ensures **idempotency** and prevents duplicate payments.  
7. **SSL/TLS Security** – All communication is **secure** between clients, banks, and the payment gateway.  
8. **System Always Logs Transactions** – All transactions (success & failure) are stored in `transactions.json`.  


---

## **2️⃣ Installation & Setup**
### **📌 Prerequisites**
- Python 3.10+
- gRPC & Protocol Buffers (`protobuf`, `grpcio`, `grpcio-tools`)
- OpenSSL (for SSL/TLS certificates)

### **📌 Install Dependencies**
```sh
pip install grpcio grpcio-tools protobuf PyJWT

📌 Generate gRPC Code from Protobuf

python -m grpc_tools.protoc -I=protos --python_out=. --grpc_python_out=. protos/payment_gateway.proto
python -m grpc_tools.protoc -I=protos --python_out=. --grpc_python_out=. protos/bank.proto

---


3️⃣ Running the System
1. Start the Payment Gateway

python payment_gateway.py

2. Start Bank Servers (Example: HDFC, ICICI, SBI)

python bank_server.py HDFC 60051
python bank_server.py ICICI 60052
python bank_server.py SBI 60053

3. Run the Client

python client.py

---

4️⃣ Usage
Authenticate a User
> Enter Username: alice
> Enter Password: alice123
✅ Login successful! Token: eyJhbGciOi...

Check Balance

> Select an option: 3
Enter your bank name: HDFC  
💰 Your Balance: ₹5000.00  


Process a Payment

> Select an option: 2
Enter your bank name: ICICI  
Enter recipient's username: bob  
Enter recipient's bank name: HDFC  
Enter amount: 1000  
📌 Payment Status: Transaction successfully completed

View Transaction History

 > Select an option: 4
📜 Transaction History:

1️⃣ [TXN: txn_1710612345] Sent ₹1000.00 to bob (ICICI) ✅ Success - 2025-03-16 14:30:45
2️⃣ [TXN: txn_1710615678] Received ₹2000.00 from charlie (HDFC) ✅ Success - 2025-03-16 15:10:30
3️⃣ [TXN: txn_1710617890] Sent ₹500.00 to dave (SBI) ❌ Failed - Insufficient Funds - 2025-03-16 16:00:10
4️⃣ [TXN: txn_1710619000] Received ₹1500.00 from eve (SBI) ✅ Success - 2025-03-16 17:20:05
  

Logout

> Select an option: 5
✅ Logout successful!

---

5️⃣ Error Handling & Retries

    Automatic Retries if Payment Gateway is down.
    Idempotent Payments prevent duplicate deductions.
    2PC Aborts if any participant fails.

---    

7️⃣ Security

    JWT Tokens for authentication & authorization.
    SSL/TLS Encryption between all components.
    gRPC Interceptors for logging & access control.
---    

8️⃣ File Structure
    
📂 certs/  
 ├── 📂 bank/        # SSL certificates for banks (HDFC, ICICI, SBI)  
 ├── 📂 client/      # Client certificates  
 ├── 📂 ca/          # Certificate Authority (CA) files  
📂 client/  
 ├── client.py       # Client implementation  
 ├── offline_payments.json  # Stores queued payments  
📂 profiles/  
 ├── bank_pb2.py, payment_gateway_pb2.py  # gRPC-generated files  
📂 server/  
 ├── bank_server.py  # Bank server logic  
 ├── payment_gateway.py  # Payment Gateway server  
 ├── transactions.json  # Stores all transactions  
 ├── users.json  # Pre-configured users  

      

---