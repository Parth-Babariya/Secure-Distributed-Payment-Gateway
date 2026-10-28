"""Runtime configuration, overridable through environment variables."""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Generated protobuf modules live in protofiles/.
PROTO_DIR = ROOT / "protofiles"
if str(PROTO_DIR) not in sys.path:
    sys.path.append(str(PROTO_DIR))

CERT_DIR = Path(os.environ.get("PG_CERT_DIR", ROOT / "certs"))
DATA_DIR = Path(os.environ.get("PG_DATA_DIR", ROOT / "data"))
USERS_FILE = Path(os.environ.get("PG_USERS_FILE", ROOT / "server" / "users.json"))
BANK_SEED_FILE = Path(os.environ.get("PG_BANK_SEED_FILE", ROOT / "server" / "bank_data.json"))

GATEWAY_ADDRESS = os.environ.get("PG_GATEWAY_ADDRESS", "localhost:50052")
GATEWAY_PORT = int(os.environ.get("PG_GATEWAY_PORT", "50052"))

# Certificate common names. Bank certificates are "<bank>.bank" so that a
# client or gateway certificate can never register itself as a bank.
GATEWAY_CN = "gateway"
CLIENT_CN = "client"


def bank_cn(bank_name):
    return f"{bank_name.lower()}.bank"


# JWT secret: set PG_JWT_SECRET in any real deployment. Without it a random
# per-process secret is used, so tokens do not survive a gateway restart.
JWT_SECRET = os.environ.get("PG_JWT_SECRET")
TOKEN_TTL_SECONDS = int(os.environ.get("PG_TOKEN_TTL_SECONDS", "900"))

# Login throttling: lock a username after this many failures in the window.
MAX_LOGIN_FAILURES = 5
LOGIN_FAILURE_WINDOW_SECONDS = 300
LOGIN_LOCKOUT_SECONDS = 60

# Largest single payment accepted (Rs 10,00,000).
MAX_PAYMENT_PAISE = 100_000_000

# Deadline for every gateway -> bank RPC.
BANK_RPC_TIMEOUT_SECONDS = float(os.environ.get("PG_BANK_RPC_TIMEOUT", "3"))
# A transaction still in PREPARING after this long is presumed aborted.
PREPARE_TIMEOUT_SECONDS = float(os.environ.get("PG_PREPARE_TIMEOUT", "30"))
# How often the recovery worker drives in-doubt transactions forward.
RECOVERY_INTERVAL_SECONDS = float(os.environ.get("PG_RECOVERY_INTERVAL", "2"))
