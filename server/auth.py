"""User authentication: password hashing, login throttling and JWT sessions."""
import hashlib
import hmac
import json
import logging
import secrets
import time
import uuid

import jwt

import config

log = logging.getLogger("auth")

SCRYPT_N, SCRYPT_R, SCRYPT_P = 2 ** 14, 8, 1


def hash_password(password, salt=None):
    """Return 'scrypt$n$r$p$salt$hash' for storing in users.json."""
    salt = salt or secrets.token_bytes(16)
    digest = hashlib.scrypt(password.encode(), salt=salt, n=SCRYPT_N, r=SCRYPT_R, p=SCRYPT_P)
    return f"scrypt${SCRYPT_N}${SCRYPT_R}${SCRYPT_P}${salt.hex()}${digest.hex()}"


def verify_password(password, stored):
    try:
        scheme, n, r, p, salt, expected = stored.split("$")
    except (AttributeError, ValueError):
        return False
    if scheme != "scrypt":
        return False
    digest = hashlib.scrypt(password.encode(), salt=bytes.fromhex(salt), n=int(n), r=int(r), p=int(p))
    return hmac.compare_digest(digest.hex(), expected)


# Verified against when the username does not exist, so a failed login takes
# the same time whether or not the user is real.
_DUMMY_HASH = hash_password("not-a-real-password")


def load_users(path=None):
    """Return {username: {"password_hash": str, "accounts": {bank_name: account_number}}}."""
    with open(path or config.USERS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    users = {}
    for entry in data.get("users", []):
        accounts = {a["bank_name"]: a["account_number"] for a in entry.get("accounts", [])}
        users[entry["username"]] = {"password_hash": entry["password_hash"], "accounts": accounts}
    return users


class LoginThrottle:
    """Rate-limits failed logins, persisted in the gateway's database.

    Two counters apply to each attempt:
      * (username, client address): an attacker guessing alice's password
        locks out only their own attempts, not alice logging in from elsewhere;
      * client address alone, with a higher limit: stops one client spraying
        guesses across many usernames.
    Once a counter reaches its limit within the window, that key is locked
    until `lockout` seconds after its most recent failure.
    """

    def __init__(self, store, max_failures=config.MAX_LOGIN_FAILURES,
                 max_client_failures=config.MAX_CLIENT_LOGIN_FAILURES,
                 window=config.LOGIN_FAILURE_WINDOW_SECONDS,
                 lockout=config.LOGIN_LOCKOUT_SECONDS):
        self.store = store
        self.window = window
        self.lockout = lockout
        self.max_failures = max_failures
        self.max_client_failures = max_client_failures

    @staticmethod
    def _account_key(username, client):
        return f"account:{username}|client:{client}"

    @staticmethod
    def _client_key(client):
        return f"client:{client}"

    def locked_for(self, username, client):
        """Seconds this attempt must wait (0 if it may proceed)."""
        now = time.time()
        remaining = 0.0
        for key, limit in ((self._account_key(username, client), self.max_failures),
                           (self._client_key(client), self.max_client_failures)):
            count, latest = self.store.login_failures(key, now - self.window)
            if count >= limit:
                remaining = max(remaining, latest + self.lockout - now)
        return remaining

    def record_failure(self, username, client):
        self.store.record_login_failure(
            [self._account_key(username, client), self._client_key(client)], time.time(), self.window)

    def record_success(self, username, client):
        # Only the account counter is cleared: logging into an account the
        # client owns must not reset its budget for guessing other accounts.
        self.store.clear_login_failures(self._account_key(username, client))


class TokenError(Exception):
    pass


class TokenManager:
    """Issues HS256 JWTs and checks them against a revocation store."""

    def __init__(self, revocations, secret=None, ttl_seconds=config.TOKEN_TTL_SECONDS):
        if not secret:
            log.warning("PG_JWT_SECRET is not set; using a random per-process secret")
            secret = secrets.token_urlsafe(32)
        if len(secret) < 32:
            raise ValueError("PG_JWT_SECRET must be at least 32 characters")
        self.secret = secret
        self.ttl = ttl_seconds
        self.revocations = revocations

    def issue(self, username):
        now = int(time.time())
        payload = {"sub": username, "jti": uuid.uuid4().hex, "iat": now, "exp": now + self.ttl}
        return jwt.encode(payload, self.secret, algorithm="HS256")

    def verify(self, token):
        """Return the token's claims or raise TokenError."""
        if not token:
            raise TokenError("Missing authentication token")
        try:
            claims = jwt.decode(token, self.secret, algorithms=["HS256"], options={"require": ["sub", "jti", "exp"]})
        except jwt.ExpiredSignatureError:
            raise TokenError("Token expired")
        except jwt.InvalidTokenError:
            raise TokenError("Invalid token")
        if self.revocations.is_revoked(claims["jti"]):
            raise TokenError("Token has been revoked")
        return claims

    def revoke(self, claims):
        self.revocations.revoke(claims["jti"], claims["exp"])
