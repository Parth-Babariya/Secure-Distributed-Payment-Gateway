"""EdDSA session tokens and signing-key rotation."""
import base64
import hashlib
import hmac
import json
import time

import jwt
import pytest
from cryptography.hazmat.primitives import serialization

import config
import jwt_keys
from auth import TokenError, TokenManager
from payment_gateway import CoordinatorLog
from rotate_jwt_key import main as rotate


def b64(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def make_tokens(tmp_path):
    keys = jwt_keys.KeyRing(tmp_path / "jwt", reload_interval=0)
    return TokenManager(CoordinatorLog(tmp_path / "gateway.db"), keys)


def test_tokens_are_eddsa_and_name_their_key(tmp_path):
    jwt_keys.generate_key(tmp_path / "jwt")
    tokens = make_tokens(tmp_path)
    token = tokens.issue("alice")
    header = jwt.get_unverified_header(token)
    assert header["alg"] == "EdDSA"
    assert header["kid"] == tokens.keys.signing_key()[0]
    assert tokens.verify(token)["sub"] == "alice"


def test_algorithm_confusion_and_unsigned_tokens_are_refused(tmp_path):
    jwt_keys.generate_key(tmp_path / "jwt")
    tokens = make_tokens(tmp_path)
    kid, key = tokens.keys.signing_key()
    claims = {"iss": config.TOKEN_ISSUER, "sub": "alice", "jti": "x", "exp": int(time.time()) + 60}
    body = b64(json.dumps(claims).encode())

    # HS256 "signed" with the (public) verification key as the HMAC secret.
    public_pem = key.public_key().public_bytes(serialization.Encoding.PEM,
                                               serialization.PublicFormat.SubjectPublicKeyInfo)
    head = b64(json.dumps({"alg": "HS256", "typ": "JWT", "kid": kid}).encode())
    mac = hmac.new(public_pem, f"{head}.{body}".encode(), hashlib.sha256).digest()
    with pytest.raises(TokenError):
        tokens.verify(f"{head}.{body}.{b64(mac)}")

    head = b64(json.dumps({"alg": "none", "kid": kid}).encode())
    with pytest.raises(TokenError):
        tokens.verify(f"{head}.{body}.")

    other = jwt.encode({**claims, "iss": "someone-else"}, key, algorithm="EdDSA", headers={"kid": kid})
    with pytest.raises(TokenError):
        tokens.verify(other)


def test_rotation_keeps_existing_sessions_valid(tmp_path):
    directory = tmp_path / "jwt"
    old_kid = jwt_keys.generate_key(directory)
    tokens = make_tokens(tmp_path)
    old_token = tokens.issue("alice")

    time.sleep(0.01)  # key ids are timestamps
    rotate(["--dir", str(directory), "new"])
    new_token = tokens.issue("bob")
    new_kid = jwt.get_unverified_header(new_token)["kid"]
    assert new_kid != old_kid
    # Both the old and the new session work after the rotation...
    assert tokens.verify(old_token)["sub"] == "alice"
    assert tokens.verify(new_token)["sub"] == "bob"

    # ...and after retiring the old key, which can then no longer sign.
    rotate(["--dir", str(directory), "retire", old_kid])
    assert dict(jwt_keys.list_keys(directory)) == {old_kid: "verify-only", new_kid: "signing"}
    assert not (directory / f"{old_kid}.key").exists()
    assert tokens.verify(old_token)["sub"] == "alice"

    # Removing it ends the sessions it signed.
    rotate(["--dir", str(directory), "remove", old_kid])
    with pytest.raises(TokenError, match="unknown key"):
        tokens.verify(old_token)
    assert tokens.verify(new_token)["sub"] == "bob"


def test_the_last_signing_key_cannot_be_retired(tmp_path):
    directory = tmp_path / "jwt"
    kid = jwt_keys.generate_key(directory)
    with pytest.raises(SystemExit):
        rotate(["--dir", str(directory), "retire", kid])
    assert dict(jwt_keys.list_keys(directory)) == {kid: "signing"}


def test_without_a_key_directory_an_in_memory_key_is_used(tmp_path):
    tokens = make_tokens(tmp_path)  # tmp_path/jwt does not exist
    assert tokens.keys.signing_key()[0].startswith("ephemeral-")
    assert tokens.verify(tokens.issue("alice"))["sub"] == "alice"
