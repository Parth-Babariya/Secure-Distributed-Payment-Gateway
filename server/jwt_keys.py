"""Ed25519 signing keys for session tokens, with rotation.

A key directory holds one file per key, named by its key id (kid):

    <kid>.key   private key: signs new tokens and verifies old ones
    <kid>.pub   public key only: verifies tokens issued before a rotation

Key ids are UTC timestamps, so they sort by age and the newest private key
is the one that signs. Rotating is therefore: add a new key (it starts
signing), wait one token lifetime, then retire the old key (keep only its
public half) and later remove it. No user is logged out at any step.

Verification needs only public keys, so a service that merely checks tokens
can be given the .pub files and can never mint a token.
"""
import datetime
import logging
import os
import re
import threading
import time
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

log = logging.getLogger("auth")

KID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def _new_kid():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _private_bytes(key):
    return key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                             serialization.NoEncryption())


def _public_bytes(key):
    return key.public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)


def _write_private(path, data):
    # Create with owner-only permissions from the start (no window where the
    # key is world-readable).
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as f:
        f.write(data)


def generate_key(directory):
    """Create a new signing key in `directory` and return its kid."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    kid = _new_kid()
    _write_private(directory / f"{kid}.key", _private_bytes(Ed25519PrivateKey.generate()))
    return kid


def retire_key(directory, kid):
    """Keep only the public half of a key: it still verifies, never signs."""
    directory = Path(directory)
    private = directory / f"{kid}.key"
    key = serialization.load_pem_private_key(private.read_bytes(), password=None)
    (directory / f"{kid}.pub").write_bytes(_public_bytes(key.public_key()))
    private.unlink()


def remove_key(directory, kid):
    for suffix in (".key", ".pub"):
        (Path(directory) / f"{kid}{suffix}").unlink(missing_ok=True)


def list_keys(directory):
    """Return [(kid, 'signing' | 'verify-only')], oldest first."""
    directory = Path(directory)
    if not directory.is_dir():
        return []
    keys = {}
    for path in directory.iterdir():
        if path.suffix in (".key", ".pub") and KID_PATTERN.match(path.stem):
            if path.suffix == ".key" or path.stem not in keys:
                keys[path.stem] = "signing" if path.suffix == ".key" else "verify-only"
    return sorted(keys.items())


class KeyRing:
    """The signing key and every verification key, re-read from the directory
    when it changes so that a rotation takes effect without a restart.

    Without a directory (or with no private key in it) an in-memory key is
    generated, and tokens do not survive a restart.
    """

    def __init__(self, directory=None, reload_interval=5.0):
        self.directory = Path(directory) if directory else None
        self.reload_interval = reload_interval
        self._lock = threading.Lock()
        self._fingerprint = None
        self._checked_at = 0.0
        self._signing = None   # (kid, private key)
        self._public = {}      # kid -> public key
        self._ephemeral = None
        self._load()

    def _listing(self):
        if self.directory is None or not self.directory.is_dir():
            return ()
        return tuple(sorted((p.name, p.stat().st_mtime_ns) for p in self.directory.iterdir()
                            if p.suffix in (".key", ".pub") and KID_PATTERN.match(p.stem)))

    def _load(self):
        listing = self._listing()
        signing, public = None, {}
        for name, _ in listing:
            path = self.directory / name
            kid = path.stem
            try:
                if path.suffix == ".key":
                    key = serialization.load_pem_private_key(path.read_bytes(), password=None)
                    if not isinstance(key, Ed25519PrivateKey):
                        raise ValueError("not an Ed25519 key")
                    public[kid] = key.public_key()
                    if signing is None or kid > signing[0]:
                        signing = (kid, key)
                elif kid not in public:
                    public[kid] = serialization.load_pem_public_key(path.read_bytes())
            except (OSError, ValueError) as e:
                log.error("Ignoring JWT key %s: %s", path, e)
        if signing is None:
            if self._ephemeral is None:
                log.warning("No JWT signing key in %s; using an in-memory key (tokens will not "
                            "survive a restart). Run scripts/gen_certs.py or scripts/rotate_jwt_key.py.",
                            self.directory)
                self._ephemeral = ("ephemeral-" + _new_kid(), Ed25519PrivateKey.generate())
            signing = self._ephemeral
            public[signing[0]] = signing[1].public_key()
        if self._signing is not None and signing[0] != self._signing[0]:
            log.info("JWT signing key is now %s", signing[0])
        self._signing, self._public, self._fingerprint = signing, public, listing

    def _refresh(self):
        now = time.monotonic()
        if now - self._checked_at < self.reload_interval:
            return
        with self._lock:
            if now - self._checked_at < self.reload_interval:
                return
            self._checked_at = now
            if self._listing() != self._fingerprint:
                self._load()

    def signing_key(self):
        """(kid, private key) used for new tokens."""
        self._refresh()
        return self._signing

    def public_key(self, kid):
        """Verification key for kid, or None if unknown."""
        self._refresh()
        return self._public.get(kid)
