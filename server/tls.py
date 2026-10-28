"""Mutual-TLS helpers shared by the gateway, banks and client."""
from pathlib import Path

import grpc

CHANNEL_OPTIONS = [
    ("grpc.keepalive_time_ms", 30000),
    ("grpc.keepalive_timeout_ms", 10000),
]


def _read(path):
    return Path(path).read_bytes()


def identity_paths(cert_dir, name):
    """Return (key, cert, ca) paths for an identity such as 'gateway' or 'hdfc'."""
    cert_dir = Path(cert_dir)
    return cert_dir / f"{name}.key", cert_dir / f"{name}.crt", cert_dir / "ca.crt"


def server_credentials(cert_dir, name):
    key, cert, ca = identity_paths(cert_dir, name)
    return grpc.ssl_server_credentials(
        [(_read(key), _read(cert))],
        root_certificates=_read(ca),
        require_client_auth=True,
    )


def channel_credentials(cert_dir, name):
    key, cert, ca = identity_paths(cert_dir, name)
    return grpc.ssl_channel_credentials(
        root_certificates=_read(ca),
        private_key=_read(key),
        certificate_chain=_read(cert),
    )


def secure_channel(target, cert_dir, name):
    return grpc.secure_channel(target, channel_credentials(cert_dir, name), options=CHANNEL_OPTIONS)


def peer_common_name(context):
    """Common name of the verified peer certificate, or None."""
    names = context.auth_context().get("x509_common_name") or []
    return names[0].decode() if names else None
