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


def gateway_channel(address, cert_dir, name):
    """Channel to the gateway. `address` may list several gateways as
    comma-separated IP:port pairs; calls are then spread round-robin over
    the ones that are up, so a gateway crash only fails the calls in flight
    on it."""
    addresses = [a.strip() for a in address.split(",") if a.strip()]
    if len(addresses) == 1:
        return secure_channel(addresses[0], cert_dir, name)
    options = CHANNEL_OPTIONS + [
        ("grpc.lb_policy_name", "round_robin"),
        # Every gateway presents the same "gateway" certificate.
        ("grpc.ssl_target_name_override", "gateway"),
    ]
    return grpc.secure_channel("ipv4:" + ",".join(addresses), channel_credentials(cert_dir, name), options=options)


def peer_host(context):
    """Caller's address without the port, e.g. '10.0.0.5' or '::1'."""
    kind, _, rest = context.peer().partition(":")
    if kind in ("ipv4", "ipv6"):
        return rest.rsplit(":", 1)[0].strip("[]")
    return context.peer()


def peer_common_name(context):
    """Common name of the verified peer certificate, or None."""
    names = context.auth_context().get("x509_common_name") or []
    return names[0].decode() if names else None
