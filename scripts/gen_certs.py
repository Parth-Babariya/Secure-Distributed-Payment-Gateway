"""Generate a local CA and mTLS certificates for every component.

Certificates are never committed: run this once after cloning (or let Docker
Compose and the test suite run it). Usage:

    python scripts/gen_certs.py [--out certs] [--banks HDFC ICICI SBI] [--force] [--split]
"""
import argparse
import datetime
import ipaddress
import sys
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BANKS = ["HDFC", "ICICI", "SBI"]
VALIDITY = datetime.timedelta(days=365)


def _name(common_name):
    return x509.Name([
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Secure Distributed Payment Gateway"),
        x509.NameAttribute(NameOID.COMMON_NAME, common_name),
    ])


def _write(path, data):
    path.write_bytes(data)


def _key_bytes(key):
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def _cert_bytes(cert):
    return cert.public_bytes(serialization.Encoding.PEM)


def make_ca():
    key = ec.generate_private_key(ec.SECP256R1())
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(_name("Payment Gateway Demo CA"))
        .issuer_name(_name("Payment Gateway Demo CA"))
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + VALIDITY)
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=False, content_commitment=False, key_encipherment=False,
                data_encipherment=False, key_agreement=False, key_cert_sign=True,
                crl_sign=True, encipher_only=False, decipher_only=False,
            ),
            critical=True,
        )
        .sign(key, hashes.SHA256())
    )
    return key, cert


def make_leaf(ca_key, ca_cert, common_name, dns_names):
    key = ec.generate_private_key(ec.SECP256R1())
    now = datetime.datetime.now(datetime.timezone.utc)
    sans = [x509.DNSName(name) for name in dns_names]
    sans.append(x509.IPAddress(ipaddress.ip_address("127.0.0.1")))
    cert = (
        x509.CertificateBuilder()
        .subject_name(_name(common_name))
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + VALIDITY)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.SubjectAlternativeName(sans), critical=False)
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH, ExtendedKeyUsageOID.CLIENT_AUTH]),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )
    return key, cert


def generate(out_dir, banks=DEFAULT_BANKS, force=False, split=False):
    """Write the CA and one certificate per component.

    By default everything goes into one directory (local development and
    tests). With split=True the CA goes into <out>/ca/ and each component gets
    <out>/<name>/ holding only its own key, its certificate and ca.crt, so a
    deployment can give each process access to nothing but its own key.
    """
    out_dir = Path(out_dir)
    ca_dir = out_dir / "ca" if split else out_dir
    ca_dir.mkdir(parents=True, exist_ok=True)
    if (ca_dir / "ca.crt").exists() and not force:
        print(f"Certificates already exist in {out_dir} (use --force to regenerate)")
        return

    ca_key, ca_cert = make_ca()
    _write(ca_dir / "ca.key", _key_bytes(ca_key))
    _write(ca_dir / "ca.crt", _cert_bytes(ca_cert))

    # Identity name -> (certificate CN, DNS names it may be reached at).
    identities = {
        "gateway": ("gateway", ["localhost", "gateway"]),
        "client": ("client", ["localhost", "client"]),
    }
    for bank in banks:
        name = bank.lower()
        identities[name] = (f"{name}.bank", ["localhost", name])

    for file_name, (common_name, dns_names) in identities.items():
        key, cert = make_leaf(ca_key, ca_cert, common_name, dns_names)
        target = out_dir / file_name if split else out_dir
        target.mkdir(parents=True, exist_ok=True)
        _write(target / f"{file_name}.key", _key_bytes(key))
        _write(target / f"{file_name}.crt", _cert_bytes(cert))
        if split:
            _write(target / "ca.crt", _cert_bytes(ca_cert))
    print(f"Wrote CA and {len(identities)} certificates to {out_dir}" + (" (one directory each)" if split else ""))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", default=str(ROOT / "certs"))
    parser.add_argument("--banks", nargs="+", default=DEFAULT_BANKS)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--split", action="store_true",
                        help="one directory per component, CA key kept apart (used by Docker Compose)")
    args = parser.parse_args(argv)
    generate(args.out, args.banks, args.force, args.split)


if __name__ == "__main__":
    sys.exit(main())
