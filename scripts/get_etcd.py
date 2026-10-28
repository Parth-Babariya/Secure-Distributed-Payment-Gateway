"""Download the etcd release used by the replicated-coordinator mode.

The archive is checked against the SHA256SUMS file published with the
release before anything is unpacked. Binaries land in .tools/etcd/ (not
committed); scripts/cluster.py and the tests look there.

Usage: python scripts/get_etcd.py
"""
import hashlib
import io
import platform
import sys
import tarfile
import urllib.request
import zipfile
from pathlib import Path

VERSION = "v3.6.16"
ROOT = Path(__file__).resolve().parent.parent
TARGET = ROOT / ".tools" / "etcd"
BASE_URL = f"https://github.com/etcd-io/etcd/releases/download/{VERSION}"


def archive_name():
    system = {"Windows": "windows", "Linux": "linux", "Darwin": "darwin"}[platform.system()]
    arch = "arm64" if platform.machine().lower() in ("arm64", "aarch64") else "amd64"
    extension = "tar.gz" if system == "linux" else "zip"
    return f"etcd-{VERSION}-{system}-{arch}.{extension}"


def binary_path(name="etcd"):
    suffix = ".exe" if platform.system() == "Windows" else ""
    return TARGET / f"{name}{suffix}"


def fetch(url):
    with urllib.request.urlopen(url, timeout=120) as response:
        return response.read()


def main():
    if binary_path().exists():
        print(f"etcd already installed at {binary_path()}")
        return
    name = archive_name()
    sums = fetch(f"{BASE_URL}/SHA256SUMS").decode()
    expected = next((line.split()[0] for line in sums.splitlines() if line.endswith(name)), None)
    if expected is None:
        sys.exit(f"{name} is not listed in SHA256SUMS")
    data = fetch(f"{BASE_URL}/{name}")
    actual = hashlib.sha256(data).hexdigest()
    if actual != expected:
        sys.exit(f"Checksum mismatch for {name}: expected {expected}, got {actual}")

    TARGET.mkdir(parents=True, exist_ok=True)
    wanted = {binary_path("etcd").name, binary_path("etcdctl").name}
    if name.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            members = {Path(m).name: m for m in archive.namelist()}
            for file_name in wanted:
                (TARGET / file_name).write_bytes(archive.read(members[file_name]))
    else:
        with tarfile.open(fileobj=io.BytesIO(data)) as archive:
            for member in archive.getmembers():
                if Path(member.name).name in wanted and member.isfile():
                    (TARGET / Path(member.name).name).write_bytes(archive.extractfile(member).read())
    for file_name in wanted:
        (TARGET / file_name).chmod(0o755)
    print(f"Installed etcd {VERSION} (sha256 {actual[:16]}...) to {TARGET}")


if __name__ == "__main__":
    main()
