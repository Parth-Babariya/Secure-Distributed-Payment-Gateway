"""Print a password hash for server/users.json.

Usage: python scripts/hash_password.py <password>
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "server"))

from auth import hash_password  # noqa: E402

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__.strip())
        sys.exit(1)
    print(hash_password(sys.argv[1]))
