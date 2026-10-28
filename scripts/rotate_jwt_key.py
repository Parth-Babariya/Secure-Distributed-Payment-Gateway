"""Manage the Ed25519 keys that sign session tokens.

    python scripts/rotate_jwt_key.py list
    python scripts/rotate_jwt_key.py new           # becomes the signing key
    python scripts/rotate_jwt_key.py retire KID    # keep only the public key
    python scripts/rotate_jwt_key.py remove KID

A rotation is `new`, then after one token lifetime (PG_TOKEN_TTL_SECONDS)
`retire` the previous key, and `remove` it once nothing it signed can still
be valid. Running gateways pick up changes within a few seconds.
The directory is PG_JWT_KEY_DIR, or certs/jwt by default.
"""
import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "server"))

import jwt_keys  # noqa: E402


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dir", default=os.environ.get("PG_JWT_KEY_DIR") or str(ROOT / "certs" / "jwt"))
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list")
    commands.add_parser("new")
    for name in ("retire", "remove"):
        commands.add_parser(name).add_argument("kid")
    args = parser.parse_args(argv)

    keys = dict(jwt_keys.list_keys(args.dir))
    if args.command == "new":
        print(f"Created {jwt_keys.generate_key(args.dir)}; it signs new tokens from now on")
    elif args.command in ("retire", "remove"):
        if args.kid not in keys:
            parser.error(f"no key {args.kid} in {args.dir}")
        signing = [kid for kid, state in keys.items() if state == "signing"]
        if signing == [args.kid]:
            parser.error(f"{args.kid} is the only signing key; create a new one first")
        if args.command == "retire":
            if keys[args.kid] != "signing":
                parser.error(f"{args.kid} is already retired")
            jwt_keys.retire_key(args.dir, args.kid)
        else:
            jwt_keys.remove_key(args.dir, args.kid)
        print(f"{args.command.capitalize()}d {args.kid}")
    else:
        newest = max((kid for kid, state in keys.items() if state == "signing"), default=None)
        for kid, state in keys.items():
            print(f"{kid}  {state}{'  (active)' if kid == newest else ''}")


if __name__ == "__main__":
    sys.exit(main())
