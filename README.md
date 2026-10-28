# Secure Distributed Payment Gateway

[![CI](https://github.com/Parth-Babariya/Secure-Distributed-Payment-Gateway/actions/workflows/ci.yml/badge.svg)](https://github.com/Parth-Babariya/Secure-Distributed-Payment-Gateway/actions/workflows/ci.yml)

A payment gateway that moves money between accounts at independent bank servers, built with Python and gRPC. Each transfer is **atomic across banks** using two-phase commit (2PC) with durable logs and automatic crash recovery. Every connection uses mutual TLS, and users authenticate with revocable JWT sessions.

The headline result: a chaos test sends payments continuously while hard-killing the gateway and banks at random. Afterwards it checks every payment against both banks' ledgers and confirms that each one either fully happened or did not happen at all, and that no money was created or lost.

The full design report, with correctness arguments and evaluation, is [Report.pdf](Report.pdf) (LaTeX source in [report/](report/)).

## Architecture

```mermaid
flowchart LR
    C[Client CLI<br/>offline queue] -- "mTLS + JWT" --> G
    subgraph G[Payment Gateway — 2PC coordinator]
        A[Auth: scrypt passwords,<br/>JWT, login throttling]
        P[Coordinator]
        R[Recovery worker]
        L[(gateway.db<br/>coordinator log)]
        P --> L
        R --> L
    end
    P -- "mTLS (gateway cert only)" --> H[HDFC bank]
    P -- "mTLS" --> I[ICICI bank]
    P -- "mTLS" --> S[SBI bank]
    H --> HL[(hdfc.db<br/>ledger)]
    I --> IL[(icici.db)]
    S --> SL[(sbi.db)]
    H -. "RegisterBank<br/>(cert CN must match)" .-> G
```

| Component | File | Role |
|---|---|---|
| Payment gateway | [server/payment_gateway.py](server/payment_gateway.py) | Authenticates users, validates payments, coordinates 2PC, recovers in-doubt transactions |
| Bank server | [server/bank_server.py](server/bank_server.py) | 2PC participant; durable SQLite ledger with fund reservations |
| Client | [client/client.py](client/client.py) | Interactive CLI; queues payments on disk while the gateway is unreachable |
| Interceptors | [server/interceptor.py](server/interceptor.py) | JWT authorization, certificate-identity checks, access logging with status and latency |
| Certificates | [scripts/gen_certs.py](scripts/gen_certs.py) | Generates a local CA and one certificate per component (never committed) |

## How a payment works

```
PREPARING --both banks vote yes--> COMMITTING --both acknowledge--> COMMITTED
    |
    +--a "no" vote, bank unreachable, or prepare timeout--> ABORTING --both acknowledge--> ABORTED
```

1. **Validate.** The gateway checks the JWT, that the amount is positive and within limits, and that the sender owns the source account and the receiver account exists. Amounts are integer **paise**, never floats.
2. **Log.** The payment is inserted as `PREPARING`. The client-generated transaction id is the primary key, so a retry with the same id returns the stored outcome instead of charging twice.
3. **Phase 1 (prepare).** Both banks are asked in parallel. The sender's bank *reserves* the funds, which reduces the available balance; the receiver's bank checks that the account exists. Each bank writes its vote to its own SQLite ledger before replying, so a bank that crashes after voting yes still honours the decision when it restarts.
4. **Decision.** The gateway durably records `COMMITTING` (or `ABORTING`) *before* telling any bank, using a compare-and-set on the status.
5. **Phase 2.** Commit and abort are idempotent at the banks, so the gateway can retry them safely. An abort for a transaction a bank never saw leaves a tombstone, so a delayed prepare arriving later is refused.
6. **Recovery.** A background worker, which also runs right after a restart, retries phase 2 for every `COMMITTING`/`ABORTING` payment until both banks acknowledge. A payment stuck in `PREPARING` past a timeout is aborted (*presumed abort*), because no commit decision was ever logged.

**Why double spending is impossible.** A bank's funds check and its reservation run in one serialised SQLite transaction (`BEGIN IMMEDIATE`). Two concurrent payments therefore cannot both pass the check. The test suite fires 40 concurrent ₹100 payments at a ₹1,500 balance and asserts exactly 15 succeed.

## Security model

| Threat | Mitigation |
|---|---|
| Eavesdropping or tampering | mTLS on every hop; the CA signs a separate certificate for each component |
| Leaked keys in the repository | No keys are committed. `scripts/gen_certs.py` creates them locally, in Docker, and in tests |
| One compromised container exposing every key | In Docker each container mounts only its own key, its certificate and `ca.crt`; the CA key is in a volume no running service mounts (checked in CI) |
| Calling a bank directly to mint or move money | Banks accept calls only from the certificate with CN `gateway` |
| Impersonating a bank to divert its traffic | `RegisterBank` requires the caller's certificate CN to be `<bank>.bank` |
| Using a bank or gateway certificate to act as a user | User RPCs accept only the `client` certificate, even with a valid JWT |
| Negative or oversized amounts | Validated on the server; the banks also reject amounts ≤ 0 |
| Stolen password database | Passwords are stored as salted scrypt hashes and compared in constant time; logins for unknown users take the same time |
| Password guessing | 5 failures in 5 minutes per (username, client address), and 20 per client address across all usernames, lock further attempts for 60 s. Attempts are counted per address, so an attacker cannot lock the real user out from elsewhere; the counts are stored, so a restart does not reset them |
| Token theft after logout | Each JWT has a `jti` that logout revokes; the revocation list is persisted, so it survives restarts |
| Forged or replayed session tokens | Tokens are EdDSA (Ed25519) JWTs; only the gateway holds the private key, and anything that only verifies needs just the public key. Only `EdDSA` is accepted, so `alg: none` and HS256 tokens "signed" with the public key are refused |
| A long-lived signing key | Keys rotate without logging anyone out: each token names its key (`kid`), a new key starts signing at once, and the old key keeps verifying until its tokens expire (`scripts/rotate_jwt_key.py`). In Docker the keys live in a volume only the gateway mounts |
| Reusing another user's transaction id | Idempotency is bound to the user and the payment parameters; a mismatch returns `ALREADY_EXISTS` |

## Quick start

### Docker (recommended)

```sh
docker compose up --build -d      # generates certs, starts gateway + HDFC, ICICI, SBI
docker compose run --rm client    # interactive client
docker compose down -v            # stop and delete state
```

### Local

```sh
python -m venv .venv && .venv/Scripts/activate      # or: source .venv/bin/activate
pip install -r requirements-dev.txt
python scripts/gen_certs.py

cd server
python payment_gateway.py          # terminal 1
python bank_server.py HDFC 60051   # terminal 2
python bank_server.py ICICI 60052  # terminal 3
python bank_server.py SBI 60053    # terminal 4
python ../client/client.py         # terminal 5
```

Session tokens are signed with an Ed25519 key that `gen_certs.py` puts in `certs/jwt/`. To rotate it:

```sh
python scripts/rotate_jwt_key.py new             # new key signs from now on
python scripts/rotate_jwt_key.py list
python scripts/rotate_jwt_key.py retire <old-kid>  # after one token lifetime: verify-only
```

Demo users (passwords are stored hashed in [server/users.json](server/users.json); add users with `scripts/hash_password.py`):

| User | Password | Accounts |
|---|---|---|
| alice | alice123 | HDFC 1001, SBI 3001 |
| bob | bob123 | ICICI 2001 |
| jack | jack123 | ICICI 2002 |
| carol | carol123 | HDFC 1002 |

Opening balances come from [server/bank_data.json](server/bank_data.json) the first time each bank starts. After that, each bank's ledger in `data/<bank>.db` is the source of truth.

<details>
<summary>Configuration (environment variables)</summary>

| Variable | Default | Meaning |
|---|---|---|
| `PG_JWT_KEY_DIR` | `certs/jwt/` | Ed25519 token signing keys (created by `gen_certs.py`; an in-memory key is used if empty) |
| `PG_CERT_DIR` / `PG_DATA_DIR` | `certs/` / `data/` | Certificate and database locations |
| `PG_GATEWAY_ADDRESS` | `localhost:50052` | Where banks and clients find the gateway |
| `PG_BANK_HOST` | `localhost` | Host name a bank registers itself under |
| `PG_PREPARE_TIMEOUT` | `30` | Seconds before an undecided payment is presumed aborted |
| `PG_BANK_RPC_TIMEOUT` | `3` | Deadline for each gateway → bank call |
| `PG_SQLITE_SYNC` | `FULL` | `FULL` fsyncs every commit; `NORMAL` survives process crashes but not OS crashes |
</details>

## Testing

```sh
pytest -q                                   # 39 tests, ~25 s
python scripts/chaos_demo.py --seconds 60   # kill processes mid-payment, then audit
python scripts/benchmark.py                 # throughput and latency
```

The [tests](tests/) start a gateway and three banks in-process on free ports and cover:

- **Payments.** Cross-bank transfers, insufficient funds, server-side amount validation, a specific status code for each error, idempotent retries, a concurrent double-spend attempt, and 120 random concurrent transfers that must conserve money.
- **Security.** Missing, forged, expired and revoked tokens; revocation surviving a restart; login lockout scoped to the client address and surviving a restart; password spraying across usernames; reading another user's account; bank impersonation; non-client certificates calling user RPCs; non-gateway callers hitting a bank; the per-component key layout.
- **Recovery (fault injection).**
  - A receiver bank's commit fails, then the retry succeeds.
  - A bank is down during prepare.
  - The gateway crashes after the commit decision.
  - The gateway crashes before any decision (presumed abort).
  - A bank crashes after voting yes.
  - A late prepare arrives after the abort.
- **Client.** Rupee parsing; an offline payment queued to disk and sent exactly once after the gateway returns.

CI runs the suite, the chaos demo, a check that the generated protobuf code is up to date, and a Docker Compose smoke test that also checks each container holds only its own key.

### Chaos demo

`scripts/chaos_demo.py` runs the gateway and banks as separate OS processes. Eight client threads send random payments while a random component is **hard-killed** (`SIGKILL`/`TerminateProcess`) and restarted every 1–3 s. After the load stops and recovery settles, it audits the gateway log against every bank ledger. A 40 s run on a laptop:

```
Kills:            {'GATEWAY': 4, 'SBI': 4, 'HDFC': 5, 'ICICI': 2} (15 total)
Client outcomes:  {'COMMITTED': 2241, 'ABORTED': 470, 'rpc:UNAVAILABLE': 32, 'COMMITTING': 7, 'ABORTING': 1}
Logged payments:  2747 -> {'ABORTED': 483, 'COMMITTED': 2264}
Money:            opening 2300000 paise, closing 2300000 paise
PASSED: every payment is atomic, nothing in doubt, money conserved
```

The recovery worker settled 158 payments that were in doubt when a process died: 117 committed and 41 aborted. Ten of those 41 had been left in `PREPARING` by a gateway crash and were presumed aborted.

### Benchmark

End-to-end payments (gateway and three banks as separate processes, mTLS on every hop, Windows laptop, 8 s per level):

| Concurrent clients | TPS (`FULL` fsync) | p50 / p99 ms | TPS (`NORMAL`) | p50 / p99 ms |
|---:|---:|---:|---:|---:|
| 1 | 63 | 15 / 28 | 121 | 8 / 15 |
| 4 | 118 | 33 / 48 | 203 | 18 / 44 |
| 16 | 124 | 128 / 172 | 223 | 66 / 126 |
| 32 | 119 | 247 / 636 | 241 | 129 / 206 |

Each payment makes three durable writes at the gateway and two at each bank. With `FULL` durability, fsync is the bottleneck: relaxing it to `NORMAL` roughly doubles throughput. `FULL` results also vary up to 2–3× between runs on this laptop because flush latency is noisy. The next step would be group commit: one fsync shared by the log writes of many concurrent payments.

**Parallel prepare (A/B).** Three runs each of sequential and parallel prepare were alternated under `NORMAL`. Parallel prepare cut single-client median latency from 10.2 ms to 8.1 ms and raised single-client throughput from 92 to 110 payments/s. At 4–16 clients the two were within run-to-run noise, because throughput there is limited by serialised database writes, not by the round trip. Under `FULL` the fsync noise was larger than the difference.

## Limitations and next steps

- **The coordinator is a single point of failure.** 2PC is a blocking protocol: while the gateway is down, prepared reservations stay held until it recovers, which is safe but not available. Running the coordinator log on a replicated store (Raft or Paxos) would remove this; it is what systems like Spanner do.
- **One SQLite connection per process** serialises writes. A production system would use PostgreSQL and group commit.
- **Users live in a JSON file.** A real deployment would use a user database, and keep token signing keys in a KMS or HSM rather than on disk.

## Repository layout

```
protofiles/   gRPC service definitions (.proto) and generated code
server/       payment_gateway.py, bank_server.py, auth.py, jwt_keys.py, interceptor.py, storage.py, tls.py, config.py
client/       client.py
scripts/      gen_certs.py, rotate_jwt_key.py, hash_password.py, chaos_demo.py, benchmark.py, cluster.py
tests/        pytest suite and in-process harness
```
