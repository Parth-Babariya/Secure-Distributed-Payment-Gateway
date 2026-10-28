"""Coordinator log replicated with Raft, stored in an etcd cluster.

With this store several gateways run side by side and keep no state of their
own: every payment record, the bank directory, revoked tokens and login
failures live in etcd, which replicates each write to a majority of its
nodes before acknowledging it. Any gateway can therefore run, or recover,
any payment. If a gateway dies, another one finishes its in-doubt payments,
so the gateway is no longer a single point of failure for 2PC.

Concurrency control is the same as with SQLite: every status change is a
compare-and-set, here an etcd transaction conditioned on the record's
mod_revision. Two gateways racing on one payment (one running it, another
recovering it) cannot both win; the loser follows the logged decision.

Keys (all under a prefix, "/pg/" by default):
    payments/<id>                 JSON payment record
    inflight/<id>                 present until the payment is final
    by-user/<user>/<ns>-<id>      history index, ordered by creation time
    banks/<name>                  {"host", "port"}
    revoked/<jti>                 attached to a lease that ends at token expiry
    login/<hex key>/<ns>-<rand>   one failed login, on a lease of one window

The store talks to etcd's JSON gateway (/v3/...) over mutual TLS with
certificates from a CA of its own, so only gateways can reach it.
"""
import base64
import http.client
import json
import logging
import math
import ssl
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import urlsplit

log = logging.getLogger("etcd")

PREPARING, COMMITTING, COMMITTED = "PREPARING", "COMMITTING", "COMMITTED"
ABORTING, ABORTED = "ABORTING", "ABORTED"
FINAL = (COMMITTED, ABORTED)
MAX_TXN_OPS = 100  # etcd's default limit is 128 operations per transaction


def _b64(data):
    if isinstance(data, str):
        data = data.encode()
    return base64.b64encode(data).decode()


def _unb64(text):
    return base64.b64decode(text)


def _prefix_end(prefix):
    """The range end that selects every key starting with prefix."""
    data = bytearray(prefix.encode())
    data[-1] += 1
    return bytes(data)


class EtcdError(Exception):
    pass


class EtcdUnavailable(EtcdError):
    pass


class EtcdClient:
    """Minimal etcd v3 client over the JSON gateway.

    Requests go to the first healthy endpoint; on a connection error,
    timeout or 5xx the next endpoint is tried until `retry_for` seconds have
    passed. Each thread keeps one persistent TLS connection per endpoint.
    """

    def __init__(self, endpoints, ca_file=None, cert_file=None, key_file=None, timeout=3.0, retry_for=10.0):
        self.endpoints = [urlsplit(e if "://" in e else f"https://{e}") for e in endpoints]
        if not self.endpoints:
            raise ValueError("no etcd endpoints")
        self.timeout = timeout
        self.retry_for = retry_for
        self._ssl = None
        if any(e.scheme == "https" for e in self.endpoints):
            self._ssl = ssl.create_default_context(cafile=str(ca_file))
            self._ssl.load_cert_chain(str(cert_file), str(key_file))
        self._preferred = 0
        self._local = threading.local()
        self._all_connections = []
        self._lock = threading.Lock()

    def _connection(self, index):
        connections = getattr(self._local, "connections", None)
        if connections is None:
            connections = self._local.connections = {}
        conn = connections.get(index)
        if conn is None:
            endpoint = self.endpoints[index]
            if endpoint.scheme == "https":
                conn = http.client.HTTPSConnection(endpoint.hostname, endpoint.port, timeout=self.timeout,
                                                   context=self._ssl)
            else:
                conn = http.client.HTTPConnection(endpoint.hostname, endpoint.port, timeout=self.timeout)
            connections[index] = conn
            with self._lock:
                self._all_connections.append(conn)
        return conn

    def _drop(self, index):
        conn = self._local.connections.pop(index, None)
        if conn is not None:
            conn.close()

    def call(self, path, body):
        deadline = time.monotonic() + self.retry_for
        payload = json.dumps(body).encode()
        attempt = 0
        last_error = None
        while True:
            index = (self._preferred + attempt) % len(self.endpoints)
            try:
                conn = self._connection(index)
                conn.request("POST", path, payload, {"Content-Type": "application/json"})
                response = conn.getresponse()
                data = response.read()
                if response.status >= 500:
                    raise EtcdUnavailable(f"HTTP {response.status}: {data[:200]!r}")
                result = json.loads(data) if data else {}
                if response.status != 200 or "error" in result:
                    raise EtcdError(result.get("message") or result.get("error") or f"HTTP {response.status}")
                self._preferred = index
                return result
            except (OSError, http.client.HTTPException, EtcdUnavailable) as e:
                self._drop(index)
                last_error = e
            attempt += 1
            if time.monotonic() > deadline:
                raise EtcdUnavailable(f"no etcd endpoint answered: {last_error}")
            if attempt % len(self.endpoints) == 0:
                time.sleep(0.2)

    # -- KV helpers --------------------------------------------------------
    def get(self, key):
        """Return (value bytes, mod_revision) or (None, 0)."""
        kvs = self.call("/v3/kv/range", {"key": _b64(key)}).get("kvs", [])
        if not kvs:
            return None, 0
        return _unb64(kvs[0].get("value", "")), int(kvs[0]["mod_revision"])

    def get_prefix(self, prefix, keys_only=False):
        """Return [(key str, value bytes)] sorted by key."""
        body = {"key": _b64(prefix), "range_end": _b64(_prefix_end(prefix)),
                "sort_order": "ASCEND", "sort_target": "KEY", "keys_only": keys_only}
        kvs = self.call("/v3/kv/range", body).get("kvs", [])
        return [(_unb64(kv["key"]).decode(), _unb64(kv.get("value", ""))) for kv in kvs]

    def count(self, key):
        return int(self.call("/v3/kv/range", {"key": _b64(key), "count_only": True}).get("count", 0))

    def put(self, key, value, lease=None):
        body = {"key": _b64(key), "value": _b64(value)}
        if lease:
            body["lease"] = lease
        self.call("/v3/kv/put", body)

    def delete_prefix(self, prefix):
        self.call("/v3/kv/deleterange", {"key": _b64(prefix), "range_end": _b64(_prefix_end(prefix))})

    def grant_lease(self, ttl_seconds):
        return self.call("/v3/lease/grant", {"TTL": int(ttl_seconds)})["ID"]

    def txn(self, compare, success, failure=()):
        """Return (succeeded, responses)."""
        result = self.call("/v3/kv/txn", {"compare": compare, "success": list(success), "failure": list(failure)})
        return bool(result.get("succeeded")), result.get("responses", [])

    def get_many(self, keys):
        """Values for many keys in as few round trips as possible."""
        values = {}
        keys = list(keys)
        for start in range(0, len(keys), MAX_TXN_OPS):
            chunk = keys[start:start + MAX_TXN_OPS]
            _, responses = self.txn([], [{"request_range": {"key": _b64(k)}} for k in chunk])
            for key, response in zip(chunk, responses):
                kvs = response.get("response_range", {}).get("kvs", [])
                if kvs:
                    values[key] = _unb64(kvs[0].get("value", ""))
        return values

    def close(self):
        with self._lock:
            for conn in self._all_connections:
                conn.close()
            self._all_connections.clear()


def _put_op(key, value, lease=None):
    request = {"key": _b64(key), "value": _b64(value)}
    if lease:
        request["lease"] = lease
    return {"request_put": request}


def _delete_op(key):
    return {"request_delete_range": {"key": _b64(key)}}


class EtcdCoordinatorLog:
    """Drop-in replacement for the SQLite CoordinatorLog, backed by etcd."""

    def __init__(self, client, prefix="/pg/"):
        self.etcd = client
        self.prefix = prefix

    def _key(self, *parts):
        return self.prefix + "/".join(parts)

    @staticmethod
    def _row(value):
        row = json.loads(value)
        row.pop("attempt", None)
        return row

    # -- payments --------------------------------------------------------
    def create_payment(self, payment):
        """Insert a new PREPARING payment. Returns (created, row)."""
        txn_id = payment["transaction_id"]
        now = time.time()
        attempt = uuid.uuid4().hex
        row = {**payment, "status": PREPARING, "message": "", "created_at": now, "updated_at": now}
        value = json.dumps({**row, "attempt": attempt})
        key = self._key("payments", txn_id)
        order = f"{time.time_ns():020d}-{txn_id}"
        ops = [_put_op(key, value), _put_op(self._key("inflight", txn_id), "")]
        for user in {payment["sender_username"], payment["receiver_username"]}:
            ops.append(_put_op(self._key("by-user", user, order), txn_id))
        created, responses = self.etcd.txn(
            [{"key": _b64(key), "result": "EQUAL", "target": "CREATE", "create_revision": "0"}],
            ops, [{"request_range": {"key": _b64(key)}}])
        if created:
            return True, row
        existing = json.loads(_unb64(responses[0]["response_range"]["kvs"][0]["value"]))
        # The same attempt means our own write, applied although its reply
        # was lost and the request was retried.
        return existing.pop("attempt", None) == attempt, existing

    def get_payment(self, txn_id):
        value, _ = self.etcd.get(self._key("payments", txn_id))
        return self._row(value) if value is not None else None

    def transition(self, txn_id, from_states, to_state, message=None):
        """Compare-and-set the status. Returns True if this call made the change."""
        key = self._key("payments", txn_id)
        while True:
            value, revision = self.etcd.get(key)
            if value is None:
                return False
            row = json.loads(value)
            if row["status"] not in from_states:
                return False
            row["status"] = to_state
            if message is not None:
                row["message"] = message
            row["updated_at"] = time.time()
            ops = [_put_op(key, json.dumps(row))]
            if to_state in FINAL:
                ops.append(_delete_op(self._key("inflight", txn_id)))
            succeeded, _ = self.etcd.txn(
                [{"key": _b64(key), "result": "EQUAL", "target": "MOD", "mod_revision": str(revision)}], ops)
            if succeeded:
                return True
            # Someone else changed the record first: re-read and decide again.

    def _payments(self, txn_ids):
        values = self.etcd.get_many(self._key("payments", t) for t in txn_ids)
        return [self._row(v) for v in values.values()]

    def in_doubt(self, prepare_timeout, grace=0.0):
        """Payments the recovery worker must drive forward."""
        now = time.time()
        inflight = self._key("inflight", "")
        ids = [key[len(inflight):] for key, _ in self.etcd.get_prefix(inflight, keys_only=True)]
        rows = [row for row in self._payments(ids)
                if (row["status"] in (COMMITTING, ABORTING) and row["updated_at"] < now - grace)
                or (row["status"] == PREPARING and row["updated_at"] < now - prepare_timeout)]
        return sorted(rows, key=lambda row: row["created_at"])

    def history(self, username):
        index = self.etcd.get_prefix(self._key("by-user", username, ""))
        ids = [value.decode() for _, value in index]
        rows = {row["transaction_id"]: row for row in self._payments(ids)}
        return [rows[t] for t in ids if t in rows]

    # -- bank directory ----------------------------------------------------
    def upsert_bank(self, name, host, port):
        self.etcd.put(self._key("banks", name), json.dumps({"host": host, "port": port}))

    def get_bank(self, name):
        value, _ = self.etcd.get(self._key("banks", name))
        if value is None:
            return None
        location = json.loads(value)
        return location["host"], location["port"]

    # -- token revocation ----------------------------------------------------
    def revoke(self, jti, expires_at):
        ttl = math.ceil(expires_at - time.time())
        if ttl <= 0:
            return  # already expired, nothing to remember
        # The entry disappears by itself once the token could no longer be used.
        self.etcd.put(self._key("revoked", jti), "1", lease=self.etcd.grant_lease(ttl + 1))

    def is_revoked(self, jti):
        return self.etcd.count(self._key("revoked", jti)) > 0

    # -- failed logins -------------------------------------------------------
    def _login_prefix(self, key):
        return self._key("login", key.encode().hex(), "")

    def record_login_failure(self, keys, at, window):
        lease = self.etcd.grant_lease(math.ceil(window) + 1)
        entry = f"{int(at * 1e9):020d}-{uuid.uuid4().hex[:8]}"
        self.etcd.txn([], [_put_op(self._login_prefix(k) + entry, json.dumps(at), lease) for k in keys])

    def login_failures(self, key, since):
        """Return (count, latest timestamp) of failures for key since `since`."""
        times = [json.loads(v) for _, v in self.etcd.get_prefix(self._login_prefix(key))]
        recent = [t for t in times if t >= since]
        return len(recent), max(recent, default=0.0)

    def clear_login_failures(self, key):
        self.etcd.delete_prefix(self._login_prefix(key))

    def close(self):
        self.etcd.close()


def connect(endpoints, cert_dir, prefix="/pg/"):
    """An EtcdCoordinatorLog using the gateway's etcd client certificate in
    <cert_dir> (ca.crt, gateway.crt, gateway.key)."""
    cert_dir = Path(cert_dir)
    client = EtcdClient(endpoints, ca_file=cert_dir / "ca.crt", cert_file=cert_dir / "gateway.crt",
                        key_file=cert_dir / "gateway.key")
    return EtcdCoordinatorLog(client, prefix)
