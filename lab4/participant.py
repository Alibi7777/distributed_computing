#!/usr/bin/env python3
"""
Lab 4 Starter — Participant (2PC/3PC) (HTTP, standard library only)
===================================================================

2PC endpoints:
- POST /prepare   {"txid":"TX1","op":{...}} -> {"vote":"YES"/"NO"}
- POST /commit    {"txid":"TX1"}
- POST /abort     {"txid":"TX1"}

3PC endpoints:
- POST /can_commit {"txid":"TX1","op":{...}} -> {"vote":"YES"/"NO"}
- POST /precommit  {"txid":"TX1"}

GET:
- /status
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import argparse
import json
import os
import threading
import time
from typing import Dict, Any, Optional

lock = threading.Lock()

NODE_ID: str = ""
PORT: int = 8001

# In-memory key-value resource
kv: Dict[str, str] = {}

# TX table: TX[txid] = {"state": "...", "op": dict|None, "ts": float}
TX: Dict[str, Dict[str, Any]] = {}

WAL_PATH: Optional[str] = None

# 3PC termination / demo timeouts
TERMINATION_CHECK_INTERVAL_S = 1.0
TERMINATION_TIMEOUT_S = (
    6.0  # after this time without coordinator -> auto decision (demo)
)


def jdump(obj: Any) -> bytes:
    return json.dumps(obj).encode("utf-8")


def jload(b: bytes) -> Any:
    return json.loads(b.decode("utf-8"))


def log(msg: str) -> None:
    # log format aligned to lab requirements
    print(f"[Node {NODE_ID}] {msg}", flush=True)


def wal_append(line: str) -> None:
    """Append to WAL with flush+fsync (durability)."""
    if not WAL_PATH:
        return
    with open(WAL_PATH, "a", encoding="utf-8") as f:
        f.write(line.rstrip("\n") + "\n")
        f.flush()
        os.fsync(f.fileno())


def validate_op(op: dict) -> bool:
    t = str(op.get("type", "")).upper()
    if t != "SET":
        return False
    if not str(op.get("key", "")).strip():
        return False
    return True


def apply_op(op: dict) -> None:
    t = str(op.get("type", "")).upper()
    if t == "SET":
        k = str(op["key"])
        v = str(op.get("value", ""))
        kv[k] = v
        return
    # optional: extend ops here


def wal_replay() -> None:
    """
    WAL replay on startup (recommended).
    Minimal semantics:
    - PREPARE/ CAN_COMMIT YES -> READY + store op
    - PREPARE/ CAN_COMMIT NO  -> ABORTED + store op
    - PRECOMMIT -> PRECOMMIT
    - COMMIT -> apply op and COMMITTED
    - ABORT -> ABORTED
    """
    if not WAL_PATH:
        return
    if not os.path.exists(WAL_PATH):
        return

    log(f"WAL replay from {WAL_PATH}")

    # Keep last known op per txid
    last_op: Dict[str, Optional[dict]] = {}

    with open(WAL_PATH, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue

            # formats we write:
            # {txid} PREPARE YES {json_op}
            # {txid} CAN_COMMIT YES {json_op}
            # {txid} PRECOMMIT
            # {txid} COMMIT
            # {txid} ABORT
            parts = line.split(" ", 3)
            txid = parts[0]
            action = parts[1]

            if action in ("PREPARE", "CAN_COMMIT"):
                # need vote + op_json
                if len(parts) < 4:
                    continue
                vote = parts[2].strip().upper()
                op_json = parts[3].strip()
                try:
                    op = json.loads(op_json)
                except Exception:
                    op = None

                last_op[txid] = op
                state = "READY" if vote == "YES" else "ABORTED"
                TX[txid] = {"state": state, "op": op, "ts": time.time()}

            elif action == "PRECOMMIT":
                op = last_op.get(txid) or TX.get(txid, {}).get("op")
                TX[txid] = {"state": "PRECOMMIT", "op": op, "ts": time.time()}

            elif action == "COMMIT":
                op = last_op.get(txid) or TX.get(txid, {}).get("op")
                if isinstance(op, dict):
                    apply_op(op)
                TX[txid] = {"state": "COMMITTED", "op": op, "ts": time.time()}

            elif action == "ABORT":
                op = last_op.get(txid) or TX.get(txid, {}).get("op")
                TX[txid] = {"state": "ABORTED", "op": op, "ts": time.time()}


def termination_worker() -> None:
    """
    3PC termination logic (demo-friendly):
    - If tx is PRECOMMIT and coordinator disappears, after timeout we COMMIT locally.
    - If tx is READY and coordinator disappears, after timeout we ABORT locally.

    This is a simplified lab assumption ("bounded delay, no partitions") to demonstrate non-blocking.
    """
    while True:
        time.sleep(TERMINATION_CHECK_INTERVAL_S)
        now = time.time()

        with lock:
            for txid, rec in list(TX.items()):
                st = rec.get("state")
                ts = float(rec.get("ts", now))
                age = now - ts

                if age < TERMINATION_TIMEOUT_S:
                    continue

                # terminate only if still waiting
                if st == "PRECOMMIT":
                    op = rec.get("op")
                    if isinstance(op, dict):
                        apply_op(op)
                    rec["state"] = "COMMITTED"
                    rec["ts"] = now
                    wal_append(f"{txid} COMMIT")
                    log(f"{txid} TERMINATION -> COMMIT (timeout after PRECOMMIT)")

                elif st == "READY":
                    rec["state"] = "ABORTED"
                    rec["ts"] = now
                    wal_append(f"{txid} ABORT")
                    log(f"{txid} TERMINATION -> ABORT (timeout in READY)")


class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, obj: dict):
        data = jdump(obj)
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path.startswith("/status"):
            with lock:
                self._send(
                    200,
                    {
                        "ok": True,
                        "node": NODE_ID,
                        "port": PORT,
                        "kv": kv,
                        "tx": TX,
                        "wal": WAL_PATH,
                    },
                )
            return
        self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = jload(raw)
        except Exception:
            self._send(400, {"ok": False, "error": "invalid json"})
            return

        # ---------------- 2PC ----------------
        if self.path == "/prepare":
            txid = str(body.get("txid", "")).strip()
            op = body.get("op", None)
            if not txid or not isinstance(op, dict):
                self._send(400, {"ok": False, "error": "txid and op required"})
                return

            vote = "YES" if validate_op(op) else "NO"
            state = "READY" if vote == "YES" else "ABORTED"

            with lock:
                TX[txid] = {"state": state, "op": op, "ts": time.time()}
                wal_append(f"{txid} PREPARE {vote} {json.dumps(op)}")

            log(f"{txid} VOTE-{vote}")
            self._send(200, {"ok": True, "vote": vote, "state": state})
            return

        if self.path == "/commit":
            txid = str(body.get("txid", "")).strip()
            if not txid:
                self._send(400, {"ok": False, "error": "txid required"})
                return

            with lock:
                rec = TX.get(txid)
                if not rec:
                    self._send(409, {"ok": False, "error": "unknown txid"})
                    return
                if rec["state"] not in ("READY", "PRECOMMIT"):
                    self._send(
                        409,
                        {
                            "ok": False,
                            "error": f"cannot commit from state={rec['state']}",
                        },
                    )
                    return

                op = rec.get("op")
                if isinstance(op, dict):
                    apply_op(op)
                rec["state"] = "COMMITTED"
                rec["ts"] = time.time()
                wal_append(f"{txid} COMMIT")

            log(f"{txid} COMMIT")
            self._send(200, {"ok": True, "txid": txid, "state": "COMMITTED"})
            return

        if self.path == "/abort":
            txid = str(body.get("txid", "")).strip()
            if not txid:
                self._send(400, {"ok": False, "error": "txid required"})
                return

            with lock:
                rec = TX.get(txid)
                if rec:
                    rec["state"] = "ABORTED"
                    rec["ts"] = time.time()
                else:
                    TX[txid] = {"state": "ABORTED", "op": None, "ts": time.time()}
                wal_append(f"{txid} ABORT")

            log(f"{txid} ABORT")
            self._send(200, {"ok": True, "txid": txid, "state": "ABORTED"})
            return

        # ---------------- 3PC ----------------
        if self.path == "/can_commit":
            txid = str(body.get("txid", "")).strip()
            op = body.get("op", None)
            if not txid or not isinstance(op, dict):
                self._send(400, {"ok": False, "error": "txid and op required"})
                return

            vote = "YES" if validate_op(op) else "NO"
            state = "READY" if vote == "YES" else "ABORTED"

            with lock:
                TX[txid] = {"state": state, "op": op, "ts": time.time()}
                wal_append(f"{txid} CAN_COMMIT {vote} {json.dumps(op)}")

            log(f"{txid} VOTE-{vote} (CAN_COMMIT)")
            self._send(200, {"ok": True, "vote": vote, "state": state})
            return

        if self.path == "/precommit":
            txid = str(body.get("txid", "")).strip()
            if not txid:
                self._send(400, {"ok": False, "error": "txid required"})
                return

            with lock:
                rec = TX.get(txid)
                if not rec or rec["state"] != "READY":
                    self._send(
                        409, {"ok": False, "error": "precommit requires READY state"}
                    )
                    return
                rec["state"] = "PRECOMMIT"
                rec["ts"] = time.time()
                wal_append(f"{txid} PRECOMMIT")

            log(f"{txid} PRECOMMIT")
            self._send(200, {"ok": True, "txid": txid, "state": "PRECOMMIT"})
            return

        self._send(404, {"ok": False, "error": "not found"})

    def log_message(self, fmt, *args):
        return


def main():
    global NODE_ID, PORT, WAL_PATH

    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True)
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8001)
    ap.add_argument(
        "--wal", default="", help="Optional WAL path (/tmp/participant_B.wal)"
    )
    args = ap.parse_args()

    NODE_ID = args.id
    PORT = args.port
    WAL_PATH = args.wal.strip() or None

    # WAL replay before serving
    with lock:
        wal_replay()

    # Start termination thread
    t = threading.Thread(target=termination_worker, daemon=True)
    t.start()

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    log(f"Participant listening on {args.host}:{args.port} wal={WAL_PATH}")
    server.serve_forever()


if __name__ == "__main__":
    main()
