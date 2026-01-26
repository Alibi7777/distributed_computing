#!/usr/bin/env python3
"""
Lab 4 Starter — Coordinator (2PC/3PC) (HTTP, standard library only)
===================================================================

Endpoints (JSON):
- POST /tx/start   {"txid":"TX1","op":{"type":"SET","key":"x","value":"5"}, "protocol":"2PC"|"3PC"}
- GET  /status

Participants are addressed by base URL (e.g., http://10.0.1.12:8001).

Failure injection (manual):
- Kill coordinator between phases to demonstrate blocking (2PC).
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib import request
import argparse
import json
import threading
import time
from typing import Dict, Any, List, Tuple

lock = threading.Lock()

NODE_ID: str = ""
PORT: int = 8000
PARTICIPANTS: List[str] = []
TIMEOUT_S: float = 2.0

TX: Dict[str, Dict[str, Any]] = {}

RETRY_TRIES = 3
RETRY_SLEEP_S = 0.25


def jdump(obj: Any) -> bytes:
    return json.dumps(obj).encode("utf-8")


def jload(b: bytes) -> Any:
    return json.loads(b.decode("utf-8"))


def log(msg: str) -> None:
    print(f"[Coordinator {NODE_ID}] {msg}", flush=True)


def post_json(url: str, payload: dict, timeout: float = TIMEOUT_S) -> Tuple[int, dict]:
    data = jdump(payload)
    req = request.Request(
        url, data=data, headers={"Content-Type": "application/json"}, method="POST"
    )
    with request.urlopen(req, timeout=timeout) as resp:
        return resp.status, jload(resp.read())


def post_json_retry(url: str, payload: dict, tries: int = RETRY_TRIES) -> bool:
    """Best-effort retries for decision propagation."""
    for i in range(tries):
        try:
            post_json(url, payload)
            return True
        except Exception as e:
            if i == tries - 1:
                log(f"POST failed {url} after {tries} tries: {e}")
                return False
            time.sleep(RETRY_SLEEP_S)
    return False


def two_pc(txid: str, op: dict) -> dict:
    with lock:
        TX[txid] = {
            "txid": txid,
            "protocol": "2PC",
            "state": "PREPARE_SENT",
            "op": op,
            "votes": {},
            "decision": None,
            "participants": list(PARTICIPANTS),
            "ts": time.time(),
        }

    log(f"{txid} PREPARE")
    votes = {}
    all_yes = True

    # Phase 1: Prepare/Vote
    for p in PARTICIPANTS:
        try:
            _, resp = post_json(p.rstrip("/") + "/prepare", {"txid": txid, "op": op})
            vote = str(resp.get("vote", "NO")).upper()
            votes[p] = vote
            log(f"{txid} VOTE-{vote} from {p}")
            if vote != "YES":
                all_yes = False
        except Exception as e:
            votes[p] = "NO_TIMEOUT"
            log(f"{txid} VOTE-NO_TIMEOUT from {p} err={e}")
            all_yes = False

    decision = "COMMIT" if all_yes else "ABORT"
    log(f"{txid} GLOBAL-{decision}")

    with lock:
        TX[txid]["votes"] = votes
        TX[txid]["decision"] = decision
        TX[txid]["state"] = f"{decision}_SENT"

    # Phase 2: Decision propagation (with retries)
    endpoint = "/commit" if decision == "COMMIT" else "/abort"
    ack = {}
    for p in PARTICIPANTS:
        ok = post_json_retry(p.rstrip("/") + endpoint, {"txid": txid})
        ack[p] = "OK" if ok else "FAILED"

    with lock:
        TX[txid]["acks"] = ack
        TX[txid]["state"] = "DONE"

    return {
        "ok": True,
        "txid": txid,
        "protocol": "2PC",
        "decision": decision,
        "votes": votes,
        "acks": ack,
    }


def three_pc(txid: str, op: dict) -> dict:
    with lock:
        TX[txid] = {
            "txid": txid,
            "protocol": "3PC",
            "state": "CAN_COMMIT_SENT",
            "op": op,
            "votes": {},
            "decision": None,
            "participants": list(PARTICIPANTS),
            "ts": time.time(),
        }

    # Phase 1: CanCommit?
    log(f"{txid} CAN_COMMIT")
    votes = {}
    all_yes = True

    for p in PARTICIPANTS:
        try:
            _, resp = post_json(p.rstrip("/") + "/can_commit", {"txid": txid, "op": op})
            vote = str(resp.get("vote", "NO")).upper()
            votes[p] = vote
            log(f"{txid} VOTE-{vote} from {p}")
            if vote != "YES":
                all_yes = False
        except Exception as e:
            votes[p] = "NO_TIMEOUT"
            log(f"{txid} VOTE-NO_TIMEOUT from {p} err={e}")
            all_yes = False

    with lock:
        TX[txid]["votes"] = votes

    if not all_yes:
        # Abort path
        log(f"{txid} GLOBAL-ABORT")
        with lock:
            TX[txid]["decision"] = "ABORT"
            TX[txid]["state"] = "ABORT_SENT"

        ack = {}
        for p in PARTICIPANTS:
            ok = post_json_retry(p.rstrip("/") + "/abort", {"txid": txid})
            ack[p] = "OK" if ok else "FAILED"

        with lock:
            TX[txid]["acks"] = ack
            TX[txid]["state"] = "DONE"

        return {
            "ok": True,
            "txid": txid,
            "protocol": "3PC",
            "decision": "ABORT",
            "votes": votes,
            "acks": ack,
        }

    # Phase 2: PreCommit
    log(f"{txid} PRECOMMIT")
    with lock:
        TX[txid]["decision"] = "PRECOMMIT"
        TX[txid]["state"] = "PRECOMMIT_SENT"

    precommit_ack = {}
    for p in PARTICIPANTS:
        ok = post_json_retry(p.rstrip("/") + "/precommit", {"txid": txid})
        precommit_ack[p] = "OK" if ok else "FAILED"

    # Phase 3: DoCommit (we reuse /commit endpoint)
    log(f"{txid} DOCOMMIT")
    with lock:
        TX[txid]["decision"] = "COMMIT"
        TX[txid]["state"] = "DOCOMMIT_SENT"

    commit_ack = {}
    for p in PARTICIPANTS:
        ok = post_json_retry(p.rstrip("/") + "/commit", {"txid": txid})
        commit_ack[p] = "OK" if ok else "FAILED"

    with lock:
        TX[txid]["precommit_acks"] = precommit_ack
        TX[txid]["commit_acks"] = commit_ack
        TX[txid]["state"] = "DONE"

    return {
        "ok": True,
        "txid": txid,
        "protocol": "3PC",
        "decision": "COMMIT",
        "votes": votes,
        "precommit_acks": precommit_ack,
        "commit_acks": commit_ack,
    }


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
                        "participants": PARTICIPANTS,
                        "tx": TX,
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

        if self.path == "/tx/start":
            txid = str(body.get("txid", "")).strip()
            op = body.get("op", None)
            protocol = str(body.get("protocol", "2PC")).upper()

            if not txid or not isinstance(op, dict):
                self._send(400, {"ok": False, "error": "txid and op required"})
                return
            if protocol not in ("2PC", "3PC"):
                self._send(400, {"ok": False, "error": "protocol must be 2PC or 3PC"})
                return

            if protocol == "2PC":
                result = two_pc(txid, op)
            else:
                result = three_pc(txid, op)

            self._send(200, result)
            return

        self._send(404, {"ok": False, "error": "not found"})

    def log_message(self, fmt, *args):
        return


def main():
    global NODE_ID, PORT, PARTICIPANTS, TIMEOUT_S

    ap = argparse.ArgumentParser()
    ap.add_argument("--id", default="COORD")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument(
        "--participants",
        required=True,
        help="Comma-separated participant base URLs (http://IP:PORT)",
    )
    ap.add_argument("--timeout", type=float, default=2.0)
    args = ap.parse_args()

    NODE_ID = args.id
    PORT = args.port
    TIMEOUT_S = args.timeout
    PARTICIPANTS = [p.strip() for p in args.participants.split(",") if p.strip()]

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    log(f"Coordinator listening on {args.host}:{args.port} participants={PARTICIPANTS}")
    server.serve_forever()


if __name__ == "__main__":
    main()
