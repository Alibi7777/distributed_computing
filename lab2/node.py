#!/usr/bin/env python3
"""
Lab 2 Node
Lamport Clock + Replicated Key–Value Store (LWW)
Standard library only.

Endpoints:
  POST /put        {"key": "...", "value": ...}
  GET  /get?key=...
  POST /replicate  {"key":"...", "value":..., "ts": <lamport>, "origin":"A"}
  GET  /status
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib import request, parse
import argparse
import json
import threading
import time
from typing import Dict, Any, Tuple, List
from collections import deque

lock = threading.Lock()

LAMPORT = 0
STORE: Dict[str, Tuple[Any, int, str]] = {}  # key -> (value, ts, origin)
NODE_ID = ""
PEERS: List[str] = []  # base URLs, e.g. http://172.31.75.68:8001

# Scenario A: delay A -> C (port 8002) by 2 seconds
DELAY_RULES = {}  # (from_id, peer_url) -> seconds

# Scenario C: retry queue for peers that are offline
PENDING = (
    {}
)  # peer_url -> deque of items: {"payload": bytes, "next": float, "attempt": int}


def lamport_tick_local() -> int:
    """Local event: L = L + 1"""
    global LAMPORT
    with lock:
        LAMPORT += 1
        return LAMPORT


def lamport_on_receive(received_ts: int) -> int:
    """Receive event: L = max(L, received_ts) + 1"""
    global LAMPORT
    with lock:
        LAMPORT = max(LAMPORT, received_ts) + 1
        return LAMPORT


def get_lamport() -> int:
    with lock:
        return LAMPORT


def apply_lww(key: str, value: Any, ts: int, origin: str) -> bool:
    """
    Last-writer-wins:
      - higher Lamport ts wins
      - tie break by origin lexicographic
    """
    with lock:
        cur = STORE.get(key)
        if cur is None:
            STORE[key] = (value, ts, origin)
            return True

        _, cur_ts, cur_origin = cur
        if ts > cur_ts or (ts == cur_ts and origin > cur_origin):
            STORE[key] = (value, ts, origin)
            return True

        return False


def _backoff_s(attempt: int) -> float:
    # 0:0.3s, 1:0.6s, 2:1.2s, 3:2.4s, ... capped
    return min(5.0, 0.3 * (2**attempt))


def _send_once(peer: str, payload: bytes, timeout_s: float) -> None:
    # Scenario A delay
    delay_s = DELAY_RULES.get((NODE_ID, peer), 0.0)
    if delay_s > 0:
        time.sleep(delay_s)

    url = peer.rstrip("/") + "/replicate"
    req = request.Request(
        url, data=payload, headers={"Content-Type": "application/json"}, method="POST"
    )
    with request.urlopen(req, timeout=timeout_s) as resp:
        _ = resp.read()


def _enqueue(peer: str, payload: bytes) -> None:
    now = time.monotonic()
    with lock:
        if peer not in PENDING:
            PENDING[peer] = deque()
        PENDING[peer].append({"payload": payload, "next": now, "attempt": 0})


def replicate_to_peers(
    key: str, value: Any, ts: int, origin: str, timeout_s: float = 2.0
) -> None:
    """
    Send update to all peers.
    - No infinite loop: we ONLY replicate from /put (not from /replicate).
    - If peer is down: enqueue and background thread retries until it comes back.
    """
    payload = json.dumps(
        {"key": key, "value": value, "ts": ts, "origin": origin}
    ).encode("utf-8")

    for peer in PEERS:
        try:
            _send_once(peer, payload, timeout_s=timeout_s)
        except Exception:
            _enqueue(peer, payload)


def pending_sender_loop(timeout_s: float = 2.0, tick_s: float = 0.15) -> None:
    """Background retry loop for Scenario C."""
    while True:
        now = time.monotonic()

        # Take due items (don’t hold lock during network calls)
        due = []
        with lock:
            for peer, q in PENDING.items():
                if q and q[0]["next"] <= now:
                    due.append((peer, q[0]))

        for peer, item in due:
            try:
                _send_once(peer, item["payload"], timeout_s=timeout_s)
                with lock:
                    # pop only if it’s still the same item at head
                    if PENDING.get(peer) and PENDING[peer][0] is item:
                        PENDING[peer].popleft()
            except Exception:
                with lock:
                    if PENDING.get(peer) and PENDING[peer][0] is item:
                        item["attempt"] += 1
                        item["next"] = time.monotonic() + _backoff_s(item["attempt"])

        time.sleep(tick_s)


class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, obj: Dict[str, Any]) -> None:
        data = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path.startswith("/get"):
            qs = parse.urlparse(self.path).query
            params = parse.parse_qs(qs)
            key = params.get("key", [""])[0]

            with lock:
                cur = STORE.get(key)

            if cur is None:
                self._send(
                    404,
                    {
                        "ok": False,
                        "error": "key not found",
                        "key": key,
                        "lamport": get_lamport(),
                    },
                )
            else:
                value, ts, origin = cur
                self._send(
                    200,
                    {
                        "ok": True,
                        "key": key,
                        "value": value,
                        "ts": ts,
                        "origin": origin,
                        "lamport": get_lamport(),
                    },
                )
            return

        if self.path.startswith("/status"):
            with lock:
                snapshot = {
                    k: {"value": v, "ts": ts, "origin": o}
                    for k, (v, ts, o) in STORE.items()
                }
                pending_counts = {peer: len(q) for peer, q in PENDING.items()}
            self._send(
                200,
                {
                    "ok": True,
                    "node": NODE_ID,
                    "lamport": get_lamport(),
                    "peers": PEERS,
                    "pending": pending_counts,
                    "store": snapshot,
                },
            )
            return

        self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length > 0 else b"{}"
        try:
            body = json.loads(raw.decode("utf-8"))
        except Exception:
            self._send(400, {"ok": False, "error": "invalid json"})
            return

        if self.path == "/put":
            key = str(body.get("key", ""))
            value = body.get("value", None)
            if not key:
                self._send(400, {"ok": False, "error": "key required"})
                return

            ts = lamport_tick_local()
            applied = apply_lww(key, value, ts, NODE_ID)
            print(
                f"[{NODE_ID}] PUT key={key} value={value} lamport={ts} applied={applied}"
            )

            # replicate ONLY from PUT (prevents infinite loops)
            t = threading.Thread(
                target=replicate_to_peers, args=(key, value, ts, NODE_ID), daemon=True
            )
            t.start()

            self._send(
                200,
                {
                    "ok": True,
                    "node": NODE_ID,
                    "key": key,
                    "value": value,
                    "ts": ts,
                    "applied": applied,
                    "lamport": get_lamport(),
                },
            )
            return

        if self.path == "/replicate":
            key = str(body.get("key", ""))
            value = body.get("value", None)
            ts = int(body.get("ts", 0))
            origin = str(body.get("origin", ""))
            if not key or not origin or ts <= 0:
                self._send(400, {"ok": False, "error": "key, origin, ts required"})
                return

            new_clock = lamport_on_receive(ts)
            applied = apply_lww(key, value, ts, origin)
            print(
                f"[{NODE_ID}] RECV replicate key={key} value={value} ts={ts} origin={origin} -> lamport={new_clock} applied={applied}"
            )

            self._send(
                200,
                {
                    "ok": True,
                    "node": NODE_ID,
                    "lamport": get_lamport(),
                    "applied": applied,
                },
            )
            return

        self._send(404, {"ok": False, "error": "not found"})

    def log_message(self, fmt, *args):
        return


def main():
    global NODE_ID, PEERS, LAMPORT, DELAY_RULES

    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True, help="Node ID: A, B, or C")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument(
        "--peers", default="", help="Comma-separated base URLs of peers"
    )
    args = parser.parse_args()

    NODE_ID = args.id
    PEERS = [p.strip() for p in args.peers.split(",") if p.strip()]
    LAMPORT = 0

    # Scenario A: delay only A -> C (port 8002) by ~2 seconds
    if NODE_ID == "A":
        for peer in PEERS:
            if peer.endswith(":8002"):
                DELAY_RULES[(NODE_ID, peer)] = 2.0

    # start retry loop (Scenario C)
    if PEERS:
        th = threading.Thread(target=pending_sender_loop, daemon=True)
        th.start()

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"[{NODE_ID}] listening on {args.host}:{args.port} peers={PEERS}")
    server.serve_forever()


if __name__ == "__main__":
    main()
