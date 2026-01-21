#!/usr/bin/env python3
import argparse
import random
import threading
import time
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple

import requests
from flask import Flask, request, jsonify

# ----------------------------
# Configuration defaults
# ----------------------------
HEARTBEAT_INTERVAL = 0.25  # seconds
ELECTION_TIMEOUT_RANGE = (1.5, 3.0)  # seconds (randomized per node + per reset)
RPC_TIMEOUT = 0.6  # seconds

app = Flask(__name__)


# ----------------------------
# Log entry
# ----------------------------
@dataclass
class LogEntry:
    term: int
    cmd: str


# ----------------------------
# Raft node state (in-memory)
# ----------------------------
class RaftNode:
    def __init__(self, node_id: str, host: str, port: int, peers: List[str]):
        self.id = node_id
        self.host = host
        self.port = port
        self.addr = f"http://{host}:{port}"

        # peers are addresses like http://172.31.75.68:8001
        self.peers = peers

        # Persistent (in real Raft), but in this lab we keep in memory
        self.currentTerm: int = 0
        self.votedFor: Optional[str] = None
        self.log: List[LogEntry] = []
        self.commitIndex: int = -1

        # Volatile
        self.state: str = "Follower"  # Follower | Candidate | Leader
        self.leaderId: Optional[str] = None

        # Leader-only replication tracking (simplified)
        self.nextIndex: Dict[str, int] = {p: 0 for p in peers}
        self.matchIndex: Dict[str, int] = {p: -1 for p in peers}

        # Timing
        self.last_heartbeat_time = time.time()
        self.election_deadline = self._new_election_deadline()

        # Concurrency
        self.lock = threading.RLock()
        self.stop_event = threading.Event()

    # ----------------------------
    # Utilities / logging
    # ----------------------------
    def _now(self) -> float:
        return time.time()

    def _new_election_deadline(self) -> float:
        return self._now() + random.uniform(*ELECTION_TIMEOUT_RANGE)

    def reset_election_deadline(self):
        self.last_heartbeat_time = self._now()
        self.election_deadline = self._new_election_deadline()

    def log_print(self, msg: str):
        print(f"[Node {self.id}] {msg}", flush=True)

    def majority(self) -> int:
        n = 1 + len(self.peers)
        return (n // 2) + 1

    # ----------------------------
    # Raft: state transitions
    # ----------------------------
    def become_follower(self, term: int, leader_id: Optional[str] = None):
        with self.lock:
            if term > self.currentTerm:
                self.currentTerm = term
                self.votedFor = None

            if self.state != "Follower":
                self.log_print(f"→ Follower (term {self.currentTerm})")

            self.state = "Follower"
            self.leaderId = leader_id
            self.reset_election_deadline()

    def become_candidate(self):
        # IMPORTANT: do not call start_election while holding self.lock too long
        with self.lock:
            self.state = "Candidate"
            self.currentTerm += 1
            term = self.currentTerm

            self.votedFor = self.id
            self.leaderId = None
            self.reset_election_deadline()

            self.log_print(f"Timeout → Candidate (term {term})")

        self.start_election(term)

    def become_leader(self):
        with self.lock:
            self.state = "Leader"
            self.leaderId = self.id

            # initialize leader indices
            for p in self.peers:
                self.nextIndex[p] = len(self.log)
                self.matchIndex[p] = -1

            self.log_print(
                f"Received majority votes → Leader (term {self.currentTerm})"
            )

        # Send an immediate heartbeat so followers stop timing out
        self.replicate_to_all(self.currentTerm, force_heartbeat=True)

    # ----------------------------
    # Raft: election
    # ----------------------------
    def last_log_info(self) -> Tuple[int, int]:
        with self.lock:
            if not self.log:
                return (-1, 0)
            idx = len(self.log) - 1
            return (idx, self.log[idx].term)

    def is_log_up_to_date(self, lastLogIndex: int, lastLogTerm: int) -> bool:
        my_last_index, my_last_term = self.last_log_info()
        if lastLogTerm != my_last_term:
            return lastLogTerm > my_last_term
        return lastLogIndex >= my_last_index

    def start_election(self, term: int):
        votes_lock = threading.Lock()
        votes_granted = 1  # self-vote

        last_index, last_term = self.last_log_info()

        def request_vote_from_peer(peer: str):
            nonlocal votes_granted
            payload = {
                "term": term,
                "candidateId": self.id,
                "lastLogIndex": last_index,
                "lastLogTerm": last_term,
            }
            try:
                r = requests.post(
                    f"{peer}/request_vote", json=payload, timeout=RPC_TIMEOUT
                )
                data = r.json()
            except Exception:
                return

            with self.lock:
                # If we learn higher term, step down
                if data.get("term", 0) > self.currentTerm:
                    self.become_follower(data["term"])
                    return

                # If not candidate anymore or term changed, ignore
                if self.state != "Candidate" or self.currentTerm != term:
                    return

            if data.get("voteGranted"):
                with votes_lock:
                    votes_granted += 1

        threads = []
        for p in self.peers:
            t = threading.Thread(target=request_vote_from_peer, args=(p,), daemon=True)
            t.start()
            threads.append(t)

        for t in threads:
            t.join(timeout=RPC_TIMEOUT)

        with self.lock:
            if self.state == "Candidate" and self.currentTerm == term:
                if votes_granted >= self.majority():
                    self.become_leader()
                else:
                    self.log_print(
                        f"Election failed (votes={votes_granted}); staying Candidate"
                    )

    # ----------------------------
    # Raft: leader heartbeat + replication loop
    # ----------------------------
    def leader_loop(self):
        while not self.stop_event.is_set():
            time.sleep(HEARTBEAT_INTERVAL)
            with self.lock:
                if self.state != "Leader":
                    continue
                term = self.currentTerm
            self.replicate_to_all(term, force_heartbeat=True)

    def replicate_to_all(self, term: int, force_heartbeat: bool = False):
        def replicate_peer(peer: str):
            # retry loop with backoff
            while True:
                with self.lock:
                    if self.state != "Leader" or self.currentTerm != term:
                        return

                    ni = self.nextIndex[peer]
                    entries = [asdict(e) for e in self.log[ni:]]

                    # Heartbeat if no entries, or if forced
                    payload = {
                        "term": term,
                        "leaderId": self.id,
                        "startIndex": ni,
                        "entries": (
                            entries if entries else ([] if force_heartbeat else [])
                        ),
                        "leaderCommit": self.commitIndex,
                    }

                try:
                    r = requests.post(
                        f"{peer}/append_entries", json=payload, timeout=RPC_TIMEOUT
                    )
                    data = r.json()
                except Exception:
                    return

                with self.lock:
                    # Higher term => step down
                    if data.get("term", 0) > self.currentTerm:
                        self.become_follower(data["term"])
                        return

                    if self.state != "Leader" or self.currentTerm != term:
                        return

                    success = data.get("success", False)

                    if success:
                        match = payload["startIndex"] + len(payload["entries"]) - 1
                        self.matchIndex[peer] = max(self.matchIndex[peer], match)
                        self.nextIndex[peer] = len(self.log)

                        if payload["entries"]:
                            self.log_print(
                                f"[Leader {self.id}] Replicated {len(payload['entries'])} entries → {peer}"
                            )

                        self.advance_commit_index()
                        return

                    # follower can't append at startIndex; back off and retry
                    self.nextIndex[peer] = max(0, self.nextIndex[peer] - 1)

        threads = []
        for p in self.peers:
            t = threading.Thread(target=replicate_peer, args=(p,), daemon=True)
            t.start()
            threads.append(t)

        for t in threads:
            t.join(timeout=RPC_TIMEOUT)

    def advance_commit_index(self):
        with self.lock:
            if not self.log:
                return

            for idx in range(len(self.log) - 1, self.commitIndex, -1):
                replicated = 1  # leader itself
                for p in self.peers:
                    if self.matchIndex[p] >= idx:
                        replicated += 1

                if replicated >= self.majority():
                    old = self.commitIndex
                    self.commitIndex = idx
                    if self.commitIndex != old:
                        self.log_print(
                            f"[Leader {self.id}] Entry committed (index={self.commitIndex})"
                        )
                    return

    # ----------------------------
    # Background loop: follower/candidate timeouts
    # ----------------------------
    def timeout_loop(self):
        while not self.stop_event.is_set():
            time.sleep(0.05)
            with self.lock:
                if self.state == "Leader":
                    continue
                if self._now() >= self.election_deadline:
                    # drop lock before starting election to reduce contention
                    pass
                else:
                    continue
            self.become_candidate()

    # ----------------------------
    # Client command handling (leader only)
    # ----------------------------
    def handle_client_command(self, cmd: str) -> Dict:
        with self.lock:
            if self.state != "Leader":
                return {
                    "ok": False,
                    "error": "NOT_LEADER",
                    "leaderId": self.leaderId,
                    "term": self.currentTerm,
                }

            entry = LogEntry(term=self.currentTerm, cmd=cmd)
            self.log.append(entry)
            idx = len(self.log) - 1
            self.log_print(
                f"[Leader {self.id}] Append log entry (term={entry.term}, cmd={entry.cmd}, index={idx})"
            )

            term = self.currentTerm

        # replicate outside lock
        self.replicate_to_all(term, force_heartbeat=False)

        with self.lock:
            committed = self.commitIndex >= idx
            return {
                "ok": True,
                "index": idx,
                "committed": committed,
                "term": self.currentTerm,
            }

    # ----------------------------
    # Start threads
    # ----------------------------
    def start(self):
        threading.Thread(target=self.timeout_loop, daemon=True).start()
        threading.Thread(target=self.leader_loop, daemon=True).start()


node: RaftNode = None


# ----------------------------
# Flask endpoints
# ----------------------------
@app.route("/status", methods=["GET"])
def status():
    with node.lock:
        return jsonify(
            {
                "id": node.id,
                "state": node.state,
                "term": node.currentTerm,
                "leaderId": node.leaderId,
                "commitIndex": node.commitIndex,
                "logLen": len(node.log),
                "log": [asdict(e) for e in node.log],
            }
        )


@app.route("/request_vote", methods=["POST"])
def request_vote():
    data = request.get_json(force=True)
    term = int(data["term"])
    candidateId = data["candidateId"]
    lastLogIndex = int(data.get("lastLogIndex", -1))
    lastLogTerm = int(data.get("lastLogTerm", 0))

    with node.lock:
        if term > node.currentTerm:
            node.become_follower(term)

        if term < node.currentTerm:
            return jsonify({"term": node.currentTerm, "voteGranted": False})

        can_vote = node.votedFor is None or node.votedFor == candidateId
        up_to_date = node.is_log_up_to_date(lastLogIndex, lastLogTerm)

        if can_vote and up_to_date:
            node.votedFor = candidateId
            node.reset_election_deadline()
            return jsonify({"term": node.currentTerm, "voteGranted": True})

        return jsonify({"term": node.currentTerm, "voteGranted": False})


@app.route("/append_entries", methods=["POST"])
def append_entries():
    data = request.get_json(force=True)
    term = int(data["term"])
    leaderId = data["leaderId"]
    startIndex = int(data.get("startIndex", 0))
    entries = data.get("entries", [])
    leaderCommit = int(data.get("leaderCommit", -1))

    with node.lock:
        if term > node.currentTerm:
            node.become_follower(term, leader_id=leaderId)

        if term < node.currentTerm:
            return jsonify({"term": node.currentTerm, "success": False})

        # accept leader
        node.state = "Follower"
        node.leaderId = leaderId
        node.reset_election_deadline()

        # simple consistency rule: accept only if startIndex == len(log)
        if startIndex != len(node.log):
            return jsonify({"term": node.currentTerm, "success": False})

        if entries:
            for e in entries:
                node.log.append(LogEntry(term=int(e["term"]), cmd=e["cmd"]))
            node.log_print(
                f"Append success ({len(entries)} entries) from Leader {leaderId}"
            )

        if leaderCommit > node.commitIndex:
            node.commitIndex = min(leaderCommit, len(node.log) - 1)

        return jsonify({"term": node.currentTerm, "success": True})


@app.route("/client_command", methods=["POST"])
def client_command():
    data = request.get_json(force=True)
    cmd = data["cmd"]
    res = node.handle_client_command(cmd)
    return jsonify(res)


def parse_peers(peers_raw: str) -> List[str]:
    peers = []
    if peers_raw.strip():
        for x in peers_raw.split(","):
            x = x.strip()
            if not x:
                continue
            if x.startswith("http://") or x.startswith("https://"):
                peers.append(x)
            else:
                peers.append(f"http://{x}")
    return peers


def main():
    global node
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True)
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument(
        "--peers", default="", help="comma-separated host:port list (no self)"
    )
    args = ap.parse_args()

    peers = parse_peers(args.peers)
    node = RaftNode(node_id=args.id, host=args.host, port=args.port, peers=peers)
    node.log_print(f"Starting on {args.host}:{args.port} with peers={peers}")
    node.start()

    # threaded=True to allow concurrent RPCs
    app.run(host=args.host, port=args.port, threaded=True)


if __name__ == "__main__":
    main()
