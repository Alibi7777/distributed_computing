# Lab 2 Starter Code (Python) — 3 Nodes (A/B/C)

This starter kit implements a minimal **Lamport clock + replicated key–value store** using only the **Python standard library**.

## Files
- `node.py`  — Node server (HTTP JSON), Lamport clock, replication, LWW conflict resolution
- `client.py` — Small CLI client to PUT/GET/STATUS

## Ports / Security Group
Open the node port (e.g. 8000/8001/8002) on each EC2 instance for inbound traffic from peer nodes.

## Run on EC2

### A
python3 node.py --id A --port 8000 --peers http://<IP-B>:8001,http://<IP-C>:8002

### B
python3 node.py --id B --port 8001 --peers http://<IP-A>:8000,http://<IP-C>:8002

### C
python3 node.py --id C --port 8002 --peers http://<IP-A>:8000,http://<IP-B>:8001

## Use the client
python3 client.py --node http://<IP-A>:8000 put x 1
python3 client.py --node http://<IP-B>:8001 put x 2
python3 client.py --node http://<IP-C>:8002 status

## Required Experiment Ideas 
1. **Delay / reorder**: add `time.sleep(2)` inside `replicate_to_peers()` before sending to one peer.
2. **Concurrent writes**: send `PUT x 1` to node A and `PUT x 2` to node B quickly.
3. **Temporary outage**: stop node B, do updates on node A, restart node B, observe convergence.

## Notes
- Conflict resolution is **last-writer-wins** using Lamport timestamp; ties are broken by origin ID.
- Extend as needed (vector clocks, snapshots, stronger semantics).

## Where to add code
Search '# YOUR CODE HERE' in node.py:
- Implement Lamport clock rules 
- Add delay rules 
- Improve retries/backoff
- Optional: vector clocks



# Lab 2 — Logical Clocks and Replication Consistency (3 Nodes)

Implements:
- Lamport clocks (local/send/receive)
- Replicated key–value store
- Eventual consistency via Last-Writer-Wins (Lamport ts, tie-break by origin)

## Run (3 nodes)
A:
python3 node.py --id A --port 8000 --peers http://172.31.75.68:8001,http://172.31.72.154:8002

B:
python3 node.py --id B --port 8001 --peers http://172.31.71.90:8000,http://172.31.72.154:8002

C:
python3 node.py --id C --port 8002 --peers http://172.31.71.90:8000,http://172.31.75.68:8001

## Client examples
python3 client.py --node http://172.31.71.90:8000 put x 1
python3 client.py --node http://172.31.75.68:8001 get x
python3 client.py --node http://172.31.72.154:8002 status
