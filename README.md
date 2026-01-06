# Lab 2 — Logical Clocks and Replication Consistency (Lamport + LWW)

## Topology
3 AWS EC2 nodes (Ubuntu 22.04):
- Node A: 172.31.71.90:8000
- Node B: 172.31.75.68:8001
- Node C: 172.31.72.154:8002

## Run servers
On each node (use private IPs for peers):

### A
python3 node.py --id A --port 8000 --peers http://172.31.75.68:8001,http://172.31.72.154:8002

### B
python3 node.py --id B --port 8001 --peers http://172.31.71.90:8000,http://172.31.72.154:8002

### C
python3 node.py --id C --port 8002 --peers http://172.31.71.90:8000,http://172.31.75.68:8001

## Verify connectivity
From node A:
nc -vz 172.31.75.68 8001
nc -vz 172.31.72.154 8002

## Client commands
PUT:
python3 client.py --node http://172.31.71.90:8000 put x 1

GET:
python3 client.py --node http://172.31.75.68:8001 get x

STATUS:
python3 client.py --node http://172.31.72.154:8002 status

## Consistency
Eventual consistency with Last-Writer-Wins (LWW) using Lamport timestamp (ts).
On receive: L = max(L, t) + 1.
Updates are applied if ts is newer; older writes are ignored.
