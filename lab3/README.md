# Lab 3 — Raft Lite (Consensus Deployment)

## Run (3 nodes example)

Node A:
python3 node.py --id A --port 8000 --peers 172.31.75.68:8001,172.31.72.154:8002

Node B:
python3 node.py --id B --port 8001 --peers 172.31.71.90:8000,172.31.72.154:8002

Node C:
python3 node.py --id C --port 8002 --peers 172.31.71.90:8000,172.31.75.68:8001

## Status check
curl -s http://127.0.0.1:8000/status

## Client command
curl -X POST http://127.0.0.1:8000/client_command \
  -H "Content-Type: application/json" \
  -d '{"cmd":"set x=1"}'
