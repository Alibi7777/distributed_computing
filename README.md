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



# Lab 4 — Distributed Transactions (2PC / 3PC)

## Run
Coordinator:
python3 coordinator.py --id COORD --port 8000 --participants http://IP_B:8001,http://IP_C:8002

Participant:
python3 participant.py --id B --port 8001 --wal /tmp/participant_B.wal
python3 participant.py --id C --port 8002 --wal /tmp/participant_C.wal

Client:
python3 client.py --coord http://COORD_IP:8000 status
python3 client.py --coord http://COORD_IP:8000 start TX1 2PC SET x 5
python3 client.py --coord http://COORD_IP:8000 start TX2 3PC SET z 777
