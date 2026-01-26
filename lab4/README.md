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
