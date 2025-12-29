import socket
import json
import uuid
import time

SERVER_IP = "44.214.138.30"  # server public IP
PORT = 5000
TIMEOUT = 2
RETRIES = 3

request = {"request_id": str(uuid.uuid4()), "method": "add", "params": {"a": 5, "b": 7}}

for attempt in range(1, RETRIES + 1):
    print(f"Attempt {attempt}")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(TIMEOUT)
            s.connect((SERVER_IP, PORT))
            s.sendall(json.dumps(request).encode())

            response = s.recv(4096)
            response = json.loads(response.decode())

            print("RPC result:", response)
            break

    except socket.timeout:
        print("Timeout, retrying...")
    except Exception as e:
        print("Error:", e)

else:
    print("RPC failed after retries")
