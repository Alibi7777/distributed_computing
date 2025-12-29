import socket
import json
import time

HOST = "0.0.0.0"
PORT = 5000


def handle_request(request):
    method = request.get("method")
    params = request.get("params", {})

    if method == "add":
        return params.get("a", 0) + params.get("b", 0)
    else:
        raise ValueError("Unknown method")


with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    print("RPC Server listening on port 5000")

    while True:
        conn, addr = s.accept()
        with conn:
            print(f"Connected by {addr}")
            data = conn.recv(4096)
            if not data:
                continue

            request = json.loads(data.decode())
            print("Received:", request)

            try:
                result = handle_request(request)
                response = {
                    "request_id": request["request_id"],
                    "result": result,
                    "status": "OK",
                }
            except Exception as e:
                response = {
                    "request_id": request["request_id"],
                    "error": str(e),
                    "status": "ERROR",
                }

            conn.sendall(json.dumps(response).encode())
