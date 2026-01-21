#!/usr/bin/env python3
import argparse
import requests


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--node", required=True, help="http://IP:PORT")
    ap.add_argument("--cmd", required=True, help='e.g. "SET x=5"')
    args = ap.parse_args()

    node = args.node.rstrip("/")

    try:
        r = requests.post(f"{node}/client_command", json={"cmd": args.cmd}, timeout=2)
        data = r.json()
    except Exception as e:
        print("Request failed:", e)
        return

    print("Response:", data)

    # Optional: show status afterwards
    try:
        s = requests.get(f"{node}/status", timeout=2).json()
        print("Status:", s)
    except Exception:
        pass


if __name__ == "__main__":
    main()
