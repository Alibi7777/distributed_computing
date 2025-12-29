# RPC Implementation on AWS EC2

## Description
This project implements a simple Remote Procedure Call (RPC) system using Python sockets and JSON serialization.

The system consists of:
- RPC Server (EC2 instance)
- RPC Client (EC2 instance)

The client sends a remote request to the server, the server executes the function and returns the result.

## RPC Method
- add(a, b): returns the sum of two numbers

## Technologies
- Python 3
- TCP sockets
- JSON
- AWS EC2 (Ubuntu 22.04)

## How to Run

### Server
```bash
python3 server.py
# distributed_computing
