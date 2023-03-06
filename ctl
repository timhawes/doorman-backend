#!/usr/bin/env python3

import json
import os
import socket
import sys

COMMAND_SOCKET = os.environ["COMMAND_SOCKET"]

if len(sys.argv) < 2:
    print(f"Usage: {sys.argv[0]} id cmd [message]")
    sys.exit(1)

packet = {"id": sys.argv[1], "cmd": sys.argv[2]}
if len(sys.argv) >= 4:
    packet["message"] = json.loads(sys.argv[3])

sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
sock.connect(COMMAND_SOCKET)
sock.sendall(json.dumps(packet).encode())
sock.shutdown(socket.SHUT_WR)
response = sock.recv(8192)
sock.close()

try:
    data = json.loads(response.decode())
    print(json.dumps(data, indent=2, sort_keys=True))
except:
    print(response)
