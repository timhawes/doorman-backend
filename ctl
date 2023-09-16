#!/usr/bin/env python3

import json
import os
import socket
import sys

COMMAND_SOCKET = os.environ["COMMAND_SOCKET"]

if len(sys.argv) == 2:
    packet = {"cmd": sys.argv[1]}
elif len(sys.argv) == 3:
    packet = {"cmd": sys.argv[1], "id": sys.argv[2]}
elif len(sys.argv) == 4:
    packet = {"cmd": sys.argv[1], "id": sys.argv[2], "message": json.loads(sys.argv[3])}
else:
    print(f"Usage: {sys.argv[0]} cmd [id [message]]")
    sys.exit(1)

sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
sock.connect(COMMAND_SOCKET)
sock.sendall(json.dumps(packet).encode())
sock.shutdown(socket.SHUT_WR)

response = b""
while chunk := sock.recv(512):
    response += chunk

sock.close()

try:
    data = json.loads(response.decode())
    print(json.dumps(data, indent=2, sort_keys=True))
except:
    print(response)
