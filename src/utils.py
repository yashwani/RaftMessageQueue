import json
from typing import Dict
import zmq


def to_json_str(message: Dict):
    return json.dumps(message)


def from_json_str(message: str):
    return json.loads(message)


def send_json(socket, message: Dict, nonblocking=False):
    if nonblocking:
        socket.send_json(message, flags=zmq.NOBLOCK)
        return
    socket.send_json(message)


def recv_json(socket, nonblocking=False):
    if nonblocking:
        return from_json_str(socket.recv(flags=zmq.NOBLOCK))
    return from_json_str(socket.recv())

