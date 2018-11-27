import os
import sys
import math
import json
import errno
import struct
import signal
import socket
import asyncore
from io import StringIO

from kazoo.client import KazooClient


class RPCHandler(asyncore.dispatcher_with_send):
    def __init__(self, sock, addr):
        super().__init__(sock=sock)
        self.addr = addr
        self.handlers = {
            'ping': self.ping,
            'pi': self.pi
        }
        self.rbuf = StringIO()

    def handle_connect(self):
        while True:
            content = self.recv(1024)
            if content:
                self.rbuf.write(content)
            if len(content) < 1024:
                break
        self.handle_rpc()

    def handle_rpc(self):
        while True:
            self.rbuf.seek(0)
            len_prefix = self.rbuf.read(4)
            if len(len_prefix) < 4:
                break
            length, = struct.unpack('I', len_prefix.encode())
            body = self.rbuf.read(length)
            if len(body) < length:
                break