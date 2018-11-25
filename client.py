import json
import time
import struct
import socket


def rpc(sock, in_, params):
    req = json.dumps({'in': in_, 'params': params})
    len_prefix = struct.pack('I', len(req))
    sock.sendall(len_prefix)
    sock.sendall(str.encode(req))
    len_prefix = sock.recv(4)
    length, = struct.unpack('I', len_prefix)
    body = sock.recv(length).decode()
    resp = json.loads(body)
    return resp['out'], resp['result']


if __name__ == '__main__':
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('localhost', 8080))
    for i in range(10):
        out, res = rpc(s, 'ping', 'rpc test %i' % i)
        print(out, res)
        time.sleep(1)
    s.close()