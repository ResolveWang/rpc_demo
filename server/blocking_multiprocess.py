import os
import json
import struct
import socket


def send_result(conn, out, result):
    resp = json.dumps({'out': out, 'result': result})
    len_prefix = struct.pack('I', len(resp))
    conn.sendall(len_prefix)
    conn.sendall(str.encode(resp))


def ping(conn, params):
    send_result(conn, 'pong', params)


def handle_conn(conn, addr, handlers):
    print(addr, 'comes')
    while True:
        len_prefix = conn.recv(4)
        if not len_prefix:
            print(addr, 'bye')
            conn.close()
            break

        length, = struct.unpack('I', len_prefix)
        body = conn.recv(length).decode()
        req = json.loads(body)
        in_ = req['in']
        params = req['params']
        print(in_, params)
        handler = handlers[in_]
        handler(conn, params)


def loop(sock, handlers):
    while True:
        conn, addr = sock.accept()
        pid = os.fork()
        if pid < 0:
            return

        if pid == 0:
            sock.close()
            handle_conn(conn, addr, handlers)
            break

        if pid > 0:
            conn.close()
            continue


if __name__ == '__main__':
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('localhost', 8080))
    sock.listen(1)
    handles = {
        'ping': ping
    }
    loop(sock, handles)