import os
import json
import struct
import socket


def handle_conn(conn, addr, handlers):
    print(addr, 'comes')
    while True:
        len_prefix = conn.recv(4)
        if not len_prefix:
            print('addr', 'bye')
            conn.close()
            break
        length, = struct.unpack('I', len_prefix)
        body = conn.recv(length)
        req = json.loads(body)
        in_ = req['in']
        params = req['params']
        print(in_, params)
        handler = handlers[in_]
        handler(conn, params)


def loop_slave(pr, handlers):
    while True:
        bufsize = 1
        ancsize = socket.CMSG_LEN(struct.calcsize('i'))
        msg, ancdata, flags, addr = pr.recvmsg(bufsize, ancsize)
        cmsg_level, cmsg_type, cmsg_data = ancdata[0]
        fd = struct.unpack('i', cmsg_data)[0]
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, fileno=fd)
        handle_conn(sock, sock.getpeername(), handlers)


def send_result(conn, out, result):
    resp = json.dumps({'out': out, 'result': result}).encode()
    len_prefix = struct.pack('I', len(resp))
    conn.sendall(len_prefix)
    conn.sendall(resp)


def ping(conn, params):
    send_result(conn, 'pong', params)


def loop_master(serv_sock, pws):
    idx = 0
    while True:
        sock, addr = serv_sock.accept()
        pw = pws[idx % len(pws)]
        msg = [b'x']
        ancdata = [(
            socket.SOL_SOCKET,
            socket.SCM_RIGHTS,
            struct.pack('i', sock.fileno())
        )]
        pw.sendmsg(msg, ancdata)
        sock.close()
        idx += 1


def prefork(serv_sock, n):
    pws = []
    for i in range(n):
        pr, pw = socket.socketpair()
        pid = os.fork()
        if pid < 0:
            return pws
        if pid > 0:
            pr.close()
            pws.append(pw)
            continue
        if pid == 0:
            serv_sock.close()
            pw.close()
            return pr
    return pws


if __name__ == '__main__':
    serv_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    serv_sock.bind(('localhost', 8080))
    serv_sock.listen(1)
    pws_or_pr = prefork(serv_sock, 10)
    if hasattr(pws_or_pr, '__len__'):
        if pws_or_pr:
            loop_master(serv_sock, pws_or_pr)
        else:
            serv_sock.close()
    else:
        handlers = {
            'ping': ping
        }
        loop_slave(pws_or_pr, handlers)