
import socket, selectors, types

from joyqueue.model.models import ServerConfig


class Server(object):

    def __init__(self, server_config):
        self._config = server_config

    def start(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((self._config.get('host'), self._config.get('port')))
        s.listen()
        print('server start success')
        while True:
            conn, addr = s.accept()
            with conn:
                while True:
                    data = conn.recv(1024)
                    if not data:
                        break
                    print(data)
                    conn.sendall(data)


class NIOServer(object):

    def __init__(self, server_config):
        self._selector = selectors.DefaultSelector()
        self._config = server_config

    def start(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((self._config.get('host'), self._config.get('port')))
        s.listen()
        s.setblocking(False)
        self._selector.register(s, selectors.EVENT_READ, data=None)
        print('nio server start')
        while True:
            events = self._selector.select(timeout=None)
            for key, mask in events:
                if key.data is None:
                    if mask & selectors.EVENT_READ:
                        print('read event data none')
                    else:
                        print('write event')

                    self._accept(key.fileobj)
                else:
                    self._service_conn(key, mask)

    def _accept(self, new_sock):
        conn, addr = new_sock.accept()
        print('accepted connection from', addr)
        conn.setblocking(False)
        data = types.SimpleNamespace(addr= addr, inb=b'', outb=b'')
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        self._selector.register(conn, events, data=data)

    def _service_conn(self, key, mask):
        sock = key.fileobj
        data = key.data
        if mask & selectors.EVENT_READ:
            recv_data = sock.recv(1024)
            if recv_data:
                data.outb += recv_data
            else:
                print('closing connection to', data.addr)
                self._selector.unregister(sock)
                sock.close()
        if mask & selectors.EVENT_WRITE:
            #print('event write')
            if len(data.outb) > 0:
                print('echoing', repr(data.outb))
                sent_size = sock.send(data.outb)
                data.outb = data.outb[sent_size:]


if __name__ == '__main__':
    server_config = ServerConfig('localhost', 50088)
    server = NIOServer(server_config)
    server.start()
