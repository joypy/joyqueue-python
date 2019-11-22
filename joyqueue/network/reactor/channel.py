
from joyqueue.network.reactor.interface import Channel,Writer
import threading


class DefaultChannel(Channel):

    def __init__(self, sock, data=None):
        self._sock = sock
        self._data = data

    def write(self, bytes):
        self._sock.sendall(bytes)

    def writeBuf(self, bytes):
        self._data.bufout.append(bytes)

    def receive(self):
        return self._sock.recv(1024)


class DefaultFuture(object):

    def __init__(self):
        self._done = threading.Event()
        self._result = None

    def finish(self, result):
        self._result = result
        self._done.set()

    def state(self):
        return self._done.is_set()

    def get(self):
        if not self._done.is_set():
            self._done.wait()
        return self._result


class SocketWriter(Writer):

    def __init__(self, sock):
        self._sock = sock

    def write(self, buf):
        size = self._sock.send(buf)
        return size