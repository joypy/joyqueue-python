
import threading


class ByteBuffer(object):

    def __init__(self):
        self._buffer = b''
        self._lock = threading.Lock()

    def append(self, bytes):
        self._lock.acquire()
        self._buffer += bytes
        self._lock.release()

    def size(self):
        return len(self._buffer)

    def write(self, writer):
        self._lock.acquire()
        size = writer.write(self._buffer)
        self._buffer = self._buffer[size:]
        self._lock.release()

    def __repr__(self) -> str:
        return str(self._buffer)