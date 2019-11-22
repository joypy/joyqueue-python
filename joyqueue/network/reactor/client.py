
import socket, selectors, types, time, threading
from joyqueue.network.reactor.channel import DefaultChannel, SocketWriter
from joyqueue.network.reactor.buffer import ByteBuffer
from joyqueue.model.models import ServerConfig
from joyqueue.network.reactor.channel import DefaultFuture


class Client(object):
    def __init__(self):
        pass

    def makeConnection(self, host, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        return DefaultChannel(sock)


class NIOClient(object):
    def __init__(self, server_config):
        self._selector = selectors.DefaultSelector()
        self._config = server_config
        self._connection_id = 0
        self._connecting_futures = {}

    def connect(self, host, port):
        server_addr = (host, port)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setblocking(False)
        s.connect_ex(server_addr)
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        self._connection_id += 1
        data = types.SimpleNamespace(conn_id=self._connection_id,
                                     bufin=b'',
                                     connected=False,
                                     bufout=ByteBuffer())
        self._selector.register(s, events, data=data)
        # print('nio server start')
        channel_future = DefaultFuture()
        self._connecting_futures[str(data.conn_id)] = channel_future
        return channel_future

    def event_loop(self):
        print('start to loop')
        while True:
            events = self._selector.select(timeout=None)
            for key, mask in events:
                self._service_conn(key, mask)
            print('sleep 1 seconds')
            time.sleep(1)

    def _accept(self, key):
        print('client accept from')

    def _service_conn(self, key, mask):
        sock = key.fileobj
        data = key.data
        if mask & selectors.EVENT_READ:
            recv_data = sock.recv(1024)
            if recv_data:
                print('response:'+str(recv_data))
            else:
                print('closing connection to', data.addr)
                self._selector.unregister(sock)
                sock.close()
        if not data.connected:
            channel_future = self._connecting_futures[str(data.conn_id)]
            if channel_future:
                # sock = key.fileobj
                # data = key.data
                channel_future.finish(key)
                data.connected = True
                # bind a buffer writer
                data.writer = SocketWriter(sock)
        if mask & selectors.EVENT_WRITE and data.connected:
            #print('event write')
            if data.bufout.size() > 0:
                print('write buffer to server:{}'.format(str(data.bufout)))
                data.bufout.write(data.writer)


def client():
    server_config = ServerConfig('localhost', 50088)
    client = Client()
    channel = client.makeConnection(server_config.get('host'), server_config.get('port'))
    channel.write(b'first message,hello joy net!')
    print('response' + str(channel.receive()))
    channel.write(b'second message,hello joy net!')
    print('response' + str(channel.receive()))


if __name__ == '__main__':
    server_config = ServerConfig('localhost', 50088)
    client = NIOClient(server_config)
    thread = threading.Thread(target=client.event_loop)
    thread.start()
    futureClient = client.connect(server_config.get('host'), server_config.get('port'))
    key = futureClient.get()
    channel = DefaultChannel(key.fileobj, key.data)
    channel.write(b'first message')
    channel.write(b'second message')
    channel.writeBuf(b'third message')

    print('connect finish')
    time.sleep(2)

