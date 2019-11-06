
import abc
from twisted.internet import defer,reactor, protocol

"Abstract channel "


class Channel(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def write(self, command):
        pass

    def connected(self):
        pass


class ChannelFactory(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def createChannel(self, host, port):
        pass


@defer.inlineCallbacks
class TwistedChannelFactory(ChannelFactory):

    def createChannel(self, host, port):
        client_factory = TwistedChannelFactory()
        reactor.connectTCP(host, port, client_factory)
        channel = yield client_factory.deferred
        return channel


class TwistedChannel(Channel, protocol.Protocol):

    def __init__(self, connDeferred):
        self.__replyQueue = defer.DeferredQueue()
        self.__connDeferred = connDeferred
        self.__connected = False

    def write(self, command):
        self.transport.write(command)
        return self.replyQueue.get()

    def dataReceived(self, data):
        self.replyQueue.put(data)

    def connectionLost(self, reason):
        self.__connected = False

    def connected(self):
        return self.__connected

    def connectionMade(self):
        self.__connDeferred.callback(self)
        self.__connected = True


class TwistedClientFactory(protocol.ClientFactory):
    def __init__(self):
        self.protocol = TwistedChannel
        self.deferred = defer.Deferred()

    def clientConnectionFailed(self, connector, reason):
        print("Connection failed - goodbye!")
        reactor.stop()

    def buildProtocol(self, addr):
        print('Connected')
        p = self.protocol(self.deferred)
        p.factory = self
        return p

    def clientConnectionLost(self, connector, reason):
        print("Connection lost, reconnect! {}".format(reason))
        connector.connect()
