
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

    @defer.inlineCallbacks
    @abc.abstractmethod
    def createChannel(self, host, port):
        pass


class TwistedChannelFactory(ChannelFactory):

    def createChannel(self, host, port):
        client_factory = TwistedClientFactory()
        reactor.connectTCP(host, port, client_factory)
        return client_factory.deferred


class TwistedChannel(Channel, protocol.Protocol):

    def __init__(self, connDeferred):
        self.__replyQueue = defer.DeferredQueue()
        self.__connDeferred = connDeferred
        self.__connected = False

    def write(self, command):
        self.transport.write(command)
        return self.__replyQueue.get()

    def dataReceived(self, data):
        self.__replyQueue.put(data)

    def connectionLost(self, reason):
        self.__connected = False

    def connected(self):
        return self.__connected

    def connectionMade(self):
        print('connected')
        try:
            self.__connDeferred.callback(self)
        except defer.AlreadyCalledError:
            print('already connected')
            pass

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
