
import abc
import logging
from twisted.internet import defer, reactor, protocol
from joyqueue.network.receiver import LengthBasedProtocol
from joyqueue.protocol.command import Command

log = logging.getLogger(__name__)

"Abstract channel: network connection "


class Channel(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def write(self, command):
        pass

    @abc.abstractmethod
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


class TwistedChannel(Channel, LengthBasedProtocol):

    def __init__(self, connDeferred):
        self._replyQueue = defer.DeferredQueue()
        self._connDeferred = connDeferred
        self._connected = False
        self._length_offset = 4
        self._max_length = 4096

    def write(self, command):
        self.send(command.encode())
        return self._replyQueue.get()

    def messageReceived(self, msg):
        # parser response
        response = Command.decode(msg)
        self._replyQueue.put(response)

    def connectionLost(self, reason):
        self._connected = False

    def connected(self):
        return self._connected

    def connectionMade(self):
        log.info('connected')
        try:
            self._connDeferred.callback(self)
        except defer.AlreadyCalledError:
            log.warning('already connected')
            pass
        self._connected = True


class TwistedClientFactory(protocol.ClientFactory):
    def __init__(self):
        self.protocol = TwistedChannel
        self.deferred = defer.Deferred()

    def clientConnectionFailed(self, connector, reason):
        log.info("Connection failed - goodbye!")
        reactor.stop()

    def buildProtocol(self, addr):
        log.info('Connected')
        p = self.protocol(self.deferred)
        p.factory = self
        return p

    def clientConnectionLost(self, connector, reason):
        log.info("Connection lost, reconnect! {}".format(reason))
        connector.connect()
