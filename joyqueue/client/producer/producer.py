
import abc,time
from joyqueue.network.channel import TwistedChannelFactory
from joyqueue.model.configs import NameServerConfig
from twisted.internet import reactor, defer


class Producer(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def syncSend(self, msg):
        pass

    @abc.abstractmethod
    def asyncSend(self, msg, callback):
        pass

    @abc.abstractmethod
    def start(self):
        pass


class JoyQueueProducer(Producer):

    def __init__(self, config):
        self._config = config
        self._channelFactory = TwistedChannelFactory()
        self._channel = None

    @defer.inlineCallbacks
    def syncSend(self, msg):

        response = yield self._channel.write(msg)
        defer.returnValue(response)

    def asyncSend(self, msg, callback):
        self._channel.write(msg).addCallback(callback)

    @defer.inlineCallbacks
    def start(self):
        self._channel = yield self._channelFactory.createChannel(
            self._config.get('host'),
            self._config.get('port'))
        # resp = yield self.syncSend('first msg'.encode('utf-8'))
        # messageListener(resp)


def messageListener(msg):
    print('message response:{} '.format(msg))


@defer.inlineCallbacks
def main():
    config = NameServerConfig()
    hostAndPort = config.address.split(':')
    if len(hostAndPort) < 2:
        raise ValueError('wrong host and port')
    host = hostAndPort[0]
    port = int(hostAndPort[1])
    producerConfig = {'host': host, 'port': port}
    producer = JoyQueueProducer(producerConfig)
    yield producer.start()
    response = yield producer.syncSend('fist msg'.encode('utf-8'))
    print(response)
    producer.asyncSend('second msg'.encode('utf-8'), messageListener)
    producer.asyncSend('third msg'.encode('utf-8'), messageListener)
    # wait sync message back
    time.sleep(5)


if __name__ == '__main__':
    main()
    # start reactor single instance
    reactor.run()
