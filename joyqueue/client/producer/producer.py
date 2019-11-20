
import time
from joyqueue.network.channel import TwistedChannelFactory
from joyqueue.model.configs import NameServerConfig
from twisted.internet import reactor, defer
from joyqueue.client.client import DefaultAuthClientTransport
from joyqueue.exception.errors import NoAvailablePartition, NoAvailableBroker
from joyqueue.client.producer.interface import Producer,ProducerManager


class JoyQueueProducer(Producer):

    def __init__(self, app, clusterMetadataManager, producerManager):
        self._app = app
        self._clusterMetadataManager = clusterMetadataManager
        self._producerManager = producerManager
        self._channelFactory = TwistedChannelFactory()
        self._channel = None

    @defer.inlineCallbacks
    def syncSend(self, topic, msg):
        partitions_metadata = self._clusterMetadataManager.partitionMetadata(topic, self._app)
        if len(partitions_metadata) <= 0:
            raise NoAvailablePartition('no available partitions')
        partition_metadata = None
        for p in partitions_metadata:
            if p.get('leader'):
                partition_metadata = p
        if partition_metadata.get('leader') is None:
            raise NoAvailablePartition('no leader for partition {}'.format(partition_metadata.get('id')))
        producer_client = self._producerManager.getProducerClient(partition_metadata.get('leader'), topic, self._app)
        # message command
        # header
        response = yield producer_client.sync(msg)

        defer.returnValue(response)

    def asyncSend(self, topic, msg, callback):
        self._channel.write(msg).addCallback(callback)

    @defer.inlineCallbacks
    def start(self):
        self._channel = yield self._channelFactory.createChannel(
            self._config.get('host'),
            self._config.get('port'))
        # resp = yield self.syncSend('first msg'.encode('utf-8'))
        # messageListener(resp)


class DefaultProducerClientManager(ProducerManager):

    def __init__(self, clientManager, clusterMetadataManager):
        self._clientManager = clientManager
        self._clusterMetadataManager = clusterMetadataManager

    def getProducerClient(self, broker_node, topic, app_config):
        host, port = broker_node.host, broker_node.port
        client = yield self._clientManager.getClient(host, port)
        auth_client = DefaultAuthClientTransport(client)
        auth_client.auth(app_config)
        self.addProducer(client, topic, app_config)
        return client

    def addProducer(self, client, topic, app):

        pass


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
