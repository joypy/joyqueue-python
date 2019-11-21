
import logging
from collections import defaultdict
from twisted.internet import reactor, defer
from joyqueue.exception.errors import NoAvailablePartition
from joyqueue.network.channel import TwistedChannelFactory
from joyqueue.protocol.metadata import ProducerRequest
from joyqueue.protocol.command import Command
from joyqueue.protocol.header import JoyQueueHeader
from joyqueue.protocol.message import ProduceMessageRequest
from joyqueue.model.configs import NameServerConfig
from joyqueue.client.client import DefaultAuthClientTransport, DefaultClientManager
from joyqueue.client.producer.interface import Producer, ProducerManager
from joyqueue.client.cluster import ClusterMetadataManager, default_application as Application
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)


class JoyQueueProducer(Producer):

    def __init__(self, app_config, clusterMetadataManager, producerManager):
        self._app = app_config.get('app')
        self._app_config = app_config
        self._clusterMetadataManager = clusterMetadataManager
        self._producerManager = producerManager
        self._channelFactory = TwistedChannelFactory()
        self._channel = None

    @defer.inlineCallbacks
    def syncSend(self, topic, msg):
        partitions_metadata = yield self._clusterMetadataManager.topicMetadata(topic, self._app)
        if len(partitions_metadata) <= 0:
            raise NoAvailablePartition('no available partitions')
        partition_metadata = None
        for p in partitions_metadata:
            if p.get('leader'):
                partition_metadata = p
        if partition_metadata.get('leader') is None:
            raise NoAvailablePartition('no leader for partition {}'.format(partition_metadata.get('id')))
        producer_client = yield self._producerManager.getProducerClient(partition_metadata.get('leader'), topic, self._app_config)
        request = ProduceMessageRequest.default_messages_request(topic, self._app,  msg)
        log.info(request)
        header = JoyQueueHeader.defaultHeader(request)
        produce_message_request = Command(header, request)
        response = yield producer_client.sync(produce_message_request)
        print(response)
        defer.returnValue(response)

    def asyncSend(self, topic, msg, callback):
        self._channel.write(msg).addCallback(callback)

    @defer.inlineCallbacks
    def start(self):
        pass


class DefaultProducerClientManager(ProducerManager):

    def __init__(self, clientManager, clusterMetadataManager):
        self._clientManager = clientManager
        self._clusterMetadataManager = clusterMetadataManager
        self._broker_app_auth_clients = defaultdict(None)
        self._broker_app_producer = defaultdict(None)
    """
      Multi topic of same app client on broker can multiplex 
    """
    @defer.inlineCallbacks
    def getProducerClient(self, broker_node, topic, app_config):
        host, port = broker_node.host, broker_node.port
        broker_app_auth_client_key = host + ':' + str(port) + ':' + app_config.get('app')
        broker_app_auth_client = self._broker_app_auth_clients.get(broker_app_auth_client_key)
        if broker_app_auth_client is None:
            client = yield self._clientManager.getClient(host, port)
            auth_client = DefaultAuthClientTransport(client)
            yield auth_client.auth(app_config)
            self._broker_app_auth_clients[broker_app_auth_client_key] = auth_client
        auth_client = self._broker_app_auth_clients[broker_app_auth_client_key]
        broker_app_producer_key = broker_app_auth_client_key + ':' + topic
        broker_app_producer = self._broker_app_producer.get(broker_app_producer_key)
        if broker_app_producer is None:
            yield self.addProducer(auth_client.getClient(), topic, app_config.get('app'))
            self._broker_app_producer[broker_app_producer_key] = client
        return auth_client.getClient()

    @defer.inlineCallbacks
    def addProducer(self, client, topic, app):
        producer_request = ProducerRequest([topic], app, 0)
        header = JoyQueueHeader.defaultHeader(producer_request)
        add_producer_command = Command(header, producer_request)
        response = yield client.sync(add_producer_command)
        re_header = response._header
        if not re_header.status:
            log.info('add producer success')
            return True
        else:
            log.info('add producer failed,{}'.format(re_header.error))
            return False


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
    application = Application()
    producerConfig = {'host': host, 'port': port, 'app': application}
    client_manager = DefaultClientManager()
    cluster_metadata_manager = ClusterMetadataManager(producerConfig, client_manager)
    yield cluster_metadata_manager.start()
    producer_manager = DefaultProducerClientManager(client_manager, cluster_metadata_manager)
    producer = JoyQueueProducer(application, cluster_metadata_manager, producer_manager)
    # producer.default_messages_request('test_topic', b'abcd')
    #yield producer.start()
    topic = 'test_topic'
    response = yield producer.syncSend(topic, '7 msg'.encode('utf-8'))
    response = yield producer.syncSend(topic, '8 msg'.encode('utf-8'))
    # print(response)
    reactor.stop()


if __name__ == '__main__':
    main()
    #default_messages_request('test_topic',b'abc', 'consumer')
    # start reactor single instance
    reactor.run()
