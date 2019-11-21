
import logging
from twisted.internet import defer, reactor
from collections import defaultdict
from joyqueue.exception.errors import NoAvailablePartition
from joyqueue.client.client import DefaultClientManager
from joyqueue.client.consumer.interface import Consumer, ConsumerManager
from joyqueue.protocol.metadata import ConsumerRequest
from joyqueue.protocol.header import JoyQueueHeader
from joyqueue.protocol.message import FetchMessageRequest, ConsumeAck
from joyqueue.client.cluster import ClusterMetadataManager, default_application as Application
from joyqueue.protocol.command import Command
from joyqueue.model.configs import NameServerConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)


class DefaultConsumerClientManager(ConsumerManager):

    def __init__(self, clientManager, clusterMetadataManager):
        self._clientManager = clientManager
        self._clusterMetadataManager = clusterMetadataManager
        self._broker_app_clients = defaultdict(None)
        self._broker_app_consumer_clients = defaultdict(None)

    @defer.inlineCallbacks
    def getConsumerClient(self, broker_node, topic, app_config):
        app = app_config.get('app') + '.' + app_config.get('namespace')
        host, port = broker_node.host, broker_node.port
        broker_app_auth_client_key = host + ':' + str(port) + ':' + app
        broker_app_producer_client_key = broker_app_auth_client_key + ':' + topic
        client = yield self._clientManager.getAuthClient(host, port, app_config)
        broker_app_consumer_client = self._broker_app_consumer_clients.get(broker_app_producer_client_key)
        if broker_app_consumer_client is None:
            yield self.addConsumer(client, topic, app)
            self._broker_app_consumer_clients[broker_app_producer_client_key] = client
        return client

    @defer.inlineCallbacks
    def addConsumer(self, client, topic, app):
        consumer_request = ConsumerRequest([topic], app, 0)
        header = JoyQueueHeader.defaultHeader(consumer_request)
        add_consumer_command = Command(header, consumer_request)
        response = yield client.sync(add_consumer_command)
        re_header = response._header
        if not re_header.status:
            log.info('add producer success')
            return True
        else:
            log.info('add producer failed,{}'.format(re_header.error))
            return False


class JoyQueueConsumer(Consumer):

    def __init__(self, app_config, clusterMetadataManager, consumerManager):
        self._app = app_config.get('app') + '.' + app_config.get('namespace')
        self._app_config = app_config
        self._clusterMetadataManager = clusterMetadataManager
        self._consumerManager = consumerManager

    def subscribe(self, topic, MessageListener):
        pass

    @defer.inlineCallbacks
    def pull(self, topic):
        #self._clusterMetadataManager
        partitions_metadata = yield self._clusterMetadataManager.topicMetadata(topic, self._app)
        if len(partitions_metadata) <= 0:
            raise NoAvailablePartition('no available partitions')
        partition_metadata = None
        for p in partitions_metadata:
            if p.get('leader'):
                partition_metadata = p
        if partition_metadata.get('leader') is None:
            raise NoAvailablePartition('no leader for partition {}'.format(partition_metadata.get('id')))
        consumer_client = yield self._consumerManager.getConsumerClient(partition_metadata.get('leader'), topic, self._app_config)
        request = FetchMessageRequest.default_fetch_message_request(topic, self._app, 10)
        log.info(request)
        header = JoyQueueHeader.defaultHeader(request)
        produce_message_request = Command(header, request)
        response = yield consumer_client.sync(produce_message_request)
        # log.info(response)
        re_header = response._header
        if not re_header.status:
            log.info('consume message success:{}'.format(response._body.data[0].messages))
            yield self.ack(response._body.data[0].topic, response._body.data[0].messages)
            return True
        else:
            log.info('add producer failed,{}'.format(re_header.error))
            return False

    @defer.inlineCallbacks
    def ack(self, topic, consumed):
        if consumed and len(consumed) > 0:
            consume_ack_request, topic, partition = self.ack_request(topic, consumed)
            partition_metadata = yield self._clusterMetadataManager.topicPartitionMetadata(topic, partition, self._app)
            consumer_client = yield self._consumerManager.getConsumerClient(partition_metadata.get('leader'),
                                                                            topic, self._app_config)
            header = JoyQueueHeader.defaultHeader(consume_ack_request)
            consume_message_ack_request = Command(header, consume_ack_request)
            response = yield consumer_client.sync(consume_message_ack_request)
            log.info(response)
        else:
            log.info('ignore empty message ack')

    def ack_request(self, topic, consumed):
        """
        ack message of a partition
        """
        app = self._app
        partition = consumed[0].partition
        topics_ack = []
        partitions = []
        index_state = []
        for m in consumed:
            index_state.append((partition, m.index, 0))
        partitions.append((partition, index_state))
        topics_ack.append((topic, partitions))
        consume_ack = ConsumeAck(topics_ack, app)
        receive = ConsumeAck.decode(consume_ack.encode())
        log.info(consume_ack)
        log.info(receive)
        return consume_ack, topic, partition



@defer.inlineCallbacks
def main():
    config = NameServerConfig()
    hostAndPort = config.address.split(':')
    if len(hostAndPort) < 2:
        raise ValueError('wrong host and port')
    host = hostAndPort[0]
    port = int(hostAndPort[1])
    application = Application()
    #application.set('app', 'test.abc')
    producerConfig = {'host': host, 'port': port, 'app': application}
    client_manager = DefaultClientManager()
    cluster_metadata_manager = ClusterMetadataManager(producerConfig, client_manager)
    yield cluster_metadata_manager.start()
    consumer_manager = DefaultConsumerClientManager(client_manager, cluster_metadata_manager)
    consumer = JoyQueueConsumer(application, cluster_metadata_manager, consumer_manager)
    topic = 'test_topic'
    yield consumer.pull(topic)
    reactor.stop()


if __name__ == '__main__':
    main()
    #default_messages_request('test_topic',b'abc', 'consumer')
    # start reactor single instance
    reactor.run()