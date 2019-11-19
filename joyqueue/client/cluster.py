
from collections import defaultdict
from twisted.internet import reactor, defer
from joyqueue.model.configs import NameServerConfig
from joyqueue.model.models import Application
from joyqueue.protocol.metadata import MetadataRequest
from joyqueue.protocol.header import JoyQueueHeader
from joyqueue.protocol.command import Command
import logging, time
from joyqueue.client.client import DefaultClientManager,DefaultAuthClientTransport
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)


class ClusterMetadataManager(object):

    def __init__(self, cluster_config, client_manager):
        self._cluster_config = cluster_config
        self._metadata = defaultdict(None)
        self._client_manager = client_manager
        self._client = None
        self._auth_client = None

    @defer.inlineCallbacks
    def start(self):
        self._client = yield self._client_manager.getClient(
                                self._cluster_config.get('host'),
                                self._cluster_config.get('port'))
        self._auth_client = DefaultAuthClientTransport(self._client)
        # auth
        yield self._auth_client.auth(self._cluster_config['app'])

    def metadata(self, topic):
        pass

    @defer.inlineCallbacks
    def updateMetadata(self, topic, app):
        topics = [topic]
        request = MetadataRequest(topics, app)
        header = defaultHeader(request)
        command = Command(header, request)
        response = yield self._client.sync(command)
        self._metadata[topic] = response
        log.info(response)


def defaultHeader(request):
    magic = ~(0xCAFEBEBE ^ 0xFFFFFFFF)
    version = 2
    # request and qos level receive
    identity = 0b00000010
    requestId = 78237
    type = request.TYPE
    send_time = int(time.time())
    status = 0
    error = None
    return JoyQueueHeader(magic, version, identity, requestId, type, send_time,
                          status, error)


def default_application():
    user_name = 'test'
    password = '123456'
    app = 'test'
    token = 'a833f52af2964f259c7878020afba48c'
    region = None
    namespace = 'abc'
    language = 2
    version = '2.1.0'
    ip = '127.0.0.1'
    t = int(time.time())
    sequence = 1000
    return Application(user_name, password, app, token, region, namespace, language, version,
                       ip, t, sequence)


@defer.inlineCallbacks
def main():
    log.info('metadata start')
    config = NameServerConfig()
    hostAndPort = config.address.split(':')
    if len(hostAndPort) < 2:
        raise ValueError('wrong host and port')
    host = hostAndPort[0]
    port = int(hostAndPort[1])
    application = default_application()
    producerConfig = {'host': host, 'port': port, 'app': application}
    client_manager = DefaultClientManager()
    cluster_metadata_manager = ClusterMetadataManager(producerConfig, client_manager)
    yield cluster_metadata_manager.start()
    # yield cluster_metadata_manager._client
    topic = 'test_topic'
    app = application.get('app')+'.'+application.get('namespace')
    yield cluster_metadata_manager.updateMetadata(topic, app)
    # yield cluster_metadata_manager.updateMetadata(topic)
    reactor.stop()


if __name__ == '__main__':
    main()
    log.info("start to run")
    reactor.run()