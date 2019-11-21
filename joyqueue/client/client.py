from joyqueue.network.transport import Transport
from twisted.internet import defer
from joyqueue.network.channel import TwistedChannelFactory
from collections import defaultdict
from joyqueue.client.interface import ClientFactory, ClientManager, AuthClientTransport
from joyqueue.protocol.metadata import ConnectionRequest
from joyqueue.protocol.header import JoyQueueHeader
from joyqueue.protocol.command import Command
from joyqueue.exception.errors import AuthApplicationError
import time, logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

log = logging.getLogger(__name__)


class ClientTransport(Transport):

    def __init__(self, channel):
        self._channel = channel

    def oneway(self, command, timeout=None):
        pass

    def async(self, command, callback=None, timeout=None):
        future = self._channel.write(command)
        if callback:
            future.addCallback(callback)
        else:
            return future

    @defer.inlineCallbacks
    def sync(self, command, timeout=None):
        response = yield self._channel.write(command)
        return response


class DefaultAuthClientTransport(AuthClientTransport):

    def __init__(self, client):
        self._client = client

    @defer.inlineCallbacks
    def auth(self, app_config):
        con_request = ConnectionRequest(app_config.get('username'),
                                        app_config.get('password'),
                                        app_config.get('app'),
                                        app_config.get('token'),
                                        app_config.get('region'),
                                        app_config.get('namespace'),
                                        app_config.get('language'),
                                        app_config.get('version'),
                                        app_config.get('ip'),
                                        int(time.time()),
                                        app_config.get('sequence'))
        header = JoyQueueHeader.defaultHeader(con_request)
        command = Command(header, con_request)
        response = yield self._client.sync(command)
        re_header = response._header
        if not re_header.status:
            log.info('add connection success')
            return True
        else:
            log.info('add connection failed,{}'.format(re_header.error))
            return False

    def getClient(self):
        return self._client


class DefaultClientFactory(ClientFactory):

    def __init__(self):
        self._channelFactory = TwistedChannelFactory()
        self._connections = defaultdict(None)

    @defer.inlineCallbacks
    def createClient(self, ip, port):
        channel = yield self._channelFactory.createChannel(ip, port)
        return ClientTransport(channel)


class DefaultClientManager(ClientManager):
    def __init__(self):
        self._clientFactory = DefaultClientFactory()
        self._clients = defaultdict(None)
        self._auth_clients = defaultdict(None)

    def getClient(self, ip, port):
        client_key = ip+':'+str(port)
        try:
            client = self._clients.get(client_key)
            if client is None:
                client = self._clientFactory.createClient(ip, port)
                self._clients[client_key] = client
            return client
        except Exception as ex:
            print(ex)

    @defer.inlineCallbacks
    def getAuthClient(self, ip, port, app_config):
        app = app_config.get('app') + '.' + app_config.get('namespace')
        auth_client_key = ip + ':' + str(port) + ':' + app
        auth_client = self._auth_clients.get(auth_client_key)
        if auth_client is None:
            client = yield self.getClient(ip, port)
            auth_client = DefaultAuthClientTransport(client)
            result = yield auth_client.auth(app_config)
            if result:
                self._auth_clients[auth_client_key] = auth_client
            else:
                raise AuthApplicationError('auth application:{} failed'.format(app))
        return auth_client.getClient()

    def releaseClient(self, ip, port):
        client_key = ip+':'+port
        to_be_release = self._clients[client_key]
        self._clients[client_key] = None
        return to_be_release

