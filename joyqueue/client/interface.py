
import abc


class ClientFactory(object):

    @abc.abstractmethod
    def createClient(self, ip, port):
        pass


class ClientManager(object):

    @abc.abstractmethod
    def getClient(self, ip, port):
        pass

    @abc.abstractmethod
    def releaseClient(self, ip, port):
        pass


class AuthClientTransport(object):

        @abc.abstractmethod
        def auth(self, auth_config):
            pass

        @abc.abstractmethod
        def getClient(self):
            pass