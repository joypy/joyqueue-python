
import abc
"Real Application layer connection interface"


class ChannelFactory(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def createChannel(self, ip, port):
        pass


class Transport(object):
    __metaclass__ = abc.ABCMeta

    " Oneway request  with timeout"
    @abc.abstractmethod
    def oneway(self, command, timeout=None):
        pass

    " Async request  with timeout"
    @abc.abstractmethod
    def async(self, command, callback, timeout=None):
        pass

    " Sync request  with timeout"
    @abc.abstractmethod
    def sync(self, command, timeout=None):
        pass


