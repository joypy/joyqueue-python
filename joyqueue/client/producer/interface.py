import abc


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