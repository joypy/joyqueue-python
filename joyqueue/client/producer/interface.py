import abc


class Producer(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def syncSend(self, topic, msg):
        pass

    @abc.abstractmethod
    def asyncSend(self,topic ,msg, callback):
        pass

    @abc.abstractmethod
    def start(self):
        pass


class ProducerManager(object):

    """

    Handle fetch metadata and add connection

    """
    @abc.abstractmethod
    def getProducer(self, topic, app):
        pass
