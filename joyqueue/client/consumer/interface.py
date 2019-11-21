import abc


class MessageListener(object):
    __metaclass__ = abc.ABCMeta
    @abc.abstractmethod
    def onMessage(self,msgs):
        pass


class Consumer(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def subscribe(self, topic, MessageListener):
        pass

    @abc.abstractmethod
    def pull(self, topic):
        pass

    @abc.abstractmethod
    def ack(self, consumed):
        pass


class ConsumerManager(object):

    """

    Handle fetch metadata and add connection

    """
    @abc.abstractmethod
    def getConsumerClient(self, broker_node, topic, app_config):
        pass

    @abc.abstractmethod
    def addConsumer(self ,client,topic,app):
        pass