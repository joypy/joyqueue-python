import abc


class Channel(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def write(self, bytes):
        pass

    @abc.abstractmethod
    def receive(self):
        pass


class Writer(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def write(self, buf):
        pass