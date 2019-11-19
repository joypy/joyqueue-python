import abc
from joyqueue.protocol.struct import Struct


class Header(Struct):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def VERSION(self):
        """
        Header version
        """
        pass

    @abc.abstractproperty
    def MAGIC(self):
        """
        Header version
        """
        pass

    @abc.abstractproperty
    def SCHEMA(self):
        """
        Serialize and Deserialize fields and types
        """
        pass


class Request(Struct):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def TYPE(self):
        """
        Request type
        """
        pass

    @abc.abstractproperty
    def SCHEMA(self):
        """
        Serialize and Deserialize fields and types
        """
        pass


class Response(Struct):
    __metaclass__ = abc.ABCMeta


    @abc.abstractproperty
    def TYPE(self):
        """
        Response type
        """
        pass

    @abc.abstractproperty
    def SCHEMA(self):
        """
        Serialize and Deserialize fields and types
        """
        pass


