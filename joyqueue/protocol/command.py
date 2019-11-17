
import io
from joyqueue.protocol.struct import Struct
from joyqueue.util import WeakMethod
from joyqueue.protocol.codec import ResponseDecodeFactory
from joyqueue.protocol.header import JoyQueueHeader
from joyqueue.protocol.version import Version


class Command(Struct):
    VERSION = Version()
    CODEC_FACTORY = ResponseDecodeFactory()
    HEADER = JoyQueueHeader

    def __init__(self, header, body):
        self._header = header
        self._body = body
        self.encode = WeakMethod(self._encode_self)

    def _encode_self(self):
        if self._header is None:
            raise IllegalArgumentError("header can't be None ")
        if self._body is None:
            raise IllegalArgumentError("body can't be None ")
        return self._header.encode() + self._body.encode()

    @classmethod
    def decode(cls, data):
        if isinstance(data, bytes):
            data = io.BytesIO(data)
        header = Command.HEADER.decode(data)
        if header.version != header.VERSION:
            raise UnexpectedVersionError('version inconsistency')
        decoder = Command.CODEC_FACTORY.get(header.type)
        body = decoder.decode(data)
        return cls(header, body)

    def __repr__(self):
        return self._header.__repr__() + self._body.__repr__()

    def __hash__(self):
        return self._header.__hash__() + self._body.__hash__()

    def __eq__(self, other):
        if isinstance(other, Command):
            if other._header is not None:
                if not other._header.__eq__(self._header):
                        return False
            elif self._header is not None:
                if not self._header.__eq__(other._header):
                    return False
            if other._body is not None:
                if not other._body.__eq__(self._body):
                    return False
                else:
                    return True
            elif self._body is not None:
                if not self._body.__eq__(other._body):
                    return False
                else:
                    return True
        return False



class UnexpectedVersionError(AssertionError):
    """
    Raised when header version not equal with response header version
    """

class IllegalArgumentError(AssertionError):
    """
    Raised when meet a illegal argument
    """