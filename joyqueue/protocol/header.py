from joyqueue.protocol.types import (
    Int8, Int32, Int64, String, Schema
)
from joyqueue.protocol.interface import Header
from joyqueue.util import crc32, WeakMethod
import time


class JoyQueueHeader(Header):
    MAGIC = ~(0xCAFEBEBE ^ 0xFFFFFFFF)
    VERSION = 2
    SCHEMA = Schema(('magic', Int32),
                    ('version', Int8),
                    ('identity', Int8),
                    ('requestId', Int32),
                    ('type', Int8),
                    ('send_time', Int64),
                    ('status', Int8),
                    ('error', String('utf-8')))

    def __init__(self, magic, version, identity, requestId, type, send_time, status, error):
        self.magic = magic
        self.version = version
        self.identity = identity
        self.requestId = requestId
        self.type = type
        self.send_time = send_time
        self.status = status
        self.error = error
        self.encode = WeakMethod(self._encode_self)

    def _encode_self(self):
        # return super()._encode_self()
        request_or_response = self.identity & 0b00000001
        if request_or_response == 0:
            values = (self.magic, self.version, self.identity, self.requestId, self.type, self.send_time)
            base_fields = self.SCHEMA.fields
            bits = []
            for i in range(len(values)):
                bits.append(base_fields[i].encode(values[i]))
            return b''.join(bits)
        else:
            return super().encode()

    @classmethod
    def defaultHeader(cls, request):
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



