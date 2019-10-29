from joyqueue.protocol.struct import Struct
from joyqueue.protocol.types import (
    Int8, Int32, Int64, Bytes, String, Schema, AbstractType
)
from joyqueue.util import WeakMethod
import io

class Header(Struct):
    SCHEMAS = [
        Schema(
            ('length', Int32),
            ('magic', Int32),
            ('version', Int8),
            ('identity', Int8),
            ('requestId', Int32),
            ('type', Int8),
            ('send_time', Int64),
            ('status', Int8),
            ('error', String('utf-8')))
    ]
    SCHEMA = SCHEMAS[0]

    def __init__(self,
                 length,
                 magic,
                 version,
                 identity,
                 requestId,
                 type,
                 send_time,
                 status,
                 error):
        self.length = length
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
        version = 0
        fields = (self.length, self.magic, self.version, self.identity,
                  self.requestId, self.type, self.send_time,self.status,
                  self.error)
        return Header.SCHEMAS[version].encode(fields)

    @classmethod
    def decode(cls, data):
        if isinstance(data, bytes):
            data = io.BytesIO(data)
        length, magic, version, identity, requestId, type, send_time, status, error = \
                                [field.decode(data) for field in cls.SCHEMAS[0].fields]
        return cls(length, magic, version, identity, requestId,type ,send_time, status, error)

    def __hash__(self):
        return hash(self._encode_self(recalc_crc=False))






