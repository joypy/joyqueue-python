from __future__ import absolute_import

import io
import time

from joyqueue.protocol.frame import JoyqueueBytes
from joyqueue.protocol.struct import Struct
from joyqueue.protocol.types import (
    Int8, Int32, Int64, Bytes,String, Schema, AbstractType
)
from joyqueue.util import crc32, WeakMethod


class Message(Struct):
    SCHEMAS = [
        Schema(
            ('length', Int32),
            ('partition', Int8),
            ('index', Int64),
            ('term', Int32),
            ('system_code', Int8),
            ('priority', Int8),
            ('send_time', Int64),
            ('store_time', Int32),
            ('body_crc', Int64),
            ('flag', Int8),
            ('body', Bytes),
            ('bussiness_id', String('utf-8')),
            ('app', String('utf-8'))),
    ]
    SCHEMA = SCHEMAS[0]
    CODEC_MASK = 0x07
    CODEC_GZIP = 0x01
    CODEC_SNAPPY = 0x02
    CODEC_LZ4 = 0x03
    TIMESTAMP_TYPE_MASK = 0x08
    HEADER_SIZE = 40  # length(4),partition(1),index(8),term(4),system_code(1),
                      # priority(1),send_time(8),store_time(4),body_crc(8),flag(1)

    def __init__(self, body, bussiness_id, app, length=-1, partition=-1,
        index=-1, term=-1, system_code=-1, priority=-1, send_time=-1, store_time=-1, body_crc=-1, flag=-1):
        assert body is None or isinstance(body, bytes), 'value must be bytes'
        self.length = length
        self.partition = partition
        self.index = index
        self.term = term
        self.system_code = system_code
        self.priority = priority
        self.send_time = send_time
        self.store_time = store_time
        self.body_crc = body_crc
        self.flag = flag
        self.body = body
        self.bussiness_id = bussiness_id
        self.app = app
        self.encode = WeakMethod(self._encode_self)

    @property
    def timestamp_type(self):
        """0 for CreateTime; 1 for LogAppendTime; None if unsupported.

        Value is determined by broker; produced messages should always set to 0
        Requires Kafka >= 0.10 / message version >= 1
        """
        if self.magic == 0:
            return None
        elif self.attributes & self.TIMESTAMP_TYPE_MASK:
            return 1
        else:
            return 0

    def _encode_self(self, recalc_crc=True):
        version = 0
        fields = (self.length, self.partition, self.index, self.term,
                  self.system_code, self.priority, self.send_time,
                  self.store_time, self.body_crc, self.flag,
                  self.body, self.bussiness_id, self.app)
        message = Message.SCHEMAS[version].encode(fields)
        return message

    @classmethod
    def decode(cls, data):
        if isinstance(data, bytes):
            data = io.BytesIO(data)
        # Partial decode required to determine message version
        base_fields = cls.SCHEMAS[0].fields[0:10]
        length, partition, index, term, system_code, priority, send_time,  \
        store_time, body_crc, flag = [field.decode(data) for field in base_fields]
        remaining = cls.SCHEMAS[0].fields[10:]
        body, bussiness_id, app = [field.decode(data) for field in remaining]
        msg = cls(body, bussiness_id, app, length, partition, index, term,
                  system_code, priority, send_time, flag)
        return msg

    def validate_crc(self):
        pass

    def is_compressed(self):
        return self.attributes & self.CODEC_MASK != 0

    def decompress(self):
        pass

    def __hash__(self):
        return hash(self._encode_self(recalc_crc=False))


class PartialMessage(bytes):
    def __repr__(self):
        return 'PartialMessage(%s)' % (self,)


class MessageSet(AbstractType):
    ITEM = Schema(
        ('offset', Int64),
        ('message', Bytes)
    )
    HEADER_SIZE = 12  # offset + message_size

    @classmethod
    def encode(cls, items, prepend_size=True):
        # RecordAccumulator encodes messagesets internally
        if isinstance(items, (io.BytesIO, JoyqueueBytes)):
            size = Int32.decode(items)
            if prepend_size:
                # rewind and return all the bytes
                items.seek(items.tell() - 4)
                size += 4
            return items.read(size)

        encoded_values = []
        for (offset, message) in items:
            encoded_values.append(Int64.encode(offset))
            encoded_values.append(Bytes.encode(message))
        encoded = b''.join(encoded_values)
        if prepend_size:
            return Bytes.encode(encoded)
        else:
            return encoded

    @classmethod
    def decode(cls, data, bytes_to_read=None):
        """Compressed messages should pass in bytes_to_read (via message size)
        otherwise, we decode from data as Int32
        """
        if isinstance(data, bytes):
            data = io.BytesIO(data)
        if bytes_to_read is None:
            bytes_to_read = Int32.decode(data)

        # if FetchRequest max_bytes is smaller than the available message set
        # the server returns partial data for the final message
        # So create an internal buffer to avoid over-reading
        raw = io.BytesIO(data.read(bytes_to_read))

        items = []
        while bytes_to_read:
            try:
                offset = Int64.decode(raw)
                msg_bytes = Bytes.decode(raw)
                bytes_to_read -= 8 + 4 + len(msg_bytes)
                items.append((offset, len(msg_bytes), Message.decode(msg_bytes)))
            except ValueError:
                # PartialMessage to signal that max_bytes may be too small
                items.append((None, None, PartialMessage()))
                break
        return items

    @classmethod
    def repr(cls, messages):
        if isinstance(messages, (JoyqueueBytes, io.BytesIO)):
            offset = messages.tell()
            decoded = cls.decode(messages)
            messages.seek(offset)
            messages = decoded
        return str([cls.ITEM.repr(m) for m in messages])
