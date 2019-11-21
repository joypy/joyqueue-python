from __future__ import absolute_import

import io , time
from joyqueue.protocol.frame import JoyqueueBytes
from joyqueue.protocol.types import (
    Int8, Int16, Int32, Int64, Array, Bytes, String, ByteString, Schema, AbstractType
)
from joyqueue.protocol.interface import Request, Response
from joyqueue.protocol.command_key import (PRODUCE_MESSAGE_REQUEST, PRODUCE_MESSAGE_RESPONSE,
                                           FETCH_TOPIC_MESSAGE_REQUEST, FETCH_TOPIC_MESSAGE_RESPONSE,
                                           COMMIT_ACK_REQUEST, COMMIT_ACK_RESPONSE)
from joyqueue.util import WeakMethod
UTF8String = String('utf-8')


class Message(Request):
    SCHEMA = Schema(
            ('length', Int32),
            ('partition', Int16),
            ('index', Int64),
            ('term', Int32),
            ('magic', Int16),
            ('system_code', Int16),
            ('priority', Int8),
            ('clientIp', Bytes),
            ('send_time', Int64),
            ('store_time', Int32),
            ('body_crc', Int64),
            ('flag', Int16),
            ('body', Bytes),
            ('bussiness_id', ByteString('utf-8')),
            ('attributes', String('utf-8')),
            ('extension', Bytes),
            ('app', ByteString('utf-8')))
    CODEC_MASK = 0x07
    CODEC_GZIP = 0x01
    CODEC_SNAPPY = 0x02
    CODEC_LZ4 = 0x03
    TIMESTAMP_TYPE_MASK = 0x08
    SYSTEM_CODE = 0x000000000000
    MAGIC = 0x1234
    CLIENT_IP = b'abcdefabcdef'
    HEADER_SIZE = 40  # length(4),partition(1),index(8),term(4),system_code(1),
                      # priority(1),send_time(8),store_time(4),body_crc(8),flag(1)
    TYPE = 'Message'

    def __init__(self, body, bussiness_id, attributes, extension, app, length=-1, partition=0, index=-1,
        term=-1, magic= MAGIC, system_code= 0, priority=-1, client_ip= CLIENT_IP, send_time= int(time.time())*1000, store_time=1, body_crc= -1, flag= 0):
        assert body is None or isinstance(body, bytes), 'value must be bytes'
        self.length = length
        self.partition = partition
        self.index = index
        self.term = term
        self.magic = magic
        self.system_code = system_code
        self.priority = priority
        self.clientIp = client_ip
        self.send_time = send_time
        self.store_time = store_time
        self.body_crc = body_crc
        self.flag = flag
        self.body = body
        self.bussiness_id = bussiness_id
        self.attributes = attributes
        self.extension = extension
        self.app = app
        self.encode = WeakMethod(self._encode_self)

    def _encode_self(self, recalc_crc=True):
        fields = (self.length, self.partition, self.index, self.term, self.magic,
                  self.system_code, self.priority, self.clientIp, self.send_time, self.store_time,
                  self.body_crc, self.flag,self.body, self.bussiness_id, self.attributes,
                  self.extension, self.app)
        message = self.SCHEMA.encode(fields)
        return message

    @classmethod
    def encode(cls, item):
        it = cls(*item)
        return it.encode()

    @classmethod
    def decode(cls, data):
        if isinstance(data, bytes):
            data = io.BytesIO(data)
        # Partial decode required to determine message version
        base_fields = cls.SCHEMA.fields[0:7]
        length, partition, index, term, magic, system_code, priority = [field.decode(data) for field in base_fields]
        client_ip = data.read(16)
        remaining = cls.SCHEMA.fields[8:]
        send_time, store_time, body_crc, flag, body, bussiness_id, attr, extension, app = [field.decode(data) for field in remaining]
        msg = cls(body, bussiness_id, attr, extension, app, length, partition, index, term, magic,
                  system_code, priority, client_ip, send_time, store_time, body_crc, flag)
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


class TopicProduceMessage(Request):
    SCHEMA = Schema(('topic', UTF8String),
                    ('txId', UTF8String),
                    ('timeout', Int32),
                    ('qosLevel', Int8),
                    ('messages', Array(Message)))
    TYPE = 'Topic produce message'


class ProduceMessageRequest(Request):
    SCHEMA = Schema(('topics', Array(TopicProduceMessage)),
                    ('app', UTF8String))
    TYPE = PRODUCE_MESSAGE_REQUEST

    @classmethod
    def default_messages_request(cls, topic, producer, msg):
        msgs = [(msg, 'deault_bussiness_id', None, None, producer)]
        topic_messages = [(topic, None, 5000, 2, msgs)]
        return ProduceMessageRequest(topic_messages, producer)


class ProducePartitionAck(Request):
    SCHEMA = Schema(('partition', Int16),
                    ('index', Int64),
                    ('startTime', Int64))
    TYPE = 'Partition ack'


class TopicProducePartitionAck(Request):
    SCHEMA = Schema(('topic', UTF8String),
                    ('code', Int32),
                    ('result', Array(ProducePartitionAck)))
    TYPE = 'Topic partition ack'


class ProduceMessageResponse(Response):
    SCHEMA = Schema(('data', Array(TopicProducePartitionAck)))
    TYPE = PRODUCE_MESSAGE_RESPONSE


class FetchTopic(Request):
    SCHEMA = Schema(('topic', UTF8String),
                    ('count', Int16))
    TYPE = 'Fetch topic'


class FetchMessageRequest(Request):
    SCHEMA = Schema(('topics', Array(FetchTopic)),
                    ('app', UTF8String),
                    ('ackTimeout', Int32),
                    ('longPollTimeout', Int32))
    TYPE = FETCH_TOPIC_MESSAGE_REQUEST

    @classmethod
    def default_fetch_message_request(cls, topic, app, batch_size):
        topics = [(topic, batch_size)]
        fetch_message = (topics, app, 5000, 10000)
        return FetchMessageRequest(*fetch_message)


class TopicMessage(Request):
    SCHEMA = Schema(('topic', UTF8String),
                    ('messages', Array(Message)),
                    ('code', Int32))
    TYPE = 'Topic produce message'


class FetchMessageResponse(Response):
    SCHEMA = Schema(('data', Array(TopicMessage)))
    TYPE = FETCH_TOPIC_MESSAGE_RESPONSE


class ConsumePartitionAck(Request):
    SCHEMA = Schema(('partition', Int16),
                    ('index', Int64),
                    ('type', Int8))
    TYPE = 'Consume partition ack'


class ConsumePartitionGroupAck(Request):
    SCHEMA = Schema(('partition', Int16),
                    ('data', Array(ConsumePartitionAck)))
    TYPE = 'Consume partition group ack'


class ConsumeTopicAck(Request):
    SCHEMA = Schema(('topic', UTF8String),
                    ('partitions', ConsumePartitionGroupAck))
    TYPE = 'Consume topic ack'


class ConsumeAck(Request):
    SCHEMA = Schema(('topics', ConsumeTopicAck),
                    ('app', UTF8String))
    TYPE = 'Consume ack'


class ConsumePartitionIndexAckResponse(Response):
    SCHEMA = Schema(('partition', Int16),
                    ('index', Int64),
                    ('type', Int8))
    TYPE = 'Consume partition index ack'


class ConsumePartitionAckResponse(Response):
    SCHEMA = Schema(('partition', Int16),
                    ('data', Array(ConsumePartitionIndexAckResponse)))
    TYPE = 'Consume partition ack'


class ConsumeTopicPartitionGroupAckResponse(Response):
    SCHEMA = Schema(('topic', UTF8String),
                    ('partitions', Array(ConsumePartitionAckResponse)))
    TYPE = 'Consume topic partition group ack'


class ConsumeAck(Request):
    SCHEMA = Schema(('topics', Array(ConsumeTopicPartitionGroupAckResponse)),
                    ('app', UTF8String))
    TYPE = COMMIT_ACK_REQUEST


class ConsumePartitionAckState(Response):
    SCHEMA = Schema(('partition', Int16),
                    ('code', Int8))
    TYPE = 'Consume partition ack state'


class ConsumeTopicPartitionAckState(Response):
    SCHEMA = Schema(('topic', UTF8String),
                    ('partitionData', Array(ConsumePartitionAckState)))
    TYPE = 'Consume topic partition ack state'


class ConsumeAckResponse(Response):
    SCHEMA = Schema(('data', Array(ConsumeTopicPartitionAckState)))
    TYPE = COMMIT_ACK_RESPONSE