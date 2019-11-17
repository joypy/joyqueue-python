from joyqueue.protocol.interface import Request, Response
from joyqueue.protocol.types import Schema, Array, String, Int16, Int32, Boolean
from joyqueue.protocol.command_key import FETCH_CLUSTER_REQUEST, FETCH_CLUSTER_RESPONSE
from io import BytesIO
from joyqueue.util import WeakMethod

UTF8String = String('utf-8')


class MetadataRequest(Request):
    # STR = String('utf-8')
    SCHEMA = Schema(('topics', Array(UTF8String)),
                    ('app', UTF8String))
    TYPE = FETCH_CLUSTER_REQUEST


class Weight(Response):
    SCHEMA = Schema(('brokerId', UTF8String),
                    ('weight', Int16))


class ProducerPolicy(Response):
     SCHEMA = Schema(('nearby', Boolean),
                     ('single', Boolean),
                     ('archive', Boolean),
                     ('weight', Array(Weight)),
                     ('blackList', Array(UTF8String)),
                     ('timeout', Int32))
     TYPE = 'Producer Policy'


class ConsumerPolicy(Response):
     SCHEMA = Schema(('nearby', Boolean),
                     ('pause', Boolean),
                     ('archive', Boolean),
                     ('retry', Boolean),
                     ('seq', Boolean),
                     ('ackTimeout', Int32),
                     ('backSize', Int16),
                     ('concurrent', Boolean),
                     ('concurrentSize', Int32),
                     ('delay', Int32),
                     ('backList', Array(UTF8String)),
                     ('errTimes', Int32),
                     ('maxPartitionNum', Int32),
                     ('readRetryProportion', Int32))
     TYPE = 'Consumer policy'


class PartitionGroup(Response):
     SCHEMA = Schema(('id', Int32),
                      ('leader', Int32),
                      ('partitions', Array(Int32)))
     TYPE = 'Partition Group'


class Topic(Response):
    SCHEMA = Schema(('code', UTF8String),
                    ('hasProducerPolicy', Boolean),
                    ('producerPolicy', ProducerPolicy),
                    ('hasConsumerPolicy', Boolean),
                    ('consumerPolicy', ConsumerPolicy),
                    ('type', Int32),
                    ('partitionGroups', Array(PartitionGroup)))
    TYPE = 'Topic'

    def __init__(self, code, hasProducerPolicy, producerPolicy,hasConsumerPolicy,consumerPolicy,type,partitionGroups):
        self.code = code
        self.hasProducerPolicy = hasProducerPolicy
        self.producerPolicy = producerPolicy
        self.hasConsumerPolicy = hasConsumerPolicy
        self.consumerPolicy = consumerPolicy
        self.type = type
        self.partitionGroups = partitionGroups
        self.encode = WeakMethod(self._encode_self)

    def _encode_self(self):
        list = []
        list.append(self.SCHEMA.fields[0].encode(self.code))
        list.append(self.SCHEMA.fields[1].encode(self.hasProducerPolicy))
        if self.hasProducerPolicy:
            list.append(self.SCHEMA.fields[2].encode(self.producerPolicy))

        list.append(self.SCHEMA.fields[3].encode(self.hasConsumerPolicy))
        if self.hasConsumerPolicy:
            list.append(self.SCHEMA.fields[4].encode(self.consumerPolicy))

        list.append(self.SCHEMA.fields[5].encode(self.type))
        list.append(self.SCHEMA.fields[6].encode(self.partitionGroups))
        return b''.join(list)

    @classmethod
    def encode(cls, item):
        it = cls(*item)
        return it.encode()

    @classmethod
    def decode(cls, data):
        if isinstance(data, bytes):
            data = BytesIO(data)
        base_fields = cls.SCHEMA.fields[0:2]
        code, has_producer_policy = [f.decode(data) for f in base_fields]
        producerPolicy = None
        consumerPolicy = None
        if has_producer_policy:
            producerPolicy = cls.SCHEMA.fields[2].decode(data)
        has_consumer_policy = cls.SCHEMA.fields[3].decode(data)
        if has_consumer_policy:
            consumerPolicy = cls.SCHEMA.fields[4].decode(data)
        type, partitionGroups = [f.decode(data) for f in cls.SCHEMA.fields[5:]]
        return cls(code, has_producer_policy, producerPolicy, has_consumer_policy, consumerPolicy, type, partitionGroups)


class Broker(Response):
    SCHEMA = Schema(('id', Int32),
                    ('host', UTF8String),
                    ('port', Int32),
                    ('dataCenter', UTF8String),
                    ('nearby', Boolean),
                    ('weight', Int32))

    TYPE = 'Broker'


class MetadataResponse(Response):
    SCHEMA = Schema(('topics', Array(Topic)),
                    ('brokers', Array(Broker)))
    TYPE = FETCH_CLUSTER_RESPONSE


