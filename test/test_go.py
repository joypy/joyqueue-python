import pytest
from joyqueue.protocol.types import Schema
from joyqueue.protocol.types import (
  Int8, Int32, Int64, Bytes,String, Schema, AbstractType
)
from collections import namedtuple


def test_named_tuple():
    TopicPartition = namedtuple("TopicPartition",
                                ["topic", "partition"])
    topic_partition = TopicPartition('abc_topic', 0)
    assert topic_partition.topic == 'abc_topic'
    assert topic_partition.partition == 0
    print(topic_partition.topic)
    print(topic_partition.partition)



def test_schema():
    header_schema = Schema(('length', Int32),
                           ('magic', Int32),
                           ('version', Int8))
    print(header_schema.names)
    print(header_schema.fields)
