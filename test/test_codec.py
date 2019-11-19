# pytest -s test_codec
from joyqueue.protocol.message import Message
from joyqueue.protocol.property import Property
from joyqueue.protocol.metadata import MetadataRequest, MetadataResponse,PartitionGroup
from joyqueue.protocol.header import JoyQueueHeader
from joyqueue.protocol.command import Command
from joyqueue.protocol.types import Array,String
from joyqueue.protocol.struct import StructArray
from joyqueue.protocol.code import JoyQueueCode
import time
import importlib
import sys
importlib.reload(sys)


def test_array_string():
    topics = ['test_topic']
    bytes=Array(String()).encode(topics)
    print(bytes)

def test_msg_codec():
    msg = b'abncdfasdf'
    bussiness_id = 'abc_id'
    app = 'test_app'
    length = 100
    partition = 1
    index = 999999
    term = -1
    system_code = 2
    priority = 10
    t = time.time()
    store_time = 99
    body_crc = 13039503950
    attr = {'id':'983485943','abc':'abc'}
    pro = Property()
    for k, v in attr.items():
        pro.put(k, v)
    extension = b'extension'
    flag = 2

    m = Message(msg, bussiness_id, pro, extension, app, length, partition, index,
                term, system_code, priority, int(t), store_time, body_crc, flag)
    byteMsg = m.encode()
    ## class method
    receiveMsg = Message.decode(byteMsg)

    print(receiveMsg)
    assert receiveMsg.body == msg
    assert receiveMsg.bussiness_id == bussiness_id
    assert receiveMsg.attributes == pro
    assert receiveMsg.extension == extension
    assert receiveMsg.app == app
    assert receiveMsg.length == length
    assert receiveMsg.partition == partition
    assert receiveMsg.index == index
    assert receiveMsg.term == term
    assert receiveMsg.system_code == system_code
    assert receiveMsg.priority == priority
    assert receiveMsg.send_time == int(t)
    assert receiveMsg.store_time == store_time
    assert receiveMsg.body_crc == body_crc
    assert receiveMsg.flag == flag


def test_header():
    magic = ~(0xCAFEBEBE ^ 0xFFFFFFFF)
    version = 2
    identity = 5
    requestId = 78237
    type = 5
    send_time = int(time.time())
    status = -1
    error = 'no permission'
    Header = JoyQueueHeader[0]
    h = Header(magic, version, identity, requestId, type, send_time,
               status, error)
    hbytes = h.encode()
    receiveHeader = Header.decode(hbytes)
    assert h == receiveHeader


def test_property():
    pro = Property()
    pro.put('a', 'abdgdg')
    pro.put('b', 'bjkjkj')
    pro.put('c', 'abcjjj')

    str = pro.encode()
    print(str)
    receivedProperty = Property.decode(str)

    print(receivedProperty)
    assert receivedProperty.get('a') == pro.get('a')
    assert receivedProperty.get('b') == pro.get('b')
    assert receivedProperty.get('c') == pro.get('c')


def test_property_none():
    pro = Property()
    # pro.put('a', 'abdgdg')
    # pro.put('b', 'bjkjkj')
    # pro.put('c', 'abcjjj')

    bytes = pro.encode()
    print(bytes)
    receivedProperty = Property.decode(bytes)
    print(receivedProperty)
    print(receivedProperty.get('a'))
    assert receivedProperty.get('a') is None
    assert receivedProperty.get('b') is None
    assert receivedProperty.get('c') is None


def test_metadata_request():
    metadata = MetadataRequest[0]
    topics = ['aaaa_topic', 'bbb_topic']
    app = None
    mt = metadata(topics, app)
    bytes = mt.encode()
    receivedMetadata = metadata.decode(bytes)
    assert receivedMetadata == mt


def test_metadata_response():
    produce_policy = (True, True, True, (('id-1', 2), ('id-2', 8)), ('ip1','ip2'), 1000)
    consume_policy = (True, True, True, True, True, 1000, 16, True, 100, 5000, ('black-ip-1','black-ip-2'), 100, \
                      5, 50)
    partition_groups = ((1, 1000000889, (1,2)), (2, 1000000889, (3, 4)))
    brokers = ((1000000889, 'broker-id-host', 50088, 'huitian', True, 23),
               (1000000899, 'broker-id-host_1', 50088, 'huitian1', True, 24))
    topics = [('topic_a', True, produce_policy, True, consume_policy, 1, partition_groups),
              ('topic_b', False, None, False, None, 1, partition_groups)]
    meta_response = MetadataResponse(topics, brokers)
    bytes = meta_response.encode()

    # Deserialize
    receive = MetadataResponse.decode(bytes)
    # print(receive)
    assert str(meta_response) == str(receive)


def test_struct_array():
    partition_groups = ((1, 1000000889, (1, 2)), (2, 1000000889, (3, 4)))
    pg = Array(PartitionGroup)
    bytes = pg.encode(partition_groups)
    receive = pg.decode(bytes)
    assert pg == receive


def test_command():
    topics = ['aaaa_topic', 'bbb_topic']
    app = None
    m = MetadataRequest(topics, app)

    identity = 5
    requestId = 78237
    send_time = int(time.time())
    error = 'no permission'
    h = JoyQueueHeader(magic=JoyQueueHeader.MAGIC,
               version=JoyQueueHeader.VERSION,
               identity=identity,
               requestId=requestId,
               type=m.TYPE,
               send_time=send_time,
               status=JoyQueueCode.SUCCESS.value.code,
               error=error)
    command = Command(h, m)
    bytes = command.encode()
    newCommand = Command.decode(bytes)
    assert command == newCommand


if __name__ == '__main__':
    test_array_string()










