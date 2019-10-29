# pytest -s
from joyqueue.protocol.message import Message
from joyqueue.protocol.header import Header
import time
import importlib
import sys
importlib.reload(sys)


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
    flag = 2
    m = Message(msg, bussiness_id, app, length, partition, index,
                term, system_code, priority, int(t), store_time, body_crc, flag)
    byteMsg = m.encode()
    ## class method
    receiveMsg = Message.decode(byteMsg)

    print(receiveMsg)
    assert receiveMsg.body == msg
    assert receiveMsg.bussiness_id == bussiness_id
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
    length = 10
    magic = ~(0xCAFEBEBE ^ 0xFFFFFFFF)
    version = 2
    identity = 5
    requestId = 78237
    type = 5
    send_time = int(time.time())
    status = -1
    error = 'no permission'
    h = Header(length, magic, version, identity, requestId, type, send_time,
               status, error)
    hbytes = h.encode()
    receivedHeader = Header.decode(hbytes)

    print(receivedHeader)
    assert receivedHeader.length == length
    assert receivedHeader.magic == magic
    assert receivedHeader.version == version
    assert receivedHeader.identity == identity
    assert receivedHeader.requestId == requestId
    assert receivedHeader.type == type
    assert receivedHeader.send_time == send_time
    assert receivedHeader.status == status
    assert receivedHeader.error == error









