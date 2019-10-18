
from joyqueue.protocol.types import Int8, Int32, String
from joyqueue.protocol.message import Message
import time

import importlib
import sys
import io

importlib.reload(sys)

if __name__ == '__main__':
    a = 12
    abyte = Int8.encode(a)
    print(abyte)
    print(Int8.decode(io.BytesIO(abyte)))
    now = time.time()
    msg=b'abncdfasdf'
    msg_len = len(b'abncdfasdf')
    strBytes = String().encode('dafsdf')
    nes = Int32.encode(msg_len)
    code = nes+msg
    list= []
    list.append(code)
    m = Message(b'abncdfasdf', 'ddsdfbid', 'test_app', 10, 0, 10089,
                100, 90, 8, int(now), 1000, 888, 2)
    byteMsg = m.encode()
    receiveMsg = Message.decode(byteMsg)

    print([i for i in range(1, 10)])
    print(m)
    print(receiveMsg)
    print(receiveMsg.SCHEMA.names)
    print(receiveMsg.SCHEMA.fields)
    print('get attrs:')
    print([getattr(receiveMsg, name) for name in receiveMsg.SCHEMA.names])


