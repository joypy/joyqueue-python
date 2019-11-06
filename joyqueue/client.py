

# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.


"""
An example client. Run simpleserv.py first before running this.
"""
from __future__ import print_function

from twisted.internet import defer
from twisted.internet import reactor, protocol
import  time


# a client protocol


class EchoClient(protocol.Protocol):
    """Once connected, send a message, then print the result."""
    def __init__(self):
        self.replyQueue = defer.DeferredQueue()

    def connectionMade(self):
        print('connected')
        # self.transport.write(b"hello, world!")
        self.factory.deferred.callback(self)

    def dataReceived(self, data):
        "As soon as any data is received, write it back."
        # print("Server said:", str(data))
        # self.sendMessage("client msg")
        # self.factory.deferred.callback(data)
        self.replyQueue.put(data)

    def sendMessage(self, msg):
        self.transport.write('MESSAGE {} \n'.format(msg).encode('utf-8'))
        return self.replyQueue.get()

    def connectionLost(self, reason):
        print("connection lost {}".format(reason))


class EchoFactory(protocol.ClientFactory):

    def __init__(self):
        self.protocol = EchoClient
        self.deferred = defer.Deferred()

    def clientConnectionFailed(self, connector, reason):
        print("Connection failed - goodbye!")
        reactor.stop()

    def buildProtocol(self, addr):
        print('Connected')
        p = self.protocol()
        p.factory = self
        return p

    def clientConnectionLost(self, connector, reason):
        print("Connection lost, reconnect! {}".format(reason))
        connector.connect()


def makeConnection():
    f = EchoFactory()
    reactor.connectTCP("localhost", 8000, f)
    return f.deferred


def asyncCallback(response):
    print('ascync callback {}'.format(response))


# this connects the protocol to a server running on port 8000
@defer.inlineCallbacks
def main():
    # wait connection ready
    con = yield makeConnection()
    response = yield con.sendMessage('first msg')
    print('sync response:{}'.format(response))
    con.sendMessage('second msg').addCallback(asyncCallback)
    # print('reactor stop!')
    # time.sleep(5)


# this only runs if the module was *not* imported
if __name__ == '__main__':
    main()
    reactor.run()
