
# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.


from twisted.internet import reactor, protocol
from joyqueue.network.receiver import LengthBasedProtocol


class Echo(LengthBasedProtocol):
    """This is just about the simplest possible protocol"""
    def __init__(self, length_offset, max_length):
        super().__init__(length_offset, max_length)

    def messageReceived(self, data):
        "As soon as any data is received, write it back."

        print(data)
        self.send(data)


class JoyQueueServerFactory(protocol.ServerFactory):

    def __init__(self, length_offset, max_length):
        self._length_offset = length_offset
        self._max_length = max_length

    def buildProtocol(self, addr):

        return Echo(self._length_offset, self._max_length)


def main():
    """This runs the protocol on port 8000"""
    factory = JoyQueueServerFactory(4,4096)
    factory.protocol = Echo
    reactor.listenTCP(8000, factory)
    reactor.run()


# this only runs if the module was *not* imported
if __name__ == '__main__':
    main()
