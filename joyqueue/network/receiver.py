from twisted.internet import protocol
from twisted.protocols.basic import _PauseableMixin
from joyqueue.protocol.types import Int32
import io
"""
   Length based protocol,message format:
   length(bytes)+4|bytes
   
"""


class LengthBasedProtocol(protocol.Protocol, _PauseableMixin):

    _unprocessed = b""
    _compatibilityOffset = 0
    _PREFIX_LENGTH = 4

    def __init__(self, length_offset, max_length):
        self._length_offset = length_offset
        self._max_length = max_length

    def send(self, msg):
        if len(msg) > self._max_length:
            raise MessageTooLongError("Try to send {} bytes whereas maximum is {}".format(len(msg),
                                                                                          self.__max_length))
        length = len(msg)+self._length_offset
        self.transport.write(Int32.encode(length)+msg)

    def dataReceived(self, data):
        """
         Convert int prefixed strings into calls to stringReceived.
        """
        # Try to minimize string copying (via slices) by keeping one buffer
        # containing all the data we have so far and a separate offset into that
        # buffer.
        alldata = self._unprocessed + data
        offset = 0
        self._unprocessed = alldata

        while len(alldata) >= (offset + self._PREFIX_LENGTH) and not self.paused:
            messageStart = offset + self._PREFIX_LENGTH
            length = Int32.decode(io.BytesIO(alldata[offset:messageStart]))
            ## length offset
            length -= self._length_offset
            if length > self._max_length:
                self._unprocessed = alldata
                self._compatibilityOffset = offset
                self.lengthLimitExceeded(length)
                return
            messageEnd = messageStart + length
            if len(alldata) < messageEnd:
                break

            # Here we have to slice the working buffer so we can send just the
            # netstring into the stringReceived callback.
            packet = alldata[messageStart:messageEnd]
            offset = messageEnd
            self._compatibilityOffset = offset
            self.messageReceived(packet)

        # Slice off all the data that has been processed, avoiding holding onto
        # memory to store it, and update the compatibility attributes to reflect
        # that change.
        self._unprocessed = alldata[offset:]
        self._compatibilityOffset = 0

    def messageReceived(self, msg):
        """
        Override this for notification when each complete string is received.

        @param message: The complete string which was received with all
            framing (length prefix, etc) removed.
        @type bytes: C{bytes}
        """
        raise NotImplementedError

    def lengthLimitExceeded(self, length):
        """
        Callback invoked when a length prefix greater than C{MAX_LENGTH} is
        received.  The default implementation disconnects the transport.
        Override this.

        @param length: The length prefix which was received.
        @type length: C{int}
        """
        self.transport.loseConnection()


class MessageTooLongError(AssertionError):
   """
   Raised when trying to send a message too long for a length prefixed
   protocol.
   """