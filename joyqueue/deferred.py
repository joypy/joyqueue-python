from  twisted.internet.defer import  inlineCallbacks,returnValue
from  twisted.python.failure import Failure
from twisted.internet import reactor, defer
import time


def loadRemoteData(callback):
    time.sleep(1)
    callback('hello \n')


def loadRemoteGreeting(callback):
    time.sleep(5)
    callback('world \n')


@defer.inlineCallbacks
def retrieveRemoteData():
    d1 = defer.Deferred()
    reactor.callInThread(loadRemoteData, d1.callback)

    r1 = yield d1
    print('first {}'.format(r1))
    d2 = defer.Deferred()
    reactor.callInThread(loadRemoteGreeting, d2.callback)

    r2 = yield d2
    print('second {}'.format(r2))
    returnValue(r1 + r2)


def printMsg(msg):
    print('received {}'.format(msg))


if __name__ == '__main__':
    d = retrieveRemoteData()
    d.addCallback(printMsg)
    print('to run \n')
    reactor.run()


