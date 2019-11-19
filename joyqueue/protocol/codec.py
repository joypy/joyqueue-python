
from collections import defaultdict
from joyqueue.protocol.interface import Struct
from joyqueue.protocol.metadata import (MetadataRequest, MetadataResponse,
                                        ConnectionRequest,ConnectionResponse)


class ResponseDecodeFactory:

    def __init__(self):
        self._map = defaultdict(Struct)
        self._init()

    def _init(self):
        self.add(MetadataRequest)
        self.add(MetadataResponse)
        self.add(ConnectionRequest)
        self.add(ConnectionResponse)

    def get(self, type):
        return self._map.get(type)

    def add(self, request):
        t = request.TYPE
        self._map[t] = request