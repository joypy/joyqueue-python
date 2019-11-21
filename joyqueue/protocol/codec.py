
from collections import defaultdict
from joyqueue.protocol.interface import Struct
from joyqueue.protocol.metadata import (MetadataRequest, MetadataResponse,
                                        ConnectionRequest, ConnectionResponse,
                                        ProducerRequest, ProducerResponse,
                                        ConsumerRequest,ConsumerResponse)
from joyqueue.protocol.message import (ProduceMessageRequest, ProduceMessageResponse,
                                       FetchMessageRequest, FetchMessageResponse,
                                       ConsumeAck, ConsumeAckResponse)


class ResponseDecodeFactory:

    def __init__(self):
        self._map = defaultdict(Struct)
        self._init()

    def _init(self):
        self.add(MetadataRequest)
        self.add(MetadataResponse)
        self.add(ConnectionRequest)
        self.add(ConnectionResponse)
        self.add(ProducerRequest)
        self.add(ProducerResponse)
        self.add(ProduceMessageRequest)
        self.add(ProduceMessageResponse)
        self.add(ConsumerRequest)
        self.add(ConsumerResponse)
        self.add(FetchMessageRequest)
        self.add(FetchMessageResponse)
        self.add(ConsumeAck)
        self.add(ConsumeAckResponse)

    def get(self, type):
        return self._map.get(type)

    def add(self, request):
        t = request.TYPE
        self._map[t] = request