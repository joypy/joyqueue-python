
from joyqueue.protocol.struct import Struct
from joyqueue.protocol.types import String, Int16
from collections import defaultdict
from joyqueue.util import WeakMethod
# property k=v\n
#          k=v


class Property(Struct):
    KV_SPLIT = '='
    NEWLINE = '\n'

    def __init__(self):
        self.__dict = defaultdict(None)
        self.__codec = String('utf-8')
        self.encode = WeakMethod(self._encode_self)
        self.__kv_split = Property.KV_SPLIT
        self.__newline = Property.NEWLINE
        self.encode = WeakMethod(self._encode_self)
        self.str_cache = None

    def put(self, key, value):
        if key is None or value is None:
            raise ValueError("key or value can't be none ")
        self.__dict[str(key)] = str(value)

    def get(self, key):
        return self.__dict.get(str(key))

    def _encode_self(self):
        length = len(self.__dict)
        properties = ''
        if length > 0:
            i = 0
            for k, v in self.__dict.items():
                if i > 0:
                    properties += self.__newline
                properties += k+self.__kv_split+v
                i += 1
        return properties

    @classmethod
    def decode(cls, data):
        length = len(data)
        pro = cls()
        if length > 0:
            properties = data.split(Property.NEWLINE)
            for p in properties:
                it = p.split(Property.KV_SPLIT)
                if len(it) == 2:
                    pro.put(it[0], it[1])
                else:
                    raise IOError('wrong property string')
        return pro

    def __repr__(self):
        if self.str_cache is not None:
            return self.str_cache
        return self._encode_self()

    def __hash__(self):
        return super().__hash__()