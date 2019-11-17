from __future__ import  absolute_import

import struct
from struct import error
from joyqueue.protocol.abstract import  AbstractType
import io

def _pack(f,value):
    try:
       return f(value)
    except error as e:
       raise ValueError("Error encounter when attempting to convert value:"
                        "{!r} to struct format:'{}',hit error:{}"
                        .format(value, f, e))


def _unpack(f, data):
    try:
        (value, ) = f(data)
        return value
    except error as e:
        raise ValueError("Error encounter when attempting to convert value:"
                    "{!r} to struct format:'{}',hit error:{}"
                    .format(data, f, e))


class Int8(AbstractType):
    _pack = struct.Struct('>b').pack
    _unpack = struct.Struct('>b').unpack

    @classmethod
    def encode(cls, value):
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data):
        bytes=data.read(1)
        return _unpack(cls._unpack, bytes)


class Boolean(AbstractType):
    _pack = struct.Struct('>?').pack
    _unpack = struct.Struct('>?').unpack

    @classmethod
    def encode(cls, value):
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data):
        return _unpack(cls._unpack, data.read(1))


class Int16(AbstractType):
    _pack = struct.Struct('>h').pack
    _unpack = struct.Struct('>h').unpack

    @classmethod
    def encode(cls, value):
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data):
        return _unpack(cls._unpack, data.read(2))


class Int32(AbstractType):
    _pack = struct.Struct('>i').pack
    _unpack = struct.Struct('>i').unpack

    @classmethod
    def encode(cls, value):
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data):
        bytes= data.read(4)
        return _unpack(cls._unpack, bytes)


class Int64(AbstractType):
    _pack = struct.Struct('>q').pack
    _unpack = struct.Struct('>q').unpack

    @classmethod
    def encode(cls, value):
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data):
        return _unpack(cls._unpack, data.read(8))


class Double64(AbstractType):
    _pack = struct.Struct('>d').pack
    _unpack = struct.Struct('>d').unpack

    @classmethod
    def encode(cls, value):
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data):
        return _unpack(cls._unpack, data.read(8))


class String(AbstractType):
    def __init__(self, encoding='utf-8'):
        self.encoding = encoding

    def encode(self, value):
        if value is None:
            return Int16.encode(-1)
        value = str(value).encode(self.encoding)
        return Int16.encode(len(value)) + value

    def decode(self, data):
        length = Int16.decode(data)
        if length < 0:
            return None
        value = data.read(length)
        if len(value) != length:
            raise ValueError('Buffer underflow decoding string')
        return value.decode(self.encoding)


class Property(AbstractType):
    KV_SPLIT = b'='
    NEWLINE = b'\n'
    #
    # def __init__(self):
    #     self.__dict = defaultdict(None)
    #     self.__codec = String('utf-8')
    #     self.encode = WeakMethod(self._encode_self)
    #     self.__kv_split = Property.KV_SPLIT
    #     self.__newline = Property.NEWLINE
    #     self.encode = WeakMethod(self._encode_self)
    #
    # def put(self, key, value):
    #     if key is None or value is None:
    #         raise ValueError("key or value can't be none ")
    #     self.__dict[str(key)] = str(value)
    #
    # def get(self, key):
    #     return self.__dict.get(str(key))


    @classmethod
    def encode(cls, dict):
        length = len(dict)
        CODEC = String('utf-8')
        bytelist = []
        if length > 0:
            for k, v in dict.items():
                bytes = b''
                bytes += CODEC.encode(k)
                bytes += Property.KV_SPLIT
                bytes += CODEC.encode(v)
                bytes += Property.NEWLINE
                bytelist.append(bytes)
        bytes = b''.join(bytelist)
        return Int16.encode(len(bytes))+bytes

    @classmethod
    def decode(cls, data):
        if isinstance(data, bytes):
            data = io.BytesIO(data)
        pro = cls()
        len = Int16.decode(data)
        while len > 0:
            k, v, length = cls.read_key_value(data)
            pro.put(k, v)
            len -= length
        return pro

    @classmethod
    def read_key_value(cls, data):
        length = 6
        CODEC = String('utf-8')
        key = CODEC.decode(data)
        kv_split = data.read(1)
        if kv_split != Property.KV_SPLIT:
            raise IOError('wrong format property')
        value = CODEC.decode(data)
        newline = data.read(1)
        if newline != Property.NEWLINE:
            raise IOError('wrong format property')
        length += len(key)
        length += len(value)
        return key, value, length

    def __hash__(self):
        return super().__hash__()


class Bytes(AbstractType):
    @classmethod
    def encode(cls, value):
        try:
          if value is None:
               return Int32.encode(-1)
          else:
               body_len = len(value)
               bytes = Int32.encode(body_len)
               new= bytes+value
               return new
        except Exception as e:
          print(e)

    @classmethod
    def decode(cls, data):
        length = Int32.decode(data)
        if length < 0:
            return None
        value = data.read(length)
        if len(value) != length:
            raise ValueError('Buffer underrun decoding Bytes')
        return value

    @classmethod
    def repr(cls, value):
        return repr(value[:100] + b'...' if value is not None and len(value) > 100 else value)


class Schema(AbstractType):
    def __init__(self, *fields):
        if fields:
            self.names, self.fields = zip(*fields)
        else:
            self.names, self.fields = (), ()

    def encode(self, item):
        if len(item) != len(self.fields):
            raise ValueError('Item field count does not match Schema')

        list=[]
        for i, field in enumerate(self.fields):
          it = item[i]
          fbytes = field.encode(it)
          list.append(
              fbytes
          )
        return b''.join(list)

    def decode(self, data):
        return tuple([field.decode(data) for field in self.fields])

    def __len__(self):
        return len(self.fields)

    def repr(self, value):
        key_vals = []
        try:
            for i in range(len(self)):
                try:
                    field_val = getattr(value, self.names[i])
                except AttributeError:
                    field_val = value[i]
                key_vals.append('%s=%s' % (self.names[i], self.fields[i].repr(field_val)))
            return '(' + ', '.join(key_vals) + ')'
        except Exception:
            return repr(value)


class Array(AbstractType):
    def __init__(self, *array_of):
        if len(array_of) > 1:
            self.array_of = Schema(*array_of)
        elif len(array_of) == 1 and (isinstance(array_of[0], AbstractType) or
                                     issubclass(array_of[0], AbstractType) or
                                     isinstance(array_of[0], Schema)):
            self.array_of = array_of[0]
        else:
            raise ValueError('Array instantiated with no array_of type')

    def encode(self, items):
        if items is None:
            return Int16.encode(-1)
        return b''.join(
            [Int16.encode(len(items))] +
            [self.array_of.encode(item) for item in items]
        )

    def decode(self, data):
        length = Int16.decode(data)
        if length == -1:
            return None
        return [self.array_of.decode(data) for _ in range(length)]

    def repr(self, list_of_items):
        if list_of_items is None:
            return 'NULL'
        return '[' + ', '.join([self.array_of.repr(item) for item in list_of_items]) + ']'



