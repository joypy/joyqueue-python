from __future__ import absolute_import
import abc
from io import BytesIO
from joyqueue.protocol.abstract import AbstractType
from joyqueue.protocol.types import Schema, Int16
from joyqueue.util import WeakMethod
from collections import Iterable


class Struct(AbstractType):
    SCHEMA = Schema()

    def __init__(self, *args, **kwargs):
        if len(args) == len(self.SCHEMA.fields):
            for i, name in enumerate(self.SCHEMA.names):
                self.__dict__[name] = args[i]
        elif len(args) > 0:
            raise ValueError('Args must be empty or mirror schema')
        else:
            for name in self.SCHEMA.names:
                self.__dict__[name] = kwargs.pop(name, None)
            if kwargs:
                raise ValueError('Keyword(s) not in schema %s: %s'
                                 % (list(self.SCHEMA.names),
                                    ', '.join(kwargs.keys())))

        # overloading encode() to support both class and instance
        # Without WeakMethod() this creates circular ref, which
        # causes instances to "leak" to garbage
        self.encode = WeakMethod(self._encode_self)

    @classmethod
    def encode(cls, item):  # pylint: disable=E0202
        bits = []
        for i, field in enumerate(cls.SCHEMA.fields):
            bits.append(field.encode(item[i]))
        return b''.join(bits)

    def _encode_self(self):
        return self.SCHEMA.encode([self.__dict__[name] for name in self.SCHEMA.names])

    @classmethod
    def decode(cls, data):
        if isinstance(data, bytes):
            data = BytesIO(data)
        return cls(*[field.decode(data) for field in cls.SCHEMA.fields])

    @classmethod
    def repr(cls, value):
        if isinstance(value, Iterable):
            return cls(*value).__repr__()
        else:
            return repr(value)

    def __repr__(self):
        key_vals = []
        for name, field in zip(self.SCHEMA.names, self.SCHEMA.fields):
            key_vals.append('%s=%s' % (name, field.repr(self.__dict__[name])))
        return self.__class__.__name__ + '(' + ', '.join(key_vals) + ')'

    def __hash__(self):
        return hash(self.encode())

    def __eq__(self, other):
        if self.SCHEMA != other.SCHEMA:
            return False
        for attr in self.SCHEMA.names:
            if self.__dict__[attr] != other.__dict__[attr]:
                return False
        return True


"""
  Call instance encode, 
  
"""


class StructArray(AbstractType):
    def __init__(self, *array_of):
        if len(array_of) > 1:
            self.array_of = Schema(*array_of)
        elif len(array_of) == 1 and (isinstance(array_of[0], AbstractType) or
                                     issubclass(array_of[0], AbstractType) or
                                     isinstance(array_of[0], Schema)):
            self.array_of = array_of[0]
        else:
            raise ValueError('Array instantiated with no array_of type')

    """
      list.append(self.array_of(*item).encode())
          diff
      it = self.array_of(*item)
      list.append(it.encode())
    """
    def encode(self, items):
        if items is None:
            return Int16.encode(-1)
        list = []
        for item in items:
            it = self.array_of(*item)
            list.append(it.encode())
        return b''.join(
            [Int16.encode(len(items))] + list
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


class Model(object):
    __metaclass__ = abc.ABCMeta
    SCHEMA = Schema()

    def __init__(self, *args, **kwargs):
        if len(args) == len(self.SCHEMA.fields):
            for i, name in enumerate(self.SCHEMA.names):
                self.__dict__[name] = args[i]
        elif len(args) > 0:
            raise ValueError('Args must be empty or mirror schema')
        else:
            for name in self.SCHEMA.names:
                self.__dict__[name] = kwargs.pop(name, None)
            if kwargs:
                raise ValueError('Keyword(s) not in schema %s: %s'
                                 % (list(self.SCHEMA.names),
                                    ', '.join(kwargs.keys())))

    def get(self, property_key):
        return self.__dict__[property_key]

    def set(self, key, value):
        self.__dict__[key] = value

    @abc.abstractproperty
    def TYPE(self):
        """
        Response type
        """
        pass

    @abc.abstractproperty
    def SCHEMA(self):
        """
        Serialize and Deserialize fields and types
        """
        pass

    def __repr__(self):
        key_vals = []
        for name in self.SCHEMA.names:
            key_vals.append('%s=%s' % (name, self.__dict__[name]))
        return self.__class__.__name__ + '(' + ', '.join(key_vals) + ')'