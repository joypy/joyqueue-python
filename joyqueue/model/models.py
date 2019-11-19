from collections import namedtuple
from joyqueue.protocol.types import Schema, String, Int8, Int64
from joyqueue.protocol.struct import Model

MetadataRequest = namedtuple('MetadataRequest',
                             ['topics', 'app'])

Command = namedtuple('Command',
                     ['header', 'body'])
UTF8String = String('utf-8')


class Application(Model):
    SCHEMA = Schema(('username', UTF8String),
                    ('password', UTF8String),
                    ('app', UTF8String),
                    ('token', UTF8String),
                    ('region', UTF8String),
                    ('namespace', UTF8String),
                    ('language', Int8),
                    ('version', UTF8String),
                    ('ip', UTF8String),
                    ('time', Int64),
                    ('sequence', Int64))
    TYPE = 'Application'