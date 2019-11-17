from collections import namedtuple


MetadataRequest = namedtuple('MetadataRequest',
                             ['topics', 'app'])

Command = namedtuple('Command',
                     ['header', 'body'])