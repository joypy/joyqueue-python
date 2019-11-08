from collections import namedtuple

NAME_SERVER_CONFIG_FILEDS = ('address',
                             'region',
                             'namespace',
                             'username',
                             'password',
                             'app',
                             'token',
                             'updateMetadataInterval',
                             'tempMetadataInterval',
                             'updateMetadataThread',
                             'updateMetadataQueueSize')
DEFAULT_NAME_SERVER_CONFIG = ('localhost:8000',
                              None,
                              None,
                              None,
                              None,
                              None,
                              None,
                              1000*30,
                              1000,
                              1,
                              1024)
NameServerConfig = namedtuple('NameServerConfig', NAME_SERVER_CONFIG_FILEDS)
NameServerConfig.__new__.__defaults__ = DEFAULT_NAME_SERVER_CONFIG*len(DEFAULT_NAME_SERVER_CONFIG)








