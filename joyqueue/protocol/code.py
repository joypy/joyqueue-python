from enum import Enum
from collections import namedtuple

Code = namedtuple('Code', ['code', 'message'])


class JoyQueueCode(Enum):
    SUCCESS = Code(0, '成功')