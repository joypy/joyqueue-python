from joyqueue.model.configs import NameServerConfig
from joyqueue.protocol.code import JoyQueueCode


def test_name_server_config():
    nameServerConfig = NameServerConfig()
    print(nameServerConfig)


def test_enum():

    print(JoyQueueCode.SUCCESS.value.code)
    print(JoyQueueCode.SUCCESS.value.message)


if __name__ == '__main__':
    test_enum()