
from joyqueue.model.models import Application
import time


def test_model():
    user_name = 'test'
    password = '123456'
    app = 'test'
    token = ''
    region = None
    namespace = None
    version = '2.1.0'
    ip = '127.0.0.1'
    t = int(time.time())
    sequence = 1000
    application = Application(user_name, password, app, token, region, namespace, version,
                              ip, t, sequence)
    print(application.get('version'))
    print(application)


if __name__ == '__main__':
    test_model()