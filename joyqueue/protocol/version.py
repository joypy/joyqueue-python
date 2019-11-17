from collections import defaultdict

"""
  Current request and response command version
  
"""


class Version:

    def __init__(self):
        self._header_version = 2
        self._response_version = 2
        self._request_version = 2

    def request(self):
        return self._request_version

    def response(self):
        return self._response_version

    def header(self):
        return self._header_version
