
from collections import defaultdict


class ClusterMetadata(object):

    def __init__(self, cluster_config):
        self._cluster_config = cluster_config
        self._metadata = defaultdict(None)

    def metadata(self, topic):
        pass

    def updateMetadata(self, topic):
        pass