

class TopicNotExist(Exception):
    """
    Topic not exist
    """


class NoAvailableBroker(Exception):
    """
    Broekr not available
    """


class NoAvailablePartition(Exception):
    """
    Partition not available
    """