from datetime import datetime, timedelta


class RefreshPolicyAlways:
    """Policy to establish when a DistributedDict should update its content.
    This policy requires the client to update at every read.
    This is the default policy.
    """
    def can_update(self):
        return True


class RefreshPolicyLock:
    """Policy to establish when a DistributedDict should update its content.
    This policy requires the client to update whenever the lock is set.
    """
    def __init__(self, status=True):
        self.lock = status

    def can_update(self):
        return self.lock


class RefreshPolicyCount:
    """Policy to establish when a DistributedDict should update its content.
    This policy requires the client to update every `maximum` iterations.
    """
    def __init__(self, maximum=10):
        self.counter = 0
        self.maximum = maximum

    def can_update(self):
        self.counter += 1
        if self.counter == self.maximum:
            self.counter = 0
            return True
        else:
            return False


class RefreshPolicyTime:
    """Policy to establish when a DistributedDict should update its content.
    This policy requires the client to update every `delta` time interval.
    """
    def __init__(self, delta=timedelta(minutes=1)):
        self.delta = delta()
        self.last_refresh = None

    def can_update(self):
        if self.last_refresh is None or\
                datetime.now() - self.last_refresh > self.delta:
            self.last_refresh = datetime.now()
            return True
        else:
            return False
