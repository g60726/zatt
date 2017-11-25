import collections
import logging
from zatt.client.abstractClient import AbstractClient
from zatt.client.refresh_policies import RefreshPolicyAlways

logger = logging.getLogger(__name__)

class DistributedDict(collections.UserDict, AbstractClient):
    """Client for zatt instances with dictionary based state machines."""
    def __init__(self, addr, port, append_retry_attempts=3,
                 refresh_policy=RefreshPolicyAlways()):
        super().__init__()
        self.data['cluster'] = [(addr, port)]
        self.append_retry_attempts = append_retry_attempts
        self.refresh_policy = refresh_policy
        self.refresh(force=True)

    def __getitem__(self, key):
        logger.info("get_item!")
        self.refresh()
        return self.data[key]

    def __setitem__(self, key, value):
        logger.info("set_item")
        self._append_log({'action': 'change', 'key': key, 'value': value})

    def __delitem__(self, key):
        self.refresh(force=True)
        del self.data[self.__keytransform__(key)]
        self._append_log({'action': 'delete', 'key': key})

    def __keytransform__(self, key):
        return key

    def __repr__(self):
        self.refresh()
        return super().__repr__()

    def refresh(self, force=False):
        if force or self.refresh_policy.can_update():
            self.data = self._get_state()

    def _append_log(self, payload):
        logger.info("append_log")
        for attempt in range(self.append_retry_attempts):
            response = super()._append_log(payload)
            if response['success']:
                logger.info("success")
                break
        # TODO: logging
        return response

if __name__ == '__main__':
    import sys
    if len(sys.argv) == 3:
        d = DistributedDict('127.0.0.1', 9111)
        d[sys.argv[1]] = sys.argv[2]
