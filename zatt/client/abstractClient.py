import socket
import random
import msgpack
import logging

logger = logging.getLogger(__name__)

class AbstractClient:
    """Abstract client. Contains primitives for implementing functioning
    clients."""

    def _request(self, message):
        self.server_address = tuple(random.choice(self.data['cluster']))
        while True:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect(self.server_address)
                sock.send(msgpack.packb(message, use_bin_type=True))

                buff = bytes()
                while True:
                    block = sock.recv(128)
                    if not block:
                        break
                    buff += block
                resp = msgpack.unpackb(buff, encoding='utf-8')
            except socket.error:
                # Try a different server
                self.server_address = tuple(random.choice(self.data['cluster']))
                continue
            finally:
                sock.close()

            if 'type' in resp and resp['type'] == 'redirect':
                if resp['leader'] is not None:
                    self.server_address = tuple(resp['leader'])
            else:
                break
        return resp

    def _get_state(self):
        """Retrive remote state machine."""
        self.server_address = tuple(random.choice(self.data['cluster']))
        resp = self._request({'type': 'get'})
        return resp

    def _append_log(self, payload):
        """Append to remote log."""
        return self._request({'type': 'append', 'data': payload})

    @property
    def diagnostic(self):
        return self._request({'type': 'diagnostic'})

    def config_cluster(self, action, address, port):
        return self._request({'type': 'config', 'action': action,
                              'address': address, 'port': port})
