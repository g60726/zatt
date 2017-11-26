import asyncio
import logging
from multiprocessing import Process
from zatt.client.clientMain import setup
from zatt.server.config import update_config_json

logger = logging.getLogger(__name__)

class ClientProcess():
    def __init__(self, client_ids, client_config):
        if type(client_ids) is int:
            client_ids = range(client_ids)
        self._generate_configs(client_ids, client_config)
        self.clients = {}
        for config in self.configs.values():
            logger.debug('Generating client', config['test_id'])
            self.clients[config['test_id']] = (Process(target=self._run_client,
                                                       args=(config,)))

    def start(self, n):
        if type(n) is int:
            n = [n]
        for x in n:
            logger.debug('Starting client', x)
            self.clients[x].start()

    def stop(self, n):
        if type(n) is int:
            n = [n]
        for x in n:
            logger.debug('Stopping client', x)
            if self.running[x]:
                self.clients[x].terminate()
                self.clients[x] = Process(target=self._run_client,
                                          args=(self.configs[x],))

    @property
    def running(self):
        return {k: v.is_alive() for (k, v) in self.clients.items()}

    @property
    def ids(self):
        return list(self.configs.keys())

    def _generate_configs(self, client_ids, client_config):
        self.configs = {}
        default = {'debug': False, 'address': ['127.0.0.1', 5254],
                   'cluster': set(), 'storage': 'zatt.persist', 
                   'private_key': 0, 'public_keys': dict()}

        for client_id in client_ids:
            config = default.copy()
            config['test_id'] = client_id
            self.configs[client_id] = \
                update_config_json(client_config, str(client_id), config, True)

    def _run_client(self, config):
        setup(config)
        loop = asyncio.get_event_loop()
        loop.run_forever()