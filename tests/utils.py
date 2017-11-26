import copy
import asyncio
import shutil
import random
import string
import logging
from multiprocessing import Process
from zatt.server.main import setup
from zatt.common import crypto

logger = logging.getLogger(__name__)
public_keys = ["-----BEGIN PUBLIC KEY-----\nMHYwEAYHKoZIzj0CAQYFK4EEACIDYgAEiz3mluwdZZS+LuGffKRpClWgPmHLHLmz\noZUY0p6/XrifQfZaE6N7/DWeScoeEmDvIXPN0aXeB4LGrSUNLZNAaVdBnYGV/nfU\nlTopcn6TPZl2dsmF1qHsKLC4W23AV+f2\n-----END PUBLIC KEY-----\n",
                "-----BEGIN PUBLIC KEY-----\nMHYwEAYHKoZIzj0CAQYFK4EEACIDYgAEuGMenqgcZyPfqXITdfr8en7M/CHuZNV0\nCwo97zAobS2I7wqA/ABraS2PM/tMAAfyi5JkdlQS/7V2WURaoumYoWK4ZCVzB3Hc\nb/3AEJo8j9o5+zrZD/dA8Ae3rjZVOAYA\n-----END PUBLIC KEY-----\n",
                "-----BEGIN PUBLIC KEY-----\nMHYwEAYHKoZIzj0CAQYFK4EEACIDYgAEigLJgNliEQBfZw9KUecRy62Q8fuk5JTs\napGfOkPRRAyKfRbIAYNGEWC90gy8FadXU1euPjfrwOx4MI9TXYa3Rmr3et3GLivb\nsfB73+N8A4HMznw0Mi68WVW9CZ2zKyFT\n-----END PUBLIC KEY-----\n"]
private_keys = ["-----BEGIN PRIVATE KEY-----\nMIG2AgEAMBAGByqGSM49AgEGBSuBBAAiBIGeMIGbAgEBBDAiRJFvfy3bsGyLjIVe\nrf74ynN55LpzhAoKAI7Lb/RGYkEtrD0QqpqkE2MD0Hg5EpyhZANiAASLPeaW7B1l\nlL4u4Z98pGkKVaA+YcscubOhlRjSnr9euJ9B9loTo3v8NZ5Jyh4SYO8hc83Rpd4H\ngsatJQ0tk0BpV0GdgZX+d9SVOilyfpM9mXZ2yYXWoewosLhbbcBX5/Y=\n-----END PRIVATE KEY-----\n",
                "-----BEGIN PRIVATE KEY-----\nMIG2AgEAMBAGByqGSM49AgEGBSuBBAAiBIGeMIGbAgEBBDAJnfWGV0Kqrtwo4PV3\nfFjtxm+6xLR3zPRytl2s1Z4nFYIxTo7gbuvE0moTs0zzk9OhZANiAAS4Yx6eqBxn\nI9+pchN1+vx6fsz8Ie5k1XQLCj3vMChtLYjvCoD8AGtpLY8z+0wAB/KLkmR2VBL/\ntXZZRFqi6ZihYrhkJXMHcdxv/cAQmjyP2jn7OtkP90DwB7euNlU4BgA=\n-----END PRIVATE KEY-----\n",
                "-----BEGIN PRIVATE KEY-----\nMIG2AgEAMBAGByqGSM49AgEGBSuBBAAiBIGeMIGbAgEBBDC+OTbc0Zi9Vlke/ZAe\n5XGVH/Du/yftThEtXmsUJ5TiKwmfrb4UaWmX99GAAlZQX1ahZANiAASKAsmA2WIR\nAF9nD0pR5xHLrZDx+6TklOxqkZ86Q9FEDIp9FsgBg0YRYL3SDLwVp1dTV64+N+vA\n7Hgwj1NdhrdGavd63cYuK9ux8Hvf43wDgczOfDQyLrxZVb0JnbMrIVM=\n-----END PRIVATE KEY-----\n"]

class Pool:
    def __init__(self, server_ids):
        if type(server_ids) is int:
            server_ids = range(server_ids)
        self._generate_configs(server_ids)
        self.servers = {}
        for config in self.configs.values():
            logger.debug('Generating server', config['test_id'])
            self.servers[config['test_id']] = (Process(target=self._run_server,
                                                       args=(config,)))

    def start(self, n):
        if type(n) is int:
            n = [n]
        for x in n:
            logger.debug('Starting server', x)
            self.servers[x].start()

    def stop(self, n):
        if type(n) is int:
            n = [n]
        for x in n:
            logger.debug('Stopping server', x)
            if self.running[x]:
                self.servers[x].terminate()
                self.servers[x] = Process(target=self._run_server,
                                          args=(self.configs[x],))

    def rm(self, n):
        if type(n) is int:
            n = [n]
        for x in n:
            shutil.rmtree(self.configs[x]['storage'])
            logger.debug('Removing files related to server', x)

    @property
    def running(self):
        return {k: v.is_alive() for (k, v) in self.servers.items()}

    @property
    def ids(self):
        return list(self.configs.keys())

    def _generate_configs(self, server_ids):
        shared = {'cluster': set(), 'storage': '{}.persist', 'debug': False,
            'public_keys': dict()}

        # for server_id in server_ids:
        #     cluster_tuple = ('127.0.0.1', 9110 + server_id)
        #     shared['cluster'].add(cluster_tuple)
        #     shared['public_keys'][(cluster_tuple)] = crypto.load_asymm_pub_key(\
        #         public_keys[server_id].encode('utf-8'))

        self.configs = {}
        for server_id in server_ids:
            config = copy.deepcopy(shared)
            for serv in server_ids:
                cluster_tuple = ('127.0.0.1', 9110 + serv)
                config['cluster'].add(cluster_tuple)
                config['public_keys'][(cluster_tuple)] = crypto.\
                    load_asymm_pub_key(public_keys[serv].encode('utf-8'))
            config['storage'] = config['storage'].format(server_id)
            config['address'] = ('127.0.0.1', 9110 + server_id)
            config['test_id'] = server_id
            config['private_key'] = crypto.load_asymm_pr_key( \
                private_keys[server_id].encode('utf-8'))
            self.configs[server_id] = config

    def _run_server(self, config):
        setup(config)
        loop = asyncio.get_event_loop()
        loop.run_forever()


def get_random_string(lenght=12, allowed_chars=None):
    random_gen = random.SystemRandom()
    if allowed_chars is None:
        allowed_chars = string.ascii_letters + string.digits
    return ''.join([random_gen.choice(allowed_chars) for _ in range(lenght)])
