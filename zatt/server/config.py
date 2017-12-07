import argparse
import os
import json
from zatt.common import crypto

parser = argparse.ArgumentParser(description=('Zatt. An implementation of '
                                              'the Raft algorithm for '
                                              'distributed consensus'))
parser.add_argument('-c', '--config', dest='path_conf',
                    help='Config file path. Default: zatt.persist/config')
parser.add_argument('-s', '--storage', help=('Path for the persistent state'
                    ' directory. Default: zatt.persist'))
parser.add_argument('-a', '--address', help=('This node address. Default: '
                    '127.0.0.1'))
parser.add_argument('-p', '--port', help='This node port. Default: 5254',
                    type=int)
parser.add_argument('-i', '--id', help='Node id. Alternate way to\
                    specify server configs.', type=str)
parser.add_argument('--remote-address', action='append', default=[],
                    help='Remote node address')
parser.add_argument('--remote-port', action='append', default=[], type=int,
                    help='Remote node port')
parser.add_argument('--debug', action='store_true', help='Enable debug mode')


def update_config_json(file, node_id, config, client=False):
    if os.path.isfile(file):
        with open(file, 'r') as f:
            config.update(json.loads(f.read()))

        cluster = config['cluster']
        clients = config['clients']
        config['public_keys'] = {(cluster[key][0], cluster[key][1]): \
                                    crypto.load_asymm_pub_key( \
                                        cluster[key][2].encode("utf-8"))
                                            for key in config['cluster']}
        config['client_keys'] = {(clients[key][0], clients[key][1]): \
                                    crypto.load_asymm_pub_key( \
                                        clients[key][2].encode("utf-8"))
                                            for key in config['clients']}
        config['cluster'] = {(cluster[key][0], cluster[key][1]) \
                                    for key in config['cluster']}
        config['clients'] = {(clients[key][0], clients[key][1]) \
                                    for key in config['clients']}

        if node_id is not None:
            with open(file, 'r') as f:
                conf_file = json.loads(f.read())
                config['address'][0] = conf_file['cluster'][node_id][0]
                config['address'][1] = conf_file['cluster'][node_id][1]
                config['private_key'] = crypto.load_asymm_pr_key( \
                        config['private_key'][node_id].encode("utf-8"))
                config['storage'] = config['storage'][node_id]
                config['address'] = tuple(config['address'])
                config['id'] = int(node_id)

                if not client:
                    del config['client_private_key']
                else:
                    config['client_private_key'] = crypto.load_asymm_pr_key( \
                        config['client_private_key'][node_id].encode("utf-8"))
                    config['client_address'] = \
                        (conf_file['clients'][node_id][0], \
                        conf_file['clients'][node_id][1])

    return config


class Config:
    """Collect and merge CLI and file based config.
    This class is a singleton based on the Borg pattern."""
    __shared_state = {}

    def __new__(cls, *p, **k):
        if '_instance' not in cls.__dict__:
            cls._instance = object.__new__(cls)
        return cls._instance

    def __init__(self, config={}):
        if config is None:
            self.__dict__ = {}
        elif config:
            self.__dict__ = config
        else:
            self.__dict__ = self._get()

    def _get(self):
        default = {'debug': False, 'address': ['127.0.0.1', 5254],
                   'cluster': set(), 'storage': 'zatt.persist', 
                   'private_key': 0, 'public_keys': dict()}

        environ = {k[5:].lower(): v for (k, v) in os.environ.items()
                   if k.startswith('ZATT_')}
        if {'address', 'port'}.issubset(environ):
            environ['address'] = (environ['address'], int(environ['port']))
            del environ['port']
        if {'remote_address', 'remote_port'}.issubset(environ):
            environ['cluster'] = {(a, int(p)) for (a, p) in
                                  zip(environ['remote_address'].split(','),
                                      environ['remote_port'].split(','))}
            del environ['remote_address']
            del environ['remote_port']

        config = default.copy()
        config.update(environ)

        cmdline = parser.parse_args().__dict__

        if 'path_conf' not in config:
            config['path_conf'] = os.path.join(config['storage'], 'conf')
        path_conf = cmdline['path_conf'] if cmdline['path_conf']\
            else config['path_conf']

        node_id = str(cmdline['id']) if 'id' in cmdline else None
        config = update_config_json(path_conf, node_id, config)

        if cmdline['address']:
            config['address'][0] = cmdline['address']
        if cmdline['port']:
            config['address'][1] = cmdline['port']
        del cmdline['port']
        del cmdline['address']

        if cmdline['remote_address'] and cmdline['remote_port']:
            config['cluster'].add(*zip(cmdline['remote_address'],
                                       cmdline['remote_port']))
        del cmdline['remote_address']
        del cmdline['remote_port']

        for k, v in cmdline.items():
            if v is not None:
                config[k] = v

        config['address'] = tuple(config['address'])
        config['cluster'].add(config['address'])
        if type(config['debug']) is str:
            config['debug'] = True if config['debug'] == 'true' else False
        return config

config = Config(None)
