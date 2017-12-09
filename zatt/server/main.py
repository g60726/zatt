import asyncio
import argparse
import logging
from zatt.server.protocols import Orchestrator, PeerProtocol, ClientProtocol
from zatt.server.config import Config
from zatt.server.logger import start_logger
from zatt.client import clientMain
from zatt.client.clientProcess import ClientProcess
from zatt.chaos.chaosProcess import ChaosProcess

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
parser.add_argument('--type', dest='type', default="server",
                    help='client, server, or chaos')

logger = logging.getLogger(__name__)

def setup(config={}):
    """Setup a node."""
    config = Config(config=config)   
    start_logger()

    loop = asyncio.get_event_loop()
    orchestrator = Orchestrator()
    coro = loop.create_datagram_endpoint(lambda: PeerProtocol(orchestrator),
                                         local_addr=config.address)
    transport, _ = loop.run_until_complete(coro)
    orchestrator.peer_transport = transport

    coro = loop.create_server(lambda: ClientProtocol(orchestrator),
                              *config.address)
    server = loop.run_until_complete(coro)

    logger.info('Serving on %s', config.address)
    return server


def run():
    cmdline = parser.parse_args().__dict__

    """Start a node."""
    if cmdline['type'] == 'server':
        server = setup()
        loop = asyncio.get_event_loop()
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass

        # Close the server
        server.close()
        loop.run_until_complete(server.wait_closed())
        loop.close()
    elif cmdline['type'] == 'client':
        client_pool = ClientProcess(2, cmdline['path_conf'])
        client_pool.start(client_pool.ids)
        while True:
            pass
    elif cmdline['type'] == 'chaos':
        client_pool = ChaosProcess(cmdline['id'], cmdline['path_conf'])
        client_pool.start(client_pool.ids)
        while True:
            pass

if __name__ == '__main__':
    run()
