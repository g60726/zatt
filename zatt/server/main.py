import asyncio
import logging
from .protocols import Orchestrator, PeerProtocol, ClientProtocol
from .config import Config
from .logger import start_logger


def setup(config={}):
    config = Config(config=config)
    start_logger()
    logger = logging.getLogger(__name__)

    loop = asyncio.get_event_loop()
    orchestrator = Orchestrator()
    coro = loop.create_datagram_endpoint(lambda: PeerProtocol(orchestrator),
                                         local_addr=config.cluster[config.id])
    transport, _ = loop.run_until_complete(coro)
    orchestrator.peer_transport = transport

    coro = loop.create_server(lambda: ClientProtocol(orchestrator),
                              *config.cluster[config.id])
    server = loop.run_until_complete(coro)

    logger.info('Serving on {}'.format(config.cluster[config.id]))
    return server


def run():
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

if __name__ == '__main__':
    run()
