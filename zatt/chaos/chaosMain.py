import asyncio
import logging
from zatt.chaos.chaosStates import ChaosMonkey
from zatt.server.protocols import Orchestrator, ClientProtocol
from zatt.server.config import Config
from zatt.server.logger import start_logger

logger = logging.getLogger(__name__)

def setup(config={}):
    """Setup a node."""
    config = Config(config=config)
    start_logger()

    loop = asyncio.get_event_loop()
    orchestrator = Orchestrator(ChaosMonkey)
    coro = loop.create_server(lambda: ClientProtocol(orchestrator),
                              *config.address)
    server = loop.run_until_complete(coro)

    logger.info('Serving Chaos on %s', config.address)
    return server


def run():
    """Start a node."""
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
