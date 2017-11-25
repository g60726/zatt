import logging
import asyncio
from datetime import datetime
from .config import config
from logging.config import dictConfig

PROD_FORMAT = "%(levelname)s at %(asctime)s in %(funcName)s in %(filename) at line %(lineno)d: %(message)s"
DEV_FORMAT = "%(levelname)s at %(asctime)s in %(funcName)s in %(filename) at line %(lineno)d: %(message)s"

def tick():
    """Unobtrusive periodic timestamp for debug log."""
    logger = logging.getLogger(__name__)
    logger.debug('Tick: %s', datetime.now().isoformat('T'))
    loop = asyncio.get_event_loop()
    loop.call_later(1, tick)


def start_logger():
    """Configure logging verbosity according to the --debug CLI option."""
    logging_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'prod': {'format': PROD_FORMAT},
            'develop': {'format': DEV_FORMAT}
        },
        'handlers': {
            'console': {'class': 'logging.StreamHandler',
                        'formatter': 'prod',
                        'level': logging.DEBUG}
            },
        'loggers': {
            '': {'handlers': ['console'],
                 'level': 'INFO',
                 'propagate': True}
            }
        }
    if config.debug:
        logging_config['handlers']['console']['formatter'] = 'develop'
        logging_config['loggers']['']['level'] = logging.DEBUG
        loop = asyncio.get_event_loop()
        loop.call_later(1, tick)

    dictConfig(logging_config)
