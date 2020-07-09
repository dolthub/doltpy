import logging
import atexit
import psutil


LOG_LEVEL = logging.WARN
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)
HANDLERS = []
SQL_LOG_FILE = None

logging.basicConfig(level=LOG_LEVEL,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M:%S')


def get_logger(name: str):
    """
    Returns a logger with the given name
    :param name:
    :return:
    """
    logger = logging.getLogger(name)

    for handler in HANDLERS:
        logger.addHandler(handler)

    return logger


def cleanup():
    logger = get_logger(__name__)
    logger.info('Before exiting cleaning up child processes')
    all_processes = psutil.Process().children(recursive=True)
    if all_processes:
        for p in all_processes:
            p.kill()
        logger.info('Cleaned up, exiting')
    else:
        logger.info('No processes to clean up, exiting')


def register_cleanup():
    atexit.register(cleanup)

