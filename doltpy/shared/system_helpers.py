import atexit
import logging
from typing import List

import psutil  # type: ignore

HANDLERS: List[str] = []
SQL_LOG_FILE = None


def cleanup():
    logger = logging.getLogger(__name__)
    logger.info("Before exiting cleaning up child processes")
    all_processes = psutil.Process().children(recursive=True)
    if all_processes:
        for p in all_processes:
            p.kill()
        logger.info("Cleaned up, exiting")
    else:
        logger.info("No processes to clean up, exiting")


def register_cleanup():
    atexit.register(cleanup)
