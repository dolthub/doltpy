import logging


def config_cli_logger():
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        datefmt='%Y%m%d %H:%M:%S',
                        level=logging.INFO)