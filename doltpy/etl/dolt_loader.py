import argparse
from doltpy.core import Dolt
from doltpy.etl.loaders import resolve_function, DoltLoaderBuilder
import logging
from doltpy.etl.cli_logging_config_helper import config_cli_logger

logger = logging.getLogger(__name__)


def loader(loader_builder: DoltLoaderBuilder, dolt_dir: str, dry_run: bool):
    """
    This function takes a `DoltLoaderBuilder`, repo and remote settings, and attempts to execute the loaders returned
    by the builder.
    :param loader_builder:
    :param dolt_dir:
    :param dry_run:
    :return:
    """
    logger.info(
        '''Commencing load to Dolt with the following options:
                - dolt_dir  {dolt_dir}
        '''.format(dolt_dir=dolt_dir)
    )

    if not dry_run:
        loaders = loader_builder()
        for dolt_loader in loaders:
            dolt_loader(Dolt(dolt_dir))


def main():
    """
    Used as a function backing shim for surfacing command line tool.
    :return:
    """
    config_cli_logger()
    parser = argparse.ArgumentParser()
    parser.add_argument('dolt_load_module', help='Fully qualified path to a module providing a set of loaders')
    parser.add_argument('--dolt-dir', type=str, help='The directory of the Dolt repo being loaded to', required=True)
    parser.add_argument('--dry-run', action='store_true', help="Print out parameters, but don't do anything")
    args = parser.parse_args()
    logger.info('Resolving loaders for module path {}'.format(args.dolt_load_module))
    loader(loader_builder=resolve_function(args.dolt_load_module), dolt_dir=args.dolt_dir, dry_run=args.dry_run)


if __name__ == '__main__':
    main()
