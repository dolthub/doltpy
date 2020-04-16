import argparse
from doltpy.core import Dolt, clone_repo
import os
import tempfile
from doltpy.etl.loaders import resolve_function, DoltLoaderBuilder
import logging
from doltpy.etl.cli_logging_config_helper import config_cli_logger

logger = logging.getLogger(__name__)


def loader(loader_builder: DoltLoaderBuilder,
           dolt_dir: str,
           clone: bool,
           push: bool,
           remote_name: str,
           dry_run: bool,
           remote_url: str):
    """
    This function takes a `DoltLoaderBuilder`, repo and remote settings, and attempts to execute the loaders returned
    by the builder.
    :param loader_builder:
    :param dolt_dir:
    :param clone:
    :param push:
    :param remote_name:
    :param dry_run:
    :param remote_url:
    :return:
    """
    if clone:
        assert remote_url, 'If clone is True then remote must be passed'
        temp_dir = tempfile.mkdtemp()
        logger.info('Clone is set to true, so ignoring dolt_dir')
        if clone:
            logger.info('Clone set to True, cloning remote {}'.format(remote_url))
        repo = clone_repo(remote_url, temp_dir)
    else:
        assert os.path.exists(os.path.join(dolt_dir, '.dolt')), 'Repo must exist locally if not cloned'
        repo = Dolt(dolt_dir)

    logger.info(
        '''Commencing to load to DoltHub with the following options:
                        - dolt_dir  {dolt_dir}
                        - clone     {clone}
                        - remote    {remote}
                        - push      {push}
        '''.format(dolt_dir=repo.repo_dir,
                   push=push,
                   clone=clone,
                   remote=remote_name))

    if not dry_run:
        loaders = loader_builder()
        for dolt_loader in loaders:
            branch = dolt_loader(repo)
            if push:
                logger.info('Pushing changes to remote {} on branch {}'.format(remote_name, branch))
                repo.push(remote_name, branch)


def main():
    """
    Used as a function backing shim for surfacing command line tool.
    :return:
    """
    config_cli_logger()
    parser = argparse.ArgumentParser()
    parser.add_argument('dolt_load_module', help='Fully qualified path to a module providing a set of loaders')
    parser.add_argument('--dolt-dir', type=str, help='The directory of the Dolt repo being loaded to')
    parser.add_argument('--clone', action='store_true', help='Clone the remote to the local machine')
    parser.add_argument('--remote-url', type=str, help='DoltHub remote being used', required=True)
    parser.add_argument('--remote-name', type=str, default='origin', help='Alias for remote, default is origin')
    parser.add_argument('--push', action='store_true', help='Push changes to remote, must specify arg --remote')
    parser.add_argument('--dry-run', action='store_true', help="Print out parameters, but don't do anything")
    args = parser.parse_args()
    logger.info('Resolving loaders for module path {}'.format(args.dolt_load_module))
    loader(loader_builder=resolve_function(args.dolt_load_module),
           dolt_dir=args.dolt_dir,
           clone=args.clone,
           push=args.push,
           remote_name=args.remote_name,
           dry_run=args.dry_run,
           remote_url=args.remote_url)


if __name__ == '__main__':
    main()
