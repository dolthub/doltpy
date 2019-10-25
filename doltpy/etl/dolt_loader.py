import argparse
from doltpy.core import Dolt
from typing import List
from doltpy.etl.loaders import load_to_dolt, resolve_function, DoltTableLoader, resolve_branch
import logging
from doltpy.etl.cli_logging_config_helper import config_cli_logger

logger = logging.getLogger(__name__)


def loader(loaders: List[DoltTableLoader], dolt_dir: str, commit: bool, message: str, dry_run: bool, branch: str):
    logger.info(
        '''Commencing load to Dolt with the following options, and the following options
                - dolt_dir  {dolt_dir}
                - commit    {commit}
                - branch    {branch}
        '''.format(dolt_dir=dolt_dir,
                   commit=commit,
                   branch=branch)
    )

    if not dry_run:
        load_to_dolt(Dolt(dolt_dir), loaders, commit, message, branch)


def main():
    config_cli_logger()
    parser = argparse.ArgumentParser()
    parser.add_argument('dolt_load_module', help='Fully qualified path to a module providing a set of loaders')
    parser.add_argument('--dolt-dir', type=str, help='The directory of the Dolt repo being loaded to', required=True)
    parser.add_argument('--commit', action='store_true')
    parser.add_argument('--message', type=str, help='Commit message to assciate created commit (requires --commit)')
    parser.add_argument('--branch', type=str, help='Branch to write to, default is master')
    parser.add_argument('--branch-generator', type=str, help='A module path to generate a branch name programmatically')
    parser.add_argument('--dry-run', action='store_true', help="Print out parameters, but don't do anything")
    args = parser.parse_args()
    logger.info('Resolving loaders for module path {}'.format(args.dolt_load_module))
    loader(loaders=resolve_function(args.dolt_load_module),
           dolt_dir=args.dolt_dir,
           commit=args.commit,
           message=args.message,
           dry_run=args.dry_run,
           branch=resolve_branch(args.branch, args.branch_generator, 'master'))


if __name__ == '__main__':
    main()
