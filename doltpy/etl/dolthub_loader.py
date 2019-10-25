import argparse
from doltpy.core import Dolt
import os
import tempfile
from doltpy.etl.loaders import load_to_dolt, resolve_function, DoltTableLoader, resolve_branch
from typing import List
import logging
from doltpy.etl.cli_logging_config_helper import config_cli_logger

logger = logging.getLogger(__name__)


def loader(loaders: List[DoltTableLoader],
           dolt_dir: str,
           clone: bool,
           branch: str,
           commit: bool,
           push: bool,
           remote_name: str,
           message: str,
           dry_run: bool,
           remote_url: str):
    if clone:
        assert remote_url, 'If clone is True then remote must be passed'
        temp_dir = tempfile.mkdtemp()
        logger.info('Clone is set to true, so ignoring dolt_dir')
        repo = Dolt(temp_dir)
        if clone:
            logger.info('Clone set to True, cloning remote {}'.format(remote_url))
        repo.clone(remote_url)
    else:
        assert os.path.exists(os.path.join(dolt_dir, '.dolt')), 'Repo must exist locally if not cloned'
        repo = Dolt(dolt_dir)

    logger.info(
        '''Commencing to load to DoltHub with the following options, and the following options
                        - dolt_dir  {dolt_dir}
                        - commit    {commit}
                        - branch    {branch}
                        - clone     {clone}
                        - remote    {remote}
                        - push      {push}
        '''.format(dolt_dir=repo.repo_dir,
                   commit=commit,
                   branch=branch,
                   push=push,
                   clone=clone,
                   remote=remote_name))

    if not dry_run:
        load_to_dolt(repo, loaders, commit, message, branch)

        if push:
            logger.info('Pushing changes to remote {} on branch {}'.format(remote_name, branch))
            repo.push(remote_name, branch)


def main():
    config_cli_logger()
    parser = argparse.ArgumentParser()
    parser.add_argument('dolt_load_module', help='Fully qualified path to a module providing a set of loaders')
    parser.add_argument('--dolt-dir', type=str, help='The directory of the Dolt repo being loaded to')
    parser.add_argument('--commit', action='store_true')
    parser.add_argument('--message', type=str, help=' Commit message to assciate created commit (requires --commit)')
    parser.add_argument('--branch', type=str, help='Branch to write to, default is master')
    parser.add_argument('--branch-generator', type=str, help='A module path to generate a branch name programmatically')
    parser.add_argument('--clone', action='store_true', help='Clone the remote to the local machine')
    parser.add_argument('--remote-url', type=str, help='DoltHub remote being used', required=True)
    parser.add_argument('--remote-name', type=str, default='origin', help='Alias for remote, default is origin')
    parser.add_argument('--push', action='store_true', help='Push changes to remote, must sepcify arg --remote')
    parser.add_argument('--dry-run', action='store_true', help="Print out parameters, but don't do anything")
    args = parser.parse_args()
    logger.info('Resolving loaders for module path {}'.format(args.dolt_load_module))
    loader(loaders=resolve_function(args.dolt_load_module),
           dolt_dir=args.dolt_dir,
           clone=args.clone,
           commit=args.commit,
           push=args.push,
           remote_name=args.remote_name,
           message=args.message,
           dry_run=args.dry_run,
           branch=resolve_branch(args.branch, args.branch_generator, 'master'),
           remote_url=args.remote_url)


if __name__ == '__main__':
    main()
