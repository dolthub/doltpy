from doltpy.core import Dolt
import os
import tempfile
from doltpy.etl.loaders import DoltLoader
from doltpy.core.system_helpers import get_logger
from typing import List, Union

logger = get_logger(__name__)


def load_to_dolthub(loader_or_loaders: Union[DoltLoader, List[DoltLoader]],
                    clone: bool,
                    push: bool,
                    remote_name: str,
                    remote_url: str,
                    dolt_dir: str = None,
                    dry_run: bool = False):
    """
    This function takes a `DoltLoaderBuilder`, repo and remote settings, and attempts to execute the loaders returned
    by the builder.
    :param loader_or_loaders:
    :param dolt_dir:
    :param clone:
    :param push:
    :param remote_name:
    :param dry_run:
    :param remote_url:
    :return:
    """
    if type(loader_or_loaders) == list:
        loaders = loader_or_loaders
    else:
        loaders = [loader_or_loaders]

    if clone:
        assert remote_url, 'If clone is True then remote must be passed'
        temp_dir = tempfile.mkdtemp()
        logger.info('Clone is set to true, so ignoring dolt_dir')
        if clone:
            logger.info('Clone set to True, cloning remote {}'.format(remote_url))
        repo = Dolt.clone(remote_url, temp_dir)
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
        for dolt_loader in loaders:
            branch = dolt_loader(repo)
            if push:
                logger.info('Pushing changes to remote {} on branch {}'.format(remote_name, branch))
                repo.push(remote_name, branch)


def load_to_dolt(loader_or_loaders: Union[DoltLoader, List[DoltLoader]], dolt_dir: str, dry_run: bool):
    """
    This function takes a `DoltLoaderBuilder`, repo and remote settings, and attempts to execute the loaders returned
    by the builder.
    :param loader_or_loaders:
    :param dolt_dir:
    :param dry_run:
    :return:
    """
    if type(loader_or_loaders) == list:
        loaders = loader_or_loaders
    else:
        loaders = [loader_or_loaders]

    logger.info(
        '''Commencing load to Dolt with the following options:
                - dolt_dir  {dolt_dir}
        '''.format(dolt_dir=dolt_dir)
    )

    if not dry_run:
        for dolt_loader in loaders:
            dolt_loader(Dolt(dolt_dir))
