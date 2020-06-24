from doltpy.core import Dolt
import os
import tempfile
from doltpy.etl.loaders import DoltLoaderBuilder
from doltpy.core.system_helpers import get_logger


logger = get_logger(__name__)


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
        loaders = loader_builder()
        for dolt_loader in loaders:
            branch = dolt_loader(repo)
            if push:
                logger.info('Pushing changes to remote {} on branch {}'.format(remote_name, branch))
                repo.push(remote_name, branch)
