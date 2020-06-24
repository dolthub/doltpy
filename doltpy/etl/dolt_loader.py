from doltpy.core import Dolt
from doltpy.etl.loaders import DoltLoaderBuilder
from doltpy.core.system_helpers import get_logger

logger = get_logger(__name__)


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
