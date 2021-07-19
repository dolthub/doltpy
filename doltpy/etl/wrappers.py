import logging
from typing import List, Union, Optional

from ..cli import Dolt, DoltHubContext
from ..etl.loaders import DoltLoader
from ..shared.helpers import to_list

logger = logging.getLogger(__name__)


def load_to_dolthub(
    loader_or_loaders: Union[DoltLoader, List[DoltLoader]],
    clone: bool,
    push: bool,
    remote_name: str,
    remote_url: str,
    dolt_dir: Optional[str] = None,
    dry_run: bool = False,
):
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
    with DoltHubContext(remote_url, dolt_dir, remote_name) as dolthub_context:
        logger.info(
            f"""Commencing to load to DoltHub with the following options:
                            - dolt_dir  {dolthub_context.dolt.repo_dir}
                            - clone     {clone}
                            - remote    {remote_name}
                            - push      {push}
            """
        )
        if not dry_run:
            for dolt_loader in to_list(loader_or_loaders):
                branch = dolt_loader(dolthub_context.dolt)
                if push:
                    logger.info(f"Pushing changes to remote {remote_name} on branch {branch}")
                    dolthub_context.dolt.push(remote_name, branch)


def load_to_dolt(loader_or_loaders: Union[DoltLoader, List[DoltLoader]], dolt_dir: str, dry_run: bool):
    """
    This function takes a `DoltLoaderBuilder`, repo and remote settings, and attempts to execute the loaders returned
    by the builder.
    :param loader_or_loaders:
    :param dolt_dir:
    :param dry_run:
    :return:
    """

    logger.info(
        """Commencing load to Dolt with the following options:
                - dolt_dir  {dolt_dir}
        """.format(
            dolt_dir=dolt_dir
        )
    )

    if not dry_run:
        for dolt_loader in to_list(loader_or_loaders):
            dolt_loader(Dolt(dolt_dir))
