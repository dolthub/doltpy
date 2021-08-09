import logging
import datetime

from typing import List, Optional

from doltcli import Dolt  # type: ignore
from doltcli.utils import (  # type: ignore
    _import_helper,
    CREATE,
    FORCE_CREATE,
    REPLACE,
    UPDATE,
    write_columns,
    write_file,
    write_rows,
)
import pandas as pd  # type: ignore


def write_pandas(
    dolt: Dolt,
    table: str,
    df: pd.DataFrame,
    import_mode: Optional[str] = None,
    primary_key: Optional[List[str]] = None,
    commit: Optional[bool] = False,
    commit_message: Optional[str] = None,
    commit_date: Optional[datetime.datetime] = None,
    do_continue: Optional[bool] = False,
    do_gc: Optional[bool] = True,
):
    """

    :param dolt:
    :param table:
    :param df:
    :param import_mode:
    :param primary_key:
    :param commit:
    :param commit_message:
    :param commit_date:
    :return:
    """

    def writer(filepath: str):
        clean = df.dropna(subset=primary_key)
        clean.to_csv(filepath, index=False)
        return filepath

    _import_helper(
        dolt=dolt,
        table=table,
        write_import_file=writer,
        primary_key=primary_key,
        import_mode=import_mode,
        commit=commit,
        commit_message=commit_message,
        commit_date=commit_date,
        do_continue=do_continue,
        do_gc=do_gc,
    )
