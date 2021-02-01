import csv
import datetime
import io
import logging
import os
import tempfile
from typing import Any, Callable, List, Mapping, Optional, Set

import pandas as pd  # type: ignore

from doltpy.cli import Dolt
from doltpy.shared.helpers import columns_to_rows

logger = logging.getLogger(__name__)

CREATE, FORCE_CREATE, REPLACE, UPDATE = "create", "force_create", "replace", "update"
IMPORT_MODES_TO_FLAGS = {
    CREATE: ["-c"],
    FORCE_CREATE: ["-f", "-c"],
    REPLACE: ["-r"],
    UPDATE: ["-u"],
}


def write_file(
    dolt: Dolt,
    table: str,
    file_handle: io.StringIO,
    # TODO what to do about this?
    filetype: str = "csv",
    import_mode: Optional[str] = None,
    primary_key: Optional[List[str]] = None,
    commit: Optional[bool] = False,
    commit_message: Optional[str] = None,
    commit_date: Optional[datetime.datetime] = None,
):
    def writer(filepath: str):
        with open(filepath, "w") as f:
            f.writelines(file_handle.readlines())

    _import_helper(
        dolt=dolt,
        table=table,
        write_import_file=writer,
        primary_key=primary_key,
        import_mode=import_mode,
        commit=commit,
        commit_message=commit_message,
        commit_date=commit_date,
    )


def write_columns(
    dolt: Dolt,
    table: str,
    columns: Mapping[str, List[Any]],
    import_mode: Optional[str] = None,
    primary_key: Optional[List[str]] = None,
    commit: Optional[bool] = False,
    commit_message: Optional[str] = None,
    commit_date: Optional[datetime.datetime] = None,
):
    """

    :param dolt:
    :param table:
    :param columns:
    :param import_mode:
    :param primary_key:
    :param commit:
    :param commit_message:
    :param commit_date:
    :return:
    """

    def writer(filepath: str):
        if len(list(set(len(col) for col in columns.values()))) != 1:
            raise ValueError("Must pass columns of identical length")

        with open(filepath, "w") as f:
            csv_writer = csv.DictWriter(f, columns.keys())
            rows = columns_to_rows(columns)
            csv_writer.writeheader()
            csv_writer.writerows(rows)

    _import_helper(
        dolt=dolt,
        table=table,
        write_import_file=writer,
        primary_key=primary_key,
        import_mode=import_mode,
        commit=commit,
        commit_message=commit_message,
        commit_date=commit_date,
    )


def write_rows(
    dolt: Dolt,
    table: str,
    rows: List[dict],
    import_mode: Optional[str] = None,
    primary_key: Optional[List[str]] = None,
    commit: Optional[bool] = False,
    commit_message: Optional[str] = None,
    commit_date: Optional[datetime.datetime] = None,
):
    """

    :param dolt:
    :param table:
    :param rows:
    :param import_mode:
    :param primary_key:
    :param commit:
    :param commit_message:
    :param commit_date:
    :return:
    """

    def writer(filepath: str):
        with open(filepath, "w") as f:
            fieldnames: Set[str] = set()
            for row in rows:
                fieldnames = fieldnames.union(set(row.keys()))

            csv_writer = csv.DictWriter(f, fieldnames)
            csv_writer.writeheader()
            csv_writer.writerows(rows)

    _import_helper(
        dolt=dolt,
        table=table,
        write_import_file=writer,
        primary_key=primary_key,
        import_mode=import_mode,
        commit=commit,
        commit_message=commit_message,
        commit_date=commit_date,
    )


def write_pandas(
    dolt: Dolt,
    table: str,
    df: pd.DataFrame,
    import_mode: Optional[str] = None,
    primary_key: Optional[List[str]] = None,
    commit: Optional[bool] = False,
    commit_message: Optional[str] = None,
    commit_date: Optional[datetime.datetime] = None,
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

    _import_helper(
        dolt=dolt,
        table=table,
        write_import_file=writer,
        primary_key=primary_key,
        import_mode=import_mode,
        commit=commit,
        commit_message=commit_message,
        commit_date=commit_date,
    )


def _import_helper(
    dolt: Dolt,
    table: str,
    write_import_file: Callable[[str], None],
    import_mode: Optional[str] = None,
    primary_key: Optional[List[str]] = None,
    commit: Optional[bool] = False,
    commit_message: Optional[str] = None,
    commit_date: Optional[datetime.datetime] = None,
) -> None:
    import_mode = _get_import_mode_and_flags(dolt, table, import_mode)
    logger.info(f"Importing to table {table} in dolt directory located in {dolt.repo_dir()}, import mode {import_mode}")

    fname = tempfile.mktemp(suffix=".csv")
    import_flags = IMPORT_MODES_TO_FLAGS[import_mode]
    try:
        write_import_file(fname)
        args = ["table", "import", table] + import_flags
        if primary_key:
            args += ["--pk={}".format(",".join(primary_key))]

        dolt.execute(args + [fname])

        if commit:
            msg = commit_message or f"Committing write to table {table} in {import_mode} mode"
            dolt.add(table)
            dolt.commit(msg, date=commit_date)
    finally:
        if os.path.exists(fname):
            os.remove(fname)


def _get_import_mode_and_flags(dolt: Dolt, table: str, import_mode: Optional[str] = None) -> str:
    import_modes = IMPORT_MODES_TO_FLAGS.keys()
    if import_mode and import_mode not in import_modes:
        raise ValueError(f"update_mode must be one of: {import_modes}")
    else:
        if table in [table.name for table in dolt.ls()]:
            logger.info(f'No import mode specified, table exists, using "{UPDATE}"')
            import_mode = UPDATE
        else:
            logger.info(f'No import mode specified, table exists, using "{CREATE}"')
            import_mode = CREATE

    return import_mode
