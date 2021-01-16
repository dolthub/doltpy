from typing import List, Callable, Mapping, Any
import pandas as pd
from doltpy.cli import Dolt
from doltpy.cli.helpers import columns_to_rows
import logging
import io
import tempfile
import os
import csv
from datetime import datetime

logger = logging.getLogger(__name__)

CREATE, FORCE_CREATE, REPLACE, UPDATE = 'create', 'force_create', 'replace', 'update'
IMPORT_MODES_TO_FLAGS = {CREATE: ['-c'],
                         FORCE_CREATE: ['-f', '-c'],
                         REPLACE: ['-r'],
                         UPDATE: ['-u']}


def write_file(dolt: Dolt,
               table: str,
               file_handle: io.StringIO,
               # TODO what to do about this?
               filetype: str = 'csv',
               import_mode: str = None,
               primary_key: List[str] = None,
               commit: bool = False,
               commit_message: str = None,
               commit_date: datetime = None):
    def writer(filepath: str):
        with open(filepath, 'w') as f:
            f.writelines(file_handle.readlines())

    _import_helper(dolt, table, writer, primary_key, import_mode, commit, commit_message, commit_date)


def write_columns(dolt: Dolt,
                  table: str,
                  columns: Mapping[str, List[Any]],
                  import_mode: str = None,
                  primary_key: List[str] = None,
                  commit: bool = True,
                  commit_message: str = False,
                  commit_date: datetime = None):
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
        assert len(list(set(len(col) for col in columns.values()))) == 1, 'Must pass columns of identical length'

        with open(filepath, 'w') as f:
            csv_writer = csv.DictWriter(f, columns.keys())
            rows = columns_to_rows(columns)
            csv_writer.writeheader()
            csv_writer.writerows(rows)

    _import_helper(dolt, table, writer, primary_key, import_mode, commit, commit_message, commit_date)


def write_rows(dolt: Dolt,
               table: str,
               rows: List[dict],
               import_mode: str = None,
               primary_key: List[str] = None,
               commit: bool = False,
               commit_message: str = None,
               commit_date: datetime = None):
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
        with open(filepath, 'w') as f:
            fieldnames = set()
            for row in rows:
                fieldnames = fieldnames.union(set(row.keys()))

            csv_writer = csv.DictWriter(f, fieldnames)
            csv_writer.writeheader()
            csv_writer.writerows(rows)

    _import_helper(dolt, table, writer, primary_key, import_mode, commit, commit_message, commit_date)


def write_pandas(dolt: Dolt,
                 table: str,
                 df: pd.DataFrame,
                 import_mode: str = None,
                 primary_key: List[str] = None,
                 commit: bool = False,
                 commit_message: str = None,
                 commit_date: datetime = None):
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

    _import_helper(dolt, table, writer, primary_key, import_mode, commit, commit_message, commit_date)


def _import_helper(dolt: Dolt,
                   table: str,
                   write_import_file: Callable[[str], None],
                   primary_key: List[str],
                   import_mode: str,
                   commit: bool,
                   commit_message: str,
                   commit_date: datetime) -> None:
    import_mode = _get_import_mode_and_flags(dolt, import_mode, table)
    logger.info(f'Importing to table {table} in dolt directory located in {dolt.repo_dir()}, import mode {import_mode}')

    fname = tempfile.mktemp(suffix='.csv')
    import_flags = IMPORT_MODES_TO_FLAGS[import_mode]
    try:
        write_import_file(fname)
        args = ['table', 'import', table] + import_flags
        if primary_key:
            args += ['--pk={}'.format(','.join(primary_key))]

        dolt.execute(args + [fname])

        if commit:
            msg = commit_message or f'Committing write to table {table} in {import_mode} mode'
            dolt.add(table)
            dolt.commit(msg, date=commit_date)
    finally:
        if os.path.exists(fname):
            os.remove(fname)


def _get_import_mode_and_flags(dolt: Dolt, import_mode: str, table: str) -> str:
    import_modes = IMPORT_MODES_TO_FLAGS.keys()
    if import_mode is not None:
        assert import_mode in import_modes, f'update_mode must be one of: {import_modes}'
    else:
        if table in [table.name for table in dolt.ls()]:
            logger.info(f'No import mode specified, table exists, using "{UPDATE}"')
            import_mode = UPDATE
        else:
            logger.info(f'No import mode specified, table exists, using "{CREATE}"')
            import_mode = CREATE

    return import_mode
