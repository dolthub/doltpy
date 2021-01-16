from typing import List, Any, Mapping, Iterable

from doltpy.shared import get_logger
from doltpy.sql.schema_tools import infer_table_schema
from datetime import datetime, date, time
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine
import math
import io
import pandas as pd
import csv

DEFAULT_BATCH_SIZE = 100000
logger = get_logger(__name__)


def write_columns(engine: Engine,
                  table: str,
                  columns: Mapping[str, List[Any]],
                  on_duplicate_key_update: bool = None,
                  create_if_not_exists: bool = True,
                  primary_keys: List[str] = None,
                  commit: bool = True,
                  commit_message: str = None,
                  commit_date: datetime = None,
                  batch_size: int = DEFAULT_BATCH_SIZE):
    """

    :param engine:
    :param table:
    :param columns:
    :param on_duplicate_key_update:
    :param create_if_not_exists:
    :param primary_keys:
    :param commit:
    :param commit_message:
    :param commit_date:
    :param batch_size
    :return:
    """
    rows = []
    for column, values in columns.items():
        for i, value in enumerate(values):
            if rows[i]:
                rows[i][column] = value
            else:
                rows[i] = {column: value}

    return _write_rows(engine,
                       table,
                       rows,
                       on_duplicate_key_update,
                       create_if_not_exists,
                       primary_keys, commit,
                       commit_message,
                       commit_date,
                       batch_size)


def write_file(engine: Engine,
               table: str,
               file_handle: io.StringIO,
               on_duplicate_key_update: bool = None,
               create_if_not_exists: bool = True,
               primary_keys: List[str] = None,
               commit: bool = True,
               commit_message: str = None,
               commit_date: datetime = None,
               batch_size: int = DEFAULT_BATCH_SIZE):
    """

    :param engine:
    :param table:
    :param file_handle:
    :param on_duplicate_key_update:
    :param create_if_not_exists:
    :param primary_keys:
    :param commit:
    :param commit_message:
    :param commit_date:
    :param batch_size:
    :return:
    """
    reader = csv.DictReader(file_handle)
    rows = [row for row in reader]
    return _write_rows(engine,
                       table,
                       rows,
                       on_duplicate_key_update,
                       create_if_not_exists,
                       primary_keys, commit,
                       commit_message,
                       commit_date,
                       batch_size)


def write_rows(engine: Engine,
               table: str,
               rows: List[dict],
               on_duplicate_key_update: bool = None,
               create_if_not_exists: bool = True,
               primary_keys: List[str] = None,
               commit: bool = False,
               commit_message: str = None,
               commit_date: datetime = None,
               batch_size: int = DEFAULT_BATCH_SIZE):
    """

    :param engine:
    :param table:
    :param rows:
    :param on_duplicate_key_update:
    :param create_if_not_exists:
    :param primary_keys:
    :param commit:
    :param commit_message:
    :param commit_date:
    :param batch_size:
    :return:
    """
    return _write_rows(engine,
                       table,
                       rows,
                       on_duplicate_key_update,
                       create_if_not_exists,
                       primary_keys, commit,
                       commit_message,
                       commit_date,
                       batch_size)



def write_pandas(engine: Engine,
                 table: str,
                 df: pd.DataFrame,
                 on_duplicate_key_update: bool = None,
                 create_if_not_exists: bool = True,
                 primary_keys: List[str] = None,
                 commit: bool = False,
                 commit_message: str = None,
                 commit_date: datetime = None,
                 batch_size: int = DEFAULT_BATCH_SIZE):
    """

    :param engine:
    :param table:
    :param df:
    :param on_duplicate_key_update:
    :param create_if_not_exists:
    :param primary_keys:
    :param commit:
    :param commit_message:
    :param commit_date:
    :param batch_size:
    :return:
    """
    rows = df.to_records('records')
    return _write_rows(engine,
                       table,
                       rows,
                       on_duplicate_key_update,
                       create_if_not_exists,
                       primary_keys, commit,
                       commit_message,
                       commit_date,
                       batch_size)


def _write_rows(engine: Engine,
                table_name: str,
                rows: Iterable[dict],
                on_duplicate_key_update: bool = None,
                create_if_not_exists: bool = True,
                primary_keys: List[str] = None,
                commit: bool = False,
                commit_message: str = None,
                commit_date: datetime = None,
                batch_size: int = DEFAULT_BATCH_SIZE):
    metadata = MetaData(bind=engine)
    metadata.reflect()

    # If the table does not exist, create it using type inference to build a create statement
    if table_name not in metadata.tables and create_if_not_exists:
        assert primary_keys, 'primary_keys need to be provided when inferring a schema'
        infer_table_schema(metadata, table_name, rows, primary_keys)
        metadata.reflect()

    clean_rows = _coerce_dates(rows)

    logger.info(f'Inserting {len(list(rows))} rows into table {table_name}')

    metadata.reflect()
    table = metadata.tables[table_name]
    for i in range(max(1, math.ceil(len(clean_rows) / batch_size))):
        batch_start = i * batch_size
        batch_end = min((i + 1) * batch_size, len(clean_rows))
        batch = clean_rows[batch_start:batch_end]
        logger.info(f'Writing records {batch_start} through {batch_end} of {len(clean_rows)} rows to Dolt')
        with engine.connect() as conn:
            conn.execute(table.insert(), batch)
            statement = table.insert().values(batch)
            if on_duplicate_key_update:
                update_dict = {el.name: el for el in statement.inserted if not el.primary_key}
                statement = statement.on_duplicate_key_update(update_dict)

            conn.execute(statement)

    # do the commit here
    if commit:
        add_query = f'''SELECT DOLT_ADD('{table_name}')'''
        commit_query = f'''SELECT DOLT_COMMIT('-m', '{commit_message}')'''
        with engine.connect() as conn:
            conn.execute(add_query)
            result = conn.execute(commit_query)
            return result[0]['commit_hash']


def _coerce_dates(data: Iterable[dict]) -> List[dict]:
    """
    This is required to get dates into a string format that will be correctly picked up by the connector wire protocol.
    :param data:
    :return:
    """
    data_copy = []
    for row in data:
        row_copy = {}
        for col, val in row.items():
            if type(val) == date:
                row_copy[col] = datetime.combine(val, time())
            else:
                row_copy[col] = val
        data_copy.append(row_copy)

    return data_copy