from typing import List, Any, Mapping, Iterable, Tuple
from doltpy.sql.schema_tools import infer_table_schema
from doltpy.shared import columns_to_rows
from doltpy.sql import commit_tables
from datetime import datetime, date, time
from sqlalchemy import MetaData, bindparam, Table
from sqlalchemy.engine import Engine
from sqlalchemy.sql import select
import math
import pandas as pd
import csv
import logging
from copy import deepcopy

DEFAULT_BATCH_SIZE = 100000
logger = logging.getLogger(__name__)


def write_columns(engine: Engine,
                  table: str,
                  columns: Mapping[str, List[Any]],
                  on_duplicate_key_update: bool = True,
                  create_if_not_exists: bool = False,
                  primary_key: List[str] = None,
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
    :param primary_key:
    :param commit:
    :param commit_message:
    :param commit_date:
    :param batch_size
    :return:
    """
    rows = columns_to_rows(columns)
    return write_rows(engine,
                      table,
                      rows,
                      on_duplicate_key_update,
                      create_if_not_exists,
                      primary_key, commit,
                      commit_message,
                      commit_date,
                      batch_size)


def write_file(engine: Engine,
               table: str,
               file_path: str,
               on_duplicate_key_update: bool = True,
               create_if_not_exists: bool = False,
               primary_key: List[str] = None,
               commit: bool = True,
               commit_message: str = None,
               commit_date: datetime = None,
               batch_size: int = DEFAULT_BATCH_SIZE):
    """

    :param engine:
    :param table:
    :param file_path:
    :param on_duplicate_key_update:
    :param create_if_not_exists:
    :param primary_key:
    :param commit:
    :param commit_message:
    :param commit_date:
    :param batch_size:
    :return:
    """
    with open(file_path, 'r') as file_handle:
        reader = csv.DictReader(file_handle)
        rows = [row for row in reader]
    return write_rows(engine,
                      table,
                      rows,
                      on_duplicate_key_update,
                      create_if_not_exists,
                      primary_key, commit,
                      commit_message,
                      commit_date,
                      batch_size)


def write_pandas(engine: Engine,
                 table: str,
                 df: pd.DataFrame,
                 on_duplicate_key_update: bool = True,
                 create_if_not_exists: bool = False,
                 primary_key: List[str] = None,
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
    :param primary_key:
    :param commit:
    :param commit_message:
    :param commit_date:
    :param batch_size:
    :return:
    """
    return write_rows(engine,
                      table,
                      df.to_dict('records'),
                      on_duplicate_key_update,
                      create_if_not_exists,
                      primary_key, commit,
                      commit_message,
                      commit_date,
                      batch_size)


def write_rows(engine: Engine,
               table_name: str,
               rows: Iterable[dict],
               on_duplicate_key_update: bool = True,
               create_if_not_exists: bool = False,
               primary_key: List[str] = None,
               commit: bool = False,
               commit_message: str = None,
               commit_date: datetime = None,
               batch_size: int = DEFAULT_BATCH_SIZE):
    """

    :param engine:
    :param table_name:
    :param rows:
    :param on_duplicate_key_update:
    :param create_if_not_exists:
    :param primary_key:
    :param commit:
    :param commit_message:
    :param commit_date:
    :param batch_size:
    :return:
    """
    metadata = MetaData(bind=engine)
    metadata.reflect()

    if table_name not in metadata.tables and create_if_not_exists:
        infer_table_schema(metadata, table_name, rows, primary_key)
        metadata.reflect()

    table = metadata.tables[table_name]

    rows = list(rows)
    for i in range(max(1, math.ceil(len(rows) / batch_size))):
        batch_start = i * batch_size
        batch_end = min((i + 1) * batch_size, len(rows))
        batch = rows[batch_start:batch_end]
        logger.info(f'Writing records {batch_start} through {batch_end} of {len(rows)} rows to Dolt')
        write_batch(engine, table, batch, on_duplicate_key_update)

    if commit:
        commit_tables(engine, commit_message, table_name)


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


def write_batch(engine: Engine, table: Table, rows: List[dict], on_duplicate_key_update: bool):
    coerced_data = list(clean_types(rows))
    inserts, updates = get_inserts_and_updates(engine, table, coerced_data)

    if not on_duplicate_key_update and updates:
        raise ValueError('Duplicate keys present, but on_duplicate_key_update is off')

    if inserts:
        logger.info('Inserting {} rows'.format(len(inserts)))
        with engine.connect() as conn:
            conn.execute(table.insert(), inserts)

    # We need to prefix the columns with "_" in order to use bindparam properly
    _updates = deepcopy(updates)
    for dic in _updates:
        for col in list(dic.keys()):
            dic['_{}'.format(col)] = dic.pop(col)

    if _updates:
        logger.info('Updating {} rows'.format(len(_updates)))
        with engine.connect() as conn:
            statement = table.update()
            for pk_col in [col.name for col in table.columns if col.primary_key]:
                statement = statement.where(table.c[pk_col] == bindparam('_{}'.format(pk_col)))
            non_pk_cols = [col.name for col in table.columns if not col.primary_key]
            statement = statement.values({col: bindparam('_{}'.format(col)) for col in non_pk_cols})
            conn.execute(statement, _updates)


def clean_types(data: Iterable[dict]) -> List[dict]:
    """
    MySQL does not support native array or JSON types, additionally mysql-connector-python does not support
    datetime.date (though that seems like a bug in the connector). This implements a very crude transformation of array
    types and coerces datetime.date values to equivalents. This is quite an experimental feature and is currently a way
    to transform array valued data read from Postgres to Dolt.
    :param data:
    :return:
    """
    data_copy = []
    for row in data:
        row_copy = {}
        for col, val in row.items():
            if type(val) == date:
                row_copy[col] = datetime.combine(val, time())
            elif type(val) == list:
                if not val:
                    row_copy[col] = None
                else:
                    row_copy[col] = ','.join(str(el) if el is not None else 'NULL' for el in val)
            elif type(val) == dict:
                row_copy[col] = str(val)
            elif pd.isna(val):
                row_copy[col] = None
            else:
                row_copy[col] = val

        data_copy.append(row_copy)

    return data_copy


def get_existing_pks(engine: Engine, table: Table) -> Mapping[int, dict]:
    """
    Creates an index of hashes of the values of the primary keys in the table provided.
    :param engine:
    :param table:
    :return:
    """
    with engine.connect() as conn:
        pk_cols = [table.c[col.name] for col in table.columns if col.primary_key]
        query = select(pk_cols)
        result = conn.execute(query)
        return {hash_row_els(dict(row), [col.name for col in pk_cols]): dict(row) for row in result}


def hash_row_els(row: dict, cols: List[str]) -> int:
    return hash(frozenset({col: row[col] for col in cols}.items()))


def get_inserts_and_updates(engine: Engine, table: Table, data: List[dict]) -> Tuple[List[dict], List[dict]]:
    existing_pks = get_existing_pks(engine, table)
    if not existing_pks:
        return data, []

    existing_pks_set = set(existing_pks.keys())
    pk_cols = [col.name for col in table.columns if col.primary_key]
    inserts, updates = [], []
    for row in data:
        row_hash = hash_row_els(row, pk_cols)

        if row_hash in existing_pks_set:
            updates.append(row)
        else:
            inserts.append(row)

    return inserts, updates

