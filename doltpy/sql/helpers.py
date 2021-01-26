from sqlalchemy import String, DateTime, Date, Integer, Float, Table, MetaData, Column
from datetime import datetime, date, time
from doltpy.shared import rows_to_columns
from typing import List, Mapping, Iterable, Tuple, Any
from sqlalchemy.engine import Engine
from sqlalchemy.sql import select
import pandas as pd
import logging

logger = logging.getLogger(__name__)


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
                    row_copy[col] = ",".join(
                        str(el) if el is not None else "NULL" for el in val
                    )
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
        return {
            hash_row_els(dict(row), [col.name for col in pk_cols]): dict(row)
            for row in result
        }


def hash_row_els(row: dict, cols: List[str]) -> int:
    return hash(frozenset({col: row[col] for col in cols}.items()))


def get_inserts_and_updates(
    engine: Engine, table: Table, data: List[dict]
) -> Tuple[List[dict], List[dict]]:
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


def infer_table_schema(
    metadata: MetaData, table_name: str, rows: Iterable[dict], primary_key: List[str]
):
    # generate and execute a create table statement
    cols_to_types = {}
    columns = rows_to_columns(rows)
    for col_name, list_of_values in columns.items():
        # Just take the first value to by the type
        first_non_null = None
        for val in list_of_values:
            if val is not None:
                first_non_null = val
                break
            raise ValueError("Cannot provide an empty list, types cannot be inferred")
        cols_to_types[col_name] = _get_col_type(first_non_null, list_of_values)

    table = _get_table_def(metadata, table_name, cols_to_types, primary_key)
    table.create()


def _get_col_type(sample_value: Any, values: Any):
    if type(sample_value) == str:
        return String(2 * max(len(val) for val in values))
    elif type(sample_value) == int:
        return Integer
    elif type(sample_value) == float:
        return Float
    elif type(sample_value) == datetime:
        return DateTime
    elif type(sample_value) == date:
        return Date
    else:
        raise ValueError("Value of type {} is unsupported".format(type(sample_value)))


def _get_table_def(
    metadata,
    table_name: str,
    cols_with_types: Mapping[str, str],
    primary_key: List[str] = None,
):
    _primary_key = primary_key or []
    columns = [
        Column(col_name, col_type, primary_key=col_name in _primary_key)
        for col_name, col_type in cols_with_types.items()
    ]
    return Table(table_name, metadata, *columns)
