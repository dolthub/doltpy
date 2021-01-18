from sqlalchemy.engine import Engine
import pandas as pd
from typing import List, Mapping
from doltpy.shared import rows_to_columns


def read_columns(engine: Engine, table: str, as_of: str = None) -> Mapping[str, list]:
    return read_columns_sql(engine, _get_read_table_asof_query(table, as_of))


def read_rows(engine: Engine, table: str, as_of: str = None) -> List[dict]:
    return read_rows_sql(engine, _get_read_table_asof_query(table, as_of))


def read_pandas(engine: Engine, table: str, as_of: str = None) -> pd.DataFrame:
    return read_pandas_sql(engine, _get_read_table_asof_query(table, as_of))


def _get_read_table_asof_query(table: str, as_of: str = None) -> str:
    base_query = 'SELECT * FROM {table}'.format(table=table)
    return f'{base_query} AS OF "{as_of}"' if as_of else base_query


def read_columns_sql(engine: Engine, sql: str) -> Mapping[str, list]:
    rows = _read_table_sql(engine, sql)
    columns = rows_to_columns(rows)
    return columns


def read_rows_sql(engine: Engine, sql: str) -> List[dict]:
    return _read_table_sql(engine, sql)


def read_pandas_sql(engine: Engine, sql: str) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(sql, conn)


def _read_table_sql(engine: Engine, sql: str) -> List[dict]:
    with engine.connect() as conn:
        result = conn.execute(sql)
        return [dict(row) for row in result]

