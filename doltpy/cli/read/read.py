import logging
from typing import List, Mapping

import pandas as pd  # type: ignore

from doltpy.cli import Dolt
from doltpy.shared.helpers import rows_to_columns

logger = logging.getLogger(__name__)


def read_columns(dolt: Dolt, table: str, as_of: str = None) -> Mapping[str, list]:
    return read_columns_sql(dolt, _get_read_table_asof_query(table, as_of))


def read_rows(dolt: Dolt, table: str, as_of: str = None) -> List[dict]:
    return read_rows_sql(dolt, _get_read_table_asof_query(table, as_of))


def read_pandas(dolt: Dolt, table: str, as_of: str = None) -> pd.DataFrame:
    return read_pandas_sql(dolt, _get_read_table_asof_query(table, as_of))


def _get_read_table_asof_query(table: str, as_of: str = None) -> str:
    base_query = f"SELECT * FROM `{table}`"
    return f'{base_query} AS OF "{as_of}"' if as_of else base_query


def read_columns_sql(dolt: Dolt, sql: str) -> Mapping[str, list]:
    rows = _read_table_sql(dolt, sql)
    columns = rows_to_columns(rows)
    return columns


def read_rows_sql(dolt: Dolt, sql: str) -> List[dict]:
    return _read_table_sql(dolt, sql)


def read_pandas_sql(dolt: Dolt, sql: str) -> pd.DataFrame:
    rows = _read_table_sql(dolt, sql)
    return pd.DataFrame(rows)


def _read_table_sql(dolt: Dolt, sql: str) -> List[dict]:
    return dolt.sql(sql, result_format="csv")
