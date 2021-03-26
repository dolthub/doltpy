import logging
from typing import List, Mapping
import io

from doltcli import Dolt  # type: ignore
from doltcli.utils import (  # type: ignore
    get_read_table_asof_query,
    read_table_sql,
    read_rows_sql,
    read_rows,
    read_columns,
    read_columns_sql,
)
import pandas as pd  # type: ignore

logger = logging.getLogger(__name__)


def parse_to_pandas(sql_output: str) -> pd.DataFrame:
    return pd.read_csv(sql_output)


def read_pandas_sql(dolt: Dolt, sql: str) -> pd.DataFrame:
    return read_table_sql(dolt, sql, result_parser=parse_to_pandas)


def read_pandas(dolt: Dolt, table: str, as_of: str = None) -> pd.DataFrame:
    return read_pandas_sql(dolt, get_read_table_asof_query(table, as_of))
