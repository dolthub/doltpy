import logging
from typing import List, Mapping

from doltcli import Dolt  # type: ignore
from doltcli.utils import (  # type: ignore
    _get_read_table_asof_query,
    _read_table_sql,
    read_rows_sql,
    read_rows,
    read_columns,
    read_columns_sql,
)
import pandas as pd  # type: ignore

logger = logging.getLogger(__name__)


def read_pandas_sql(dolt: Dolt, sql: str) -> pd.DataFrame:
    rows = _read_table_sql(dolt, sql)
    return pd.DataFrame(rows)


def read_pandas(dolt: Dolt, table: str, as_of: str = None) -> pd.DataFrame:
    return read_pandas_sql(dolt, _get_read_table_asof_query(table, as_of))
