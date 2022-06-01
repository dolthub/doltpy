import logging
from pathlib import Path
import random
import string
from tempfile import TemporaryDirectory
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

read_formats = {
    "parquet": ".parquet",
    "pq": ".parquet",
    "csv": ".csv",
}

logger = logging.getLogger(__name__)


def parse_to_pandas(sql_output: str) -> pd.DataFrame:
    return pd.read_csv(sql_output)


def read_pandas_sql(dolt: Dolt, sql: str) -> pd.DataFrame:
    return read_table_sql(dolt, sql, result_parser=parse_to_pandas)


def read_pandas_parquet(dolt: Dolt, table: str, asof: str = None) -> pd.DataFrame:
    # TODO: either dolt export should support as of, or sql query should
    #       support parquet output format
    ab = dolt.active_branch
    letters = string.ascii_lowercase
    tmp_branch = "".join(random.choice(letters) for i in range(10))
    try:
        dolt.checkout(tmp_branch, checkout_branch=True, start_point=asof)
        with TemporaryDirectory() as tmpdir:
            fpath = Path(tmpdir) / "tmp.parquet"
            dolt.table_export(table, filename=str(fpath))
            return pd.read_parquet(fpath)
    finally:
        dolt.checkout(ab)
        dolt.branch(tmp_branch, delete=True)


def read_pandas(dolt: Dolt, table: str, as_of: str = None, fmt="csv") -> pd.DataFrame:
    if fmt == "csv":
        return read_pandas_sql(dolt, get_read_table_asof_query(table, as_of))
    elif fmt == "parquet" or fmt == "pq":
        return read_pandas_parquet(dolt, table, as_of)
    else:
        raise RuntimeError(f"unexpected read format: {fmt}; expected: 'parquet' or 'csv'")
