from collections import OrderedDict
from dataclasses import dataclass
import csv
import logging
import math
import os
import datetime

# from doltpy.shared import SQL_LOG_FILE
from subprocess import STDOUT, Popen
from typing import Any, Dict, Iterable, List, Mapping, Union, Optional

import pandas as pd  # type: ignore
import sqlalchemy as sa  # type: ignore
import numpy as np  # type: ignore
from retry import retry
from sqlalchemy import create_engine  # type: ignore
from sqlalchemy.engine import Engine  # type: ignore
from sqlalchemy.dialects.mysql import insert  # type: ignore

from ..cli import Dolt, Commit
from ..shared import columns_to_rows, rows_to_columns, to_list
from ..sql.helpers import infer_table_schema, clean_types

logger = logging.getLogger(__name__)

DEFAULT_HOST, DEFAULT_PORT = "127.0.0.1", 3306
SQL_LOG_FILE = "temp_log"
DEFAULT_BATCH_SIZE = 100000


@dataclass
class ServerConfig:
    branch: Optional[str] = None
    config: Optional[str] = None
    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    user: Optional[str] = None
    password: Optional[str] = None
    timeout: Optional[int] = None
    readonly: Optional[bool] = None
    loglevel: Optional[str] = None
    multi_db_dir: Optional[str] = None
    no_auto_commit: Optional[bool] = None
    max_connections: Optional[int] = None
    echo: bool = False


@dataclass
class DoltSQLContext:
    database: str
    server_config: ServerConfig
    engine: Engine

    def _get_engine(self) -> Engine:
        """
        Get a connection to ths server process that this repo is running, raise an exception if it is not running.
        :return:
        """
        database = self.database
        user = self.server_config.user
        host = self.server_config.host
        port = self.server_config.port
        password = self.server_config.password

        logger.info(f"Creating engine for Dolt SQL Server instance running on {host}:{port}")

        def inner():
            if password is not None:
                return create_engine(
                    f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}",
                    echo=self.server_config.echo,
                )
            else:
                return create_engine(
                    f"mysql+mysqlconnector://{user}@{host}:{port}/{database}",
                    echo=self.server_config.echo,
                )

        return inner()

    @retry(
        delay=2,
        tries=10,
        exceptions=(
            sa.exc.OperationalError,
            sa.exc.DatabaseError,
            sa.exc.InterfaceError,
        ),
    )
    def verify_connection(self) -> bool:
        with self.engine.connect() as conn:
            conn.close()
            return True

    def commit_tables(
        self,
        commit_message: Optional[str] = None,
        table_or_tables: Optional[Union[str, List[str]]] = None,
        allow_emtpy: bool = False,
    ) -> str:
        tables = to_list(table_or_tables)

        with self.engine.connect() as conn:
            if tables:
                for table in tables:
                    conn.execute(f"SELECT DOLT_ADD('{table}')")
                dolt_commit_args = f"'-m', '{commit_message}'"
            else:
                dolt_commit_args = f"'-a', '-m', '{commit_message}'"
            result = [dict(row) for row in conn.execute(f"SELECT DOLT_COMMIT({dolt_commit_args}) as commit_hash")]
            print(result)
            assert len(result) == 1, "Expected a single returned row with a commit hash"
            return result[0]["commit_hash"]

    def execute(
        self,
        sql: str,
        commit: bool = False,
        commit_message: Optional[str] = None,
        allow_emtpy: bool = False,
    ) -> Optional[str]:
        with self.engine.connect() as conn:
            conn.execute(sql)

        if commit:
            if not commit_message:
                raise ValueError("Passed commit as True, but no commit message")
            return self.commit_tables(commit_message, None, allow_emtpy=allow_emtpy)

        return None

    def write_columns(
        self,
        table: str,
        columns: Mapping[str, List[Any]],
        on_duplicate_key_update: bool = True,
        create_if_not_exists: bool = False,
        primary_key: Optional[List[str]] = None,
        commit: bool = True,
        commit_message: Optional[str] = None,
        commit_date: Optional[datetime.datetime] = None,
        allow_empty: bool = False,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ):

        rows = columns_to_rows(columns)
        return self.write_rows(
            table,
            rows,
            on_duplicate_key_update,
            create_if_not_exists,
            primary_key,
            commit,
            commit_message,
            commit_date,
            allow_empty,
            batch_size,
        )

    def write_file(
        self,
        table: str,
        file_path: str,
        on_duplicate_key_update: bool = True,
        create_if_not_exists: bool = False,
        primary_key: Optional[List[str]] = None,
        commit: bool = True,
        commit_message: Optional[str] = None,
        commit_date: Optional[datetime.datetime] = None,
        allow_empty: bool = False,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ):

        with open(file_path, "r") as file_handle:
            reader = csv.DictReader(file_handle)
            rows = [row for row in reader]
        return self.write_rows(
            table,
            rows,
            on_duplicate_key_update,
            create_if_not_exists,
            primary_key,
            commit,
            commit_message,
            commit_date,
            allow_empty,
            batch_size,
        )

    def write_pandas(
        self,
        table: str,
        df: pd.DataFrame,
        on_duplicate_key_update: bool = True,
        create_if_not_exists: bool = False,
        primary_key: Optional[List[str]] = None,
        commit: bool = False,
        commit_message: Optional[str] = None,
        commit_date: Optional[datetime.datetime] = None,
        allow_empty: bool = False,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ):
        dt_columns = df.select_dtypes(include=[np.datetime64]).columns
        rows = df.to_dict("records")

        for row in rows:
            for column in dt_columns:
                if pd.isna(row[column]):
                    row[column] = None

        return self.write_rows(
            table,
            rows,
            on_duplicate_key_update,
            create_if_not_exists,
            primary_key,
            commit,
            commit_message,
            commit_date,
            allow_empty,
            batch_size,
        )

    def write_rows(
        self,
        table_name: str,
        rows: Iterable[dict],
        on_duplicate_key_update: bool = True,
        create_if_not_exists: bool = False,
        primary_key: Optional[List[str]] = None,
        commit: bool = False,
        commit_message: Optional[str] = None,
        commit_date: Optional[datetime.datetime] = None,
        allow_empty: bool = False,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ):

        metadata = sa.MetaData(bind=self.engine)
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
            logger.info(f"Writing records {batch_start} through {batch_end} of {len(rows)} rows to Dolt")
            self._write_batch(table, batch, on_duplicate_key_update)

        if commit:
            return self.commit_tables(commit_message, table_name, allow_empty)

    @classmethod
    def _coerce_dates(cls, data: Iterable[dict]) -> List[dict]:
        """
        This is required to get dates into a string format that will be correctly picked up by the connector wire
        protocol.
        :param data:
        :return:
        """
        data_copy = []
        for row in data:
            row_copy = {}
            for col, val in row.items():
                if isinstance(val, datetime.date):
                    row_copy[col] = datetime.datetime.combine(val, datetime.time())
                else:
                    row_copy[col] = val
            data_copy.append(row_copy)

        return data_copy

    def _write_batch(self, table: sa.Table, rows: List[dict], on_duplicate_key_update: bool):
        rows = list(clean_types(rows))

        logger.info(f"Updating {len(rows)} rows")
        with self.engine.connect() as conn:
            statement = insert(table).values(rows)
            if on_duplicate_key_update:
                update_dict = {el.name: el for el in statement.inserted if not el.primary_key}
                statement = statement.on_duplicate_key_update(update_dict)

            conn.execute(statement)

    def read_columns(self, table: str, as_of: Optional[str] = None) -> Mapping[str, list]:
        return self.read_columns_sql(self._get_read_table_asof_query(table, as_of))

    def read_rows(self, table: str, as_of: Optional[str] = None) -> List[dict]:
        return self.read_rows_sql(self._get_read_table_asof_query(table, as_of))

    def read_pandas(self, table: str, as_of: Optional[str] = None) -> pd.DataFrame:
        return self.read_pandas_sql(self._get_read_table_asof_query(table, as_of))

    @classmethod
    def _get_read_table_asof_query(cls, table: str, as_of: Optional[str] = None) -> str:
        base_query = f"SELECT * FROM `{table}`"
        return f'{base_query} AS OF "{as_of}"' if as_of else base_query

    def read_columns_sql(self, sql: str) -> Mapping[str, list]:
        rows = self._read_table_sql(sql)
        columns = rows_to_columns(rows)
        return columns

    def read_rows_sql(self, sql: str) -> List[dict]:
        return self._read_table_sql(sql)

    def read_pandas_sql(self, sql: str) -> pd.DataFrame:
        with self.engine.connect() as conn:
            return pd.read_sql(sql, conn)

    def _read_table_sql(self, sql: str) -> List[dict]:
        with self.engine.connect() as conn:
            result = conn.execute(sql)
            return [dict(row) for row in result]

    def log(self) -> Dict:
        with self.engine.connect() as conn:
            res = conn.execute(Commit.get_log_table_query())
            commit_data = [dict(row) for row in res]
            commits = Commit.parse_dolt_log_table(commit_data)
            return commits

    # TODO
    #  we likely want to support committish semantics here, i.e. anything that can resolve to a commit
    def diff(
        self, from_commit: str, to_commit: str, table_or_tables: Union[str, List[str]]
    ) -> Mapping[str, pd.DataFrame]:
        tables = [table_or_tables] if isinstance(table_or_tables, str) else table_or_tables

        def get_query(table: str) -> str:
            return f"""
                SELECT
                    *
                FROM
                    dolt_diff_{table}
                WHERE
                    from_commit = '{from_commit}'
                    AND to_COMMIT = '{to_commit}'
            """

        result = {table: self.read_pandas_sql(get_query(table)) for table in tables}

        return result

    def tables(self) -> List[str]:
        with self.engine.connect() as conn:
            result = conn.execute("SHOW TABLES")
            return [row["Table"] for row in result]


class DoltSQLEngineContext(DoltSQLContext):
    def __init__(self, dolt: Dolt, server_config: ServerConfig):
        self.dolt = dolt
        self.database = dolt.repo_name
        self.server_config = server_config
        self.engine = self._get_engine()
        self.verify_connection()


class DoltSQLServerContext(DoltSQLContext):
    def __init__(self, dolt: Dolt, server_config: ServerConfig):
        self.dolt = dolt
        self.database = dolt.repo_name
        self.server_config = server_config
        self.engine = self._get_engine()
        self.server = None
        self.checkout_branch = None

    def __enter__(self):
        if not self.dolt.status().is_clean:
            # TODO better error messages
            raise ValueError("DoltSQLServerManager does not support ")
        if self.server_config.branch:
            current_branch, _ = self.dolt.branch()
            if current_branch.name != self.server_config.branch:
                self.dolt.checkout(self.server_config.branch)
                self.checkout_branch = current_branch.name

        self.start_server()
        self.verify_connection()
        return self

    def __exit__(self, *args):
        self.stop_server()
        if self.checkout_branch:
            self.dolt.checkout(self.checkout_branch)

    def start_server(self):
        """
        Start a MySQL Server process on local host using the parameters to configure behavior. The parameters are
        self-explanatory, but the config is a way to provide them as a YAML file rather than as function
        arguments.
        :return:
        """

        def inner(server_args):
            if self.server is not None:
                logger.warning("Server already running")

            log_file = SQL_LOG_FILE or os.path.join(self.dolt.repo_dir, "mysql_server.log")

            proc = Popen(
                args=["dolt"] + server_args,
                cwd=self.dolt.repo_dir,
                stdout=open(log_file, "w"),
                stderr=STDOUT,
            )

            self.server = proc

        args = ["sql-server"]

        if self.server_config.config:
            args.extend(["--config", self.server_config.config])
        else:
            if self.server_config.host:
                args.extend(["--host", self.server_config.host])
            if self.server_config.port:
                args.extend(["--port", str(self.server_config.port)])
            if self.server_config.user:
                args.extend(["--user", self.server_config.user])
            if self.server_config.password:
                args.extend(["--password", self.server_config.password])
            if self.server_config.timeout:
                args.extend(["--timeout", str(self.server_config.timeout)])
            if self.server_config.readonly:
                args.extend(["--readonly"])
            if self.server_config.loglevel:
                args.extend(["--loglevel", self.server_config.loglevel])
            if self.server_config.multi_db_dir:
                args.extend(["--multi-db-dir", self.server_config.multi_db_dir])
            if self.server_config.no_auto_commit:
                args.extend(["--no-auto-commit"])
            if self.server_config.max_connections:
                args.extend(["--max-connections", str(self.server_config.max_connections)])

        inner(args)

    def stop_server(self):
        """
        Stop the MySQL Server process this repo is running.
        :return:
        """
        if self.server is None:
            logger.warning("Server is not running")
            return

        self.server.kill()
        self.server = None
