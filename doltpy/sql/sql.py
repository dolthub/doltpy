import copy
from collections import OrderedDict
import csv
import logging
import math
import os
import datetime

# from doltpy.shared import SQL_LOG_FILE
from subprocess import STDOUT, Popen
from typing import Any, Iterable, List, Mapping, Union, Optional

import pandas as pd # type: ignore
import sqlalchemy as sa  # type: ignore
from retry import retry
from sqlalchemy import create_engine  # type: ignore
from sqlalchemy.engine import Engine  # type: ignore

from doltpy.cli import Dolt
from doltpy.shared import columns_to_rows, rows_to_columns
from doltpy.sql.helpers import infer_table_schema, get_inserts_and_updates, clean_types

logger = logging.getLogger(__name__)

DEFAULT_HOST, DEFAULT_PORT = "127.0.0.1", 3306
SQL_LOG_FILE = "temp_log"
DEFAULT_BATCH_SIZE = 100000


class ServerConfig:
    def __init__(
        self,
        branch: Optional[str] = None,
        config: Optional[str] = None,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        user: Optional[str] = None,
        password: Optional[str] = None,
        timeout: int = None,
        readonly: bool = None,
        loglevel: Optional[str] = None,
        multi_db_dir: Optional[str] = None,
        no_auto_commit: bool = None,
        echo: bool = False,
    ):
        self.branch = branch
        self.config = config
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.timeout = timeout
        self.readonly = readonly
        self.loglevel = loglevel
        self.multi_db_dir = multi_db_dir
        self.no_auto_commit = no_auto_commit
        self.echo = echo


class DoltCommit:
    """
    Represents metadata about a commit, including a ref, timestamp, and author, to make it easier to sort and present
    to the user.
    """
    def __init__(self,
                 ref: str,
                 ts: datetime,
                 author: str,
                 email: str,
                 message: str,
                 parent_or_parents: Union[str, Tuple[str, str]] = None):
        self.hash = ref
        self.ts = ts
        self.author = author
        self.email = email
        self.message = message
        self.parent_or_parents = parent_or_parents

    def __str__(self):
        return f'{self.hash}: {self.author} @ {self.ts}, {self.message}'

    def is_merge(self):
        return isinstance(self.parent_or_parents, tuple)

    def append_merge_parent(self, other_merge_parent: str):
        if isinstance(self.parent_or_parents, tuple):
            raise ValueError('Already has a merge parent set')
        self.parent_or_parents = (self.parent_or_parents, other_merge_parent)


class DoltSQLContext:
    dolt: Dolt
    server_config: ServerConfig
    engine: Engine

    def _get_engine(self) -> Engine:
        """
        Get a connection to ths server process that this repo is running, raise an exception if it is not running.
        :return:
        """
        database = self.dolt.repo_name
        user = self.server_config.user
        host = self.server_config.host
        port = self.server_config.port

        logger.info(
            f"Creating engine for Dolt SQL Server instance running on {host}:{port}"
        )

        def inner():
            return create_engine(
                f"mysql+mysqlconnector://{user}@{host}:{port}/{database}",
                echo=self.server_config.echo,
            )

        return inner()

    @retry(delay=2, tries=10, exceptions=(sa.exc.OperationalError, sa.exc.DatabaseError, sa.exc.InterfaceError))
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
        if isinstance(table_or_tables, str):
            tables = [table_or_tables]
        elif isinstance(table_or_tables, list):
            tables = table_or_tables
        with self.engine.connect() as conn:
            if tables:
                for table in tables:
                    conn.execute(f"SELECT DOLT_ADD('{table}')")
                dolt_commit_args = f"'-m', '{commit_message}'"
            else:
                dolt_commit_args = f"'-a', '-m', '{commit_message}'"
            result = [
                dict(row)
                for row in conn.execute(f"SELECT DOLT_COMMIT({dolt_commit_args})")
            ]
            assert len(result) == 1, "Expected a single returned row with a commit hash"
            return result[0]["commit_hash"]

    def execute(
        self,
        sql: str,
        commit: bool = False,
        commit_message: Optional[str] = None,
        allow_emtpy: bool = False,
    ):
        with self.engine.connect() as conn:
            conn.execute(sql)

        if commit:
            if not commit_message:
                raise ValueError("Passed commit as True, but no commit message")
            return self.commit_tables(commit_message, None, allow_emtpy=allow_emtpy)

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

        return self.write_rows(
            table,
            df.to_dict("records"),
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
            logger.info(
                f"Writing records {batch_start} through {batch_end} of {len(rows)} rows to Dolt"
            )
            self.write_batch(table, batch, on_duplicate_key_update)

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

    def write_batch(
        self, table: sa.Table, rows: List[dict], on_duplicate_key_update: bool
    ):
        coerced_data = list(clean_types(rows))
        inserts, updates = get_inserts_and_updates(self.engine, table, coerced_data)

        if not on_duplicate_key_update and updates:
            raise ValueError(
                "Duplicate keys present, but on_duplicate_key_update is off"
            )

        if inserts:
            logger.info(f"Inserting {len(inserts)} rows")
            with self.engine.connect() as conn:
                conn.execute(table.insert(), inserts)

        # We need to prefix the columns with "_" in order to use bindparam properly
        _updates = copy.deepcopy(updates)
        for dic in _updates:
            for col in list(dic.keys()):
                dic[f"_{col}"] = dic.pop(col)

        if _updates:
            logger.info(f"Updating {len(_updates)} rows")
            with self.engine.connect() as conn:
                statement = table.update()
                for pk_col in [col.name for col in table.columns if col.primary_key]:
                    statement = statement.where(
                        table.c[pk_col] == sa.bindparam(f"_{pk_col}")
                    )
                non_pk_cols = [col.name for col in table.columns if not col.primary_key]
                statement = statement.values(
                    {col: sa.bindparam(f"_{col}") for col in non_pk_cols}
                )
                conn.execute(statement, _updates)

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

    def log(self) -> OrderedDict:
        query = f'''
            SELECT
                da.`commit_hash`,
                dca.`parent_hash`,
                `email`,
                `date`,
                `message`
            FROM
                dolt_commits AS dc
                LEFT OUTER JOIN dolt_commit_ancestors AS dca
                    ON dc.commit_hash = dca.commit_hash
            ORDER BY
                `date` DESC
        '''
        with self.engine.connect() as conn:
            commits = OrderedDict()
            res = conn.execute(query)
            for row in res:
                ref = row['commit_hash']
                if ref in commits:
                    commits[ref].append_merge_parent(row['parent_hash'])
                else:
                    ref = row['commit_hash']
                    commit = DoltCommit(ref=row['commit_hash'],
                                        ts=row['date'],
                                        author=row['commiter'],
                                        email=row['email'],
                                        message=row['message'],
                                        parent_or_parents=row['parent_hash'])
                    commits[ref] = commit

            return commits


class DoltSQLEngineContext(DoltSQLContext):
    def __init__(self, dolt: Dolt, server_config: ServerConfig):
        self.dolt = dolt
        self.server_config = server_config
        self.engine = self._get_engine()
        self.verify_connection()


class DoltSQLServerContext(DoltSQLContext):
    def __init__(self, dolt: Dolt, server_config: ServerConfig):
        self.dolt = dolt
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

            log_file = SQL_LOG_FILE or os.path.join(
                self.dolt.repo_dir(), "mysql_server.log"
            )

            proc = Popen(
                args=["dolt"] + server_args,
                cwd=self.dolt.repo_dir(),
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
