from doltpy.core.dolt import Dolt, DEFAULT_HOST, DEFAULT_PORT
from doltpy.core.system_helpers import get_logger
from doltpy.etl.sql_sync.db_tools import (get_table_metadata,
                                          DoltAsTargetWriter,
                                          DoltTableUpdate,
                                          DoltAsSourceReader,
                                          DoltAsSourceUpdate,
                                          DoltAsTargetUpdate)
from typing import List, Callable, Tuple
from mysql.connector.connection import MySQLConnection
from datetime import datetime
from sqlalchemy.engine import Engine
from sqlalchemy import Table, MetaData, update

logger = get_logger(__name__)


def get_target_writer(repo: Dolt,
                      branch: str = None,
                      commit: bool = True,
                      message: str = None,
                      dolt_server_host: str = DEFAULT_HOST,
                      dolt_server_port: int = DEFAULT_PORT) -> DoltAsTargetWriter:
    """
    Given a repo, writes to the specified branch (defaults to current), and optionally commits with the provided
    message or generates a standard one.
    :param repo:
    :param branch:
    :param commit:
    :param message:
    :param dolt_server_host:
    :param dolt_server_port:
    :return:
    """
    def inner(table_data_map: DoltAsTargetUpdate):
        current_branch, _ = repo.branch()
        if branch and branch != current_branch:
            repo.checkout(branch)

        engine = repo.get_connection(host=dolt_server_host, port=dolt_server_port)

        for table_name, table_update in table_data_map.items():
            table = get_table_metadata(engine, table_name)
            data = table_update
            drop_missing_pks(engine, table, list(data))
            write_to_table(repo, table, list(data), False)

        if commit:
            for table, _ in table_data_map.items():
                repo.add(table)
            commit_message = message or 'Execute write for sync to Dolt'
            repo.commit(commit_message)

    return inner


def drop_missing_pks(engine: Engine, table: Table, data: List[dict]):
    """
    This a very basic n-squared implementation for dropping the primary keys present in Dolt that have been dropped in
    the target database.
    :param engine:
    :param table:
    :param data:
    :return:
    """
    existing_pks = get_existing_pks(engine, table)
    pk_cols = [col.name for col in table.columns if col.primary_key]
    proposed_pks = set({pk_col: row[pk_col] for pk_col in pk_cols} for row in data)
    pks_to_drop = [existing_pk for existing_pk in existing_pks if existing_pk not in proposed_pks]
    with engine.connect() as conn:
        conn.execute(table.delete(), pks_to_drop)


def get_existing_pks(engine: Engine, table: Table) -> List[dict]:
    with engine.connect() as conn:
        result = conn.execute(table.select(columns=[col.name for col in table.columns if col.primary_key]))
        return result


def get_source_reader(repo: Dolt, reader: Callable[[str, Dolt], DoltTableUpdate]) -> DoltAsSourceReader:
    """
    Returns a function that takes a list of tables and returns a mapping from the table name to the data returned by
    the passed reader. The reader is generally one of `get_table_reader_diffs` or `get_table_reader`, but it would
    be easy enough to provide some other kind of function if neither of these meet your needs.
    :param repo:
    :param reader:
    :return:
    """
    def inner(tables: List[str]) -> DoltAsSourceUpdate:
        result = {}
        repo_tables = [table.name for table in repo.ls()]
        missing_tables = [table for table in tables if table not in repo_tables]
        if missing_tables:
            logger.error('The following tables are missign, exiting:\n{}'.format(missing_tables))
            raise ValueError('Missing tables {}'.format(missing_tables))

        for table in tables:
            logger.info('Reading tables: {}'.format(tables))
            result[table] = reader(table, repo)

        return result

    return inner


def get_table_reader_diffs(commit_ref: str = None,
                           branch: str = None,
                           dolt_server_host: str = DEFAULT_HOST,
                           dolt_server_port: int = DEFAULT_PORT) -> Callable[[str, Dolt], DoltTableUpdate]:
    """
    Returns a function that reads the diff from a commit and/or branch, defaults to the HEAD of the current branch if
    neither are provided.
    :param commit_ref:
    :param branch:
    :param dolt_server_host:
    :param dolt_server_port:
    :return:
    """
    def inner(table_name: str, repo: Dolt) -> DoltTableUpdate:
        current_branch, _ = repo.branch()
        if branch and branch != current_branch:
            repo.checkout(branch)

        from_commit, to_commit = get_from_commit_to_commit(repo, commit_ref)
        engine = repo.get_connection(host=dolt_server_host, port=dolt_server_port)
        table_metadata = get_table_metadata(engine, table_name)
        pks_to_drop = get_dropped_pks(engine, table_metadata, from_commit, to_commit)
        result = _read_from_dolt_diff(engine, table_metadata, from_commit, to_commit)
        return pks_to_drop, result

    return inner


def get_dropped_pks(engine: Engine, table: Table, from_commit: str, to_commit: str) -> List[dict]:
    """
    Given table_metadata, a connection, and a pair of commits, will return the list of pks that were dropped between
    the two commits.
    :param engine:
    :param table:
    :param from_commit:
    :param to_commit:
    :return:
    """
    pks = [col.name for col in table.columns if col.primary_key]
    query = '''
        SELECT
            {pks}
        FROM
            dolt_diff_{table_name}
        WHERE
            from_commit = '{from_commit}'
            AND to_commit = '{to_commit}'
            AND diff_type = 'removed'
    '''.format(pks=','.join(['`from_{}`'.format(pk) for pk in pks]),
               table_name=table.name,
               from_commit=from_commit,
               to_commit=to_commit)

    with engine.connect() as conn:
        result = conn.execute(query)
        return [row for row in result]


def get_from_commit_to_commit(repo: Dolt, commit_ref: str = None) -> Tuple[str, str]:
    """
    Given a repo and commit it returns the commit and its parent, if no commit is provided the head and the parent of
    head are returned.
    :param repo:
    :param commit_ref:
    :return:
    """
    commits = list(repo.log().keys())
    commit_ref_index = None
    if not commit_ref:
        commit_ref_index = 0
    else:
        for i, commit in enumerate(commits):
            if commit == commit_ref:
                commit_ref_index = i
                break
    assert commit_ref_index is not None, 'commit_ref not found in commit index'
    return commits[commit_ref_index + 1], commits[commit_ref_index]


def get_table_reader(commit_ref: str = None,
                     branch: str = None,
                     dolt_server_host: str = DEFAULT_HOST,
                     dolt_server_port: int = DEFAULT_PORT) -> Callable[[str, Dolt], DoltTableUpdate]:
    """
    Returns a function that reads the entire table at a commit and/or branch, and returns the data.
    :param commit_ref:
    :param branch:
    :param dolt_server_host:
    :param dolt_server_port:
    :return:
    """
    def inner(table_name: str, repo: Dolt) -> DoltTableUpdate:
        if branch and branch != repo.log():
            repo.checkout(branch)

        engine = repo.get_connection(host=dolt_server_host, port=dolt_server_port)
        query_commit = commit_ref or list(repo.log().keys())[0]
        table = get_table_metadata(engine, table_name)
        from_commit, to_commit = get_from_commit_to_commit(repo, query_commit)
        pks_to_drop = get_dropped_pks(engine, table, from_commit, to_commit)
        result = _read_from_dolt_history(engine, table, query_commit)
        return pks_to_drop, result

    return inner


def _read_from_dolt_diff(engine: Engine, table: Table, from_commit: str, to_commit: str) -> List[dict]:
    query = '''
        SELECT
            {columns}
        FROM
            dolt_diff_{table_name}
        WHERE
            from_commit = '{from_commit}'
            AND to_commit = '{to_commit}'
            AND diff_type != 'removed'
    '''.format(columns=','.join(['`to_{}`'.format(col.name) for col in table.columns]),
               table_name=table.name,
               from_commit=from_commit,
               to_commit=to_commit)

    with engine.connect() as conn:
        result = conn.execute(query)
        return [row for row in result]


def _read_from_dolt_history(engine: Engine, table: Table, commit_ref: str) -> List[dict]:
    query = '''
        SELECT
            {columns}
        FROM
            dolt_history_{table_name}
        WHERE
            commit_hash = '{commit_ref}'
    '''.format(columns=','.join('`{}`'.format(col.name) for col in table.columns),
               table_name=table.name,
               commit_ref=commit_ref)

    with engine.connect() as conn:
        result = conn.execute(query)
        return [row for row in result]


def write_to_table(repo: Dolt,
                   table_name: str,
                   data: List[dict],
                   commit: bool = False,
                   message: str = None,
                   dolt_server_host: str = DEFAULT_HOST,
                   dolt_server_port: int = DEFAULT_PORT):
    """
    Given a repo, table, and data, will try and use the repo's MySQL Server instance to write the provided data to the
    table. Since Dolt does not yet support ON DUPLICATE KEY clause to INSERT statements we also have to separate
    updates from inserts and run sets of statements.
    :param repo:
    :param table_name:
    :param data:
    :param commit:
    :param message:
    :param dolt_server_host:
    :param dolt_server_port:
    :return:
    """
    engine = repo.get_connection(host=dolt_server_host, port=dolt_server_port)
    table = get_table_metadata(engine, table_name)
    inserts, updates = get_inserts_and_updates(engine, table, data)
    if inserts:
        logger.info('Inserting {} rows'.format(len(inserts)))
        with engine.connect() as conn:
            conn.execute(table.insert(), inserts)

    if updates:
        logger.info('Updating {} rows'.format(len(updates)))
        with engine.connect() as conn:
            conn.execute(table.update(), updates)

    if commit:
        repo.add(table.name)
        message = message or 'Inserting {} records at '.format(len(data), datetime.now())
        repo.commit(message)


def get_inserts_and_updates(engine: Engine, table: Table, data: List[dict]) -> Tuple[List[dict], List[dict]]:
    existing_pks = get_existing_pks(engine, table)
    if not existing_pks:
        return data, []

    pk_cols_to_index = {col.col_name: i for i, col in enumerate(table.columns) if col.primary_key}
    inserts, updates = [], []
    for row in data:
        row_pk = tuple(row[i] for _, i in pk_cols_to_index.items())
        if row_pk in existing_pks:
            updates.append(row)
        else:
            inserts.append(row)

    return inserts, updates
