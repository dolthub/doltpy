from doltpy.core import Dolt
from doltpy.etl.sql_sync.tools import TargetWriter, SourceReader, DatabaseUpdate, TableUpdate, TableMetadata
from doltpy.etl.sql_sync.mysql import write_to_table as write_to_mysql_table, get_table_metadata
import logging
from typing import List, Mapping, Callable, Tuple
from mysql.connector.connection import MySQLConnection
from datetime import datetime

logger = logging.getLogger(__name__)


def get_source_reader(repo: Dolt, reader: Callable[[str, Dolt], TableUpdate]) -> SourceReader:
    """
    Returns a function that takes a list of tables and returns a mapping from the table name to the data returned by
    the passed reader. The reader is generally one of `get_table_reader_diffs` or `get_table_reader`, but it would
    be easy enough to provide some other kind of function if neither of these meet your needs.
    :param repo:
    :param reader:
    :return:
    """
    def inner(tables: List[str]) -> Mapping[str, DatabaseUpdate]:
        result = {}
        repo_tables = repo.get_existing_tables()
        missing_tables = [table for table in tables if table not in repo_tables]
        if missing_tables:
            logger.error('The following tables are missign, exiting:\n{}'.format(missing_tables))
            raise ValueError('Missing tables {}'.format(missing_tables))

        for table in tables:
            logger.info('Syncing tables: {}'.format(tables))
            result[table] = reader(table, repo)

        return result

    return inner


def get_table_reader_diffs(commit_ref: str = None, branch: str = None) -> Callable[[str, Dolt], TableUpdate]:
    """
    Returns a function that reads the diff from a commit and/or branch, defaults to the HEAD of the current branch if
    neither are provided.
    :param commit_ref:
    :param branch
    :return:
    """
    def inner(table_name: str, repo: Dolt) -> TableUpdate:
        if branch and branch != repo.get_current_branch():
            repo.checkout(branch)

        from_commit, to_commit = get_from_commit_to_commit(repo, commit_ref)
        connection = repo.get_connection()
        table_metadata = get_table_metadata(table_name, connection)
        pks_to_drop = get_dropped_pks(table_metadata, connection, from_commit, to_commit)
        result = _read_from_dolt_diff(table_metadata, connection, from_commit, to_commit)
        connection.close()
        return pks_to_drop, result

    return inner


def get_dropped_pks(table_metadata: TableMetadata, conn, from_commit: str, to_commit: str) -> List[tuple]:
    """
    Given table_metadata, a connection, and a pair of commits, will return the list of pks that were dropped between
    the two commits.
    :param table_metadata:
    :param conn:
    :param from_commit:
    :param to_commit:
    :return:
    """
    pks = [col.col_name for col in table_metadata.columns if col.key]
    query = '''
        SELECT
            {pks}
        FROM
            dolt_diff_{table_name}
        WHERE
            from_commit = '{from_commit}'
            AND to_commit = '{to_commit}'
            AND diff_type = 'removed'
    '''.format(pks=','.join(['from_{}'.format(pk) for pk in pks]),
               table_name=table_metadata.name,
               from_commit=from_commit,
               to_commit=to_commit)

    cursor = conn.cursor()
    cursor.execute(query)
    result = [tup for tup in cursor]
    return result


def get_from_commit_to_commit(repo: Dolt, commit_ref: str = None) -> Tuple[str, str]:
    """
    Given a repo and commit it returns the commit and its parent, if no commit is provided the head and the parent of
    head are returned.
    :param repo:
    :param commit_ref:
    :return:
    """
    commits = list(repo.get_commits().keys())
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


def get_table_reader(commit_ref: str = None, branch: str = None) -> Callable[[str, Dolt], TableUpdate]:
    """
    Returns a function that reads the entire table at a commit and/or branch, and returns the data.
    :param commit_ref:
    :param branch:
    :return:
    """
    def inner(table_name: str, repo: Dolt) -> TableUpdate:
        if branch and branch != repo.get_current_branch():
            repo.checkout(branch)

        connection = repo.get_connection()
        query_commit = commit_ref or list(repo.get_commits().keys())[0]
        table_metadata = get_table_metadata(table_name, connection)
        from_commit, to_commit = get_from_commit_to_commit(repo, query_commit)
        pks_to_drop = get_dropped_pks(table_metadata, connection, from_commit, to_commit)
        result = _read_from_dolt_history(table_metadata, connection, query_commit)
        connection.close()
        return pks_to_drop, result

    return inner


def _read_from_dolt_diff(table_metadata: TableMetadata, conn: MySQLConnection, from_commit: str, to_commit: str):
    query = '''
        SELECT
            {columns}
        FROM
            dolt_diff_{table_name}
        WHERE
            from_commit = '{from_commit}'
            AND to_commit = '{to_commit}'
            AND diff_type != 'removed'
    '''.format(columns=','.join(['to_{}'.format(col.col_name) for col in table_metadata.columns]),
               table_name=table_metadata.name,
               from_commit=from_commit,
               to_commit=to_commit)
    return _query_helper(conn, query)


def _read_from_dolt_history(table_metadata: TableMetadata, conn: MySQLConnection, commit_ref: str):
    query = '''
        SELECT
            {columns}
        FROM
            dolt_history_{table_name}
        WHERE
            commit_hash = '{commit_ref}'
    '''.format(columns=','.join(col.col_name for col in table_metadata.columns),
               table_name=table_metadata.name,
               commit_ref=commit_ref)
    return _query_helper(conn, query)


def _query_helper(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    return [tup for tup in cursor]


def write_to_table(repo: Dolt, table: str, data: List[tuple], commit: bool = False, message: str = None):
    """
    Given a repo, table, and data, will try and use the repo's MySQL Server instance to write the provided data to the
    table.
    :param repo:
    :param table:
    :param data:
    :param commit:
    :param message:
    :return:
    """
    connection = repo.get_connection()
    table_metadata = get_table_metadata(table, connection)
    write_to_mysql_table(table_metadata, connection, data, update_on_duplicate=False)
    connection.close()
    if commit:
        repo.add_table_to_next_commit(table)
        message = message or 'Inserting {} records at '.format(len(data), datetime.now())
        repo.commit(message)
