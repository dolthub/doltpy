from doltpy.core.dolt import Dolt, DEFAULT_HOST, DEFAULT_PORT
from doltpy.etl.sql_sync.db_tools import (write_to_table as write_to_table_helper,
                                          drop_primary_keys,
                                          get_filters,
                                          DoltAsTargetWriter,
                                          TableMetadata,
                                          DoltTableUpdate,
                                          DoltAsSourceReader,
                                          DoltAsSourceUpdate,
                                          DoltAsTargetUpdate)
from doltpy.etl.sql_sync.mysql import (get_table_metadata as get_mysql_table_metadata,
                                       get_insert_query as get_mysql_insert_query)
import logging
from typing import List, Callable, Tuple
from mysql.connector.connection import MySQLConnection
from datetime import datetime

logger = logging.getLogger(__name__)


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

        conn = repo.get_connection(host=dolt_server_host, port=dolt_server_port)

        for table, table_update in table_data_map.items():
            table_metadata = get_mysql_table_metadata(table, conn)
            data = table_update
            drop_missing_pks(conn, table_metadata, list(data))
            conn.close()
            write_to_table(repo, table, list(data), False)

        conn.close()

        if commit:
            for table, _ in table_data_map.items():
                repo.add(table)
            commit_message = message or 'Execute write for sync to Dolt'
            repo.commit(commit_message)

    return inner


def drop_missing_pks(conn: MySQLConnection, table_metadata: TableMetadata, data: List[tuple]):
    """
    This a very basic n-squared implementation for dropping the primary keys present in Dolt that have been dropped in
    the target database.
    :param conn:
    :param table_metadata:
    :param data:
    :return:
    """
    pk_cols_to_index = {col.col_name: i for i, col in enumerate(table_metadata.columns) if col.key}
    pk_cols = sorted([pk_col_name for pk_col_name in pk_cols_to_index.keys()])
    existing_pks = get_existing_pks(conn, table_metadata, pk_cols)
    proposed_pks = [tuple(proposed_row[i] for _, i in pk_cols_to_index.items()) for proposed_row in data]
    pks_to_drop = [existing_pk for existing_pk in existing_pks if existing_pk not in proposed_pks]
    drop_primary_keys(conn, table_metadata, pks_to_drop)


def get_existing_pks(conn: MySQLConnection, table_metadata: TableMetadata, pks: List[str]):
    query = '''
    
        SELECT
            {pk_list}
        FROM
            {table_name}
    '''.format(pk_list=','.join(pks), table_name=table_metadata.name)
    return _query_helper(conn, query)


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
        connection = repo.get_connection(host=dolt_server_host, port=dolt_server_port)
        table_metadata = get_mysql_table_metadata(table_name, connection)
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

        connection = repo.get_connection(host=dolt_server_host, port=dolt_server_port)
        query_commit = commit_ref or list(repo.log().keys())[0]
        table_metadata = get_mysql_table_metadata(table_name, connection)
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


def write_to_table(repo: Dolt,
                   table_name: str,
                   data: List[tuple],
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
    connection = repo.get_connection(host=dolt_server_host, port=dolt_server_port)
    table_metadata = get_mysql_table_metadata(table_name, connection)
    inserts, updates = get_inserts_and_updates(connection, table_metadata, data)
    if inserts:
        logger.info('Inserting {} rows'.format(len(inserts)))
        write_to_table_helper(connection, table_metadata, get_mysql_insert_query, inserts, update_on_duplicate=False)
    if updates:
        logger.info('Updating {} rows'.format(len(updates)))
        update_rows(connection, table_metadata, updates)
    connection.close()
    if commit:
        repo.add(table_metadata.name)
        message = message or 'Inserting {} records at '.format(len(data), datetime.now())
        repo.commit(message)


def get_inserts_and_updates(connection: MySQLConnection,
                            table_metadata: TableMetadata,
                            data: List[tuple]) -> Tuple[List[tuple], List[tuple]]:
    existing_pks = get_existing_pks(connection,
                                    table_metadata,
                                    sorted(col.col_name for col in table_metadata.columns if col.key))
    if not existing_pks:
        return data, []

    pk_cols_to_index = {col.col_name: i for i, col in enumerate(table_metadata.columns) if col.key}
    inserts, updates = [], []
    for row in data:
        row_pk = tuple(row[i] for _, i in pk_cols_to_index.items())
        if row_pk in existing_pks:
            updates.append(row)
        else:
            inserts.append(row)

    return inserts, updates


def update_rows(connection: MySQLConnection, table_metadata: TableMetadata,  data: List[tuple]):
    query_template = '''
        UPDATE
            {table_name}
        SET
            {update_assignments}
        WHERE
            {pk_filter}
    '''
    update_assignments = ','.join('{} = %s'.format(col.col_name) for col in table_metadata.columns if not col.key)
    pk_cols_to_index = {col.col_name: i for i, col in enumerate(table_metadata.columns) if col.key}

    pk_filter = get_filters(list(pk_cols_to_index.keys()))
    query = query_template.format(table_name=table_metadata.name,
                                  update_assignments=update_assignments,
                                  pk_filter=pk_filter)

    rows_with_pks = []
    non_pk_cols_to_index = [i for i, col in enumerate(table_metadata.columns) if not col.key]
    for row in data:
        combined = tuple([row[i] for i in non_pk_cols_to_index] + [row[i] for _, i in pk_cols_to_index.items()])
        rows_with_pks.append(combined)

    cursor = connection.cursor()
    cursor.executemany(query, rows_with_pks)
    connection.commit()
