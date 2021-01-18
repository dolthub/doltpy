from doltpy.cli.dolt import Dolt
from doltpy.sql.sync.db_tools import (get_table_metadata,
                                      DoltAsTargetWriter,
                                      DoltTableUpdate,
                                      DoltAsSourceReader,
                                      DoltAsSourceUpdate,
                                      DoltAsTargetUpdate,
                                      drop_primary_keys)
from doltpy.sql import write_rows, commit_tables
from doltpy.sql.write import get_existing_pks, hash_row_els
from typing import List, Callable, Tuple
from sqlalchemy.engine import Engine
from sqlalchemy import Table, MetaData
import logging


logger = logging.getLogger(__name__)


def get_target_writer(engine: Engine,
                      branch: str = None,
                      commit: bool = True,
                      message: str = None) -> DoltAsTargetWriter:
    """
    Given a repo, writes to the specified branch (defaults to current), and optionally commits with the provided
    message or generates a standard one.
    :param engine:
    :param branch:
    :param commit:
    :param message:
    :return: 
    """
    def inner(table_data_map: DoltAsTargetUpdate):
        # TODO need to fix the branch interaction where
        current_branch, _ = repo.branch()
        if branch and branch != current_branch:
            repo.checkout(branch)

        metadata = MetaData(bind=engine)
        metadata.reflect()

        for table_name, table_update in table_data_map.items():
            table = metadata.tables[table_name]
            data = table_update
            drop_missing_pks(engine, table, list(data))
            write_rows(engine, table.name, list(data), on_duplicate_key_update=True)

        if commit:
            tables = [table_name for table_name, _ in table_data_map.items()]
            return commit_tables(engine, message or 'Executed SQL sync', tables, True)

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

    if not existing_pks:
        return

    pk_cols = [col.name for col in table.columns if col.primary_key]
    proposed_pks_set = set([hash_row_els(row, pk_cols) for row in data])

    pks_to_drop = []
    for pk_hash, pk in existing_pks.items():
        if pk_hash not in proposed_pks_set:
            pks_to_drop.append(pk)

    if pks_to_drop:
        drop_primary_keys(engine, table, pks_to_drop)


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
            logger.info('Reading table: {}'.format(table))
            result[table] = reader(table, repo)

        return result

    return inner


def get_table_reader_diffs(commit_ref: str = None, branch: str = None) -> Callable[[str, Dolt], DoltTableUpdate]:
    """
    Returns a function that reads the diff from a commit and/or branch, defaults to the HEAD of the current branch if
    neither are provided.
    :param commit_ref:
    :param branch:
    :return:
    """
    def inner(table_name: str, repo: Dolt) -> DoltTableUpdate:
        current_branch, _ = repo.branch()
        if branch and branch != current_branch:
            repo.checkout(branch)

        from_commit, to_commit = get_from_commit_to_commit(repo, commit_ref)
        metadata = MetaData(bind=repo.get_engine())
        metadata.reflect()
        table = metadata.tables[table_name]
        pks_to_drop = get_dropped_pks(repo.get_engine(), table, from_commit, to_commit)
        result = _read_from_dolt_diff(repo.get_engine(), table, from_commit, to_commit)
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
    '''.format(pks=','.join(['`from_{}` as {}'.format(pk, pk) for pk in pks]),
               table_name=table.name,
               from_commit=from_commit,
               to_commit=to_commit)

    return _query_helper(engine, query)


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


def get_table_reader(commit_ref: str = None, branch: str = None) -> Callable[[str, Dolt], DoltTableUpdate]:
    """
    Returns a function that reads the entire table at a commit and/or branch, and returns the data.
    :param commit_ref:
    :param branch:
    :return:
    """
    def inner(table_name: str, repo: Dolt) -> DoltTableUpdate:
        if branch and branch != repo.log():
            repo.checkout(branch)

        query_commit = commit_ref or list(repo.log().keys())[0]
        table = get_table_metadata(repo.get_engine(), table_name)
        from_commit, to_commit = get_from_commit_to_commit(repo, query_commit)
        pks_to_drop = get_dropped_pks(repo.get_engine(), table, from_commit, to_commit)
        result = _read_from_dolt_history(repo.get_engine(), table, query_commit)
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
    '''.format(columns=','.join(['`to_{}` as {}'.format(col.name, col.name) for col in table.columns]),
               table_name=table.name,
               from_commit=from_commit,
               to_commit=to_commit)

    return _query_helper(engine, query)


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

    return _query_helper(engine, query)


def _query_helper(engine: Engine, query: str):
    with engine.connect() as conn:
        result = conn.execute(query)
        return [dict(row) for row in result]
