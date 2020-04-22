from doltpy.core import Dolt
from doltpy.etl.sql_sync.tools import TargetWriter, SourceReader
from doltpy.etl.sql_sync.mysql import write_to_table as write_to_mysql_table, get_table_metadata
import logging
from typing import Iterable, List, Mapping

logger = logging.getLogger(__name__)


def get_source_reader(repo: Dolt, latest: bool = True, commit_ref: str = None, branch: str = None) -> SourceReader:
    """
    Returns a function that takes a list of tables
    :param repo:
    :param latest:
    :param commit_ref:
    :param branch:
    :return:
    """
    def inner(tables: List[str]) -> Mapping[str, Iterable[tuple]]:
        if branch is not None and branch != repo.get_current_branch():
            repo.checkout(branch)
        result = {}
        repo_tables = repo.get_existing_tables()
        missing_tables = [table for table in tables if table not in repo_tables]
        if missing_tables:
            logger.error('The following tables are missign, exiting:\n{}'.format(missing_tables))
            raise ValueError('Missing tables {}'.format(missing_tables))

        for table in tables:
            logger.info('Syncing tables: {}'.format(tables))
            result[table] = read_from_table(table, repo, latest, commit_ref)

        return result

    return inner


def get_target_writer() -> TargetWriter:
    raise NotImplemented()


def read_from_table(table_name: str, repo: Dolt, latest: bool = True, commit_ref: str = None) -> List[tuple]:
    """
    This function pulls data from Dolt in three different modes:
        - the whole table if latest is False and commit_ref is None
        - the data introduced at the latest commit if latest is True
        - the data introduced at commit_ref if commit_ref is not None
    Throws an error if both latest is True and commit_ref is not None
    :param table_name:
    :param repo:
    :param latest:
    :param commit_ref:
    :return:
    """
    assert not latest and commit_ref is None, 'Currently only a whole Dolt table can be sync read'

    latest_commit = list(repo.get_commits())[0].hash
    assert_message = 'Cannot logically retrieve both the data introduced at {} (latest) and {}'.format(latest_commit,
                                                                                                       commit_ref)
    assert not (latest and commit_ref), assert_message

    if latest or commit_ref:
        commit = latest_commit if latest else commit_ref
        logger.info('Mode is latest={}, using data at commit {}'.format(latest, commit))
        data = get_data_for_commit(table_name, repo.cnx, commit)
    else:
        logger.info('Mode is latest={}, reading whole table'.format(latest))
        data = get_data_for_table(table_name, repo.cnx)

    logger.info('Retrieved {} rows from {}'.format(len(data), table_name))
    return data


# TODO this is currently not working as it knocks over the server
def get_data_for_commit(table_name: str, conn, commit_ref: str):
    """
    Uses the dolt_diff_<table> derived tables to grab the updates introduced at a commit.
    :param table_name:
    :param conn:
    :param commit_ref:
    :return:
    """
    table_metadata = get_table_metadata(table_name, conn)
    query = '''
        SELECT
            {columns}
        FROM
            dolt_diff_{table_name}
        WHERE
            to_commit = '{commit_hash}'
    '''.format(columns=','.join(['to_{} as col'.format(col.col_name) for col in table_metadata.columns]),
               table_name=table_name,
               commit_hash=commit_ref)
    cursor = conn.cursor()
    cursor.execute(query)
    return [tup for tup in cursor]


def get_data_for_table(table_name: str, conn):
    """

    :param table_name:
    :param conn:
    :return:
    """
    table_metadata = get_table_metadata(table_name, conn)
    query = '''
        SELECT
            {columns}
        FROM
            {table_name}
    '''.format(columns=','.join(col.col_name for col in table_metadata.columns), table_name=table_name)
    cursor = conn.cursor()
    cursor.execute(query)
    return [tup for tup in cursor]


def write_to_table(repo: Dolt, table: str, data: List[tuple]):
    write_to_mysql_table(table, repo.cnx, data, drop_duplicate_pks=False)
