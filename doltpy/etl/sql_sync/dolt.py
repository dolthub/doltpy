from doltpy.core import Dolt
from doltpy.etl.sql_sync.tools import TargetWriter, SourceReader
import logging
from typing import Iterable, List, Mapping

logger = logging.getLogger(__name__)


def get_source_reader(repo: Dolt, latest: bool = True, branch: str = None) -> SourceReader:
    def inner(tables: List[str]) -> Mapping[str, Iterable[tuple]]:
        result = {}
        for table in tables:
            # TODO sort out the branch/commit ref issue, currently doing nother with either
            result[table] = read_from_table(table, repo, latest, None)

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
    :param latest:
    :param commit_ref:
    :return:
    """
    latest_commit = list(repo.get_commits())[0].hash
    assert_message = 'Cannot logically retrieve both the data introduced at {} (latest) and {}'.format(latest_commit,
                                                                                                       commit_ref)
    assert not (latest and commit_ref), assert_message

    if latest or commit_ref:
        commit = latest_commit if latest else commit_ref
        logger.info('Mode is latest={}, using data at commit {}'.format(latest, commit))
        data = get_data_for_commit(table_name, repo.cnx.cursor(), commit)
    else:
        logger.info('Mode is latest={}, reading whole table'.format(latest))
        data = get_data_for_table(table_name, repo.cnx.cursor())

    logger.info('Retrieved {} rows from {}'.format(len(data), table_name))
    return data


# TODO this isn't right
def get_data_for_commit(table_name: str, cursor, commit_ref: str = None):
    columns = get_dolt_columns(table_name, cursor)
    query = '''
        SELECT
            {columns}
        FROM
            dolt_history_{table_name}
        WHERE
            commit_hash = '{commit_hash}'
    '''.format(columns=','.join(columns),
               table_name=table_name,
               commit_hash=commit_ref)
    cursor.execute(query)
    return [tup for tup in cursor]


def get_data_for_table(table_name: str, cursor):
    """

    :param table_name:
    :param columns:
    :param cursor:
    :return:
    """
    columns = get_dolt_columns(table_name, cursor)
    query = '''
        SELECT
            {columns}
        FROM
            {table_name}
    '''.format(columns=','.join(columns), table_name=table_name)

    cursor.execute(query)
    return [tup for tup in cursor]


def get_dolt_columns(table_name: str, cursor, pks_only = False) -> List[str]:
    """

    :param table_name:
    :param cursor:
    :param pks_only:
    :return:
    """
    logger.info('Retrieving columns for from target database')
    query = 'DESCRIBE {table_name}'.format(table_name=table_name)
    cursor.execute(query)
    cols = []
    i = 0
    for field, _, _, key, _, _ in cursor:
        if pks_only and key:
            cols.append(field)
        elif not pks_only:
            cols.append(field)
        i += 1

    cols.sort()
    return cols






