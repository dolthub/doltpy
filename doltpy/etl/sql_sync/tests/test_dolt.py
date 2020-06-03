from doltpy.etl.sql_sync.tests.helpers.data_helper import (TEST_TABLE_COLUMNS,
                                                           TEST_TABLE_METADATA,
                                                           assert_tuple_array_equality,
                                                           get_expected_data,
                                                           get_expected_dolt_diffs,
                                                           FIRST_UPDATE,
                                                           SECOND_UPDATE,
                                                           THIRD_UPDATE,
                                                           FOURTH_UPDATE,
                                                           FIFTH_UPDATE,
                                                           TEST_DATA_INITIAL,
                                                           TEST_DATA_APPEND_MULTIPLE_ROWS,
                                                           TEST_DATA_APPEND_SINGLE_ROW,
                                                           TEST_DATA_UPDATE_SINGLE_ROW)
from doltpy.etl.sql_sync.tests.helpers.tools import validate_get_table_metadata
from doltpy.etl.sql_sync.dolt import get_table_reader_diffs, get_table_reader, get_target_writer
from doltpy.etl.sql_sync.mysql import get_table_metadata
from doltpy.core.dolt import Dolt
import logging

logger = logging.getLogger(__name__)


def test_get_table_reader_diffs(create_dolt_test_data_commits):
    """
    Test that get_table_reader_diffs returns only the differences between a specific commit and its immediate parent.
    :param create_dolt_test_data_commits:
    :return:
    """
    repo, table = create_dolt_test_data_commits
    commits = list(repo.log().keys())
    update_to_commit = {
        FIRST_UPDATE: commits[4],
        SECOND_UPDATE: commits[3],
        THIRD_UPDATE: commits[2],
        FOURTH_UPDATE: commits[1],
        FIFTH_UPDATE: commits[0]
    }

    for update_num, commit in update_to_commit.items():
        logger.info('comparison for commit/update_num {}/{}'.format(commit, update_num))
        dropped_pks, dolt_data = get_table_reader_diffs(commit)(table, repo)
        expected_dropped_pks, expected_data = get_expected_dolt_diffs(update_num)
        assert expected_dropped_pks == dropped_pks
        assert_tuple_array_equality(expected_data, list(dolt_data))


def test_get_table_reader(create_dolt_test_data_commits):
    """
    Test that get_table_reader returns the data in a Dolt table at a given commit, or head of current branch.
    :param create_dolt_test_data_commits:
    :return:
    """
    repo, table = create_dolt_test_data_commits
    commits = list(repo.log().keys())
    update_to_commit = {
        FIRST_UPDATE: commits[4],
        SECOND_UPDATE: commits[3],
        THIRD_UPDATE: commits[2],
        FOURTH_UPDATE: commits[1],
        FIFTH_UPDATE: commits[0]
    }

    for update_num, commit in update_to_commit.items():
        logger.info('comparison for commit/update_num {}/{}'.format(commit, update_num))
        dropped_pks, dolt_data = get_table_reader(commit)(table, repo)
        expected_dropped_pks, expected_data = get_expected_data(update_num)
        assert expected_dropped_pks == dropped_pks
        assert_tuple_array_equality(expected_data, list(dolt_data))


def test_get_target_writer(repo_with_table):
    """
    When writing to Dolt from a relational database we want to replicate the state of the database at each commit, since
    the database itself stores no history, we must read the entire dataset each time, and delete appropriate PKs
    :param repo_with_table:
    :return:
    """
    repo, dolt_table = repo_with_table

    update_sequence = [
        TEST_DATA_INITIAL,
        TEST_DATA_INITIAL + TEST_DATA_APPEND_MULTIPLE_ROWS,
        TEST_DATA_APPEND_MULTIPLE_ROWS + TEST_DATA_APPEND_SINGLE_ROW,
        TEST_DATA_APPEND_MULTIPLE_ROWS + TEST_DATA_UPDATE_SINGLE_ROW
    ]

    for update in update_sequence:
        get_target_writer(repo, commit=True)({dolt_table: update})
        result = _dolt_table_read_helper(repo, dolt_table)
        assert_tuple_array_equality(update, result)


def _dolt_table_read_helper(repo: Dolt, table_name: str):
    conn = repo.get_connection()
    table_metadata = get_table_metadata(table_name, conn)
    cursor = conn.cursor()
    query = '''
        SELECT
            {cols}
        FROM
            {table_name}
    '''.format(cols=','.join(col.col_name for col in table_metadata.columns), table_name=table_name)
    cursor.execute(query)
    result = [tup for tup in cursor]
    conn.close()
    return result


def test_get_table_metadata(create_dolt_test_data_commits):
    """
    Test that we get back manually constructed metadata we expect when querying Dolt via MySQL Sever.
    :param create_dolt_test_data_commits:
    :return:
    """
    repo, table = create_dolt_test_data_commits
    conn = repo.get_connection()
    validate_get_table_metadata(conn, table, get_table_metadata)
