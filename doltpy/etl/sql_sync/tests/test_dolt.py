from doltpy.etl.sql_sync.tests.helpers.data_helper import (TEST_TABLE_COLUMNS,
                                                           TEST_TABLE_METADATA,
                                                           assert_tuple_array_equality,
                                                           get_expected_data,
                                                           get_expected_dolt_diffs,
                                                           FIRST_UPDATE,
                                                           SECOND_UPDATE,
                                                           THIRD_UPDATE,
                                                           FOURTH_UPDATE,
                                                           FIFTH_UPDATE)
from doltpy.etl.sql_sync.dolt import get_table_reader_diffs, get_table_reader, get_table_metadata
import logging

logger = logging.getLogger(__name__)


def test_get_table_reader_diffs(create_dolt_test_data_commits):
    """
    Test that get_table_reader_diffs returns only the differences between a specific commit and its immediate parent.
    :param create_dolt_test_data_commits:
    :return:
    """
    repo, table = create_dolt_test_data_commits
    commits = list(repo.get_commits().keys())
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
    commits = list(repo.get_commits().keys())
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


def test_get_table_metadata(create_dolt_test_data_commits):
    """
    Test that we get back manually constructed metadata we expect when querying Dolt via MySQL Sever.
    :param create_dolt_test_data_commits:
    :return:
    """
    repo, table = create_dolt_test_data_commits
    conn = repo.get_connection()
    result = get_table_metadata(table, conn)
    conn.close()
    expected_columns = sorted(TEST_TABLE_COLUMNS, key=lambda col: col.col_name)
    assert TEST_TABLE_METADATA.name == result.name
    assert all(left.col_name == right.col_name for left, right in zip(expected_columns, result.columns))
