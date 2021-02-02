from .helpers.data_helper import (assert_rows_equal,
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
from doltpy.sql.sync.dolt import get_table_reader_diffs, get_table_reader, get_target_writer
from doltpy.sql.sync.db_tools import get_table_metadata, DoltTableUpdate
import logging
import pytest
from typing import Callable, Tuple, List
from sqlalchemy import Table
from doltpy.sql import DoltSQLServerContext

logger = logging.getLogger(__name__)


def test_get_table_reader_diffs(create_dolt_test_data_commits):
    """
    Test that get_table_reader_diffs returns only the differences between a specific commit and its immediate parent.
    :param create_dolt_test_data_commits:
    :return:
    """
    dssc, table = create_dolt_test_data_commits
    _test_dolt_table_reader_helper(dssc, table, get_table_reader_diffs, get_expected_dolt_diffs)


def test_get_table_reader(create_dolt_test_data_commits):
    """
    Test that get_table_reader returns the data in a Dolt table at a given commit, or head of current branch.
    :param create_dolt_test_data_commits:
    :return:
    """
    repo, table = create_dolt_test_data_commits
    _test_dolt_table_reader_helper(repo, table, get_table_reader, get_expected_data)


def _test_dolt_table_reader_helper(
        dssc: DoltSQLServerContext,
        table: Table,
        build_table_reader: Callable[[str], Callable[[str, DoltSQLServerContext], DoltTableUpdate]],
        get_expected: Callable[[int], Tuple[List[dict], List[dict]]]):
    # TODO need to do this in SQL
    commits = list(dssc.dolt.log().keys())
    update_to_commit = {
        FIRST_UPDATE: commits[4],
        SECOND_UPDATE: commits[3],
        THIRD_UPDATE: commits[2],
        FOURTH_UPDATE: commits[1],
        FIFTH_UPDATE: commits[0]
    }

    for update_num, commit in update_to_commit.items():
        logger.info('comparison for commit/update_num {}/{}'.format(commit, update_num))
        dropped_pks, dolt_data = build_table_reader(commit)(str(table.name), dssc)
        expected_dropped_pks, expected_data = get_expected(update_num)
        assert expected_dropped_pks == dropped_pks
        assert_rows_equal(expected_data, list(dolt_data))


def test_get_target_writer(db_with_table):
    """
    When writing to Dolt from a relational database we want to replicate the state of the database at each commit, since
    the database itself stores no history, we must read the entire dataset each time, and delete appropriate PKs
    :param db_with_table:
    :return:
    """
    dssc, dolt_table = db_with_table

    update_sequence = [
        TEST_DATA_INITIAL,
        TEST_DATA_INITIAL + TEST_DATA_APPEND_MULTIPLE_ROWS,
        TEST_DATA_APPEND_MULTIPLE_ROWS + TEST_DATA_APPEND_SINGLE_ROW,
        TEST_DATA_APPEND_MULTIPLE_ROWS + TEST_DATA_UPDATE_SINGLE_ROW
    ]

    for i, update in enumerate(update_sequence):
        logger.info('Making {} of {} updates and validating'.format(i, len(update_sequence)))
        get_target_writer(dssc, commit=True)({str(dolt_table.name): update})
        result = _dolt_table_read_helper(dssc, str(dolt_table.name))
        assert_rows_equal(update, result)


def _dolt_table_read_helper(dssc: DoltSQLServerContext, table_name: str):
    table = get_table_metadata(dssc.engine, table_name)
    with dssc.engine.connect() as conn:
        result = conn.execute(table.select())
        return [dict(row) for row in result]
