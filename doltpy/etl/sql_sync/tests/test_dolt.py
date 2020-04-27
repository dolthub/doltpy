from doltpy.etl.sql_sync.tests.helpers.data_helper import (TEST_TABLE_COLUMNS,
                                                           TEST_TABLE_METADATA,
                                                           TEST_DATA_INITIAL,
                                                           TEST_DATA_APPEND_MULTIPLE_ROWS,
                                                           TEST_DATA_APPEND_SINGLE_ROW,
                                                           assert_tuple_array_equality)
from doltpy.etl.sql_sync.dolt import get_table_reader_diffs, get_table_reader, get_table_metadata


def test_get_table_reader_diffs(create_dolt_test_data_commits):
    repo, table = create_dolt_test_data_commits

    # Grab the commit that introduced the single row
    commits = list(repo.get_commits().keys())
    latest, parent_of_latest, grandparent_of_latest = commits[0],  commits[1], commits[2]

    diff_parent_of_latest = get_table_reader_diffs(parent_of_latest)(table, repo)
    diff_at_latest = get_table_reader_diffs()(table, repo)

    # Check data corresponds to insertion/commits
    assert_tuple_array_equality(TEST_DATA_APPEND_SINGLE_ROW, diff_parent_of_latest)
    assert_tuple_array_equality(TEST_DATA_APPEND_MULTIPLE_ROWS, diff_at_latest)


def test_get_table_reader(create_dolt_test_data_commits):
    repo, table = create_dolt_test_data_commits

    # Grab the commit that introduced the single row
    commits = list(repo.get_commits())
    latest, parent_of_latest, grandparent_of_latest = commits[0], commits[1], commits[2]

    # Query data at latest
    data_at_grandparent_of_latest = get_table_reader(grandparent_of_latest)(table, repo)
    data_at_parent_of_latest = get_table_reader(parent_of_latest)(table, repo)
    data_at_latest = get_table_reader()(table, repo)

    # Check data corresponds to insertion/commits
    assert_tuple_array_equality(TEST_DATA_INITIAL, data_at_grandparent_of_latest)
    expected_at_parent_of_latest = TEST_DATA_INITIAL + TEST_DATA_APPEND_SINGLE_ROW
    assert_tuple_array_equality(expected_at_parent_of_latest, data_at_parent_of_latest)
    expected_at_grandparent_of_latest = expected_at_parent_of_latest + TEST_DATA_APPEND_MULTIPLE_ROWS
    assert_tuple_array_equality(expected_at_grandparent_of_latest, data_at_latest)


def test_get_table_metadata(create_dolt_test_data_commits):
    repo, table = create_dolt_test_data_commits
    conn = repo.get_connection()
    result = get_table_metadata(table, conn)
    conn.close()
    expected_columns = sorted(TEST_TABLE_COLUMNS, key=lambda col: col.col_name)
    assert TEST_TABLE_METADATA.name == result.name
    assert all(left.col_name == right.col_name for left, right in zip(expected_columns, result.columns))


def server_restart_helpr(repo):
    repo.stop_server()
    repo.start_server()
