from doltpy.etl.sql_sync.tests.helpers.data_helper import (TEST_TABLE_COLUMNS,
                                                           TEST_TABLE_METADATA,
                                                           TEST_DATA_INITIAL,
                                                           TEST_DATA_UPDATE,
                                                           assert_tuple_array_equality)
from doltpy.etl.sql_sync.dolt import (get_data_for_table,
                                      get_data_for_commit,
                                      get_table_metadata,
                                      write_to_table)
import pytest


@pytest.mark.skip
def test_get_dolt_data_for_commit(init_empty_test_repo):
    repo, table = init_empty_test_repo
    write_to_table(repo, table, TEST_DATA_UPDATE)
    commits = list(repo.get_commits())
    latest, parent_of_latest = 'HEAD', commits[-2].hash
    cursor = repo.cnx.cursor()
    data_at_latest = get_data_for_commit(table, cursor, latest)
    data_at_parent_of_latest = get_data_for_commit(table, cursor, parent_of_latest)
    assert_tuple_array_equality(TEST_DATA_INITIAL, data_at_parent_of_latest)
    assert_tuple_array_equality(TEST_DATA_UPDATE, data_at_latest)


def test_get_dolt_data_for_table(dolt_repo_with_table):
    repo, table = dolt_repo_with_table
    write_to_table(repo, table, TEST_DATA_INITIAL)
    data = get_data_for_table(table, repo.cnx)
    assert_tuple_array_equality(TEST_DATA_INITIAL, data)


def test_get_table_metadata(dolt_repo_with_table):
    repo, table = dolt_repo_with_table
    result = get_table_metadata(table, repo.cnx)
    expected_columns = sorted(TEST_TABLE_COLUMNS, key=lambda col: col.col_name)
    assert TEST_TABLE_METADATA.name == result.name
    assert all(left.col_name == right.col_name for left, right in zip(expected_columns, result.columns))
