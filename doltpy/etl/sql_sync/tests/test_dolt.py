from doltpy.etl.sql_sync.tests.helpers.data_helper import (TEST_TABLE_COLUMNS,
                                                           TEST_DATA_INITIAL,
                                                           TEST_DATA_INITIAL_COL_SORT,
                                                           TEST_DATA_UPDATE,
                                                           TEST_DATA_UPDATE_COL_SORT,
                                                           assert_tuple_array_equality)
from doltpy.etl.sql_sync.tests.helpers.db_helpers import dolt_insert_tuples
from doltpy.etl.sql_sync.dolt import get_data_for_table, get_data_for_commit, get_dolt_columns
import pytest


@pytest.mark.skip
def test_get_dolt_data_for_commit(init_empty_test_repo):
    repo, table = init_empty_test_repo
    dolt_insert_tuples(repo, table, TEST_DATA_UPDATE)
    commits = list(repo.get_commits())
    latest, parent_of_latest = 'HEAD', commits[-2].hash
    cursor = repo.cnx.cursor()
    data_at_latest = get_data_for_commit(table, cursor, latest)
    data_at_parent_of_latest = get_data_for_commit(table, cursor, parent_of_latest)
    assert_tuple_array_equality(TEST_DATA_INITIAL_COL_SORT, data_at_parent_of_latest)
    assert_tuple_array_equality(TEST_DATA_UPDATE_COL_SORT, data_at_latest)


def test_get_dolt_data_for_table(dolt_repo_with_table):
    repo, table = dolt_repo_with_table
    dolt_insert_tuples(repo, table, TEST_DATA_INITIAL)
    data = get_data_for_table(table, repo.cnx.cursor())
    assert_tuple_array_equality(TEST_DATA_INITIAL_COL_SORT, data)


def test_get_dolt_columns_all(dolt_repo_with_table):
    repo, table = dolt_repo_with_table
    columns = list(TEST_TABLE_COLUMNS.keys())
    columns.sort()
    assert columns == get_dolt_columns(table, repo.cnx.cursor(), False)


def test_get_dolt_columns_pks(dolt_repo_with_table):
    repo, table = dolt_repo_with_table
    pks = set(get_dolt_columns(table, repo.cnx.cursor(), True))
    assert {col for col, is_pk in TEST_TABLE_COLUMNS.items() if is_pk} == pks
