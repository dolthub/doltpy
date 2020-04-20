from doltpy.etl.sql_sync.tests.data_helper import (TEST_TABLE_COLUMNS,
                                                   TEST_DATA_INITIAL,
                                                   TEST_DATA_INITIAL_COL_SORT,
                                                   TEST_DATA_UPDATE,
                                                   assert_tuple_array_equality)
from doltpy.etl.sql_sync.tests.dolt_db_helper import repo_with_table, repo_with_initial_data, insert_tuples
from doltpy.etl.sql_sync.dolt import get_data_for_table, get_data_for_commit, get_dolt_columns
from doltpy.core.tests.dolt_testing_fixtures import init_repo


# TODO:
#   - fix get_data_for_commit
#   - implement target_writer tests
def test_get_dolt_data_for_commit(repo_with_initial_data):
    repo, table = repo_with_initial_data
    insert_tuples(repo, table, TEST_DATA_UPDATE)
    commits = list(repo.get_commits())
    latest, parent_of_latest = commits[-1].hash, commits[-2].hash
    cursor = repo.cnx.cursor()
    data_at_latest = get_data_for_commit(table, cursor, latest)
    data_at_parent_of_latest = get_data_for_commit(table, cursor, parent_of_latest)
    assert_tuple_array_equality(TEST_DATA_INITIAL, data_at_parent_of_latest)
    assert_tuple_array_equality(TEST_DATA_UPDATE, data_at_latest)


def test_get_dolt_data_for_table(repo_with_table):
    repo, table = repo_with_table
    insert_tuples(repo, table, TEST_DATA_INITIAL)
    data = get_data_for_table(table, repo.cnx.cursor())
    assert_tuple_array_equality(TEST_DATA_INITIAL_COL_SORT, data)


def test_get_dolt_columns_all(repo_with_table):
    repo, table = repo_with_table
    columns = list(TEST_TABLE_COLUMNS.keys())
    columns.sort()
    assert columns == get_dolt_columns(table, repo.cnx.cursor(), False)


def test_get_dolt_columns_pks(repo_with_table):
    repo, table = repo_with_table
    pks = set(get_dolt_columns(table, repo.cnx.cursor(), True))
    assert {col for col, is_pk in TEST_TABLE_COLUMNS.items() if is_pk} == pks
