from doltpy.sql import (write_file,
                        write_rows,
                        write_pandas,
                        write_columns,
                        read_rows,
                        DoltSQLServerManager)
from doltpy.sql.tests.helpers import (TEST_SERVER_CONFIG,
                                      TEST_TABLE,
                                      TEST_DATA_INITIAL,
                                      TEST_DATA_UPDATE,
                                      TEST_DATA_FINAL,
                                      compare_rows)
from doltpy.shared import rows_to_columns
import pandas as pd
import pytest


def test_write_columns(with_test_table):
    dolt = with_test_table
    with DoltSQLServerManager(dolt, TEST_SERVER_CONFIG) as dolt_sql_server_manager:
        engine = dolt_sql_server_manager.engine
        first_commit = write_columns(engine,
                                     TEST_TABLE,
                                     rows_to_columns(TEST_DATA_INITIAL),
                                     on_duplicate_key_update=True,
                                     primary_key=['id'],
                                     commit=True)
        second_commit = write_columns(engine, TEST_TABLE, rows_to_columns(TEST_DATA_UPDATE), commit=True)
        actual_asof_first_commit = read_rows(engine, TEST_TABLE, first_commit)
        actual_asof_second_commit = read_rows(engine, TEST_TABLE, second_commit)
        compare_rows(TEST_DATA_INITIAL, actual_asof_first_commit, 'name')
        compare_rows(actual_asof_second_commit, TEST_DATA_FINAL, 'name')


def test_write_rows(with_test_table):
    dolt = with_test_table
    with DoltSQLServerManager(dolt, TEST_SERVER_CONFIG) as dolt_sql_server_manager:
        engine = dolt_sql_server_manager.engine
        first_commit = write_rows(engine,
                                  TEST_TABLE,
                                  TEST_DATA_INITIAL,
                                  on_duplicate_key_update=True,
                                  primary_key=['id'],
                                  commit=True)
        second_commit = write_rows(engine, TEST_TABLE, TEST_DATA_UPDATE, commit=True)
        actual_asof_first_commit = read_rows(engine, TEST_TABLE, first_commit)
        actual_asof_second_commit = read_rows(engine, TEST_TABLE, second_commit)
        compare_rows(TEST_DATA_INITIAL, actual_asof_first_commit, 'name')
        compare_rows(TEST_DATA_FINAL, actual_asof_second_commit, 'name')


def test_write_pandas_update(with_test_table):
    dolt = with_test_table
    with DoltSQLServerManager(dolt, TEST_SERVER_CONFIG) as dolt_sql_server_manager:
        engine = dolt_sql_server_manager.engine
        first_commit = write_pandas(engine,
                                    TEST_TABLE,
                                    pd.DataFrame(TEST_DATA_INITIAL),
                                    on_duplicate_key_update=True,
                                    primary_key=['id'],
                                    commit=True)
        second_commit = write_pandas(engine, TEST_TABLE, pd.DataFrame(TEST_DATA_UPDATE), commit=True)
        actual_asof_first_commit = read_rows(engine, TEST_TABLE, first_commit)
        actual_asof_second_commit = read_rows(engine, TEST_TABLE, second_commit)
        compare_rows(TEST_DATA_INITIAL, actual_asof_first_commit, 'name')
        compare_rows(TEST_DATA_FINAL, actual_asof_second_commit, 'name')


@pytest.mark.skip()
def test_write_file(with_test_table, with_test_data_initial_file, with_test_data_final_file):
    dolt = with_test_table
    test_data_initial_file, test_data_final_file = with_test_data_initial_file, with_test_data_final_file
    with DoltSQLServerManager(dolt, TEST_SERVER_CONFIG) as dolt_sql_server_manager:
        engine = dolt_sql_server_manager.engine
        first_commit = write_file(engine,
                                  TEST_TABLE,
                                  test_data_initial_file,
                                  on_duplicate_key_update=True,
                                  primary_key=['id'],
                                  commit=True)
        second_commit = write_file(engine, TEST_TABLE, test_data_final_file, commit=True)
        actual_asof_first_commit = read_rows(engine, TEST_TABLE, first_commit)
        actual_asof_second_commit = read_rows(engine, TEST_TABLE, second_commit)
        compare_rows(TEST_DATA_INITIAL, actual_asof_first_commit, 'name')
        compare_rows(TEST_DATA_FINAL, actual_asof_second_commit, 'name')
