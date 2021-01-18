from doltpy.sql import write_rows, read_rows, read_columns, read_pandas, DoltSQLServerManager
from doltpy.shared import columns_to_rows
from doltpy.sql.tests.helpers import (TEST_SERVER_CONFIG,
                                      TEST_TABLE,
                                      TEST_DATA_INITIAL,
                                      TEST_DATA_UPDATE,
                                      TEST_DATA_FINAL,
                                      compare_rows)


def test_read_rows(with_test_table):
    dolt = with_test_table
    with DoltSQLServerManager(dolt, TEST_SERVER_CONFIG) as dolt_sql_server_manager:
        engine = dolt_sql_server_manager.engine
        first_commit = write_rows(engine, TEST_TABLE, TEST_DATA_INITIAL, commit=True)
        second_commit = write_rows(engine, TEST_TABLE, TEST_DATA_UPDATE, commit=True)
        expected_first_write = read_rows(engine, TEST_TABLE, first_commit)
        compare_rows(TEST_DATA_INITIAL, expected_first_write, 'name')
        expected_second_write = read_rows(engine, TEST_TABLE, second_commit)
        compare_rows(TEST_DATA_FINAL, expected_second_write, 'name')


def test_read_columns(with_test_table):
    dolt = with_test_table
    with DoltSQLServerManager(dolt, TEST_SERVER_CONFIG) as dolt_sql_server_manager:
        engine = dolt_sql_server_manager.engine
        first_commit = write_rows(engine, TEST_TABLE, TEST_DATA_INITIAL, commit=True)
        second_commit = write_rows(engine, TEST_TABLE, TEST_DATA_UPDATE, commit=True)
        expected_first_write = read_columns(engine, TEST_TABLE, first_commit)
        compare_rows(TEST_DATA_INITIAL, columns_to_rows(expected_first_write), 'name')
        expected_second_write = read_columns(engine, TEST_TABLE, second_commit)
        compare_rows(TEST_DATA_FINAL, columns_to_rows(expected_second_write), 'name')


def test_read_pandas(with_test_table):
    dolt = with_test_table
    with DoltSQLServerManager(dolt, TEST_SERVER_CONFIG) as dolt_sql_server_manager:
        engine = dolt_sql_server_manager.engine
        first_commit = write_rows(engine, TEST_TABLE, TEST_DATA_INITIAL, commit=True)
        second_commit = write_rows(engine, TEST_TABLE, TEST_DATA_UPDATE, commit=True)
        expected_first_write = read_pandas(engine, TEST_TABLE, first_commit).to_dict('records')
        compare_rows(TEST_DATA_INITIAL, expected_first_write, 'name')
        expected_second_write = read_pandas(engine, TEST_TABLE, second_commit).to_dict('records')
        compare_rows(TEST_DATA_FINAL, expected_second_write, 'name')

