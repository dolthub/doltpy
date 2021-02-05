from doltpy.sql import DoltSQLServerContext
from doltpy.shared import columns_to_rows
from .helpers import (TEST_SERVER_CONFIG,
                                      TEST_TABLE,
                                      TEST_DATA_INITIAL,
                                      TEST_DATA_UPDATE,
                                      TEST_DATA_FINAL,
                                      compare_rows)


def test_read_rows(with_test_table):
    dolt = with_test_table
    with DoltSQLServerContext(dolt, TEST_SERVER_CONFIG) as dssc:
        first_commit = dssc.write_rows(TEST_TABLE, TEST_DATA_INITIAL, commit=True)
        second_commit = dssc.write_rows(TEST_TABLE, TEST_DATA_UPDATE, commit=True)
        expected_first_write = dssc.read_rows(TEST_TABLE, first_commit)
        compare_rows(TEST_DATA_INITIAL, expected_first_write, 'name')
        expected_second_write = dssc.read_rows(TEST_TABLE, second_commit)
        compare_rows(TEST_DATA_FINAL, expected_second_write, 'name')


def test_read_columns(with_test_table):
    dolt = with_test_table
    with DoltSQLServerContext(dolt, TEST_SERVER_CONFIG) as dssc:
        first_commit = dssc.write_rows(TEST_TABLE, TEST_DATA_INITIAL, commit=True)
        second_commit = dssc.write_rows(TEST_TABLE, TEST_DATA_UPDATE, commit=True)
        expected_first_write = dssc.read_columns(TEST_TABLE, first_commit)
        compare_rows(TEST_DATA_INITIAL, columns_to_rows(expected_first_write), 'name')
        expected_second_write = dssc.read_columns(TEST_TABLE, second_commit)
        compare_rows(TEST_DATA_FINAL, columns_to_rows(expected_second_write), 'name')


def test_read_pandas(with_test_table):
    dolt = with_test_table
    with DoltSQLServerContext(dolt, TEST_SERVER_CONFIG) as dssc:
        first_commit = dssc.write_rows(TEST_TABLE, TEST_DATA_INITIAL, commit=True)
        second_commit = dssc.write_rows(TEST_TABLE, TEST_DATA_UPDATE, commit=True)
        expected_first_write = dssc.read_pandas(TEST_TABLE, first_commit).to_dict('records')
        compare_rows(TEST_DATA_INITIAL, expected_first_write, 'name')
        expected_second_write = dssc.read_pandas(TEST_TABLE, second_commit).to_dict('records')
        compare_rows(TEST_DATA_FINAL, expected_second_write, 'name')

