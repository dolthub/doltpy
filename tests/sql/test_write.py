from doltpy.sql import DoltSQLServerContext
from .helpers import (
    TEST_SERVER_CONFIG,
    TEST_TABLE,
    TEST_DATA_INITIAL,
    TEST_DATA_UPDATE,
    TEST_DATA_FINAL,
    compare_rows
)
from doltpy.shared import rows_to_columns
import pandas as pd
import pytest


def test_write_columns(with_test_table):
    dolt = with_test_table
    with DoltSQLServerContext(dolt, TEST_SERVER_CONFIG) as dssc:
        first_commit = dssc.write_columns(TEST_TABLE, rows_to_columns(TEST_DATA_INITIAL), primary_key=['id'], commit=True)
        second_commit = dssc.write_columns(TEST_TABLE, rows_to_columns(TEST_DATA_UPDATE), commit=True)
        actual_asof_first_commit = dssc.read_rows(TEST_TABLE, first_commit)
        actual_asof_second_commit = dssc.read_rows(TEST_TABLE, second_commit)
        compare_rows(TEST_DATA_INITIAL, actual_asof_first_commit, 'name')
        compare_rows(actual_asof_second_commit, TEST_DATA_FINAL, 'name')


def test_write_rows(with_test_table):
    dolt = with_test_table
    with DoltSQLServerContext(dolt, TEST_SERVER_CONFIG) as dssc:
        first_commit = dssc.write_rows(TEST_TABLE, TEST_DATA_INITIAL, primary_key=['id'], commit=True)
        second_commit = dssc.write_rows(TEST_TABLE, TEST_DATA_UPDATE, commit=True)
        actual_asof_first_commit = dssc.read_rows(TEST_TABLE, first_commit)
        actual_asof_second_commit = dssc.read_rows(TEST_TABLE, second_commit)
        compare_rows(TEST_DATA_INITIAL, actual_asof_first_commit, 'name')
        compare_rows(TEST_DATA_FINAL, actual_asof_second_commit, 'name')


def test_write_pandas_update(with_test_table):
    dolt = with_test_table
    with DoltSQLServerContext(dolt, TEST_SERVER_CONFIG) as dssc:
        first_commit = dssc.write_pandas(TEST_TABLE, pd.DataFrame(TEST_DATA_INITIAL), primary_key=['id'], commit=True)
        second_commit = dssc.write_pandas(TEST_TABLE, pd.DataFrame(TEST_DATA_UPDATE), commit=True)
        actual_asof_first_commit = dssc.read_rows(TEST_TABLE, first_commit)
        actual_asof_second_commit = dssc.read_rows(TEST_TABLE, second_commit)
        compare_rows(TEST_DATA_INITIAL, actual_asof_first_commit, 'name')
        compare_rows(TEST_DATA_FINAL, actual_asof_second_commit, 'name')


@pytest.mark.skip()
def test_write_file(with_test_table, with_test_data_initial_file, with_test_data_final_file):
    dolt = with_test_table
    test_data_initial_file, test_data_final_file = with_test_data_initial_file, with_test_data_final_file
    with DoltSQLServerContext(dolt, TEST_SERVER_CONFIG) as dssc:
        first_commit = dssc.write_file(TEST_TABLE, test_data_initial_file, primary_key=['id'], commit=True)
        second_commit = dssc.write_file(TEST_TABLE, test_data_final_file, commit=True)
        actual_asof_first_commit = dssc.read_rows(TEST_TABLE, first_commit)
        actual_asof_second_commit = dssc.read_rows(TEST_TABLE, second_commit)
        compare_rows(TEST_DATA_INITIAL, actual_asof_first_commit, 'name')
        compare_rows(TEST_DATA_FINAL, actual_asof_second_commit, 'name')
