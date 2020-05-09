from doltpy.etl.sql_sync.postgres import get_table_metadata, write_to_table
from doltpy.etl.sql_sync.tests.helpers.data_helper import (TEST_TABLE_COLUMNS,
                                                           TEST_TABLE_METADATA,
                                                           TEST_DATA_INITIAL,
                                                           TEST_DATA_APPEND_SINGLE_ROW,
                                                           TEST_DATA_APPEND_MULTIPLE_ROWS,
                                                           TEST_DATA_UPDATE_SINGLE_ROW,
                                                           get_data_for_comparison,
                                                           assert_tuple_array_equality,
                                                           FIRST_UPDATE,
                                                           SECOND_UPDATE,
                                                           THIRD_UPDATE,
                                                           FOURTH_UPDATE,
                                                           get_expected_data)


def test_write_to_table(postgres_with_table):
    conn, table = postgres_with_table
    write_to_table(table, conn, TEST_DATA_INITIAL)

    def _write_and_diff_helper(data, update_num):
        write_to_table(table, conn, data)
        result = get_data_for_comparison(conn)
        _, expected_data = get_expected_data(update_num)
        assert_tuple_array_equality(expected_data, result)

    _write_and_diff_helper(TEST_DATA_INITIAL, FIRST_UPDATE)
    _write_and_diff_helper(TEST_DATA_APPEND_SINGLE_ROW, SECOND_UPDATE)
    _write_and_diff_helper(TEST_DATA_APPEND_MULTIPLE_ROWS, THIRD_UPDATE)
    _write_and_diff_helper(TEST_DATA_UPDATE_SINGLE_ROW, FOURTH_UPDATE)


def test_get_table_metadata(postgres_with_table):
    conn, table = postgres_with_table
    result = get_table_metadata(table, conn)
    expected_columns = sorted(TEST_TABLE_COLUMNS, key=lambda col: col.col_name)
    assert TEST_TABLE_METADATA.name == result.name
    assert len(TEST_TABLE_METADATA.columns) == len(result.columns)
    assert all(left.col_name == right.col_name and left.key == right.key
               for left, right in zip(expected_columns, result.columns))
