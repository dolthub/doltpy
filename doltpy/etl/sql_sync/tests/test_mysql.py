from doltpy.etl.sql_sync.mysql import (get_table_metadata, write_to_table)
from doltpy.etl.sql_sync.tests.helpers.data_helper import (TEST_TABLE_COLUMNS,
                                                           TEST_TABLE_METADATA,
                                                           TEST_DATA_INITIAL,
                                                           get_data_for_comparison,
                                                           assert_tuple_array_equality)


def test_write_to_table(mysql_with_table):
    conn, table = mysql_with_table
    write_to_table(table, conn, TEST_DATA_INITIAL)
    result = get_data_for_comparison(conn)
    assert_tuple_array_equality(TEST_DATA_INITIAL, result)


def test_get_table_metadata(mysql_with_table):
    conn, table = mysql_with_table
    result = get_table_metadata(table, conn)
    expected_columns = sorted(TEST_TABLE_COLUMNS, key=lambda col: col.col_name)
    assert TEST_TABLE_METADATA.name == result.name
    assert all(left.col_name == right.col_name for left, right in zip(expected_columns, result.columns))

