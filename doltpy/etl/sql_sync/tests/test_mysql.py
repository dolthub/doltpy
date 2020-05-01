from doltpy.etl.sql_sync.mysql import get_table_metadata, write_to_table, drop_primary_keys
from doltpy.etl.sql_sync.tests.helpers.data_helper import (TEST_TABLE_COLUMNS,
                                                           TEST_TABLE_METADATA,
                                                           TEST_DATA_INITIAL,
                                                           TEST_DATA_APPEND_SINGLE_ROW,
                                                           TEST_DATA_APPEND_MULTIPLE_ROWS,
                                                           TEST_DATA_UPDATE_SINGLE_ROW,
                                                           TEST_DATA_APPEND_MULTIPLE_ROWS_WITH_DELETE,
                                                           get_data_for_comparison,
                                                           assert_tuple_array_equality,
                                                           FIRST_UPDATE,
                                                           SECOND_UPDATE,
                                                           THIRD_UPDATE,
                                                           FOURTH_UPDATE,
                                                           get_expected_data)


def test_get_table_metadata(mysql_with_table):
    """
    Verify that get_table_metadata correctly constructs the metadata associated with the test table. We manually build
    that metadata in helpers/data_helper.py to verify this.
    :param mysql_with_table:
    :return:
    """
    conn, table = mysql_with_table
    result = get_table_metadata(table, conn)
    expected_columns = sorted(TEST_TABLE_COLUMNS, key=lambda col: col.col_name)
    assert TEST_TABLE_METADATA.name == result.name
    assert all(left.col_name == right.col_name for left, right in zip(expected_columns, result.columns))


def test_write_to_table(mysql_with_table):
    """
    Ensure that writes using our write wrapper correctly show up in MySQL Server.
    :param mysql_with_table:
    :return:
    """
    conn, table = mysql_with_table
    table_metadata = get_table_metadata(table, conn)

    def _write_and_diff_helper(data, update_num):
        write_to_table(table_metadata, conn, data)
        result = get_data_for_comparison(conn)
        _, expected_data = get_expected_data(update_num)
        assert_tuple_array_equality(expected_data, result)

    _write_and_diff_helper(TEST_DATA_INITIAL, FIRST_UPDATE)
    _write_and_diff_helper(TEST_DATA_APPEND_SINGLE_ROW, SECOND_UPDATE)
    _write_and_diff_helper(TEST_DATA_APPEND_MULTIPLE_ROWS, THIRD_UPDATE)
    _write_and_diff_helper(TEST_DATA_UPDATE_SINGLE_ROW, FOURTH_UPDATE)


def test_get_table_reader():
    # test that correct values are returned
    pass


def test_drop_primary_keys(mysql_with_table):
    """
    Verify that dropping a primary key from using drop_primary_keys leaves MySQL Server in the correct state.
    :param mysql_with_table:
    :return:
    """
    conn, table = mysql_with_table
    table_metadata = get_table_metadata(table, conn)

    write_to_table(table_metadata, conn, TEST_DATA_APPEND_MULTIPLE_ROWS)
    pks_to_drop = [('Stefanos', 'Tsitsipas')]
    drop_primary_keys(conn, table_metadata, pks_to_drop)
    result = get_data_for_comparison(conn)
    assert_tuple_array_equality(TEST_DATA_APPEND_MULTIPLE_ROWS_WITH_DELETE, result)
