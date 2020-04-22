from doltpy.etl.sql_sync.tests.helpers.db_helpers import mysql_read_helper
from doltpy.etl.sql_sync.mysql import (get_mysql_columns, write_to_table)
from doltpy.etl.sql_sync.tests.helpers.data_helper import (TEST_DATA_INITIAL_COL_SORT,
                                                           TEST_TABLE_COLUMNS,
                                                           assert_tuple_array_equality)


def test_write_to_table(mysql_with_table):
    conn, table = mysql_with_table
    write_to_table(table, conn, TEST_DATA_INITIAL_COL_SORT)
    result = mysql_read_helper(conn)

    assert_tuple_array_equality(TEST_DATA_INITIAL_COL_SORT, result)


def test_get_columns(mysql_with_table):
    conn, table = mysql_with_table
    result = get_mysql_columns(table, conn)
    columns = list(TEST_TABLE_COLUMNS.keys())
    columns.sort()
    assert columns == result
