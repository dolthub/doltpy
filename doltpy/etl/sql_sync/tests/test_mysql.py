from doltpy.etl.sql_sync.mysql import get_table_metadata, get_insert_query
from doltpy.etl.sql_sync.tests.helpers.tools import (validate_get_table_metadata,
                                                     validate_write_to_table,
                                                     validate_drop_primary_keys)


def test_get_table_metadata(mysql_with_table):
    """
    Verify that get_table_metadata correctly constructs the metadata associated with the test table. We manually build
    that metadata in helpers/data_helper.py to verify this.
    :param mysql_with_table:
    :return:
    """
    conn, table = mysql_with_table
    validate_get_table_metadata(conn, table, get_table_metadata)


def test_write_to_table(mysql_with_table):
    """
    Ensure that writes using our write wrapper correctly show up in MySQL Server.
    :param mysql_with_table:
    :return:
    """
    conn, table = mysql_with_table
    validate_write_to_table(conn, table, get_table_metadata, get_insert_query)


def test_drop_primary_keys(mysql_with_table):
    """
    Verify that dropping a primary key from using drop_primary_keys leaves MySQL Server in the correct state.
    :param mysql_with_table:
    :return:
    """
    conn, table = mysql_with_table
    validate_drop_primary_keys(conn, table, get_table_metadata, get_insert_query)
