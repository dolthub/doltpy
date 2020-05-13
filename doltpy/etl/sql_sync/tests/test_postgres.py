from doltpy.etl.sql_sync.postgres import get_table_metadata, get_insert_query
from doltpy.etl.sql_sync.tests.helpers.tools import (validate_get_table_metadata,
                                                     validate_write_to_table,
                                                     validate_drop_primary_keys)


def test_get_table_metadata(postgres_with_table):
    """
    See validate_get_table_metadata docstring.
    :param postgres_with_table:
    :return:
    """
    conn, table = postgres_with_table
    validate_get_table_metadata(conn, table, get_table_metadata)


def test_write_to_table(postgres_with_table):
    """
    See validate_write_to_table docstring.
    :param postgres_with_table:
    :return:
    """
    conn, table = postgres_with_table
    validate_write_to_table(conn, table, get_table_metadata, get_insert_query)


def test_drop_primary_keys(postgres_with_table):
    """
    See validate_drop_primary_keys docstring.
    :param postgres_with_table:
    :return:
    """
    conn, table = postgres_with_table
    validate_drop_primary_keys(conn, table, get_table_metadata, get_insert_query)
