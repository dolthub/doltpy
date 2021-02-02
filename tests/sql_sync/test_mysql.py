import pytest
from .helpers.tools import validate_get_target_writer, validate_drop_primary_keys
from doltpy.sql.sync.mysql import get_target_writer


def test_write_to_table(mysql_with_table):
    engine, table = mysql_with_table
    validate_get_target_writer(engine, table, get_target_writer)


def test_drop_primary_keys(mysql_with_table):
    engine, table = mysql_with_table
    validate_drop_primary_keys(engine, table)
