import pytest
from doltpy.etl.sql_sync.tests.helpers.tools import (validate_get_target_writer,
                                                     validate_drop_primary_keys,
                                                     SQL_SYNC_SKIP_MSG)
from doltpy.etl.sql_sync.mysql import get_target_writer


@pytest.mark.skip(reason=SQL_SYNC_SKIP_MSG)
def test_write_to_table(mysql_with_table):
    """
    See validate_write_to_table docstring.
    :param mysql_with_table:
    :return:
    """
    engine, table = mysql_with_table
    validate_get_target_writer(engine, table, get_target_writer)


@pytest.mark.skip(reason=SQL_SYNC_SKIP_MSG)
def test_drop_primary_keys(mysql_with_table):
    """
    See validate_drop_primary_keys docstring.
    :param mysql_with_table:
    :return:
    """
    engine, table = mysql_with_table
    validate_drop_primary_keys(engine, table)
