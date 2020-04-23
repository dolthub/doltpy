from doltpy.etl.sql_sync import DoltSync
import logging
import pytest
from doltpy.etl.sql_sync.tests.helpers.data_helper import (TEST_DATA_UPDATE,
                                                           TEST_DATA_INITIAL,
                                                           get_data_for_comparison,
                                                           assert_tuple_array_equality)
from doltpy.etl.sql_sync.mysql import get_target_writer as get_mysql_target_writer
from doltpy.etl.sql_sync.postgres import get_target_writer as get_postgres_target_writer
from doltpy.etl.sql_sync.dolt import get_source_reader, write_to_table as write_to_dolt_table

logger = logging.getLogger(__name__)


@pytest.mark.skip
def test_dolt_to_mysql_invalid_tables():
    pass


def test_dolt_to_mysql(mysql_with_table, dolt_repo_with_table):
    mysql_conn, mysql_table = mysql_with_table
    dolt_repo, dolt_table = dolt_repo_with_table
    mysql_to_dolt_sync = DoltSync(get_source_reader(dolt_repo, latest=False),
                                  get_mysql_target_writer(mysql_conn, True),
                                  {dolt_table: mysql_table})
    _test_dolt_to_other_db_helper(mysql_to_dolt_sync, mysql_conn, dolt_repo, dolt_table)


def test_dolt_postgres(postgres_with_table, dolt_repo_with_table):
    postgres_conn, postgres_table = postgres_with_table
    dolt_repo, dolt_table = dolt_repo_with_table
    postgres_to_dolt_sync = DoltSync(get_source_reader(dolt_repo, latest=False),
                                     get_postgres_target_writer(postgres_conn, True),
                                     {dolt_table: postgres_table})
    _test_dolt_to_other_db_helper(postgres_to_dolt_sync, postgres_conn, dolt_repo, dolt_table)


def _test_dolt_to_other_db_helper(sync: DoltSync, db_conn, dolt_repo, dolt_table):
    # Make sure it works for empty tables
    sync.sync()
    assert_sync_success(dolt_repo, db_conn)

    # sync initial data
    dolt_repo.stop_server()
    dolt_repo.start_server()
    write_to_dolt_table(dolt_repo, dolt_table, TEST_DATA_INITIAL)
    sync.sync()
    assert_sync_success(dolt_repo, db_conn)

    # update mysql
    dolt_repo.stop_server()
    dolt_repo.start_server()
    write_to_dolt_table(dolt_repo, dolt_table, TEST_DATA_UPDATE)
    sync.sync()
    assert_sync_success(dolt_repo, db_conn)


def assert_sync_success(dolt_repo, conn):
    mysql_data = get_data_for_comparison(conn)
    dolt_data = get_data_for_comparison(dolt_repo.cnx)
    assert assert_tuple_array_equality(mysql_data, dolt_data)
