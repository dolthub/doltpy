from doltpy.etl.sql_sync import DoltSync, TargetWriter, SourceReader
import logging
from doltpy.etl.sql_sync.tests.data_helper import (TABLE_NAME,
                                                   INSERT_TEST_DATA_QUERY,
                                                   TEST_DATA_UPDATE,
                                                   TEST_DATA_INITIAL,
                                                   assert_tuple_array_equality)
from doltpy.etl.sql_sync.mysql import get_target_writer
from doltpy.etl.sql_sync.dolt import get_source_reader
from doltpy.etl.sql_sync.tests.dolt_db_helper import insert_tuples as dolt_insert_tuples, repo_with_table
from doltpy.etl.sql_sync.tests.db_helpers import mysql_read_helper
from doltpy.etl.sql_sync.tests.mysql_db_helper import (docker_compose_file,
                                                       mysql_with_table,
                                                       mysql_connection)
from doltpy.core.tests.dolt_testing_fixtures import init_repo

logger = logging.getLogger(__name__)


def test_dolt_to_mysql(mysql_with_table, repo_with_table):
    mysql_conn, mysql_table = mysql_with_table
    dolt_repo, dolt_table = repo_with_table

    mysql_to_dolt_sync = DoltSync(get_source_reader(dolt_repo),
                                  get_target_writer(mysql_conn),
                                  {mysql_table: dolt_table})

    # # Make sure it works for empty tables
    # sync_to_mysql(dolt_repo, 'master', {dolt_table: mysql_table}, mysql_conn)
    # mysql_data = read_mysql(mysql_conn)
    # dolt_data = read_dolt(dolt_repo)
    # assert assert_tuple_array_equality(mysql_data, dolt_data)

    # sync initial data
    dolt_insert_tuples(dolt_repo, dolt_table, TEST_DATA_INITIAL)
    mysql_to_dolt_sync.sync()
    assert_sync_success(dolt_repo, mysql_conn)

    # update mysql
    dolt_insert_tuples(dolt_repo, dolt_table, TEST_DATA_UPDATE)
    mysql_to_dolt_sync.sync()
    assert_sync_success(dolt_repo, mysql_conn)


def assert_sync_success(dolt_repo, mysql_conn):
    mysql_data = mysql_read_helper(mysql_conn)
    dolt_data = mysql_read_helper(dolt_repo.cnx)
    assert assert_tuple_array_equality(mysql_data, dolt_data)
