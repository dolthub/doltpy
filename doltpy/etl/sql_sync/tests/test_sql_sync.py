import logging
import pytest
from doltpy.etl.sql_sync.mysql import get_target_writer as get_mysql_target_writer
from doltpy.etl.sql_sync.postgres import get_target_writer as get_postgres_target_writer
from doltpy.etl.sql_sync.db_tools import get_source_reader, get_table_reader, get_table_metadata
from doltpy.etl.sql_sync.sync_tools import sync_to_dolt
from doltpy.etl.sql_sync.dolt import (get_target_writer as get_dolt_target_writer,
                                      get_table_reader as get_dolt_table_reader)
from doltpy.etl.sql_sync.tests.helpers.tools import validate_dolt_as_source, validate_dolt_as_target, SQL_SYNC_SKIP_MSG
from doltpy.etl.sql_sync.tests.helpers.data_helper import assert_rows_equal, TEST_DATA_WITH_ARRAYS, deserialize_longtext

logger = logging.getLogger(__name__)


# TODO this needs to include record deletion to make sure we are capturing deletes
@pytest.mark.skip(reason=SQL_SYNC_SKIP_MSG)
def test_dolt_to_mysql(mysql_with_table, create_dolt_test_data_commits):
    """
    Tests Dolt to MySQL, see validate_dolt_as_source for details.
    :param mysql_with_table:
    :param create_dolt_test_data_commits:
    :return:
    """
    mysql_conn, mysql_table = mysql_with_table
    dolt_repo, dolt_table = create_dolt_test_data_commits
    validate_dolt_as_source(mysql_conn, mysql_table, get_mysql_target_writer, dolt_repo, dolt_table)


@pytest.mark.skip(reason=SQL_SYNC_SKIP_MSG)
def test_mysql_to_dolt(mysql_with_table, repo_with_table):
    """
    Tests MySQL to Dolt, see validate_dolt_as_source for details.
    :param mysql_with_table:
    :param repo_with_table:
    :return:
    """
    mysql_engine, mysql_table = mysql_with_table
    dolt_repo, dolt_table = repo_with_table
    validate_dolt_as_target(mysql_engine,
                            mysql_table,
                            get_source_reader,
                            get_mysql_target_writer,
                            get_table_reader,
                            dolt_repo,
                            dolt_table)


@pytest.mark.skip(reason=SQL_SYNC_SKIP_MSG)
def test_dolt_postgres(postgres_with_table, create_dolt_test_data_commits):
    """
    Tests Dolt to Postgres, see validate_dolt_as_source for details.
    :param postgres_with_table:
    :param create_dolt_test_data_commits:
    :return:
    """
    postgres_conn, postgres_table = postgres_with_table
    dolt_repo, dolt_table = create_dolt_test_data_commits
    validate_dolt_as_source(postgres_conn, postgres_table, get_postgres_target_writer, dolt_repo, dolt_table)


@pytest.mark.skip(reason=SQL_SYNC_SKIP_MSG)
def test_postgres_to_dolt(postgres_with_table, repo_with_table):
    """
    Tests Postgres to Dolt, see validate_dolt_as_source for details.
    :param postgres_with_table:
    :param repo_with_table:
    :return:
    """
    postgres_conn, postgres_table = postgres_with_table
    dolt_repo, dolt_table = repo_with_table
    validate_dolt_as_target(postgres_conn,
                            postgres_table,
                            get_source_reader,
                            get_postgres_target_writer,
                            get_table_reader,
                            dolt_repo,
                            dolt_table)


@pytest.mark.skip(reason=SQL_SYNC_SKIP_MSG)
def test_postgres_to_dolt_array_types(postgres_with_table_with_arrays, repo_with_table_with_arrays):
    postgres_engine, postgres_table = postgres_with_table_with_arrays
    dolt_repo, dolt_table = repo_with_table_with_arrays

    get_postgres_target_writer(postgres_engine)({str(postgres_table.name): ([], TEST_DATA_WITH_ARRAYS)})
    source_reader = get_source_reader(postgres_engine, get_table_reader())
    target_writer = get_dolt_target_writer(dolt_repo, commit=True)
    sync_to_dolt(source_reader, target_writer, {str(postgres_table.name): str(dolt_table.name)})
    latest_commit = list(dolt_repo.log().keys())[0]

    _, dolt_data = get_dolt_table_reader(latest_commit)(str(dolt_table.name), dolt_repo)
    db_table_metadata = get_table_metadata(postgres_engine, str(postgres_table.name))
    db_data = get_table_reader()(postgres_engine, db_table_metadata)
    clean_dolt_data = deserialize_longtext(dolt_data)
    assert_rows_equal(clean_dolt_data, db_data, lambda dic: dic['id'])


