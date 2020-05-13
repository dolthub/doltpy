import logging
from doltpy.etl.sql_sync.mysql import (get_target_writer as get_mysql_target_writer,
                                       get_insert_query as get_mysql_insert_query,
                                       get_table_metadata as get_mysql_table_metadata,
                                       get_source_reader as get_mysql_source_reader,
                                       get_table_reader as get_mysql_table_reader)
from doltpy.etl.sql_sync.postgres import (get_target_writer as get_postgres_target_writer,
                                          get_table_metadata as get_postgres_table_metadata,
                                          get_source_reader as get_postgres_source_reader,
                                          get_table_reader as get_postgres_table_reader,
                                          get_insert_query as get_postgres_insert_query)
from doltpy.etl.sql_sync.tests.helpers.tools import validate_dolt_as_source, validate_dolt_as_target

logger = logging.getLogger(__name__)


# TODO this needs to include record deletion to make sure we are capturing deletes
def test_dolt_to_mysql(mysql_with_table, create_dolt_test_data_commits):
    """
    Verifies that given a Dolt repository that has a a series of updates applied to it (defined in the fixture
    create_dolt_test_data_commits) that after syncing at each of the commits, the Dolt repository and the target
    MySQL server instance contain the same data. Tests creates, updates, and deletes.
    :param mysql_with_table:
    :param create_dolt_test_data_commits:
    :return:
    """
    mysql_conn, mysql_table = mysql_with_table
    dolt_repo, dolt_table = create_dolt_test_data_commits
    validate_dolt_as_source(mysql_conn, mysql_table, get_mysql_target_writer, dolt_repo, dolt_table)


def test_mysql_to_dolt(mysql_with_table, repo_with_table):
    mysql_conn, mysql_table = mysql_with_table
    dolt_repo, dolt_table = repo_with_table
    validate_dolt_as_target(mysql_conn,
                            mysql_table,
                            get_mysql_table_metadata,
                            get_mysql_source_reader,
                            get_mysql_table_reader,
                            get_mysql_insert_query,
                            dolt_repo,
                            dolt_table)


def test_dolt_postgres(postgres_with_table, create_dolt_test_data_commits):
    """

    :param postgres_with_table:
    :param create_dolt_test_data_commits:
    :return:
    """
    postgres_conn, postgres_table = postgres_with_table
    dolt_repo, dolt_table = create_dolt_test_data_commits
    validate_dolt_as_source(postgres_conn, postgres_table, get_postgres_target_writer, dolt_repo, dolt_table)


def test_postgres_to_dolt(postgres_with_table, repo_with_table):
    postgres_conn, postgres_table = postgres_with_table
    dolt_repo, dolt_table = repo_with_table
    validate_dolt_as_target(postgres_conn,
                            postgres_table,
                            get_postgres_table_metadata,
                            get_postgres_source_reader,
                            get_postgres_table_reader,
                            get_postgres_insert_query,
                            dolt_repo,
                            dolt_table)
