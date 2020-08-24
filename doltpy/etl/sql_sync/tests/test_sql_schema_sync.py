from doltpy.etl.sql_sync.sync_tools import sync_schema_to_dolt
from doltpy.etl.sql_sync.postgres import POSTGRES_TO_DOLT_TYPE_MAPPINGS
from doltpy.etl.sql_sync.mysql import MYSQL_TO_DOLT_TYPE_MAPPINGS
from sqlalchemy.engine import Engine
from sqlalchemy import MetaData
from doltpy.etl.sql_sync.tests.helpers.shcema_sync_helper import TEST_SOURCE_TABLE, TEST_TARGET_TABLE
from doltpy.etl.sql_sync.tests.helpers.tools import SQL_SYNC_SKIP_MSG
import pytest

TABLE_MAP = {TEST_SOURCE_TABLE: TEST_TARGET_TABLE}

@pytest.mark.skip(reason=SQL_SYNC_SKIP_MSG)
def test_mysql_to_dolt(mysql_with_schema_sync_test_table, empty_repo_with_server_process):
    mysql_engine, mysql_table = mysql_with_schema_sync_test_table
    repo = empty_repo_with_server_process
    sync_schema_to_dolt(mysql_engine, repo, TABLE_MAP, MYSQL_TO_DOLT_TYPE_MAPPINGS)
    _compare_schema_helper(TEST_SOURCE_TABLE, mysql_engine, TEST_TARGET_TABLE, repo.engine)


@pytest.mark.skip(reason=SQL_SYNC_SKIP_MSG)
def test_mysql_to_dolt_table_exists(mysql_with_schema_sync_test_table, empty_repo_with_server_process):
    mysql_engine, mysql_table = mysql_with_schema_sync_test_table
    repo = empty_repo_with_server_process
    sync_schema_to_dolt(mysql_engine, repo, TABLE_MAP, MYSQL_TO_DOLT_TYPE_MAPPINGS)


@pytest.mark.skip(reason=SQL_SYNC_SKIP_MSG)
def test_postgres_to_dolt(postgres_with_schema_sync_test_table, empty_repo_with_server_process):
    mysql_engine, mysql_table = postgres_with_schema_sync_test_table
    repo = empty_repo_with_server_process
    sync_schema_to_dolt(mysql_engine, repo, TABLE_MAP, POSTGRES_TO_DOLT_TYPE_MAPPINGS)


@pytest.mark.skip(reason=SQL_SYNC_SKIP_MSG)
def test_postgres_to_dolt_table_exists(postgres_with_schema_sync_test_table, empty_repo_with_server_process):
    mysql_engine, mysql_table = postgres_with_schema_sync_test_table
    repo = empty_repo_with_server_process
    sync_schema_to_dolt(mysql_engine, repo, TABLE_MAP, POSTGRES_TO_DOLT_TYPE_MAPPINGS)


def _compare_schema_helper(source_table_name: str,
                           source_engine: Engine,
                           target_table_name: str,
                           target_engine: Engine):
    source_metadata = MetaData(bind=source_engine)
    source_metadata.reflect()
    target_metadata = MetaData(bind=target_engine)
    target_metadata.reflect()

    if target_table_name not in target_metadata.tables.keys():
        raise AssertionError('{} does not exist in target'.format(target_table_name))

    passed = True
    source_table, target_table = source_metadata.tables[source_table_name], target_metadata.tables[target_table_name]
    for col in source_table.columns:
        if col.name not in [col.name for col in target_table.columns]:
            passed = False

    assert passed, 'found errors'
