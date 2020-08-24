from doltpy.etl.sql_sync.sync_tools import sync_schema_to_dolt
from doltpy.etl.sql_sync.postgres import POSTGRES_TO_DOLT_TYPE_MAPPINGS
from doltpy.etl.sql_sync.mysql import MYSQL_TO_DOLT_TYPE_MAPPINGS
from doltpy.core.dolt import Dolt
from sqlalchemy.engine import Engine
from sqlalchemy import MetaData
from doltpy.etl.sql_sync.tests.helpers.schema_sync_helper import TEST_SOURCE_TABLE, TEST_TARGET_TABLE, ALTER_TABLE
from doltpy.etl.sql_sync.tests.helpers.tools import SQL_SYNC_SKIP_MSG
import pytest

TABLE_MAP = {TEST_SOURCE_TABLE: TEST_TARGET_TABLE}


@pytest.mark.skip(reason=SQL_SYNC_SKIP_MSG)
def test_mysql_to_dolt(mysql_with_schema_sync_test_table, empty_repo_with_server_process):
    mysql_engine, _ = mysql_with_schema_sync_test_table
    dolt = empty_repo_with_server_process
    _test_schema_sync_helper(mysql_engine, MYSQL_TO_DOLT_TYPE_MAPPINGS, dolt)


@pytest.mark.skip(reason=SQL_SYNC_SKIP_MSG)
def test_postgres_to_dolt(postgres_with_schema_sync_test_table, empty_repo_with_server_process):
    postgres_engine, _ = postgres_with_schema_sync_test_table
    dolt = empty_repo_with_server_process
    _test_schema_sync_helper(postgres_engine, POSTGRES_TO_DOLT_TYPE_MAPPINGS, dolt)


def _test_schema_sync_helper(source_engine: Engine, type_mapping: dict, dolt: Dolt):
    sync_schema_to_dolt(source_engine, dolt , TABLE_MAP, type_mapping)
    _compare_schema_helper(TEST_SOURCE_TABLE, source_engine, TEST_TARGET_TABLE, dolt.engine, type_mapping)

    with source_engine.connect() as conn:
        conn.execute(ALTER_TABLE)

    sync_schema_to_dolt(source_engine, dolt, TABLE_MAP, type_mapping)
    _compare_schema_helper(TEST_SOURCE_TABLE, source_engine, TEST_TARGET_TABLE, dolt.engine, type_mapping)


def _compare_schema_helper(source_table_name: str,
                           source_engine: Engine,
                           target_table_name: str,
                           target_engine: Engine,
                           type_mapping: dict):
    source_metadata = MetaData(bind=source_engine)
    source_metadata.reflect()
    target_metadata = MetaData(bind=target_engine)
    target_metadata.reflect()

    if target_table_name not in target_metadata.tables.keys():
        raise AssertionError('{} does not exist in target'.format(target_table_name))

    passed = True
    source_table, target_table = source_metadata.tables[source_table_name], target_metadata.tables[target_table_name]
    target_table_col_map = {col.name: col for col in target_table.columns}
    for source_col in source_table.columns:
        if source_col.name not in target_table_col_map.keys():
            passed = False
        else:
            target_col = target_table_col_map[source_col.name]
            if type(source_col.type) in type_mapping:
                mapped_type = type_mapping[type(source_col.type)]
                target_type = type(target_col.type)
                if mapped_type != target_type and not isinstance(mapped_type, target_type):
                    passed = False

    assert passed, 'found errors'
