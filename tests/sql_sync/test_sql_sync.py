import pytest
import logging
from doltpy.sql.sync.mysql import get_target_writer as get_mysql_target_writer
from doltpy.sql.sync.postgres import get_target_writer as get_postgres_target_writer
from doltpy.sql.sync.oracle import get_target_writer as get_oracle_target_writer
from doltpy.sql.sync.db_tools import get_source_reader, get_table_reader, get_table_metadata
from doltpy.sql.sync.sync_tools import sync_to_dolt
from doltpy.sql.sync.dolt import (get_target_writer as get_dolt_target_writer,
                                  get_table_reader as get_dolt_table_reader)
from .helpers.tools import validate_dolt_as_source, validate_dolt_as_target
from .helpers.data_helper import assert_rows_equal, TEST_DATA_WITH_ARRAYS, deserialize_longtext

logger = logging.getLogger(__name__)


def test_dolt_to_mysql(mysql_with_table, create_dolt_test_data_commits):
    """
    Tests Dolt to MySQL, see validate_dolt_as_source for details.
    """
    mysql_conn, mysql_table = mysql_with_table
    dssc, dolt_table = create_dolt_test_data_commits
    validate_dolt_as_source(mysql_conn, mysql_table, get_mysql_target_writer, dssc, dolt_table)


def test_mysql_to_dolt(mysql_with_table, db_with_table):
    """
    Tests MySQL to Dolt, see validate_dolt_as_source for details.
    """
    mysql_engine, mysql_table = mysql_with_table
    dssc, dolt_table = db_with_table
    validate_dolt_as_target(mysql_engine,
                            mysql_table,
                            get_source_reader,
                            get_mysql_target_writer,
                            get_table_reader,
                            dssc,
                            dolt_table)


def test_dolt_postgres(postgres_with_table, create_dolt_test_data_commits):
    """
    Tests Dolt to Postgres, see validate_dolt_as_source for details.
    """
    postgres_conn, postgres_table = postgres_with_table
    dssc, dolt_table = create_dolt_test_data_commits
    validate_dolt_as_source(postgres_conn, postgres_table, get_postgres_target_writer, dssc, dolt_table)


def test_postgres_to_dolt(postgres_with_table, db_with_table):
    """
    Tests Postgres to Dolt, see validate_dolt_as_source for details.
    """
    postgres_conn, postgres_table = postgres_with_table
    dssc, dolt_table = db_with_table
    validate_dolt_as_target(postgres_conn,
                            postgres_table,
                            get_source_reader,
                            get_postgres_target_writer,
                            get_table_reader,
                            dssc,
                            dolt_table)


def test_postgres_to_dolt_array_types(postgres_with_table_with_arrays, db_with_table_with_arrays):
    postgres_engine, postgres_table = postgres_with_table_with_arrays
    dssc, dolt_table = db_with_table_with_arrays

    get_postgres_target_writer(postgres_engine)({str(postgres_table.name): ([], TEST_DATA_WITH_ARRAYS)})
    source_reader = get_source_reader(postgres_engine, get_table_reader())
    target_writer = get_dolt_target_writer(dssc, commit=True)
    sync_to_dolt(source_reader, target_writer, {str(postgres_table.name): str(dolt_table.name)})
    latest_commit = list(dssc.dolt.log().keys())[0]

    _, dolt_data = get_dolt_table_reader(latest_commit)(str(dolt_table.name), dssc)
    db_table_metadata = get_table_metadata(postgres_engine, str(postgres_table.name))
    db_data = get_table_reader()(postgres_engine, db_table_metadata)
    clean_dolt_data = deserialize_longtext(dolt_data)
    assert_rows_equal(clean_dolt_data, db_data, lambda dic: dic['id'])


def test_oracle_to_mysql(oracle_with_table, create_dolt_test_data_commits):
    """
    Tests Dolt to Oracle, see validate_dolt_as_source for details.
    """
    oracle_conn, oracle_table = oracle_with_table
    dssc, dolt_table = create_dolt_test_data_commits
    validate_dolt_as_source(oracle_conn, oracle_table, get_oracle_target_writer, dssc, dolt_table)


def test_oracle_to_dolt(oracle_with_table, db_with_table):
    """
    Tests Oracle to Dolt, see validate_dolt_as_source for details.
    """
    oracle_engine, oracle_table = oracle_with_table
    dssc, dolt_table = db_with_table
    validate_dolt_as_target(oracle_engine,
                            oracle_table,
                            get_source_reader,
                            get_oracle_target_writer,
                            get_table_reader,
                            dssc,
                            dolt_table,
                            datetime_strict=False)
