import pytest
import logging
from doltpy.core import Dolt
from doltpy.etl.sql_sync.dolt import write_to_table
from doltpy.etl.sql_sync.tests.helpers.data_helper import (TABLE_NAME,
                                                           TEST_DATA_INITIAL,
                                                           TEST_DATA_APPEND_SINGLE_ROW,
                                                           TEST_DATA_APPEND_MULTIPLE_ROWS,
                                                           TEST_TABLE_METADATA,
                                                           DOLT_TABLE_WITH_ARRAYS,
                                                           get_dolt_update_row_statement,
                                                           get_dolt_drop_pk_query)
from typing import Tuple
import sqlalchemy
from sqlalchemy import Table, MetaData
from retry import retry

logger = logging.getLogger(__name__)


@pytest.fixture
def repo_with_table(request, init_empty_test_repo) -> Tuple[Dolt, Table]:
    repo = init_empty_test_repo
    return _test_table_helper(repo, request, TEST_TABLE_METADATA)


@pytest.fixture
def repo_with_table_with_arrays(request, init_empty_test_repo) -> Tuple[Dolt, Table]:
    repo = init_empty_test_repo
    return _test_table_helper(repo, request, DOLT_TABLE_WITH_ARRAYS)


def _test_table_helper(repo: Dolt, request, metadata: MetaData) -> Tuple[Dolt, Table]:
    repo.sql_server()

    @retry(delay=2, tries=10, exceptions=(
            sqlalchemy.exc.OperationalError,
            sqlalchemy.exc.DatabaseError,
            sqlalchemy.exc.InterfaceError,
    ))
    def verify_connection():
        conn = repo.engine.connect()
        conn.close()
        return repo.engine

    engine = verify_connection()
    metadata.create(engine)

    def finalize():
        if repo.server:
            repo.sql_server_stop()

    request.addfinalizer(finalize)

    return repo, metadata


@pytest.fixture
def create_dolt_test_data_commits(repo_with_table):
    """
    Given a test repo with a test table created makes series of updates to the table creating a non trivial commit
    graph to be used for testing reads against.
    :param repo_with_table:
    :return:
    """
    repo, table = repo_with_table

    write_to_table(repo, table, TEST_DATA_INITIAL, commit=True)
    write_to_table(repo, table, TEST_DATA_APPEND_SINGLE_ROW, commit=True)
    write_to_table(repo, table, TEST_DATA_APPEND_MULTIPLE_ROWS, commit=True)
    # TODO: we currently do not support ON DUPLICATE KEY syntax, so this does the update
    # write_to_table(repo, table, TEST_DATA_UPDATE_SINGLE_ROW, commit=True)
    _query_helper(repo, get_dolt_update_row_statement(table), 'Updated a row')
    _query_helper(repo, get_dolt_drop_pk_query(table), 'Updated a row')

    return repo, table


def _query_helper(repo: Dolt, query, message):
    with repo.engine.connect() as conn:
        conn.execute(query)

    repo.add(TABLE_NAME)
    repo.commit(message)


