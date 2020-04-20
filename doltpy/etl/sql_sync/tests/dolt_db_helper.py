import pytest
import logging
from doltpy.core import Dolt
from doltpy.etl.sql_sync.tests.data_helper import CREATE_TEST_TABLE, DROP_TEST_TABLE, TABLE_NAME, TEST_DATA_INITIAL
from doltpy.etl.sql_sync.tests.db_helpers import mysql_insert_helper
from doltpy.core.tests.dolt_testing_fixtures import init_repo
from typing import List, Tuple

logger = logging.getLogger(__name__)


@pytest.fixture
def repo_with_table(init_repo) -> Tuple[Dolt, str]:
    repo = init_repo
    connection = repo.start_server()
    curs1 = connection.cursor()
    curs1.execute(CREATE_TEST_TABLE)
    connection.commit()

    yield repo, TABLE_NAME

    # The SQL server seems to get into a strange state
    repo.stop_server()
    repo.start_server()
    new_conn = repo.cnx
    curs1 = new_conn.cursor()
    curs1.execute(DROP_TEST_TABLE)
    new_conn.commit()
    repo.stop_server()


@pytest.fixture
def repo_with_initial_data(repo_with_table) -> Tuple[Dolt, str]:
    repo, table = repo_with_table
    insert_tuples(repo, table, TEST_DATA_INITIAL)
    return repo, table


def insert_tuples(repo: Dolt, table: str, data: List[tuple]):
    connection = repo.cnx
    mysql_insert_helper(connection, data)
    repo.add_table_to_next_commit(table)
    repo.commit('Inserted test data')
