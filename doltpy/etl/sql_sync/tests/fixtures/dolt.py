import pytest
import logging
from doltpy.core import Dolt
from doltpy.etl.sql_sync.dolt import write_to_table
from doltpy.etl.sql_sync.tests.helpers.data_helper import DROP_TEST_TABLE, TABLE_NAME, TEST_DATA_INITIAL
from doltpy.etl.sql_sync.tests.helpers.mysql import CREATE_TEST_TABLE
from typing import Tuple

logger = logging.getLogger(__name__)


@pytest.fixture
def repo_with_table(init_empty_test_repo) -> Tuple[Dolt, str]:
    repo = init_empty_test_repo
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
    write_to_table(repo, table, TEST_DATA_INITIAL)
    return repo, table

