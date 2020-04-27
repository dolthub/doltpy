import pytest
import logging
from doltpy.core import Dolt
from doltpy.etl.sql_sync.dolt import write_to_table
from doltpy.etl.sql_sync.tests.helpers.data_helper import (DROP_TEST_TABLE,
                                                           TABLE_NAME,
                                                           TEST_DATA_INITIAL,
                                                           TEST_DATA_APPEND_SINGLE_ROW,
                                                           TEST_DATA_APPEND_MULTIPLE_ROWS)
from doltpy.etl.sql_sync.tests.helpers.mysql import CREATE_TEST_TABLE
from typing import Tuple

logger = logging.getLogger(__name__)


@pytest.fixture
def repo_with_table(request, init_empty_test_repo) -> Tuple[Dolt, str]:
    repo = init_empty_test_repo
    repo.start_server()
    connection = repo.get_connection()
    create_curs = connection.cursor()
    create_curs.execute(CREATE_TEST_TABLE)
    connection.commit()
    connection.close()

    def finalize():
        if repo.server:
            repo.stop_server()

    request.addfinalizer(finalize)

    return repo, TABLE_NAME


@pytest.fixture
def repo_with_initial_data(repo_with_table) -> Tuple[Dolt, str]:
    repo, table = repo_with_table
    write_to_table(repo, table, TEST_DATA_INITIAL)
    return repo, table


@pytest.fixture
def create_dolt_test_data_commits(repo_with_table):
    repo, table = repo_with_table

    write_to_table(repo, table, TEST_DATA_INITIAL, commit=True)
    write_to_table(repo, table, TEST_DATA_APPEND_SINGLE_ROW, commit=True)
    write_to_table(repo, table, TEST_DATA_APPEND_MULTIPLE_ROWS, commit=True)

    return repo, table
