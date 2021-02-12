import psutil
import pytest
from doltpy.cli import Dolt
from doltpy.sql import DoltSQLServerContext
from .helpers import TEST_SERVER_CONFIG, TEST_DATA_INITIAL

TEST_TABLE_ONE, TEST_TABLE_TWO = 'foo', 'bar'
COMMIT_MESSAGE = 'major update'


@pytest.fixture()
def with_test_tables(init_empty_test_repo):
    dolt = init_empty_test_repo
    _add_test_table(dolt, TEST_TABLE_ONE)
    _add_test_table(dolt, TEST_TABLE_TWO)
    return dolt


def _add_test_table(dolt: Dolt, table_name: str):
    dolt.sql(query=f'''
        CREATE TABLE `{table_name}` (
            `name` VARCHAR(32),
            `adjective` VARCHAR(32),
            `id` INT NOT NULL,
            `date_of_death` DATETIME,
            PRIMARY KEY (`id`)
        );
    ''')
    dolt.add(table_name)
    dolt.commit('Created test table')


def test_context_manager_cleanup(init_empty_test_repo):
    dolt = init_empty_test_repo
    with DoltSQLServerContext(dolt, TEST_SERVER_CONFIG) as _:
        assert _count_proc_helper('running') == 1

    assert _count_proc_helper('zombie') == 1


def _count_proc_helper(status: str):
    return len([proc for proc in psutil.Process().children(recursive=True) if proc.status() == status])


def test_commit_tables(with_test_tables):
    dolt = with_test_tables
    with DoltSQLServerContext(dolt, TEST_SERVER_CONFIG) as dssc:
        dssc.write_rows(TEST_TABLE_ONE, TEST_DATA_INITIAL, commit=False)
        dssc.write_rows(TEST_TABLE_TWO, TEST_DATA_INITIAL, commit=False)
        dssc.commit_tables(COMMIT_MESSAGE, [TEST_TABLE_ONE, TEST_TABLE_TWO], False)

    _, commit = dolt.log().popitem(last=False)
    assert commit.message == COMMIT_MESSAGE


@pytest.mark.skip()
def test_commit_tables_empty_commit():
    pass


def test_show_tables(with_test_tables):
    dolt = with_test_tables
    with DoltSQLServerContext(dolt, TEST_SERVER_CONFIG) as dssc:
        tables = dssc.tables()
        assert TEST_TABLE_ONE in tables and TEST_TABLE_TWO in tables