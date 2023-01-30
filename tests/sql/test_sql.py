import io
import psutil
import os
import shutil
from subprocess import Popen
import tempfile
import time

import pytest
from doltpy.cli import Dolt
from doltpy.sql import DoltSQLServerContext, DoltSQLEngineContext, ServerConfig
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
        assert _count_proc_helper('running') + _count_proc_helper('sleeping') >= 1

    assert _count_proc_helper('zombie') >= 1


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


@pytest.fixture(scope="function")
def sql_server():
    p = None
    d = tempfile.TemporaryDirectory()
    try:
        db_path = os.path.join(d.name, "tracks")
        db = Dolt.init(db_path)
        db.sql("create table tracks (TrackId bigint, Name text)")
        db.sql("insert into tracks values (0, 'Sue'), (1, 'L'), (2, 'M'), (3, 'Ji'), (4, 'Po')")
        db.sql("call dolt_commit('-Am', 'Init tracks')")
        p = Popen(args=["dolt", "sql-server", "-l", "trace", "--port", "3307"], cwd=db_path)
        time.sleep(.5)
        yield db
    finally:
        if p is not None:
            p.kill()
        if os.path.exists(d.name):
            shutil.rmtree(d.name)

def test_show_tables_engine(sql_server):
    dolt = sql_server
    conf = ServerConfig(user="root", host="localhost", port="3307")
    conn = DoltSQLEngineContext(dolt, conf)
    tables = conn.tables()
    assert "tracks" in tables

def test_log_file(sql_server, tmp_path):
    log_file = tmp_path / "temp_log"
    conf = ServerConfig(user="root", host="localhost", port="3306", log_file=log_file)
    with DoltSQLServerContext(sql_server, conf) as conn:
        assert len(log_file.open().read()) > 0
