from sqlalchemy.engine import Engine
from sqlalchemy import Table, MetaData
import pytest
from tests.sql_sync.helpers.data_helper import TEST_TABLE_METADATA
from typing import Tuple
from sqlalchemy import create_engine
import cx_Oracle
import sqlalchemy
from retry import retry

ORACLE_CONTAINER_NAME = 'test_oracle'
ORACLE_DB = 'XEPDB1'
ORACLE_USER = 'py_test'
ORACLE_PWD = 'py_test_password'
ORACLE_LISTENER_PORT = 1521
ORACLE_OEM_EXPRESS_PORT = 5500


@pytest.fixture(scope='session')
def oracle_service_def():
    """
    Provides the Docker service definitions for running an instance of Oracle Express Edition, result is converted to
    YAML file and then used to create a Docker compose file. Note this assumes the existence of a container image named
    'oracle/database:18.4.0-xe' in a registry that your local copy of Docker has in its image path. Details can be
    found here:
       - https://github.com/oracle/docker-images/tree/master/OracleDatabase/SingleInstance
    :return:
    """
    return {
        'image': 'oscarbatori/oracle-database:18.4.0-xe-quick',
        'container_name': ORACLE_CONTAINER_NAME,
        'ports': ['{port}:{port}'.format(port=ORACLE_LISTENER_PORT),
                  '{port}:{port}'.format(port=ORACLE_OEM_EXPRESS_PORT)]
    }


@pytest.fixture
def oracle_engine(docker_ip, docker_services) -> Engine:
    engine = create_engine('oracle+cx_oracle://', creator=lambda: _oracle_connection_helper(docker_ip))

    @retry(delay=10, tries=12, exceptions=(sqlalchemy.exc.DatabaseError))
    def verify_connection():
        conn = engine.connect()
        conn.close()
        return engine

    return verify_connection()


def _oracle_connection_helper(host):
    return cx_Oracle.connect(ORACLE_USER, ORACLE_PWD, '{}:{}/{}'.format(host, ORACLE_LISTENER_PORT, ORACLE_DB))


@pytest.fixture
def oracle_with_table(oracle_engine, request) -> Tuple[Engine, Table]:
    TEST_TABLE_METADATA.metadata.create_all(oracle_engine)

    def finalize():
        metadata = MetaData(bind=oracle_engine)
        metadata.reflect()
        reflected_table = metadata.tables[TEST_TABLE_METADATA.name]
        reflected_table.drop()

    request.addfinalizer(finalize)

    return oracle_engine, TEST_TABLE_METADATA
