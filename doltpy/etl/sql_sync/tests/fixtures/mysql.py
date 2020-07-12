from sqlalchemy.engine import Engine
from sqlalchemy import Table, MetaData
import pytest
from doltpy.etl.sql_sync.tests.fixtures.db_fixtures_helper import engine_helper

from doltpy.etl.sql_sync.tests.helpers.data_helper import TEST_TABLE_METADATA
from typing import Tuple

MYSQL_ROOT_PASSWORD = 'test'
MYSQL_USER = 'MYSQL_USER'
MYSQL_PASSWORD = 'mysql_password'
MYSQL_DATABASE = 'test_db'
MYSQL_CONTAINER_NAME = 'test_mysql'
MYSQL_PORT = 3306


@pytest.fixture(scope='session')
def mysql_service_def():
    """
    Provides the Docker service definitions for running an instance of MySQL, result is converted to YAML file and then
    used to create a Docker compose file.
    :return:
    """
    environment_dict = dict(MYSQL_ALLOW_EMPTY_PASSWORD='no',
                            MYSQL_ROOT_PASSWORD=MYSQL_ROOT_PASSWORD,
                            MYSQL_DATABASE=MYSQL_DATABASE,
                            MYSQL_USER=MYSQL_USER,
                            MYSQL_PASSWORD=MYSQL_PASSWORD)
    return {
        'image': 'mysql',
        'container_name': MYSQL_CONTAINER_NAME,
        'environment': environment_dict,
        'ports': ['{port}:{port}'.format(port=MYSQL_PORT)]
    }


@pytest.fixture
def mysql_engine(docker_ip, docker_services) -> Engine:
    return engine_helper('mysql+mysqlconnector',
                         MYSQL_USER,
                         MYSQL_PASSWORD,
                         docker_ip,
                         docker_services.port_for('mysql', MYSQL_PORT),
                         MYSQL_DATABASE)


@pytest.fixture
def mysql_with_table(mysql_engine) -> Tuple[Engine, Table]:
    TEST_TABLE_METADATA.metadata.create_all(mysql_engine)
    yield mysql_engine, TEST_TABLE_METADATA
    reflected_table = MetaData(bind=mysql_engine, reflect=True).tables[TEST_TABLE_METADATA.name]
    reflected_table.drop()
