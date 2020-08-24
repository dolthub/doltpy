from sqlalchemy.engine import Engine
from sqlalchemy import Table, MetaData
import pytest
from doltpy.etl.sql_sync.tests.fixtures.db_fixtures_helper import engine_helper

from doltpy.etl.sql_sync.tests.helpers.data_helper import TEST_TABLE_METADATA
from doltpy.etl.sql_sync.tests.helpers.schema_sync_helper import MYSQL_TABLE as MYSQL_SCHEMA_SYNC_TEST_TABLE
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
def mysql_with_table(mysql_engine, request) -> Tuple[Engine, Table]:
    return _test_table_helper(mysql_engine, TEST_TABLE_METADATA, request)


@pytest.fixture
def mysql_with_schema_sync_test_table(mysql_engine, request) -> Tuple[Engine, Table]:
    return _test_table_helper(mysql_engine, MYSQL_SCHEMA_SYNC_TEST_TABLE, request)


def _test_table_helper(mysql_engine: Engine, table_metadata: MetaData, request):
    table_metadata.metadata.create_all(mysql_engine)

    def finalize():
        metadata = MetaData(bind=mysql_engine)
        metadata.reflect()
        reflected_table = metadata.tables[table_metadata.name]
        reflected_table.drop()

    request.addfinalizer(finalize)

    return mysql_engine, table_metadata
