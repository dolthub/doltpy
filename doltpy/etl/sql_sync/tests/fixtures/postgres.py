import pytest
from doltpy.etl.sql_sync.tests.helpers.data_helper import TEST_TABLE_METADATA
from doltpy.etl.sql_sync.tests.fixtures.db_fixtures_helper import engine_helper
from sqlalchemy import MetaData

POSTGRES_CONTAINER_NAME = 'TEST_POSTGRES'
POSTGRES_DB = 'test_db'
POSTGRES_USER = 'POSTGRES_USER'
POSTGRES_PASSWORD = 'postgres_password'
POSTGRES_PORT = 5432


@pytest.fixture(scope='session')
def postgres_service_def():
    environment_dict = dict(POSTGRES_DB=POSTGRES_DB,
                            POSTGRES_USER=POSTGRES_USER,
                            POSTGRES_PASSWORD=POSTGRES_PASSWORD)
    return {
        'image': 'postgres:latest',
        'container_name': 'test_postgres',
        'environment': environment_dict,
        'ports': ['{port}:{port}'.format(port=POSTGRES_PORT)]
    }


@pytest.fixture
def postgres_engine(docker_ip, docker_services):
    return engine_helper('postgresql',
                         POSTGRES_USER,
                         POSTGRES_PASSWORD,
                         docker_ip,
                         docker_services.port_for('postgres', POSTGRES_PORT),
                         POSTGRES_DB)


@pytest.fixture
def postgres_with_table(postgres_engine):
    TEST_TABLE_METADATA.metadata.create_all(postgres_engine)
    yield postgres_engine, TEST_TABLE_METADATA
    reflected_table = MetaData(bind=postgres_engine, reflect=True).tables[TEST_TABLE_METADATA.name]
    reflected_table.drop()
