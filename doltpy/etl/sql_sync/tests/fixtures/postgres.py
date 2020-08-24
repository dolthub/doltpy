import pytest
from doltpy.etl.sql_sync.tests.helpers.data_helper import TEST_TABLE_METADATA, POSTGRES_TABLE_WITH_ARRAYS
from doltpy.etl.sql_sync.tests.helpers.schema_sync_helper import POSTGRES_TABLE as POSTGRES_SCHEMA_SYNC_TEST_TABLE
from doltpy.etl.sql_sync.tests.fixtures.db_fixtures_helper import engine_helper
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine

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
def postgres_with_table(postgres_engine, request):
    return _test_table_helper(postgres_engine, TEST_TABLE_METADATA, request)


@pytest.fixture
def postgres_with_table_with_arrays(postgres_engine, request):
    return _test_table_helper(postgres_engine, POSTGRES_TABLE_WITH_ARRAYS, request)


@pytest.fixture
def postgres_with_schema_sync_test_table(postgres_engine, request):
    return _test_table_helper(postgres_engine, POSTGRES_SCHEMA_SYNC_TEST_TABLE, request)


def _test_table_helper(postgres_engine: Engine, table_metadata: MetaData, request):
    table_metadata.metadata.create_all(postgres_engine)

    def finalize():
        metadata = MetaData(bind=postgres_engine)
        metadata.reflect()
        reflected_table = metadata.tables[table_metadata.name]
        reflected_table.drop()

    request.addfinalizer(finalize)

    return postgres_engine, table_metadata
