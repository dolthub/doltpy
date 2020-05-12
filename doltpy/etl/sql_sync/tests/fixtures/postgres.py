import pytest
import psycopg2
from retry import retry
from doltpy.etl.sql_sync.tests.helpers.data_helper import TABLE_NAME, DROP_TEST_TABLE, TEST_DATA_INITIAL
from doltpy.etl.sql_sync.tests.helpers.postgres import CREATE_TEST_TABLE
from doltpy.etl.sql_sync.postgres import get_insert_query
from doltpy.etl.sql_sync.db_tools import write_to_table

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


@retry(exceptions=Exception, delay=2, tries=10)
def get_connection(host, port):
    conn_str = "host='{host}' port='{port}' dbname='{dbname}' user='{user}' password='{password}'".format(
        host=host,
        port=port,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    return psycopg2.connect(conn_str)


@pytest.fixture
def postgres_connection(docker_ip, docker_services):
    return get_connection(docker_ip, docker_services.port_for('postgres', POSTGRES_PORT))


@pytest.fixture
def postgres_with_table(postgres_connection):
    create_cursor = postgres_connection.cursor()
    create_cursor.execute(CREATE_TEST_TABLE)
    postgres_connection.commit()
    create_cursor.close()

    yield postgres_connection, TABLE_NAME

    drop_cursor = postgres_connection.cursor()
    drop_cursor.execute(DROP_TEST_TABLE)
    postgres_connection.commit()
    drop_cursor.close()


@pytest.fixture
def postgres_with_initial_data(postgres_with_table):
    conn, table = postgres_with_table
    write_to_table(conn, TABLE_NAME, get_insert_query, TEST_DATA_INITIAL)
