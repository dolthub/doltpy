import mysql.connector as connector
import pytest
import yaml
from retry import retry
from doltpy.etl.sql_sync.tests.helpers.data_helper import TABLE_NAME, CREATE_TEST_TABLE, DROP_TEST_TABLE, TEST_DATA_INITIAL
from doltpy.etl.sql_sync.mysql import write_to_table

MYSQL_USER = 'MYSQL_USER'
MYSQL_PASSWORD = 'mysql_password'
MYSQL_DB = 'test_db'
MYSQL_CONTAINER_NAME = 'test_mysql'


@pytest.fixture(scope='session')
def docker_compose_file(tmpdir_factory):
    compose_file = tmpdir_factory.mktemp('docker_files').join('docker-compose.yml')
    environment_dict = dict(MYSQL_ALLOW_EMPTY_PASSWORD='no',
                            MYSQL_ROOT_PASSWORD='test',
                            MYSQL_DATABASE='test_db',
                            MYSQL_USER='MYSQL_USER',
                            MYSQL_PASSWORD='mysql_password')
    compose_conf = {
        'version': '2',
        'services': {
            'mysql': {
                'image': 'mysql',
                'container_name': MYSQL_CONTAINER_NAME,
                'environment': environment_dict,
                'ports': ['3306:3306']
            }
        }
    }

    with compose_file.open('w') as f:
        yaml.dump(compose_conf, stream=f)

    return compose_file.strpath


@retry(exceptions=connector.errors.DatabaseError, delay=2, tries=10)
def get_connection(host, port):
    return connector.connect(host=host, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DB, port=port)


@pytest.fixture
def mysql_connection(docker_ip, docker_services):
    return get_connection(docker_ip, docker_services.port_for('mysql', 3306))


@pytest.fixture
def mysql_with_table(mysql_connection):
    # connect to the database and create the table
    curs1 = mysql_connection.cursor()
    curs1.execute(CREATE_TEST_TABLE)
    mysql_connection.commit()

    yield mysql_connection, TABLE_NAME

    # drop table
    curs1 = mysql_connection.cursor()
    curs1.execute(DROP_TEST_TABLE)
    mysql_connection.commit()
    mysql_connection.close()


@pytest.fixture
def mysql_with_initial_data(mysql_with_table):
    conn, table = mysql_with_table
    write_to_table(TABLE_NAME, conn, TEST_DATA_INITIAL)
    return conn, table



