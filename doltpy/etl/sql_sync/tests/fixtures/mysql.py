import mysql.connector as connector
import pytest
from retry import retry
from doltpy.etl.sql_sync.tests.helpers.data_helper import TABLE_NAME, DROP_TEST_TABLE
from doltpy.etl.sql_sync.tests.helpers.mysql import CREATE_TEST_TABLE

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


@retry(exceptions=connector.errors.DatabaseError, delay=2, tries=10)
def get_connection(host, port):
    """
    Provides a connection to the MySQL instance running at the specified host/port.
    :param host:
    :param port:
    :return:
    """
    return connector.connect(host=host, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DATABASE, port=port)


@pytest.fixture
def mysql_connection(docker_ip, docker_services):
    """
    Provides a connection to the MySQL instance running on the Docker address provided by the fixture parameters.
    :param docker_ip:
    :param docker_services:
    :return:
    """
    return get_connection(docker_ip, docker_services.port_for('mysql', MYSQL_PORT))


@pytest.fixture
def mysql_with_table(mysql_connection):
    """
    Creates a test table inside the MySQL Server pointed at by the connection returned by the mysql_connection fixture
    parameter.
    :param mysql_connection:
    :return:
    """
    create_cursor = mysql_connection.cursor()
    create_cursor.execute(CREATE_TEST_TABLE)
    mysql_connection.commit()

    yield mysql_connection, TABLE_NAME

    # drop table
    drop_cursor = mysql_connection.cursor()
    drop_cursor.execute(DROP_TEST_TABLE)
    mysql_connection.commit()
    mysql_connection.close()
