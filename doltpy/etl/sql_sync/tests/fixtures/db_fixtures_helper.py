import pytest
import yaml
from sqlalchemy import create_engine
from retry import retry
import sqlalchemy


@pytest.fixture(scope='session')
def docker_compose_file(tmpdir_factory, mysql_service_def, postgres_service_def):
    compose_file = tmpdir_factory.mktemp('docker_files').join('docker-compose.yml')

    compose_conf = {
        'version': '2',
        'services': {
            'mysql': mysql_service_def,
            'postgres': postgres_service_def
        }
    }

    with compose_file.open('w') as f:
        yaml.dump(compose_conf, stream=f)

    return compose_file.strpath


def engine_helper(dialect: str, user: str, password: str, host: str, port: int, database: str):
    """

    :param dialect:
    :param user:
    :param password:
    :param host:
    :param port:
    :param database:
    :return:
    """
    engine = create_engine(
        '{dialect}://{user}:{password}@{host}:{port}/{database}'.format(
            dialect=dialect,
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        ),
        echo=True
    )

    @retry(exceptions=sqlalchemy.exc.OperationalError, delay=2, tries=10)
    def verify_connection():
        conn = engine.connect()
        conn.close()
        return engine

    return verify_connection()
