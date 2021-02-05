import pytest
import yaml
import sqlalchemy as sa
from retry import retry


@pytest.fixture(scope='session')
def docker_compose_file(tmpdir_factory, mysql_service_def, postgres_service_def, oracle_service_def):
    compose_file = tmpdir_factory.mktemp('docker_files').join('docker-compose.yml')

    compose_conf = {
        'version': '2',
        'services': {
            'mysql': mysql_service_def,
            'postgres': postgres_service_def,
            'oracle': oracle_service_def
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
    engine = sa.create_engine(
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

    @retry(delay=2, tries=10, exceptions=(
        sa.exc.OperationalError,
        sa.exc.DatabaseError,
        sa.exc.InterfaceError,
    ))
    def verify_connection():
        conn = engine.connect()
        conn.close()
        return engine

    return verify_connection()
