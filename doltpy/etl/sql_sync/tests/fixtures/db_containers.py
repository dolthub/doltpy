import pytest
import yaml


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
