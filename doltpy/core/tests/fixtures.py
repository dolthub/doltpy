import os
from doltpy.core.dolt import Dolt, ServerConfig
import pytest
import shutil
from doltpy.core.tests.helpers import get_repo_path_tmp_path
import sqlalchemy
from retry import retry

@pytest.fixture
def init_empty_test_repo(tmp_path) -> Dolt:
    repo_path, repo_data_dir = get_repo_path_tmp_path(tmp_path)
    assert not os.path.exists(repo_data_dir)
    repo = Dolt.init(repo_path)
    yield repo
    if os.path.exists(repo_data_dir):
        shutil.rmtree(repo_data_dir)


@pytest.fixture
def run_serve_mode(request, init_empty_test_repo):
    repo = init_empty_test_repo
    repo.sql_server()

    def finalize():
        if repo.server:
            repo.sql_server_stop()

    # This block ensures the server is accepting connections
    @retry(exceptions=(sqlalchemy.exc.OperationalError, sqlalchemy.exc.DatabaseError), delay=2, tries=10)
    def verify_connection():
        conn = repo.engine.connect()
        conn.close()
        return repo

    request.addfinalizer(finalize)
    return verify_connection()


@pytest.fixture
def init_other_empty_test_repo(tmp_path) -> Dolt:
    repo_path, repo_data_dir = get_repo_path_tmp_path(tmp_path, 'other')
    assert not os.path.exists(repo_data_dir)
    os.mkdir(repo_path)
    repo = Dolt.init(repo_path, ServerConfig(port=3307))
    yield repo
    if os.path.exists(repo_data_dir):
        shutil.rmtree(repo_data_dir)
