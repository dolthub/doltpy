import os
from doltpy.cli.dolt import Dolt
import pytest
import shutil
from doltpy.cli.tests.helpers import get_repo_path_tmp_path


@pytest.fixture
def init_empty_test_repo(tmp_path) -> Dolt:
    repo_path, repo_data_dir = get_repo_path_tmp_path(tmp_path)
    assert not os.path.exists(repo_data_dir)
    repo = Dolt.init(repo_path)
    yield repo
    if os.path.exists(repo_data_dir):
        shutil.rmtree(repo_data_dir)


@pytest.fixture
def init_other_empty_test_repo(tmp_path) -> Dolt:
    repo_path, repo_data_dir = get_repo_path_tmp_path(tmp_path, 'other')
    assert not os.path.exists(repo_data_dir)
    os.mkdir(repo_path)
    repo = Dolt.init(repo_path)
    yield repo
    if os.path.exists(repo_data_dir):
        shutil.rmtree(repo_data_dir)
