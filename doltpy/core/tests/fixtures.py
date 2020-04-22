import os
from doltpy.core.dolt import init_new_repo, Dolt
import pytest
import shutil
from doltpy.core.tests.helpers import get_repo_path_tmp_path


@pytest.fixture
def init_empty_test_repo(tmp_path) -> Dolt:
    repo_path, repo_data_dir = get_repo_path_tmp_path(tmp_path)
    assert not os.path.exists(repo_data_dir)
    repo = init_new_repo(repo_path)
    yield repo
    if os.path.exists(repo_data_dir):
        shutil.rmtree(repo_data_dir)
