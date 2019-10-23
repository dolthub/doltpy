import os
from doltpy.core.dolt import Dolt
import pytest
import shutil
from typing import Tuple


def get_repo_path_tmp_path(path: str) -> Tuple[str, str]:
    return path, os.path.join(path, '.dolt')


@pytest.fixture
def init_repo(tmp_path) -> Dolt:
    repo_path, repo_data_dir = get_repo_path_tmp_path(tmp_path)
    assert not os.path.exists(repo_data_dir)
    repo = Dolt(repo_path)
    repo.init_new_repo()
    yield repo
    if os.path.exists(repo_data_dir):
        shutil.rmtree(repo_data_dir)
