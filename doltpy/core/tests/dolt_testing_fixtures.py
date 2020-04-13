import os
from doltpy.core.dolt import init_new_repo, Dolt, _execute
import pytest
import shutil
from typing import Tuple


def get_repo_path_tmp_path(path: str) -> Tuple[str, str]:
    return path, os.path.join(path, '.dolt')


@pytest.fixture
def init_repo(tmp_path) -> Dolt:
    repo_path, repo_data_dir = get_repo_path_tmp_path(tmp_path)
    assert not os.path.exists(repo_data_dir)
    repo = init_new_repo(repo_path)
    _execute(['rm', 'LICENSE.md'], repo.repo_dir())
    _execute(['rm', 'README.md'], repo.repo_dir())
    yield repo
    if os.path.exists(repo_data_dir):
        shutil.rmtree(repo_data_dir)
