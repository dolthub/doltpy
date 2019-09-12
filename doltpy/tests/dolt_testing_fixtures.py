import os
from doltpy.dolt import Dolt
import pytest
import shutil

REPO_DIR = '/Users/oscarbatori/Documents/liquidata/doltpy/test_data'
REPO_DATA_DIR = os.path.join(REPO_DIR, '.doltpy')


@pytest.fixture
def init_repo() -> Dolt:
    assert not os.path.exists(REPO_DATA_DIR)
    repo = Dolt(REPO_DIR)
    repo.init_new_repo()
    yield repo
    if os.path.exists(REPO_DATA_DIR):
        shutil.rmtree(REPO_DATA_DIR)
