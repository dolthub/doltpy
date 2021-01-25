from doltpy.cli.dolt import Dolt
import pytest
from doltpy.cli.tests.helpers import get_repo_path_tmp_path


@pytest.fixture
def init_empty_test_repo(tmpdir) -> Dolt:
    return _init_helper(tmpdir)


@pytest.fixture
def init_other_empty_test_repo(tmpdir) -> Dolt:
    return _init_helper(tmpdir, 'other')


def _init_helper(path: str, ext: str = None):
    repo_path, repo_data_dir = get_repo_path_tmp_path(path, ext)
    return Dolt.init(repo_path)
