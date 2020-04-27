import os
from typing import Tuple


def get_repo_path_tmp_path(path: str) -> Tuple[str, str]:
    return path, os.path.join(path, '.dolt')