import os
from typing import Tuple


def get_repo_path_tmp_path(path: str, subpath: str = None) -> Tuple[str, str]:
    if subpath:
        return os.path.join(path, subpath), os.path.join(path, subpath, '.dolt')
    else:
        return path, os.path.join(path, '.dolt')

