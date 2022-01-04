import os
from typing import Tuple, List
import pandas as pd


def get_repo_path_tmp_path(path: str, subpath: str = None) -> Tuple[str, str]:
    if subpath:
        return os.path.join(path, subpath), os.path.join(path, subpath, '.dolt')
    else:
        return path, os.path.join(path, '.dolt')


def compare_rows(left: List[dict], right: List[dict], sorting_key: str):
    assert len(left) == len(right), f"{len(left)} != {len(right)}, expected same record count"
    sorted_left, sorted_right = sorted(left, key=lambda r: r[sorting_key]), sorted(right, key=lambda r: r[sorting_key])
    errors = []
    for l, r in zip(sorted_left, sorted_right):
        l_cols, r_cols = set(l.keys()), set(r.keys())
        assert l_cols == r_cols, f'Unequal sets of columns: {l_cols} != {r_cols}'

        for col in l_cols:
            l_val, r_val = l[col], r[col]

            if col.startswith('date') and l_val and r_val:
                l_val, r_val = l_val[:10], r_val[:10]
            if l_val != r_val and not ((l_val is None or l_val == '') and (r_val == '' or pd.isna(r_val))):
                errors.append(f'{col}: {l_val} != {r_val}')
