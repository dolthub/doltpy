from doltpy.sql import ServerConfig
from typing import List
from datetime import datetime
import pandas as pd

TEST_SERVER_CONFIG = ServerConfig(user='root')

TEST_TABLE = 'characters'
TEST_DATA_INITIAL = [
    {'name': 'Anna', 'adjective': 'tragic', 'id': 1, 'date_of_death': datetime(1877, 1, 1)},
    {'name': 'Vronksy', 'adjective': 'honorable', 'id': 2, 'date_of_death': None},
    {'name': 'Oblonksy', 'adjective': 'buffoon', 'id': 3, 'date_of_death': None},
]

TEST_DATA_UPDATE = [
    {'name': 'Vronksy', 'adjective': 'honorable', 'id': 2, 'date_of_death': datetime(1879, 1, 1)},
    {'name': 'Levin', 'adjective': 'tiresome', 'id': 4, 'date_of_death': None},
]

TEST_DATA_FINAL = [TEST_DATA_INITIAL[0], TEST_DATA_INITIAL[2]] + TEST_DATA_UPDATE


def compare_rows(left: List[dict], right: List[dict], sorting_key: str):
    assert len(left) == len(right), f'{len(left)} != {len(right)}, expected same record count'
    sorted_left, sorted_right = sorted(left, key=lambda r: r[sorting_key]), sorted(right, key=lambda r: r[sorting_key])
    errors = []
    for l, r in zip(sorted_left, sorted_right):
        keys = l.keys()
        for key in keys:
            l_val, r_val = l[key], r[key]
            if l_val != r_val:
                if not (l_val is None and pd.isna(r_val)):
                    errors.append(f'{l_val} != {r_val}')

    assert not errors, f'The following errors occurred {errors}'
