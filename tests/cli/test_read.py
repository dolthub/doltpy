from doltpy.cli import Dolt
from typing import List
import pytest
from doltpy.cli.write import write_rows, CREATE, UPDATE
from .helpers import compare_rows_helper
from doltpy.cli.read import read_rows, read_pandas, read_columns
from doltpy.shared.helpers import columns_to_rows

def compare_rows_helper(expected: List[dict], actual: List[dict]):
    assert len(expected) == len(actual), f'Unequal row counts: {len(expected)} != {len(actual)}'
    errors = []
    for l, r in zip(expected, actual):
        l_cols, r_cols = set(l.keys()), set(r.keys())
        assert l_cols == r_cols, f'Unequal sets of columns: {l_cols} != {r_cols}'
        for col in l_cols:
            l_val, r_val = l[col], r[col]
            if col.startswith('date'):
                l_val, r_val = l_val[:10], r_val[:10]
            if l_val != r_val and not (l_val is None and r_val == ''):
                errors.append(f'{col}: {l_val} != {r_val}')

    error_str = '\n'.join(errors)
    assert not errors, f'Failed with the following unequal columns:\n{error_str}'

TEST_TABLE = 'characters'
TEST_DATA_INITIAL = [
    {'name': 'Anna', 'adjective': 'tragic', 'id': '1', 'date_of_death': '1877-01-01'},
    {'name': 'Vronksy', 'adjective': 'honorable', 'id': '2', 'date_of_death': ''},
    {'name': 'Oblonksy', 'adjective': 'buffoon', 'id': '3', 'date_of_death': ''},
]

TEST_DATA_UPDATE = [
    {'name': 'Levin', 'adjective': 'tiresome', 'id': '4', 'date_of_death': ''}
]

TEST_DATA_COMBINED = TEST_DATA_INITIAL + TEST_DATA_UPDATE


@pytest.fixture()
def with_initial_test_data(init_empty_test_repo):
    dolt = init_empty_test_repo
    return _write_helper(dolt, TEST_DATA_INITIAL, CREATE)


def update_test_data(dolt: Dolt):
    _, commit = _write_helper(dolt, TEST_DATA_UPDATE, UPDATE)
    return commit


def _write_helper(dolt: Dolt, data: List[dict], update_type: str):
    write_rows(dolt, TEST_TABLE, data, update_type, ['id'], commit=True)
    commit_hash, _ = dolt.log().popitem(last=False)
    return dolt, commit_hash


def test_read_rows(with_initial_test_data):
    dolt, first_commit = with_initial_test_data
    second_commit = update_test_data(dolt)
    first_write = read_rows(dolt, TEST_TABLE, first_commit)
    compare_rows_helper(first_write, TEST_DATA_INITIAL)
    second_write = read_rows(dolt, TEST_TABLE, second_commit)
    compare_rows_helper(second_write, TEST_DATA_COMBINED)


def test_read_columns(with_initial_test_data):
    dolt, first_commit = with_initial_test_data
    second_commit = update_test_data(dolt)
    first_write = read_columns(dolt, TEST_TABLE, first_commit)
    compare_rows_helper(columns_to_rows(first_write), TEST_DATA_INITIAL)
    second_write = read_columns(dolt, TEST_TABLE, second_commit)
    compare_rows_helper(columns_to_rows(second_write), TEST_DATA_COMBINED)


def test_read_pandas(with_initial_test_data):
    dolt, first_commit = with_initial_test_data
    second_commit = update_test_data(dolt)
    first_write = read_pandas(dolt, TEST_TABLE, first_commit)
    compare_rows_helper(first_write.to_dict('records'), TEST_DATA_INITIAL)
    second_write = read_pandas(dolt, TEST_TABLE, second_commit)
    compare_rows_helper(second_write.to_dict('records'), TEST_DATA_COMBINED)

