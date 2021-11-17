from doltpy.cli import Dolt
from typing import List
import pytest
from doltpy.cli.write import write_rows, CREATE, UPDATE
from .helpers import compare_rows_helper
from doltpy.cli.read import read_pandas


TEST_TABLE = 'characters'
TEST_DATA_INITIAL = [
    {'name': 'Anna', 'adjective': 'tragic', 'id': 1, 'date_of_death': '1877-01-01'},
    {'name': 'Vronksy', 'adjective': 'honorable', 'id': 2, 'date_of_death': ''},
    {'name': 'Oblonksy', 'adjective': 'buffoon', 'id': 3, 'date_of_death': ''},
]

TEST_DATA_UPDATE = [
    {'name': 'Levin', 'adjective': 'tiresome', 'id': 4, 'date_of_death': ''}
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


def test_read_pandas(with_initial_test_data):
    dolt, first_commit = with_initial_test_data
    second_commit = update_test_data(dolt)
    first_write = read_pandas(dolt, TEST_TABLE, first_commit)
    compare_rows_helper(TEST_DATA_INITIAL, first_write.to_dict('records'))
    second_write = read_pandas(dolt, TEST_TABLE, second_commit)
    res = second_write.to_dict('records')
    res = sorted(res, key=lambda x: int(x['id']))
    compare_rows_helper(TEST_DATA_COMBINED, res)

