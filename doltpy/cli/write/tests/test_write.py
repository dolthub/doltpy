from doltpy.cli.write import write_rows, write_columns, write_pandas, CREATE
from doltpy.cli.read import read_rows
from doltpy.cli.tests import compare_rows_helper
import pandas as pd
import pytest


# Note that we use string values here as serializing via CSV does preserve type information in any meaningful way
TEST_ROWS = [
    {'name': 'Anna', 'adjective': 'tragic', 'id': '1', 'date_of_death': '1877-01-01'},
    {'name': 'Vronksy', 'adjective': 'honorable', 'id': '2', 'date_of_death': ''},
    {'name': 'Oblonksy', 'adjective': 'buffoon', 'id': '3', 'date_of_death': ''},
]

TEST_COLUMNS = {
    'name': ['Anna', 'Vronksy', 'Oblonksy'],
    'adjective': ['tragic', 'honorable', 'buffoon'],
    'id': ['1', '2', '3'],
    'date_of_birth': ['1840-01-01', '1840-01-01', '1840-01-01'],
    'date_of_death': ['1877-01-01', '', '']
}


def test_write_rows(init_empty_test_repo):
    dolt = init_empty_test_repo
    write_rows(dolt, 'characters', TEST_ROWS, CREATE, ['id'])
    actual = read_rows(dolt, 'characters')
    compare_rows_helper(TEST_ROWS, actual)


def test_write_columns(init_empty_test_repo):
    dolt = init_empty_test_repo
    write_columns(dolt, 'characters', TEST_COLUMNS, CREATE, ['id'])
    actual = read_rows(dolt, 'characters')
    expected = [{} for _ in range(len(list(TEST_COLUMNS.values())[0]))]
    for col_name in TEST_COLUMNS.keys():
        for j, val in enumerate(TEST_COLUMNS[col_name]):
            expected[j][col_name] = val

    compare_rows_helper(expected, actual)


def test_write_pandas(init_empty_test_repo):
    dolt = init_empty_test_repo
    write_pandas(dolt, 'characters', pd.DataFrame(TEST_ROWS), CREATE, ['id'])
    actual = read_rows(dolt, 'characters')
    expected = pd.DataFrame(TEST_ROWS).to_dict('records')
    compare_rows_helper(expected, actual)


DICT_OF_LISTS_UNEVEN_LENGTHS = {
    'name': ['Roger', 'Rafael', 'Novak'],
    'rank': [1, 2]
}


def test_write_columns_uneven(init_empty_test_repo):
    repo = init_empty_test_repo
    with pytest.raises(AssertionError):
        write_columns(repo, 'players', DICT_OF_LISTS_UNEVEN_LENGTHS, CREATE, ['name'])


