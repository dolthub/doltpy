from doltpy.core.write import import_dict, import_list
from doltpy.core.read import read_table
import pandas as pd

LIST_OF_DICTS = [
    {'name': 'Anna', 'adjective': 'tragic', 'id': 1},
    {'name': 'Vronksy', 'adjective': 'honorable', 'id': 2},
    {'name': 'Oblonksy', 'adjective': 'buffoon', 'id': 3},
]

DICT_OF_LISTS = {
    'name': ['Anna', 'Vronksy', 'Oblonksy'],
    'adjective': ['tragic', 'honorable', 'buffoon'],
    'id': [1, 2, 3]
}


def test_import_dict(init_empty_test_repo):
    repo = init_empty_test_repo
    import_dict(repo, 'characters', DICT_OF_LISTS, ['id'], 'create')
    df = read_table(repo, 'characters')
    expected = pd.DataFrame(DICT_OF_LISTS)
    assert df.equals(expected)


def test_import_lists(init_empty_test_repo):
    repo = init_empty_test_repo
    import_list(repo, 'characters', LIST_OF_DICTS, ['id'], 'create')
    df = read_table(repo, 'characters')
    expected = pd.DataFrame(LIST_OF_DICTS)
    assert df.equals(expected)
