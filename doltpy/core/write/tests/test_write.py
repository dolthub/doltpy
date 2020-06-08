from doltpy.core.write import import_dict, import_list
from doltpy.core.read import pandas_read_sql
import pandas as pd
from datetime import datetime

LIST_OF_DICTS = [
    {'name': 'Anna', 'adjective': 'tragic', 'id': 1, 'date_of_death': datetime(1877, 1, 1)},
    {'name': 'Vronksy', 'adjective': 'honorable', 'id': 2, 'date_of_death': None},
    {'name': 'Oblonksy', 'adjective': 'buffoon', 'id': 3, 'date_of_death': None},
]

DICT_OF_LISTS = {
    'name': ['Anna', 'Vronksy', 'Oblonksy'],
    'adjective': ['tragic', 'honorable', 'buffoon'],
    'id': [1, 2, 3],
    'date_of_death': [datetime(1877, 1, 1), None, None]
}


def test_import_dict(init_empty_test_repo, run_serve_mode):
    repo = init_empty_test_repo
    import_dict(repo, 'characters', DICT_OF_LISTS, ['id'], 'create')
    df = pandas_read_sql(repo, 'select * from characters', repo.get_connection())
    expected = pd.DataFrame(DICT_OF_LISTS)
    assert df.equals(expected)


def test_import_lists(init_empty_test_repo, run_serve_mode):
    repo = init_empty_test_repo
    import_list(repo, 'characters', LIST_OF_DICTS, ['id'], 'create')
    df = pandas_read_sql(repo, 'select * from characters', repo.get_connection())
    expected = pd.DataFrame(LIST_OF_DICTS)
    assert df.equals(expected)
