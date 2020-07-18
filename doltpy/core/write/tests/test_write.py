from doltpy.core.write import import_dict, import_list
from doltpy.core.read import pandas_read_sql
import pandas as pd
from datetime import datetime
import pytest

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
    df = pandas_read_sql('select * from characters', repo.engine)
    expected = pd.DataFrame(DICT_OF_LISTS)
    assert df.equals(expected)


def test_import_lists(init_empty_test_repo, run_serve_mode):
    repo = init_empty_test_repo
    import_list(repo, 'characters', LIST_OF_DICTS, ['id'], 'create')
    df = pandas_read_sql('select * from characters', repo.engine)
    expected = pd.DataFrame(LIST_OF_DICTS)
    assert df.equals(expected)


def test_import_dicts_chunked(init_empty_test_repo, run_serve_mode):
    repo = init_empty_test_repo
    import_list(repo, 'characters', LIST_OF_DICTS, ['id'], 'create', batch_size=2)
    df = pandas_read_sql('select * from characters', repo.engine)
    expected = pd.DataFrame(LIST_OF_DICTS)
    assert df.equals(expected)


LIST_OF_DICTS_WITH_NULLS = [
    {'name': 'Roger', 'date_of_death': None},
    {'name': 'Rafael', 'date_of_death': None},
    {'name': 'Novak', 'date_of_death': None}
]


def test_import_dict_all_nulls(init_empty_test_repo, run_serve_mode):
    repo = init_empty_test_repo
    with pytest.raises(ValueError):
        import_list(repo, 'players', LIST_OF_DICTS_WITH_NULLS, ['name'], 'create')


LIST_OF_DICTS_WITH_MISSING_KEYS = [
    {'name': 'Roger', 'top_rank': 1},
    {'name': 'Rafael', 'hand': 'left'},
    {'name': 'Novak', 'top_rank': 1}
]


def test_import_list_missing_keys(init_empty_test_repo, run_serve_mode):
    repo = init_empty_test_repo
    with pytest.raises(AssertionError):
        import_list(repo, 'players', LIST_OF_DICTS_WITH_MISSING_KEYS, ['name'], 'create')


DICT_OF_LISTS_UNEVEN_LENGTHS = {
    'name': ['Roger', 'Rafael', 'Novak'],
    'rank': [1, 2]
}


def test_import_lists_uneven(init_empty_test_repo, run_serve_mode):
    repo = init_empty_test_repo
    with pytest.raises(AssertionError):
        import_dict(repo, 'players', DICT_OF_LISTS_UNEVEN_LENGTHS, ['name'], 'create')


