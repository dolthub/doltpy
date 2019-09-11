import pytest
import pandas as pd
from etl.tools import ETLWorkload, Dataset, insert_unique_key
from doltpy.dolt import Dolt
from doltpy.tests.dolt_testing_fixtures import init_repo

MENS_MAJOR_COUNT, WOMENS_MAJOR_COUNT = 'mens_major_count', 'womens_major_count'
INITIAL_WOMENS = pd.DataFrame({'name': ['Serena'], 'major_count': [23]})
INITIAL_MENS = pd.DataFrame({'name': ['Roger'], 'major_count': [20]})
UPDATE_WOMENS = pd.DataFrame({'name': ['Margaret'], 'major_count': [24]})
UPDATE_MENS = pd.DataFrame({'name': ['Rafael'], 'major_count': [20]})


def _populate_test_data_helper(repo: Dolt, mens: pd.DataFrame, womens: pd.DataFrame):
    datasets = [Dataset(MENS_MAJOR_COUNT, lambda: mens, ['name']),
                Dataset(WOMENS_MAJOR_COUNT, lambda: womens, ['name'])]
    workload = ETLWorkload(repo, datasets)
    workload.load_to_dolt(True, 'Loaded {} and {}'.format(MENS_MAJOR_COUNT, WOMENS_MAJOR_COUNT))
    return repo


@pytest.fixture
def initial_test_data(init_repo):
    return _populate_test_data_helper(init_repo, INITIAL_MENS, INITIAL_WOMENS)


@pytest.fixture
def update_test_data(initial_test_data):
    return _populate_test_data_helper(initial_test_data, UPDATE_MENS, UPDATE_WOMENS)


def test_load_to_dolt_create(initial_test_data):
    repo = initial_test_data

    womens_data, mens_data = repo.read_table(WOMENS_MAJOR_COUNT), repo.read_table(MENS_MAJOR_COUNT)
    assert womens_data.to_pandas().iloc[0]['name'] == 'Serena'
    assert mens_data.to_pandas().iloc[0]['name'] == 'Roger'


def test_load_to_dolt_update(update_test_data):
    repo = update_test_data

    womens_data, mens_data = repo.read_table(WOMENS_MAJOR_COUNT), repo.read_table(MENS_MAJOR_COUNT)
    assert 'Margaret' in list(womens_data.to_pandas()['name'])
    assert 'Rafael' in list(mens_data.to_pandas()['name'])


def test_insert_unique_key(init_repo):
    repo = init_repo

    def generate_data():
        return pd.DataFrame({'id': [1, 1, 2], 'value': ['foo', 'foo', 'baz']})

    test_data = 'test_data'
    datasets = [Dataset(test_data, generate_data, ['hash_id'], [insert_unique_key])]
    workload = ETLWorkload(repo, datasets)
    workload.load_to_dolt(True, 'Updated test_data')

    result = repo.read_table(test_data).to_pandas()
    assert result.loc[result['id'] == 1, 'count'].iloc[0] == 2 and 'hash_id' in result.columns


def test_insert_unique_key_column_error():
    with pytest.raises(AssertionError):
        insert_unique_key(pd.DataFrame({'hash_id': ['blah']}))

    with pytest.raises(AssertionError):
        insert_unique_key(pd.DataFrame({'hash_id': ['count']}))
