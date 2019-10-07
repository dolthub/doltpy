import pytest
import pandas as pd
from doltpy.dolt import Dolt
from doltpy_etl import get_df_table_loader, load_to_dolt, insert_unique_key, get_table_transfomer
from doltpy.tests.dolt_testing_fixtures import init_repo

MENS_MAJOR_COUNT, WOMENS_MAJOR_COUNT = 'mens_major_count', 'womens_major_count'
AVERAGE_MAJOR_COUNT = 'average_major_count'
INITIAL_WOMENS = pd.DataFrame({'name': ['Serena'], 'major_count': [23]})
INITIAL_MENS = pd.DataFrame({'name': ['Roger'], 'major_count': [20]})
UPDATE_WOMENS = pd.DataFrame({'name': ['Margaret'], 'major_count': [24]})
UPDATE_MENS = pd.DataFrame({'name': ['Rafael'], 'major_count': [20]})

# TODO:
#  write test for file loader,
#  introduce the runner,
#  tests for branching logic,
#  test resolve loaders,
#  test get_table_transfomer


def _populate_test_data_helper(repo: Dolt, mens: pd.DataFrame, womens: pd.DataFrame):
    table_loaders = [get_df_table_loader(MENS_MAJOR_COUNT, lambda: mens, ['name']),
                     get_df_table_loader(WOMENS_MAJOR_COUNT, lambda: womens, ['name'])]
    load_to_dolt(repo,
                 table_loaders,
                 True,
                 'Loaded {} and {}'.format(MENS_MAJOR_COUNT, WOMENS_MAJOR_COUNT))
    return repo


def _populate_derived_data_helper(repo: Dolt):
    table_transfomers = [get_table_transfomer(get_raw_data, AVERAGE_MAJOR_COUNT, ['gender'], averager)]
    load_to_dolt(repo, table_transfomers, True, 'Updatd {}'.format(AVERAGE_MAJOR_COUNT))
    return repo

@pytest.fixture
def initial_test_data(init_repo):
    return _populate_test_data_helper(init_repo, INITIAL_MENS, INITIAL_WOMENS)

@pytest.fixture
def update_test_data(initial_test_data):
    return _populate_test_data_helper(initial_test_data, UPDATE_MENS, UPDATE_WOMENS)


def get_raw_data(repo: Dolt):
    return pd.concat([repo.read_table(MENS_MAJOR_COUNT).to_pandas().assign(gender='mens'),
                      repo.read_table(WOMENS_MAJOR_COUNT).to_pandas().assign(gender='womens')])

def averager(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby('gender').mean().reset_index()[['gender', 'major_count']].rename(columns={'major_count': 'average'})

@pytest.fixture
def initial_derived_data(initial_test_data):
    return _populate_derived_data_helper(initial_test_data)

@pytest.fixture
def initial_derived_data(initial_derived_data):
    repo = update_test_data(initial_derived_data)
    return _populate_derived_data_helper(repo)

def test_dataframe_table_loader_create(initial_test_data):
    repo = initial_test_data

    womens_data, mens_data = repo.read_table(WOMENS_MAJOR_COUNT), repo.read_table(MENS_MAJOR_COUNT)
    assert womens_data.to_pandas().iloc[0]['name'] == 'Serena'
    assert mens_data.to_pandas().iloc[0]['name'] == 'Roger'


def test_dataframe_table_loader_update(update_test_data):
    repo = update_test_data

    womens_data, mens_data = repo.read_table(WOMENS_MAJOR_COUNT), repo.read_table(MENS_MAJOR_COUNT)
    assert 'Margaret' in list(womens_data.to_pandas()['name'])
    assert 'Rafael' in list(mens_data.to_pandas()['name'])




# def test_table_transfomer_create(initial_test_data):



def test_table_transfomer_update():
    pass

def test_insert_unique_key(init_repo):
    repo = init_repo

    def generate_data():
        return pd.DataFrame({'id': [1, 1, 2], 'value': ['foo', 'foo', 'baz']})

    test_table = 'test_data'
    load_to_dolt(repo,
                 [get_df_table_loader(test_table, generate_data, ['hash_id'], transformers=[insert_unique_key])],
                 True,
                 'Updating test data')
    result = repo.read_table(test_table).to_pandas()
    assert result.loc[result['id'] == 1, 'count'].iloc[0] == 2 and 'hash_id' in result.columns


def test_insert_unique_key_column_error():
    with pytest.raises(AssertionError):
        insert_unique_key(pd.DataFrame({'hash_id': ['blah']}))

    with pytest.raises(AssertionError):
        insert_unique_key(pd.DataFrame({'hash_id': ['count']}))

# TODO look at bats test to get remotes involved
# def test_dolthub_loader()

