import io
import pytest
import pandas as pd
from doltpy.core.dolt import Dolt
from doltpy.core.write import CREATE, UPDATE
from doltpy.core.read import read_table
from doltpy.etl import (get_df_table_writer,
                        insert_unique_key,
                        get_unique_key_table_writer,
                        get_table_transformer,
                        get_bulk_table_writer,
                        get_dolt_loader,
                        get_branch_creator)


MENS_MAJOR_COUNT, WOMENS_MAJOR_COUNT = 'mens_major_count', 'womens_major_count'
AVERAGE_MAJOR_COUNT = 'average_major_count'
INITIAL_WOMENS = pd.DataFrame({'name': ['Serena'], 'major_count': [23]})
INITIAL_MENS = pd.DataFrame({'name': ['Roger'], 'major_count': [20]})
UPDATE_WOMENS = pd.DataFrame({'name': ['Margaret'], 'major_count': [24]})
UPDATE_MENS = pd.DataFrame({'name': ['Rafael'], 'major_count': [19]})
SECOND_UPDATE_WOMENS = pd.DataFrame({'name': ['Steffi'], 'major_count': [22]})
SECOND_UPDATE_MENS = pd.DataFrame({'name': ['Novak'], 'major_count': [16]})


def _populate_test_data_helper(repo: Dolt, mens: pd.DataFrame, womens: pd.DataFrame, branch: str = 'master'):
    table_loaders = [get_df_table_writer(MENS_MAJOR_COUNT, lambda: mens, ['name']),
                     get_df_table_writer(WOMENS_MAJOR_COUNT, lambda: womens, ['name'])]
    get_dolt_loader(table_loaders,
                    True,
                    'Loaded {} and {}'.format(MENS_MAJOR_COUNT, WOMENS_MAJOR_COUNT),
                    branch=branch)(repo)
    return repo


def _populate_derived_data_helper(repo: Dolt, import_mode: str):
    table_transfomers = [get_table_transformer(get_raw_data, AVERAGE_MAJOR_COUNT, ['gender'], averager, import_mode)]
    get_dolt_loader(table_transfomers, True, 'Updated {}'.format(AVERAGE_MAJOR_COUNT))(repo)
    return repo


@pytest.fixture
def initial_test_data(init_empty_test_repo):
    return _populate_test_data_helper(init_empty_test_repo, INITIAL_MENS, INITIAL_WOMENS)


@pytest.fixture
def update_test_data(initial_test_data):
    return _populate_test_data_helper(initial_test_data, UPDATE_MENS, UPDATE_WOMENS)


def get_raw_data(repo: Dolt):
    return pd.concat([read_table(repo, MENS_MAJOR_COUNT).assign(gender='mens'),
                      read_table(repo, WOMENS_MAJOR_COUNT).assign(gender='womens')])


def averager(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby('gender').mean().reset_index()[['gender', 'major_count']].rename(columns={'major_count': 'average'})


@pytest.fixture
def initial_derived_data(initial_test_data):
    repo = initial_test_data
    query = '''
        CREATE TABLE average_major_count (
            `gender` VARCHAR(16),
            `average` FLOAT,
            PRIMARY KEY (`gender`)
        )
    '''
    repo.sql(query)
    return _populate_derived_data_helper(repo, UPDATE)


@pytest.fixture
def update_derived_data(initial_derived_data):
    repo = _populate_test_data_helper(initial_derived_data, UPDATE_MENS, UPDATE_WOMENS)
    return _populate_derived_data_helper(repo, UPDATE)


def test_dataframe_table_loader_create(initial_test_data):
    repo = initial_test_data

    womens_data, mens_data = read_table(repo, WOMENS_MAJOR_COUNT), read_table(repo, MENS_MAJOR_COUNT)
    assert womens_data.iloc[0]['name'] == 'Serena'
    assert mens_data.iloc[0]['name'] == 'Roger'


def test_dataframe_table_loader_update(update_test_data):
    repo = update_test_data

    womens_data, mens_data = read_table(repo, WOMENS_MAJOR_COUNT), read_table(repo, MENS_MAJOR_COUNT)
    assert 'Margaret' in list(womens_data['name'])
    assert 'Rafael' in list(mens_data['name'])


def test_table_transfomer_create(initial_derived_data):
    repo = initial_derived_data
    avg_df = read_table(repo, AVERAGE_MAJOR_COUNT)
    assert avg_df.loc[avg_df['gender'] == 'mens', 'average'].iloc[0] == 20
    assert avg_df.loc[avg_df['gender'] == 'womens', 'average'].iloc[0] == 23


def test_table_transfomer_update(update_derived_data):
    repo = update_derived_data
    avg_df = read_table(repo, AVERAGE_MAJOR_COUNT)
    assert avg_df.loc[avg_df['gender'] == 'mens', 'average'].iloc[0] == (20 + 19) / 2
    assert avg_df.loc[avg_df['gender'] == 'womens', 'average'].iloc[0] == (23 + 24) / 2


def test_insert_unique_key(init_empty_test_repo):
    repo = init_empty_test_repo

    def generate_data():
        return pd.DataFrame({'id': [1, 1, 2], 'value': ['foo', 'foo', 'baz']})

    test_table = 'test_data'
    get_dolt_loader([get_df_table_writer(test_table, generate_data, ['hash_id'], transformers=[insert_unique_key])],
                    True,
                    'Updating test data')(repo)
    result = read_table(repo, test_table)
    assert result.loc[result['id'] == 1, 'count'].iloc[0] == 2 and 'hash_id' in result.columns


def test_insert_unique_key_column_error():
    with pytest.raises(AssertionError):
        insert_unique_key(pd.DataFrame({'hash_id': ['blah']}))

    with pytest.raises(AssertionError):
        insert_unique_key(pd.DataFrame({'hash_id': ['count']}))


def test_get_unique_key_update_writer(init_empty_test_repo):
    repo = init_empty_test_repo

    def generate_initial_data():
        return pd.DataFrame([
            {'name': 'Roger', 'id': 1},
            {'name': 'Rafael', 'id': 2},
            {'name': 'Rafael', 'id': 2},
            {'name': 'Novak', 'id': 3}
        ])

    test_table = 'test_data'
    get_dolt_loader([get_unique_key_table_writer(test_table, generate_initial_data, import_mode='create')],
                    True,
                    'Create test data')(repo)

    # Test that we have what we expect
    data = read_table(repo, test_table)
    assert [data.loc[data['name'] == player, 'count'].iloc[0] == 1 for player in ['Roger', 'Novak']]
    assert data.loc[data['name'] == 'Rafael', 'count'].iloc[0] == 2

    def generate_updated_data():
        return pd.DataFrame([
            {'name': 'Rafael', 'id': 2},
            {'name': 'Novak', 'id': 3},
            {'name': 'Andy', 'id': 4}
        ])

    get_dolt_loader([get_unique_key_table_writer(test_table, generate_updated_data)], True, 'Updating data')(repo)
    data = read_table(repo, test_table)
    assert [data.loc[data['name'] == player, 'count'].iloc[0] == 1 for player in ['Rafael', 'Novak', 'Andy']]


def test_branching(initial_test_data):
    repo = initial_test_data
    test_branch = 'new-branch'
    repo.branch(branch_name=test_branch)
    repo.checkout(test_branch)
    _populate_test_data_helper(repo, UPDATE_MENS, UPDATE_WOMENS, test_branch)

    current_branch, _ = repo.branch()
    assert current_branch.name == test_branch
    womens_data, mens_data = read_table(repo, WOMENS_MAJOR_COUNT), read_table(repo, MENS_MAJOR_COUNT)
    assert 'Margaret' in list(womens_data['name'])
    assert 'Rafael' in list(mens_data['name'])

    repo.checkout('master')
    womens_data, mens_data = read_table(repo, WOMENS_MAJOR_COUNT), read_table(repo, MENS_MAJOR_COUNT)
    assert 'Margaret' not in list(womens_data['name'])
    assert 'Rafael' not in list(mens_data['name'])


CORRUPT_CSV = """player_name,weeks_at_number_1
Roger,Federer,310
Pete Sampras,286
Novak Djokovic,272
Ivan Lendl,270
Jimmy Connors,268
Rafael Nadal,196
John McEnroe,170
Björn Borg,109,,
Andre Agassi,101
Lleyton Hewitt,80
,Stefan Edberg,72
Jim Courier,58
Gustavo Kuerten,43
"""

CLEANED_CSV = """player_name,weeks_at_number_1
Pete Sampras,286
Novak Djokovic,272
Ivan Lendl,270
Jimmy Connors,268
Rafael Nadal,196
John McEnroe,170
Andre Agassi,101
Lleyton Hewitt,80
Jim Courier,58
Gustavo Kuerten,43
"""


def test_get_bulk_table_loader(init_empty_test_repo):
    repo = init_empty_test_repo
    table = 'test_table'

    def get_data():
        return io.StringIO(CORRUPT_CSV)

    def cleaner(data: io.StringIO) -> io.StringIO:
        output = io.StringIO()
        header_line = data.readline()
        columns = header_line.split(',')
        output.write(header_line)
        for l in data.readlines():
            if len(l.split(',')) != len(columns):
                print('Corrupt line, discarding:\n{}'.format(l))
            else:
                output.write(l)

        output.seek(0)
        return output

    get_bulk_table_writer(table, get_data, ['player_name'], import_mode=CREATE, transformers=[cleaner])(repo)
    actual = read_table(repo, table)
    expected = io.StringIO(CLEANED_CSV)
    headers = [col.rstrip() for col in expected.readline().split(',')]
    assert all(headers == actual.columns)
    players_to_week_counts = actual.set_index('player_name')['weeks_at_number_1'].to_dict()
    for line in expected.readlines():
        player_name, weeks_at_number_1 = line.split(',')
        assert (player_name in players_to_week_counts and
                players_to_week_counts[player_name] == int(weeks_at_number_1.rstrip()))


def test_load_to_dolt_new_branch(initial_test_data):
    repo = initial_test_data
    test_branch = 'new-branch'

    # check we have only the expected branches in the sample data
    _, branches = repo.branch()
    assert [b.name for b in branches] == ['master']

    # load some data to a new branch
    _populate_test_data_helper(repo, UPDATE_MENS, UPDATE_WOMENS, test_branch)

    # check that we are still on the branch we started on
    current_branch, current_branches = repo.branch()
    assert current_branch.name == 'master' and [b.name for b in current_branches] == ['master', test_branch]

    # check out our new branch and confirm our data is present
    repo.checkout(test_branch)
    womens_data, mens_data = read_table(repo, WOMENS_MAJOR_COUNT), read_table(repo, MENS_MAJOR_COUNT)
    assert 'Margaret' in list(womens_data['name']) and 'Rafael' in list(mens_data['name'])


def test_multi_branch_load(initial_test_data):
    repo = initial_test_data
    first_branch, second_branch = 'first-branch', 'second-branch'

    _populate_test_data_helper(repo, UPDATE_MENS, UPDATE_WOMENS, first_branch)
    _populate_test_data_helper(repo, SECOND_UPDATE_MENS, SECOND_UPDATE_WOMENS, second_branch)

    womens_data, mens_data = read_table(repo, WOMENS_MAJOR_COUNT), read_table(repo, MENS_MAJOR_COUNT)
    assert 'Margaret' not in list(womens_data['name']) and 'Rafael' not in list(mens_data['name'])
    assert 'Steffi' not in list(womens_data['name']) and 'Novak' not in list(mens_data['name'])

    repo.checkout(first_branch)
    womens_data, mens_data = read_table(repo, WOMENS_MAJOR_COUNT), read_table(repo, MENS_MAJOR_COUNT)
    assert 'Margaret' in list(womens_data['name']) and 'Rafael' in list(mens_data['name'])

    repo.checkout(second_branch)
    womens_data, mens_data = read_table(repo, WOMENS_MAJOR_COUNT), read_table(repo, MENS_MAJOR_COUNT)
    assert 'Steffi' in list(womens_data['name']) and 'Novak' in list(mens_data['name'])


def test_branch_creator(initial_test_data):
    repo = initial_test_data
    new_branch = 'new-branch'
    _, branches = repo.branch()
    assert [b.name for b in branches] == ['master']
    branch_name = get_branch_creator(new_branch)(repo)
    assert branch_name == new_branch
    _, branches = repo.branch()
    assert new_branch in [branch.name for branch in branches]