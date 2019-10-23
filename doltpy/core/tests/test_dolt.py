import pytest
from doltpy.core.dolt import Dolt, _execute, DoltException
import shutil
import pandas as pd
import uuid
import os
from typing import Tuple
from doltpy.core.tests.dolt_testing_fixtures import get_repo_path_tmp_path, init_repo


@pytest.fixture
def create_test_data(tmp_path) -> str:
    path = os.path.join(tmp_path, str(uuid.uuid4()))
    pd.DataFrame({'name': ['Rafael', 'Novak'], 'id': [1, 2]}).to_csv(path, index_label=False)
    yield path
    os.remove(path)


@pytest.fixture
def create_test_table(init_repo, create_test_data) -> Tuple[Dolt, str]:
    repo, test_data_path = init_repo, create_test_data
    repo.import_df('test_players', pd.read_csv(test_data_path), ['id'])
    yield repo, 'test_players'
    _execute(['dolt', 'table', 'rm', 'test_players'], repo.repo_dir)


@pytest.fixture
def run_serve_mode(init_repo):
    repo = init_repo
    repo.start_server()
    yield
    repo.stop_server()


def test_init_new_repo(tmp_path):
    repo_path, repo_data_dir = get_repo_path_tmp_path(tmp_path)
    assert not os.path.exists(repo_data_dir)
    dolt = Dolt(repo_path)
    dolt.init_new_repo()
    assert os.path.exists(repo_data_dir)
    shutil.rmtree(repo_data_dir)


def test_put_row(create_test_table):
    repo, test_table = create_test_table
    repo.put_row(test_table, {'name': 'Roger', 'id': 3})
    df = repo.read_table(test_table)
    assert 'Roger' in df['name'].tolist() and 3 in df['id'].tolist()


def test_commit(create_test_table):
    repo, test_table = create_test_table
    repo.add_table_to_next_commit(test_table)
    before_commit_count = len(list(repo.get_commits()))
    repo.commit('Julianna, the very serious intellectual')
    assert repo.repo_is_clean() and len(list(repo.get_commits())) == before_commit_count + 1


def test_get_dirty_tables(create_test_table):
    repo, test_table = create_test_table
    message = 'Committing test data'

    # Some test data
    initial = pd.DataFrame({'id': [1], 'name': ['Bianca'], 'role': ['Champion']})
    appended_row = {'name': 'Serena', 'id': 2, 'role': 'Runner-up'}

    # existing, not modified
    repo.add_table_to_next_commit(test_table)
    repo.commit(message)

    # existing, modified, staged
    modified_staged = 'modified_staged'
    repo.import_df(modified_staged, initial, ['id'])
    repo.add_table_to_next_commit(modified_staged)

    # existing, modified, unstaged
    modified_unstaged = 'modified_unstaged'
    repo.import_df(modified_unstaged, initial, ['id'])
    repo.add_table_to_next_commit(modified_unstaged)

    # Commit and modify data
    repo.commit(message)
    repo.put_row(modified_staged, appended_row)
    repo.add_table_to_next_commit(modified_staged)
    repo.put_row(modified_unstaged, appended_row)

    # created, staged
    created_staged = 'created_staged'
    repo.import_df(created_staged, initial, ['id'])
    repo.add_table_to_next_commit(created_staged)

    # created, unstaged
    created_unstaged = 'created_unstaged'
    repo.import_df(created_unstaged, initial, ['id'])

    new_tables, changes = repo.get_dirty_tables()

    assert new_tables[created_staged] and not new_tables[created_unstaged]
    assert changes[modified_staged] and not changes[modified_unstaged]


def test_clean_local(create_test_table):
    repo, test_table = create_test_table
    repo.clean_local()
    assert repo.repo_is_clean()


# TODO Python sends these back as strings, causing tests to fail
@pytest.mark.skip('Currently the SQL API returns DataFrame with strings instead of ints')
def test_sql_server(create_test_table, run_serve_mode):
    repo, test_table = create_test_table
    data = repo.pandas_read_sql('SELECT * FROM {}'.format(test_table))
    assert list(data['id']) == [1, 2]


def test_branch_list(create_test_table):
    repo, _ = create_test_table
    assert repo.get_branch_list() == [repo.get_current_branch()] == ['master']
    repo.create_branch('dosac')
    assert set(repo.get_branch_list()) == {'master', 'dosac'} and repo.get_current_branch() == 'master'
    repo.checkout('dosac')
    assert repo.get_current_branch() == 'dosac'


def test_remote_list(create_test_table):
    repo, _ = create_test_table
    _execute(['dolt', 'remote', 'add', 'origin', 'blah-blah'], repo.repo_dir)
    assert repo.get_remote_list() == ['origin']
    _execute(['dolt', 'remote', 'add', 'another-origin', 'la-la-land'], repo.repo_dir)
    assert set(repo.get_remote_list()) == {'origin', 'another-origin'}


def test_checkout_non_existent_branch(create_test_table):
    repo, _ = create_test_table
    with pytest.raises(DoltException):
        repo.checkout('master')


def test_get_existing_tables(create_test_table):
    repo, test_table = create_test_table
    assert repo.get_existing_tables() == [test_table]
