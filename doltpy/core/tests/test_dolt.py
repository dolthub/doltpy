import pytest
from doltpy.core.dolt import init_new_repo, Dolt, _execute, DoltException, UPDATE
import shutil
import pandas as pd
import uuid
import os
from typing import Tuple
from doltpy.core.tests.helpers import get_repo_path_tmp_path


@pytest.fixture
def create_test_data(tmp_path) -> str:
    path = os.path.join(tmp_path, str(uuid.uuid4()))
    pd.DataFrame({'name': ['Rafael', 'Novak'], 'id': [1, 2]}).to_csv(path, index_label=False)
    yield path
    os.remove(path)


@pytest.fixture
def create_test_table(init_empty_test_repo, create_test_data) -> Tuple[Dolt, str]:
    repo, test_data_path = init_empty_test_repo, create_test_data
    repo.execute_sql_stmt('''
        CREATE TABLE `test_players` (
            `name` LONGTEXT NOT NULL COMMENT 'tag:0',
            `id` BIGINT NOT NULL COMMENT 'tag:1',
            PRIMARY KEY (`id`)
        );
    ''')
    repo.import_df('test_players', pd.read_csv(test_data_path), ['id'], UPDATE)
    yield repo, 'test_players'

    if 'test_players' in repo.get_existing_tables():
        _execute(['dolt', 'table', 'rm', 'test_players'], repo.repo_dir())


@pytest.fixture
def run_serve_mode(request, init_empty_test_repo):
    repo = init_empty_test_repo
    repo.start_server()
    connection = repo.get_connection()

    def finalize():
        if connection:
            connection.close()
        if repo.server:
            repo.stop_server()

    request.addfinalizer(finalize)
    return connection


def test_init_new_repo(tmp_path):
    repo_path, repo_data_dir = get_repo_path_tmp_path(tmp_path)
    assert not os.path.exists(repo_data_dir)
    init_new_repo(repo_path)
    assert os.path.exists(repo_data_dir)
    shutil.rmtree(repo_data_dir)


def test_commit(create_test_table):
    repo, test_table = create_test_table
    repo.add_table_to_next_commit(test_table)
    before_commit_count = len(repo.get_commits())
    repo.commit('Julianna, the very serious intellectual')
    assert repo.repo_is_clean() and len(repo.get_commits()) == before_commit_count + 1


def test_get_dirty_tables(create_test_table):
    repo, test_table = create_test_table
    message = 'Committing test data'

    # Some test data
    initial = pd.DataFrame({'id': [1], 'name': ['Bianca'], 'role': ['Champion']})
    appended_row = pd.DataFrame({'name': ['Serena'], 'id': [2], 'role': ['Runner-up']})

    def _insert_row_helper(repo, table, row):
        repo.import_df(table, row, ['id'], import_mode=UPDATE)

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
    _insert_row_helper(repo, modified_staged, appended_row)
    repo.import_df(modified_staged, appended_row, ['id'], UPDATE)
    repo.add_table_to_next_commit(modified_staged)
    repo.import_df(modified_unstaged, appended_row, ['id'], UPDATE)

    # created, staged
    created_staged = 'created_staged'
    repo.import_df(created_staged, initial, ['id'])
    repo.add_table_to_next_commit(created_staged)

    # created, unstaged
    created_unstaged = 'created_unstaged'
    repo.import_df(created_unstaged, initial, ['id'])

    new_tables, changes = repo.get_dirty_tables()

    expected_new_tables = {'created_staged': True, 'created_unstaged': False}
    expected_changes = {'modified_staged': True, 'modified_unstaged': False}

    assert new_tables == expected_new_tables
    assert expected_changes == expected_changes


def test_clean_local(create_test_table):
    repo, test_table = create_test_table
    repo.clean_local()
    assert repo.repo_is_clean()


# TODO test datetime types here
def test_sql_server(create_test_table, run_serve_mode):
    repo, test_table = create_test_table
    connection = run_serve_mode
    data = repo.pandas_read_sql('SELECT * FROM {}'.format(test_table), connection)
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
    _execute(['dolt', 'remote', 'add', 'origin', 'blah-blah'], repo.repo_dir())
    assert repo.get_remote_list() == ['origin']
    _execute(['dolt', 'remote', 'add', 'another-origin', 'la-la-land'], repo.repo_dir())
    assert set(repo.get_remote_list()) == {'origin', 'another-origin'}


def test_checkout_non_existent_branch(create_test_table):
    repo, _ = create_test_table
    with pytest.raises(DoltException):
        repo.checkout('master')


def test_get_existing_tables(create_test_table):
    repo, test_table = create_test_table
    assert repo.get_existing_tables() == [test_table]


def test_execute_sql_stmt(create_test_table):
    repo, test_table = create_test_table
    sql = '''
        INSERT INTO {table} (name, id)
        VALUES ('Roger', 3)
    '''.format(table=test_table)
    repo.execute_sql_stmt(sql)

    test_data = repo.read_table(test_table)
    assert 'Roger' in test_data['name'].to_list()


TEST_IMPORT_FILE_DATA = '''
name,id
roger,1
rafa,2
'''.lstrip()


def test_schema_import_create(init_empty_test_repo, tmp_path):
    repo = init_empty_test_repo
    table = 'test_table'
    test_file = tmp_path / 'test_data.csv'
    with open(test_file, 'w') as f:
        f.writelines(TEST_IMPORT_FILE_DATA)
    repo.schema_import_create(table, ['id'], test_file)

    new_tables, _ = repo.get_dirty_tables()
    assert new_tables == {table: False}
