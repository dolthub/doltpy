import pytest
from doltpy.cli.dolt import Dolt, _execute, DoltException
from doltpy.cli.write import write_pandas, UPDATE, CREATE
from doltpy.cli.read import read_pandas
import shutil
import pandas as pd
import uuid
import os
from typing import Tuple, List
from .helpers import get_repo_path_tmp_path


BASE_TEST_ROWS = [
    {'name': 'Rafael',  'id': 1},
    {'name': 'Novak', 'id': 2}
]


@pytest.fixture
def create_test_data(tmp_path) -> str:
    path = os.path.join(tmp_path, str(uuid.uuid4()))
    pd.DataFrame(BASE_TEST_ROWS).to_csv(path, index_label=False)
    yield path
    os.remove(path)


@pytest.fixture
def create_test_table(init_empty_test_repo, create_test_data) -> Tuple[Dolt, str]:
    repo, test_data_path = init_empty_test_repo, create_test_data
    repo.sql(query='''
        CREATE TABLE `test_players` (
            `name` LONGTEXT NOT NULL COMMENT 'tag:0',
            `id` BIGINT NOT NULL COMMENT 'tag:1',
            PRIMARY KEY (`id`)
        );
    ''')
    write_pandas(repo, 'test_players', pd.read_csv(test_data_path), UPDATE, commit=False)
    yield repo, 'test_players'

    if 'test_players' in [table.name for table in repo.ls()]:
        _execute(['table', 'rm', 'test_players'], repo.repo_dir())


def test_init(tmp_path):
    repo_path, repo_data_dir = get_repo_path_tmp_path(tmp_path)
    assert not os.path.exists(repo_data_dir)
    Dolt.init(repo_path)
    assert os.path.exists(repo_data_dir)
    shutil.rmtree(repo_data_dir)


def test_bad_repo_path(tmp_path):
    bad_repo_path = tmp_path
    with pytest.raises(ValueError):
        Dolt(bad_repo_path)


def test_commit(create_test_table):
    repo, test_table = create_test_table
    repo.add(test_table)
    before_commit_count = len(repo.log())
    repo.commit('Julianna, the very serious intellectual')
    assert repo.status().is_clean and len(repo.log()) == before_commit_count + 1


def test_merge_fast_forward(create_test_table):
    repo, test_table = create_test_table
    message_one = 'Base branch'
    message_two = 'Other branch'
    message_merge = 'merge'

    # commit the current working set to master
    repo.add(test_table)
    repo.commit(message_one)

    # create another branch from the working set
    repo.branch('other')

    # create a non-trivial commit against `other`
    repo.checkout('other')
    repo.sql('INSERT INTO `test_players` (`name`, `id`) VALUES ("Juan Martin", 5)')
    repo.add(test_table)
    repo.commit(message_two)

    # merge
    repo.checkout('master')
    repo.merge('other', message_merge)

    commits = list(repo.log().values())
    fast_forward_commit = commits[0]
    parent = commits[1]

    assert fast_forward_commit.merge is None
    assert fast_forward_commit.message == message_two
    assert parent.message == message_one


def test_merge_conflict(create_test_table):
    repo, test_table = create_test_table
    message_one = 'Base branch'
    message_two = 'Base branch new data'
    message_three = 'Other branch'
    message_merge = 'merge'
    # commit the current working set to master
    repo.add(test_table)
    repo.commit(message_one)

    # create another branch from the working set
    repo.branch('other')

    # create a non-trivial commit against `master`
    repo.sql('INSERT INTO `test_players` (`name`, `id`) VALUES ("Stan", 4)')
    repo.add(test_table)
    repo.commit(message_two)

    # create a non-trivial commit against `other`
    repo.checkout('other')
    repo.sql('INSERT INTO `test_players` (`name`, `id`) VALUES ("Marin", 4)')
    repo.add(test_table)
    repo.commit(message_three)

    # merge
    repo.checkout('master')
    repo.merge('other', message_merge)

    commits = list(repo.log().values())
    head_of_master = commits[0]

    assert head_of_master.message == message_two


def test_dolt_log(create_test_table):
    repo, test_table = create_test_table
    message_one = 'Julianna, the very serious intellectual'
    message_two = 'Added Stan the Man'
    repo.add(test_table)
    repo.commit('Julianna, the very serious intellectual')
    repo.sql('INSERT INTO `test_players` (`name`, `id`) VALUES ("Stan", 4)')
    repo.add(test_table)
    repo.commit(message_two)
    commits = list(repo.log().values())
    current_commit = commits[0]
    previous_commit = commits[1]
    assert current_commit.message == message_two
    assert previous_commit.message == message_one


def test_dolt_log_merge_commit(create_test_table):
    repo, test_table = create_test_table
    message_one = 'Base branch'
    message_two = 'Base branch new data'
    message_three = 'Other branch'
    message_merge = 'merge'
    # commit the current working set to master
    repo.add(test_table)
    repo.commit(message_one)

    # create another branch from the working set
    repo.branch('other')

    # create a non-trivial commit against `master`
    repo.sql('INSERT INTO `test_players` (`name`, `id`) VALUES ("Stan", 4)')
    repo.add(test_table)
    repo.commit(message_two)

    # create a non-trivial commit against `other`
    repo.checkout('other')
    repo.sql('INSERT INTO `test_players` (`name`, `id`) VALUES ("Juan Martin", 5)')
    repo.add(test_table)
    repo.commit(message_three)

    # merge
    repo.checkout('master')
    repo.merge('other', message_merge)

    commits = list(repo.log().values())
    merge_commit = commits[0]
    first_merge_parent = commits[1]
    second_merge_parent = commits[2]

    assert merge_commit.message == message_merge
    assert first_merge_parent.hash in merge_commit.merge
    assert second_merge_parent.hash in merge_commit.merge


def test_get_dirty_tables(create_test_table):
    repo, test_table = create_test_table
    message = 'Committing test data'

    # Some test data
    initial = pd.DataFrame({'id': [1], 'name': ['Bianca'], 'role': ['Champion']})
    appended_row = pd.DataFrame({'name': ['Serena'], 'id': [2], 'role': ['Runner-up']})

    def _insert_row_helper(repo, table, row):
        write_pandas(repo, table, row, UPDATE, commit=False)

    # existing, not modified
    repo.add(test_table)
    repo.commit(message)

    # existing, modified, staged
    modified_staged = 'modified_staged'
    write_pandas(repo, modified_staged, initial, commit=False)
    repo.add(modified_staged)

    # existing, modified, unstaged
    modified_unstaged = 'modified_unstaged'
    write_pandas(repo, modified_unstaged, initial, commit=False)
    repo.add(modified_unstaged)

    # Commit and modify data
    repo.commit(message)
    _insert_row_helper(repo, modified_staged, appended_row)
    write_pandas(repo, modified_staged, appended_row, UPDATE, commit=False)
    repo.add(modified_staged)
    write_pandas(repo, modified_unstaged, appended_row, UPDATE, commit=False)

    # created, staged
    created_staged = 'created_staged'
    write_pandas(repo, created_staged, initial, import_mode=CREATE, primary_key=['id'], commit=False)
    repo.add(created_staged)

    # created, unstaged
    created_unstaged = 'created_unstaged'
    write_pandas(repo, created_unstaged, initial, import_mode=CREATE, primary_key=['id'], commit=False)

    status = repo.status()

    expected_new_tables = {'created_staged': True, 'created_unstaged': False}
    expected_changes = {'modified_staged': True, 'modified_unstaged': False}

    assert status.added_tables == expected_new_tables
    assert status.modified_tables == expected_changes


def test_checkout_with_tables(create_test_table):
    repo, test_table = create_test_table
    repo.checkout(table_or_tables=test_table)
    assert repo.status().is_clean


def test_branch(create_test_table):
    repo, _ = create_test_table
    active_branch, branches = repo.branch()
    assert [active_branch.name] == [branch.name for branch in branches] == ['master']

    repo.checkout('dosac', checkout_branch=True)
    repo.checkout('master')
    next_active_branch, next_branches = repo.branch()
    assert set(branch.name for branch in next_branches) == {'master', 'dosac'} and next_active_branch.name == 'master'

    repo.checkout('dosac')
    different_active_branch, _ = repo.branch()
    assert different_active_branch.name == 'dosac'


# we want to make sure that we can delte a branch atomically
def test_branch_delete(create_test_table):
    repo, _ = create_test_table

    _verify_branches(repo, ['master'])

    repo.checkout('dosac', checkout_branch=True)
    repo.checkout('master')
    _verify_branches(repo, ['master', 'dosac'])

    repo.branch('dosac', delete=True)
    _verify_branches(repo, ['master'])


def test_branch_move(create_test_table):
    repo, _ = create_test_table

    _verify_branches(repo, ['master'])

    repo.branch('master', move=True, new_branch='dosac')
    _verify_branches(repo, ['dosac'])


def _verify_branches(repo: Dolt, branch_list: List[str]):
    _, branches = repo.branch()
    assert set(branch.name for branch in branches) == set(branch for branch in branch_list)


def test_remote_list(create_test_table):
    repo, _ = create_test_table
    repo.remote(add=True, name='origin', url='blah-blah')
    assert repo.remote()[0].name == 'origin'
    repo.remote(add=True, name='another-origin', url='blah-blah')
    assert set([remote.name for remote in repo.remote()]) == {'origin', 'another-origin'}


def test_checkout_non_existent_branch(create_test_table):
    repo, _ = create_test_table
    with pytest.raises(DoltException):
        repo.checkout('master')


def test_ls(create_test_table):
    repo, test_table = create_test_table
    assert [table.name for table in repo.ls()] == [test_table]


def test_ls_empty(init_empty_test_repo):
    repo = init_empty_test_repo
    assert len(repo.ls()) == 0


def test_sql(create_test_table):
    repo, test_table = create_test_table
    sql = '''
        INSERT INTO {table} (name, id)
        VALUES ('Roger', 3)
    '''.format(table=test_table)
    repo.sql(query=sql)

    test_data = read_pandas(repo, test_table)
    assert 'Roger' in test_data['name'].to_list()


def test_sql_json(create_test_table):
    repo, test_table = create_test_table
    result = repo.sql(query='SELECT * FROM `{table}`'.format(table=test_table), result_format='json')['rows']
    _verify_against_base_rows(result)


def test_sql_csv(create_test_table):
    repo, test_table = create_test_table
    result = repo.sql(query='SELECT * FROM `{table}`'.format(table=test_table), result_format='csv')
    _verify_against_base_rows(result)


def test_sql_tabular(create_test_table):
    repo, test_table = create_test_table
    result = repo.sql(query='SELECT * FROM `{table}`'.format(table=test_table), result_format='tabular')
    _verify_against_base_rows(result)


def _verify_against_base_rows(result: List[dict]):
    assert len(result) == len(BASE_TEST_ROWS)

    result_sorted = sorted(result, key=lambda el: el['id'])
    for left, right in zip(BASE_TEST_ROWS, result_sorted):
        assert set(left.keys()) == set(right.keys())
        for k in left.keys():
            # Unfortunately csv.DictReader is a stream reader and thus does not look at all values for a given column
            # and make type inference, so we have to cast everything to a string. JSON round-trips, but would not
            # preserve datetime objects for example.
            assert str(left[k]) == str(right[k])


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
    repo.schema_import(table=table, create=True, pks=['id'], filename=test_file)

    assert repo.status().added_tables == {table: False}


def test_config_global(init_empty_test_repo):
    _ = init_empty_test_repo
    current_global_config = Dolt.config_global(list=True)
    test_username, test_email = 'test_user', 'test_email'
    Dolt.config_global(add=True, name='user.name', value=test_username)
    Dolt.config_global(add=True, name='user.email', value=test_email)
    updated_config = Dolt.config_global(list=True)
    assert updated_config['user.name'] == test_username and updated_config['user.email'] == test_email
    Dolt.config_global(add=True, name='user.name', value=current_global_config['user.name'])
    Dolt.config_global(add=True, name='user.email', value=current_global_config['user.email'])
    reset_config = Dolt.config_global(list=True)
    assert reset_config['user.name'] == current_global_config['user.name']
    assert reset_config['user.email'] == current_global_config['user.email']


def test_config_local(init_empty_test_repo):
    repo = init_empty_test_repo
    current_global_config = Dolt.config_global(list=True)
    test_username, test_email = 'test_user', 'test_email'
    repo.config_local(add=True, name='user.name', value=test_username)
    repo.config_local(add=True, name='user.email', value=test_email)
    local_config = repo.config_local(list=True)
    global_config = Dolt.config_global(list=True)
    assert local_config['user.name'] == test_username and local_config['user.email'] == test_email
    assert global_config['user.name'] == current_global_config['user.name']
    assert global_config['user.email'] == current_global_config['user.email']
