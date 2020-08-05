import pytest
from doltpy.core.dolt import Dolt, _execute, DoltException
from doltpy.core.write import UPDATE, import_df
from doltpy.core.read import pandas_read_sql, read_table
import shutil
import pandas as pd
import uuid
import os
from typing import Tuple, List
from doltpy.core.tests.helpers import get_repo_path_tmp_path
import sqlalchemy
from retry import retry

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
    import_df(repo, 'test_players', pd.read_csv(test_data_path), ['id'], UPDATE)
    yield repo, 'test_players'

    if 'test_players' in [table.name for table in repo.ls()]:
        _execute(['table', 'rm', 'test_players'], repo.repo_dir())


def test_init(tmp_path):
    repo_path, repo_data_dir = get_repo_path_tmp_path(tmp_path)
    assert not os.path.exists(repo_data_dir)
    Dolt.init(repo_path)
    assert os.path.exists(repo_data_dir)
    shutil.rmtree(repo_data_dir)


def test_commit(create_test_table):
    repo, test_table = create_test_table
    repo.add(test_table)
    before_commit_count = len(repo.log())
    repo.commit('Julianna, the very serious intellectual')
    assert repo.status().is_clean and len(repo.log()) == before_commit_count + 1


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


def test_get_dirty_tables(create_test_table):
    repo, test_table = create_test_table
    message = 'Committing test data'

    # Some test data
    initial = pd.DataFrame({'id': [1], 'name': ['Bianca'], 'role': ['Champion']})
    appended_row = pd.DataFrame({'name': ['Serena'], 'id': [2], 'role': ['Runner-up']})

    def _insert_row_helper(repo, table, row):
        import_df(repo, table, row, ['id'], import_mode=UPDATE)

    # existing, not modified
    repo.add(test_table)
    repo.commit(message)

    # existing, modified, staged
    modified_staged = 'modified_staged'
    import_df(repo, modified_staged, initial, ['id'])
    repo.add(modified_staged)

    # existing, modified, unstaged
    modified_unstaged = 'modified_unstaged'
    import_df(repo, modified_unstaged, initial, ['id'])
    repo.add(modified_unstaged)

    # Commit and modify data
    repo.commit(message)
    _insert_row_helper(repo, modified_staged, appended_row)
    import_df(repo, modified_staged, appended_row, ['id'], UPDATE)
    repo.add(modified_staged)
    import_df(repo, modified_unstaged, appended_row, ['id'], UPDATE)

    # created, staged
    created_staged = 'created_staged'
    import_df(repo, created_staged, initial, ['id'])
    repo.add(created_staged)

    # created, unstaged
    created_unstaged = 'created_unstaged'
    import_df(repo, created_unstaged, initial, ['id'])

    status = repo.status()

    expected_new_tables = {'created_staged': True, 'created_unstaged': False}
    expected_changes = {'modified_staged': True, 'modified_unstaged': False}

    assert status.added_tables == expected_new_tables
    assert status.modified_tables == expected_changes


def test_checkout_with_tables(create_test_table):
    repo, test_table = create_test_table
    repo.checkout(table_or_tables=test_table)
    assert repo.status().is_clean


def test_sql_server(create_test_table, run_serve_mode):
    """
    This test ensures we can round-trip data to the database.
    :param create_test_table:
    :param run_serve_mode:
    :return:
    """
    repo, test_table = create_test_table
    data = pandas_read_sql('SELECT * FROM {}'.format(test_table), repo.engine)
    assert list(data['id']) == [1, 2]


def test_sql_server_unique(create_test_table, run_serve_mode, init_other_empty_test_repo):
    """
    This tests that if you fire up SQL server via Python, you get a connection to the SQL server instance that the repo
    is running, not another repos MySQL server instance.
    :return:
    """
    @retry(exceptions=(sqlalchemy.exc.OperationalError, sqlalchemy.exc.DatabaseError), delay=2, tries=10)
    def get_databases(dolt_repo: Dolt):
        with dolt_repo.engine.connect() as conn:
            result = conn.execute('SHOW DATABASES')
            return [tup[0] for tup in result]

    repo, test_table = create_test_table
    other_repo = init_other_empty_test_repo
    other_repo.sql_server()

    repo_databases = get_databases(repo.engine)
    other_repo_databases = get_databases(other_repo.engine)

    assert {'information_schema', repo.repo_name} == set(repo_databases)
    assert {'information_schema', other_repo.repo_name} == set(other_repo_databases)


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


def test_sql(create_test_table):
    repo, test_table = create_test_table
    sql = '''
        INSERT INTO {table} (name, id)
        VALUES ('Roger', 3)
    '''.format(table=test_table)
    repo.sql(query=sql)

    test_data = read_table(repo, test_table)
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
            # and make type inference, so we have to cast everything to a string. JSON roundtrips, but would not
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
