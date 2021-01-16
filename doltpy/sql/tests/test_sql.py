TEST_ROWS = [
    {'name': 'Anna', 'adjective': 'tragic', 'id': '1', 'date_of_death': datetime(1877, 1, 1)},
    {'name': 'Vronksy', 'adjective': 'honorable', 'id': '2', 'date_of_death': None},
    {'name': 'Oblonksy', 'adjective': 'buffoon', 'id': '3', 'date_of_death': None},
]

TEST_COLUMNS = {
    'name': ['Anna', 'Vronksy', 'Oblonksy'],
    'adjective': ['tragic', 'honorable', 'buffoon'],
    'id': [1, 2, 3],
    'date_of_birth': [date(1840, 1, 1), date(1840, 1, 1), date(1840, 1, 1)],
    'date_of_death': [datetime(1877, 1, 1), None, None]
}

def test_sql_server(create_test_table, run_serve_mode):
    """
    This test ensures we can round-trip data to the database.
    :param create_test_table:
    :param run_serve_mode:
    :return:
    """
    repo, test_table = create_test_table
    data = pandas_read_sql('SELECT * FROM {}'.format(test_table), repo.get_engine())
    assert list(data['id']) == [1, 2]


def test_sql_server_unique(create_test_table, run_serve_mode, init_other_empty_test_repo):
    """
    This tests that if you fire up SQL server via Python, you get a connection to the SQL server instance that the repo
    is running, not another repos MySQL server instance.
    :return:
    """
    @retry(delay=2, tries=10, exceptions=(
        sqlalchemy.exc.OperationalError,
        sqlalchemy.exc.DatabaseError,
        sqlalchemy.exc.InterfaceError,
    ))
    def get_databases(engine: Engine):
        with engine.connect() as conn:
            result = conn.execute('SHOW DATABASES')
            return [tup[0] for tup in result]

    repo, test_table = create_test_table
    other_repo = init_other_empty_test_repo
    other_repo.sql_server()

    repo_databases = get_databases(repo.get_engine())
    other_repo_databases = get_databases(other_repo.get_engine())

    assert {'information_schema', repo.repo_name} == set(repo_databases)
    assert {'information_schema', other_repo.repo_name} == set(other_repo_databases)