import pytest
import datetime
import csv
import os

from doltpy.cli.dolt import Dolt
from .cli.helpers import get_repo_path_tmp_path

TEST_TABLE = 'characters'
TEST_DATA_INITIAL = [
    {'name': 'Anna', 'adjective': 'tragic', 'id': 1, 'date_of_death': datetime.datetime(1877, 1, 1)},
    {'name': 'Vronksy', 'adjective': 'honorable', 'id': 2, 'date_of_death': None},
    {'name': 'Oblonksy', 'adjective': 'buffoon', 'id': 3, 'date_of_death': None},
]

TEST_DATA_UPDATE = [
    {'name': 'Vronksy', 'adjective': 'honorable', 'id': 2, 'date_of_death': datetime.datetime(1879, 1, 1)},
    {'name': 'Levin', 'adjective': 'tiresome', 'id': 4, 'date_of_death': None},
]

TEST_DATA_FINAL = [TEST_DATA_INITIAL[0], TEST_DATA_INITIAL[2]] + TEST_DATA_UPDATE


@pytest.fixture()
def with_test_data_initial_file(tmp_path):
    return _test_data_to_file(tmp_path, TEST_DATA_INITIAL)


@pytest.fixture()
def with_test_table(init_empty_test_repo):
    dolt = init_empty_test_repo
    dolt.sql(query=f'''
        CREATE TABLE `{TEST_TABLE}` (
            `name` VARCHAR(32),
            `adjective` VARCHAR(32),
            `id` INT NOT NULL,
            `date_of_death` DATETIME, 
            PRIMARY KEY (`id`)
        );
    ''')
    dolt.add(TEST_TABLE)
    dolt.commit('Created test table')
    return dolt


@pytest.fixture()
def with_test_data_initial_file(tmp_path):
    return _test_data_to_file(tmp_path, 'initial', TEST_DATA_INITIAL)


@pytest.fixture()
def with_test_data_final_file(tmp_path):
    return _test_data_to_file(tmp_path, 'final', TEST_DATA_FINAL)


def _test_data_to_file(file_path, file_name, test_data):
    path = os.path.join(file_path, file_name)
    with open(path, 'w') as fh:
        csv_writer = csv.DictWriter(fh, fieldnames=test_data[0].keys())
        csv_writer.writeheader()
        csv_writer.writerows(test_data)

    return path


@pytest.fixture
def init_empty_test_repo(tmpdir) -> Dolt:
    return _init_helper(tmpdir)


@pytest.fixture
def init_other_empty_test_repo(tmpdir) -> Dolt:
    return _init_helper(tmpdir, 'other')


def _init_helper(path: str, ext: str = None):
    repo_path, repo_data_dir = get_repo_path_tmp_path(path, ext)
    return Dolt.init(repo_path)
