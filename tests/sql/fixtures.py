import pytest
from doltpy.sql.tests.helpers import TEST_TABLE, TEST_DATA_INITIAL, TEST_DATA_FINAL
import csv
import os


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
