from datetime import datetime
import logging
from typing import Mapping, Any
from decimal import Decimal

logger = logging.getLogger(__name__)

TABLE_NAME = 'great_players'
BASE_TEST_DATA_INITIAL = [
    {'first_name': 'Novak',
     'last_name': 'Djokovic',
     'playing_style_desc': 'aggressive/baseline',
     'win_percentage': 83.0,
     'high_rank': 1,
     'turned_pro': datetime(2003, 1, 1)},
    {'first_name': 'Rafael',
     'last_name': 'Nadal',
     'playing_style_desc': 'aggressive/baseline',
     'win_percentage': 83.2,
     'high_rank': 1,
     'turned_pro': datetime(2001,1, 1)},
    {'first_name': 'Roger',
     'last_name': 'Federer',
     'playing_style_desc': 'aggressive/all-court',
     'win_percentage': 81.2,
     'high_rank': 1,
     'turned_pro': datetime(1998, 1, 1)}
]
BASE_TEST_DATA_UPDATE = [
    {'first_name': 'Stefanos',
     'last_name': 'Tsitsipas',
     'playing_style_desc': 'aggressive/all-court',
     'win_percentage': 67.6,
     'high_rank': 5,
     'turned_pro': datetime(2016, 1, 1)},
    {'first_name': 'Alexander',
     'last_name': 'Zverev',
     'playing_style_desc': 'aggressive/baseline',
     'win_percentage': 65.8,
     'high_rank': 3,
     'turned_pro': datetime(2013, 1, 1)},
    {'first_name': 'Dominic',
     'last_name': 'Thiem',
     'playing_style_desc': 'aggressive/baseline',
     'win_percentage': 65.1,
     'high_rank': 3,
     'turned_pro': datetime(2011, 1, 1)}
]

COLS_INSERTION_ORDER = ['first_name',
                        'last_name',
                        'playing_style_desc',
                        'win_percentage',
                        'high_rank',
                        'turned_pro']
COL_SORTED_ORDER = ['first_name',
                    'high_rank',
                    'last_name',
                    'playing_style_desc',
                    'turned_pro',
                    'win_percentage']


def _get_test_data_col_order(row: Mapping[str, Any]):
    return tuple(row[key] for key in COLS_INSERTION_ORDER)


def _get_test_data_sorted_col_order(row: Mapping[str, Any]):
    return tuple(row[key] for key in COL_SORTED_ORDER)


TEST_DATA_INITIAL = [_get_test_data_col_order(row) for row in BASE_TEST_DATA_INITIAL]
TEST_DATA_INITIAL_COL_SORT = [_get_test_data_sorted_col_order(row) for row in BASE_TEST_DATA_INITIAL]
TEST_DATA_UPDATE = [_get_test_data_col_order(row) for row in BASE_TEST_DATA_UPDATE]
TEST_DATA_UPDATE_COL_SORT = [_get_test_data_sorted_col_order(row) for row in BASE_TEST_DATA_UPDATE]

TEST_TABLE_COLUMNS = {
    'first_name': True,
    'last_name': True,
    'playing_style_desc': False,
    'win_percentage': False,
    'high_rank': False,
    'turned_pro': False,
}
CREATE_TEST_TABLE = '''
    CREATE TABLE {table_name} (
        `first_name` VARCHAR(256),
        `last_name` VARCHAR(256),
        `playing_style_desc` LONGTEXT,
        `win_percentage` DECIMAL, 
        `high_rank` INT,
        `turned_pro` DATETIME,
        PRIMARY KEY (`first_name`, `last_name`)
    );
'''.format(table_name=TABLE_NAME)
DATA_CHECK_QUERY = '''
    SELECT
        {columns}
    FROM
        {table_name}
    ORDER BY first_name, last_name ASC;
'''.format(columns=','.join(COL_SORTED_ORDER),
           table_name=TABLE_NAME)
INSERT_TEST_DATA_QUERY = '''
    INSERT INTO {table_name} (
        `first_name`,
        `last_name`,
        `playing_style_desc`,
        `win_percentage`,
        `high_rank`,
        `turned_pro`
    ) VALUES (%s, %s, %s, %s, %s, %s);
'''.format(table_name=TABLE_NAME)
DROP_TEST_TABLE = 'DROP TABLE {table_name}'.format(table_name=TABLE_NAME)


def assert_tuple_array_equality(left, right):
    assert len(left) == len(right)
    failed = False

    for left_tup, right_tup in zip(left, right):
        for left_el, right_el in zip(left_tup, right_tup):
            if type(right_el) == Decimal:
                if Decimal(left_el) != right_el:
                    logger.warning('Non identical values (left) {} != {} (right)'.format(left_el, right_el))
                    failed = True
            if left_el != right_el:
                logger.warning('Non identical values (left) {} != {} (right)'.format(left_el, right_el))
                failed = True

    if failed:
        raise AssertionError('Errors found')
    else:
        return True