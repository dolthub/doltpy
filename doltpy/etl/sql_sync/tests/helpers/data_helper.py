from datetime import datetime
import logging
from doltpy.etl.sql_sync.tools import TableMetadata, Column

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

TEST_TABLE_COLUMNS = [Column('first_name', 'VARCHAR(256)', 'PRI'),
                      Column('last_name', 'VARCHAR(256)', 'PRI'),
                      Column('playing_style_desc', 'LONGTEXT'),
                      Column('win_percentage', 'DECIMAL'),
                      Column('high_rank', 'INT'),
                      Column('turned_pro', 'DATETIME')]
TEST_TABLE_METADATA = TableMetadata(TABLE_NAME, TEST_TABLE_COLUMNS)

TEST_DATA_INITIAL = [tuple(el[col.col_name] for col in TEST_TABLE_METADATA.columns) for el in BASE_TEST_DATA_INITIAL]
TEST_DATA_UPDATE = [tuple(el[col.col_name] for col in TEST_TABLE_METADATA.columns) for el in BASE_TEST_DATA_UPDATE]

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


def get_data_for_comparison(conn):
    query = '''
        SELECT
            {columns}
        FROM
            {table_name}
        ORDER BY first_name, last_name ASC;
    '''.format(columns=','.join(col.col_name for col in TEST_TABLE_METADATA.columns),
               table_name=TEST_TABLE_METADATA.name)
    cursor = conn.cursor()
    cursor.execute(query)
    return [tup for tup in cursor]


DROP_TEST_TABLE = 'DROP TABLE {table_name}'.format(table_name=TABLE_NAME)


def assert_tuple_array_equality(left, right):
    assert len(left) == len(right)
    failed = False
    
    if len(left) == 0 and len(right) == 0:
        return True

    for left_tup, right_tup in zip(left, right):
        for left_el, right_el in zip(left_tup, right_tup):
            if left_el != right_el:
                logger.warning('Non identical values (left) {} != {} (right)'.format(left_el, right_el))
                failed = True

    if failed:
        raise AssertionError('Errors found')
    else:
        return True
