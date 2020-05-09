from doltpy.etl.sql_sync.tests.helpers.data_helper import TABLE_NAME


CREATE_TEST_TABLE = '''
    CREATE TABLE {table_name} (
        first_name VARCHAR(256),
        last_name VARCHAR(256),
        playing_style_desc TEXT,
        win_percentage DECIMAL(10, 2),
        high_rank INT,
        turned_pro TIMESTAMP,
        PRIMARY KEY (first_name, last_name)
    );
'''.format(table_name=TABLE_NAME)
