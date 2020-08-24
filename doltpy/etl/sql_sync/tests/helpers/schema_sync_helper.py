from sqlalchemy import Table, Column, MetaData, Integer
from sqlalchemy.dialects import postgresql, mysql


TEST_SOURCE_TABLE, TEST_TARGET_TABLE = 'test_source_table', 'test_target_table'


POSTGRES_TABLE = Table(TEST_SOURCE_TABLE,
                       MetaData(),
                       Column('id', postgresql.INTEGER, primary_key=True, autoincrement=False),
                       Column('ints', postgresql.ARRAY(postgresql.INTEGER)),
                       Column('floats', postgresql.ARRAY(postgresql.FLOAT)),
                       Column('cidr_val', postgresql.CIDR),
                       Column('inet_val', postgresql.INET),
                       Column('uuid_val', postgresql.UUID),
                       Column('bytes', postgresql.BYTEA),
                       Column('jsonb_val', postgresql.JSONB),
                       Column('json_val', postgresql.JSON))

MYSQL_TABLE = Table(TEST_SOURCE_TABLE,
                    MetaData(),
                    Column('id', mysql.INTEGER, primary_key=True, autoincrement=False),
                    Column('ints', mysql.LONGTEXT),
                    Column('floats', mysql.LONGTEXT),
                    Column('cidr_val', mysql.VARCHAR(43)),
                    Column('inet_val', mysql.VARCHAR(43)),
                    Column('uuid_val', mysql.VARCHAR(43)),
                    Column('bytes', mysql.LONGTEXT),
                    Column('jsonb_val', mysql.LONGTEXT),
                    Column('json_val', mysql.JSON))


ALTER_TABLE = '''
    ALTER TABLE {} ADD COLUMN another INTEGER
'''.format(TEST_SOURCE_TABLE)

EXPECTED_DOLT_TABLE = Table(TEST_TARGET_TABLE,
                            MetaData(),
                            Column('id', mysql.INTEGER, primary_key=True, autoincrement=False),
                            Column('ints', mysql.LONGTEXT),
                            Column('floats', mysql.LONGTEXT),
                            Column('cidr_val', mysql.VARCHAR(43)),
                            Column('inet_val', mysql.VARCHAR(43)),
                            Column('uuid_val', mysql.VARCHAR(43)),
                            Column('bytes', mysql.LONGTEXT),
                            Column('jsonb_val', mysql.LONGTEXT),
                            Column('json_val', mysql.LONGTEXT))
