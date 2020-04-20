from doltpy.etl.sql_sync.tests.data_helper import DATA_CHECK_QUERY, INSERT_TEST_DATA_QUERY


def mysql_read_helper(conn):
    cursor = conn.cursor()
    cursor.execute(DATA_CHECK_QUERY)
    return [tup for tup in cursor]


def mysql_insert_helper(connection, data):
    cursor = connection.cursor()
    cursor.executemany(INSERT_TEST_DATA_QUERY, data)
    connection.commit()

