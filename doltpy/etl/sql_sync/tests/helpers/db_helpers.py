from doltpy.etl.sql_sync.tests.helpers.data_helper import DATA_CHECK_QUERY, INSERT_TEST_DATA_QUERY
from doltpy.core import Dolt
from typing import List


def mysql_read_helper(conn):
    cursor = conn.cursor()
    cursor.execute(DATA_CHECK_QUERY)
    return [tup for tup in cursor]


def mysql_insert_tuples(connection, data):
    cursor = connection.cursor(prepared=True)
    cursor.executemany(INSERT_TEST_DATA_QUERY, data)
    connection.commit()


def dolt_insert_tuples(repo: Dolt, table: str, data: List[tuple]):
    connection = repo.cnx
    mysql_insert_tuples(connection, data)
    repo.add_table_to_next_commit(table)
    repo.commit('Inserted test data')

