from typing import List, Callable
from mysql.connector.connection import MySQLConnection
import logging
from doltpy.etl.sql_sync.db_tools import (TableMetadata,
                                          Column,
                                          DoltAsSourceWriter,
                                          DoltAsTargetReader,
                                          TableUpdate,
                                          build_target_writer,
                                          build_source_reader,
                                          get_table_reader,
                                          get_insertion_lists)

logger = logging.getLogger(__name__)


def get_target_writer(conn: MySQLConnection, update_on_duplicate: bool = True) -> DoltAsSourceWriter:
    """
    Given a database connection returns a function that when passed a mapping from table names to TableUpdate will
    apply the table update. A table update consists of primary key values to drop, and data to insert/update.
    :param conn: a database connection
    :param update_on_duplicate: indicates whether to update values when encountering duplicate PK, default True
    :return:
    """
    return build_target_writer(conn, get_table_metadata, get_insert_query, update_on_duplicate)


def get_source_reader(conn: MySQLConnection,
                      reader: Callable[[str, MySQLConnection], TableUpdate] = None) -> DoltAsTargetReader:
    """
    Given a connection and a reader provides a function that turns a set of tables in to a data structure containing
    the contents of each of the tables.
    :param conn:
    :param reader:
    :return:
    """
    reader_function = reader or get_table_reader()
    return build_source_reader(conn, get_table_metadata, reader_function)


def get_table_metadata(table_name: str, conn: MySQLConnection) -> TableMetadata:
    """
    Builds an instance of TableMetadata which is used to construct queries in a consistent manner and reason about
    primary keys.
    :param table_name:
    :param conn:
    :return:
    """
    cursor = conn.cursor()
    cursor.execute('SHOW COLUMNS FROM {table_name}'.format(table_name=table_name))
    columns = []

    for tup in cursor:
        col_name, col_type, _, key, _, _ = tup
        columns.append(Column(col_name, col_type, True if key else False))

    return TableMetadata(table_name, columns)


def get_insert_query(table_metadata: TableMetadata, update_on_duplicate: bool = True) -> str:
    """
    Provides MySQL specific insert statement which is conditionally upsert based on the value of update_on_duplicate.
    :param table_metadata:
    :param update_on_duplicate:
    :return:
    """
    col_list, wildcard_list = get_insertion_lists(table_metadata)
    base_query = '''
        INSERT INTO {table_name} (
            {cols}
        ) VALUES ({col_value_wild_cards})
    '''.format(table_name=table_metadata.name,
               cols=','.join(col_list),
               col_value_wild_cards=','.join(wildcard_list))

    if update_on_duplicate:
        update_clause = '''
        ON DUPLICATE KEY UPDATE
            {update_list}
        '''.format(update_list=','.join(['{} = VALUES({})'.format(col, col) for col in col_list]))
        return base_query + update_clause
    else:
        return base_query

