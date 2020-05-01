from typing import List, Tuple, Callable
from doltpy.etl.sql_sync.tools import (DoltAsSourceWriter,
                                       DoltAsTargetReader,
                                       DoltAsSourceUpdate,
                                       TableUpdate,
                                       TableMetadata,
                                       Column)
from mysql.connector.connection import MySQLConnection
import logging

logger = logging.getLogger(__name__)


def get_target_writer(conn: MySQLConnection, update_on_duplicate: bool = True) -> DoltAsSourceWriter:
    """
    Given a database connection returns a function that when passed a mapping from table names to TableUpdate will
    apply the table update. A table update consists of primary key values to drop, and data to insert/update.
    :param conn: a database connection
    :param update_on_duplicate: indicates whether to update values when encountering duplicate PK, default True
    :return:
    """
    def inner(table_data_map: DoltAsSourceUpdate):
        for table, table_update in table_data_map.items():
            table_metadata = get_table_metadata(table, conn)
            pks_to_drop, data = table_update
            pks_to_drop = list(pks_to_drop)
            logger.info('Dropping {} primary keys from {}'.format(len(pks_to_drop), table))
            drop_primary_keys(conn, table_metadata, pks_to_drop)
            data = list(data)
            logger.info('Writing {} rows to table {}'.format(len(data), table))
            write_to_table(table_metadata, conn, data, update_on_duplicate)

    return inner


def get_source_reader(conn: MySQLConnection,
                      reader: Callable[[str, MySQLConnection], TableUpdate]) -> DoltAsTargetReader:
    """
    Given a connection and a reader provides a function that turns a set of tables in to a data structure containing
    the contents of each of the tables.
    :param conn:
    :param reader:
    :return:
    """
    def inner(tables: List[str]):
        result = {}
        database_tables = get_tables(conn)
        missing_tables = [table for table in tables if table not in database_tables]
        if missing_tables:
            logger.error('The following tables are missign, exiting:\n{}'.format(missing_tables))
            raise ValueError('Missing tables {}'.format(missing_tables))

        for table in tables:
            logger.info('Reading tables {}'.format(table))
            result[table] = reader(table, conn)

        return result

    return inner


def get_table_reader():
    """
    When syncing from a relational database such as MySQL the database has only a single concept of state, that is the
    current state. We simply capture this state by reading out all the data in the database.
    :return:
    """
    def inner(table_name: str, conn: MySQLConnection):
        table_metadata = get_table_metadata(table_name, conn)
        query = '''
            SELECT
                {cols}
            FROM
                {table_name}
        '''.format(cols=','.join(col.col_name for col in table_metadata.columns), table_name=table_name)
        cursor = conn.cursor()
        cursor.execute(query)
        return [tup for tup in cursor]

    return inner


def write_to_table(table_metadata: TableMetadata,
                   conn: MySQLConnection,
                   data: List[tuple],
                   update_on_duplicate: bool = True):
    insert_query = get_insert_query(table_metadata, update_on_duplicate)
    cursor = conn.cursor()
    cursor.executemany(insert_query, data)
    conn.commit()


def drop_primary_keys(conn: MySQLConnection, table_metadata: TableMetadata, primary_key_values: List[tuple]):
    """
    Drops a given list of primary keys from the database represented by the conn parameter.
    :param conn:
    :param table_metadata:
    :param primary_key_values:
    :return:
    """
    if not primary_key_values:
        return
    pks = [col.col_name for col in table_metadata.columns if col.key]
    query_template = '''
        DELETE FROM 
            {table_name}
        WHERE
            {delete_filter}
    '''

    delete_filter = get_filters(pks)

    query = query_template.format(table_name=table_metadata.name, delete_filter=delete_filter)
    cursor = conn.cursor()
    cursor.executemany(query, primary_key_values)
    conn.commit()


def get_filters(cols: List[str]):
    if len(cols) == 1:
        delete_clause = '{col} = %s'.format(col=cols[0])
    else:
        base_delete_clause = '{first_col} = %s AND {rest_cols}'
        rest_cols = 'AND '.join(['{} = %s'.format(col) for col in cols[1:]])
        delete_clause = base_delete_clause.format(first_col=cols[0], rest_cols=rest_cols)

    return delete_clause


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
        columns.append(Column(col_name, col_type, key))

    return TableMetadata(table_name, columns)


def get_tables(conn: MySQLConnection) -> List[str]:
    cursor = conn.cursor()
    cursor.execute('SHOW TABLES')
    return [tup[0] for tup in cursor]


def get_insert_query(table_metadata: TableMetadata, update_on_duplicate: bool = True) -> str:
    col_list, wildcard_list = get_insertion_lists(table_metadata)
    base_query = '''
        INSERT INTO {table_name} (
            {cols}
        ) VALUES ({col_value_wild_cards})
    '''.format(table_name=table_metadata.name,
               cols=','.join(col_list),
               col_value_wild_cards=','.join(wildcard_list),)

    if update_on_duplicate:
        update_clause = '''
        ON DUPLICATE KEY UPDATE
            {update_list}
        '''.format(update_list=','.join(['{} = VALUES({})'.format(col, col) for col in col_list]))
        return base_query + update_clause
    else:
        return base_query


def get_insertion_lists(table_metadata: TableMetadata) -> Tuple[List[str], List[str]]:
    col_list, wildcard_list = [], []

    for col in table_metadata.columns:
        col_list.append(col.col_name)
        wildcard_list.append('%s')

    return col_list, wildcard_list
