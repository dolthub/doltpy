from typing import List, Tuple
from doltpy.etl.sql_sync.tools import TargetWriter, DatabaseUpdate, TableMetadata, Column
from mysql.connector import connection
import logging

logger = logging.getLogger(__name__)


def get_target_writer(conn: connection, update_on_duplicate: bool = True) -> TargetWriter:
    """
    Given a database connection returns a function that when passed a mapping from table names to TableUpdate will
    apply the table update. A table update consists of primary key values to drop, and data to insert/update.
    :param conn: a database connection
    :param update_on_duplicate: indicates whether to update values when encountering duplicate PK, default True
    :return:
    """
    def inner(table_data_map: DatabaseUpdate):
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


def write_to_table(table_metadata: TableMetadata, conn: connection, data: List[tuple], update_on_duplicate: bool = True):
    insert_query = get_insert_query(table_metadata.name, table_metadata, update_on_duplicate)
    cursor = conn.cursor()
    cursor.executemany(insert_query, data)
    conn.commit()


def drop_primary_keys(conn, table_metadata: TableMetadata, primary_key_values: List[tuple]):
    if not primary_key_values:
        return
    pks = [col.col_name for col in table_metadata.columns if col.key]
    query_template = '''
        DELETE FROM 
            {table_name}
        WHERE
            {delete_clause}
    '''

    if len(pks) == 1:
        delete_clause = '{pk} = %s'.format(pk=pks[0])
    else:
        base_delete_clause = '{first_pk} = %s AND {rest_pks}'
        rest_pks = 'AND '.join(['{} = %s'.format(pk) for pk in pks[1:]])
        delete_clause = base_delete_clause.format(first_pk=pks[0], rest_pks=rest_pks)

    query = query_template.format(table_name=table_metadata.name, delete_clause=delete_clause)
    cursor = conn.cursor()
    cursor.executemany(query, primary_key_values)
    conn.commit()


def get_table_metadata(table_name: str, conn: connection) -> TableMetadata:
    query = 'SHOW COLUMNS FROM {table_name}'.format(table_name=table_name)
    cursor = conn.cursor()
    cursor.execute(query)
    columns = []

    for tup in cursor:
        col_name, col_type, _, key, _, _ = tup
        columns.append(Column(col_name, col_type, key))

    return TableMetadata(table_name, columns)


def get_insert_query(table_name: str, table_metadata: TableMetadata, update_on_duplicate: bool = True) -> str:
    col_list, wildcard_list = get_insertion_lists(table_metadata)
    base_query = '''
        INSERT INTO {table_name} (
            {cols}
        ) VALUES ({col_value_wild_cards})
    '''.format(table_name=table_name,
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
