from typing import Mapping, List, Iterable, Tuple
from doltpy.etl.sql_sync.tools import TargetWriter, SourceReader, TableMetadata, Column
from mysql.connector import connection
import logging

logger = logging.getLogger(__name__)


def get_source_reader() -> SourceReader:
    raise NotImplemented()


def get_target_writer(conn: connection, drop_duplicates: bool = False) -> TargetWriter:
    def inner(table_data_map: Mapping[str, Iterable[tuple]]):
        for table, data in table_data_map.items():
            data = list(data)
            logger.info('Writing {} rows to table {}'.format(table, len(data)))
            write_to_table(table, conn, data, drop_duplicates)

    return inner


def write_to_table(table: str, conn: connection, data: List[tuple], drop_duplicate_pks: bool = False):
    # Get the column names in the target table
    table_metadata = get_table_metadata(table, conn)
    insert_query = get_insert_query(table, table_metadata, drop_duplicate_pks)
    cursor = conn.cursor()
    cursor.executemany(insert_query, data)
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


def get_insert_query(table_name: str, table_metadata: TableMetadata, drop_duplicate_pks: bool = True) -> str:
    col_list, wildcard_list = get_insertion_lists(table_metadata)
    if drop_duplicate_pks:
        query_template = '''
            INSERT IGNORE INTO {table_name} (
                {cols}
            ) VALUES ({col_value_wild_cards})
        '''
    else:
        query_template = '''
            INSERT INTO {table_name} (
                {cols}
            ) VALUES ({col_value_wild_cards})
        '''
    return query_template.format(table_name=table_name, cols=col_list, col_value_wild_cards=wildcard_list)


def get_insertion_lists(table_metadata: TableMetadata) -> Tuple[str, str]:
    col_list, wildcard_list = [], []

    for col in table_metadata.columns:
        col_list.append(col.col_name)
        wildcard_list.append(col.get_wildcard())

    return ','.join(col_list), ','.join(wildcard_list)
