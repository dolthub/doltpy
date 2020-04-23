from itertools import dropwhile
from typing import List, Tuple, Mapping, Iterable
from doltpy.etl.sql_sync.tools import TargetWriter, SourceReader, TableMetadata, Column
import logging

logger = logging.getLogger(__name__)


def get_source_reader() -> SourceReader:
    raise NotImplemented()


def get_target_writer(conn, drop_duplicates: bool = False) -> TargetWriter:
    def inner(table_data_map: Mapping[str, Iterable[tuple]]):
        for table, data in table_data_map.items():
            data = list(data)
            logger.info('Writing {} rows to table {}'.format(table, len(data)))
            write_to_table(table, conn, data, drop_duplicates)

    return inner


def write_to_table(table: str, conn, data: List[tuple], drop_duplicates: bool = False):
    table_metadata = get_table_metadata(table, conn)
    insert_query = get_insert_query(table, table_metadata, drop_duplicates)
    cursor = conn.cursor()
    cursor.executemany(insert_query, data)
    conn.commit()
    cursor.close()


def get_table_metadata(table: str, conn) -> TableMetadata:
    query = '''
        SELECT
            column_name, data_type
        FROM
            information_schema.columns
        WHERE
            table_name = '{table}'
    '''.format(table=table)
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [Column(col_name, col_type) for col_name, col_type in cursor]
    return TableMetadata(table, columns)


def get_insert_query(table_name: str, table_metadata: TableMetadata, drop_duplicates_pks: bool = True) -> str:
    col_list, wildcard_list = get_insertion_lists(table_metadata)
    query_template = '''
        INSERT INTO {table_name} (
            {cols}
        ) VALUES ({col_value_wild_cards})
    '''
    if drop_duplicates_pks:
        query_template += ' ON CONFLICT DO NOTHING'

    return query_template.format(table_name=table_name, cols=col_list, col_value_wild_cards=wildcard_list)


def get_insertion_lists(table_metadata: TableMetadata) -> Tuple[str, str]:
    col_list, wildcard_list = [], []

    for col in table_metadata.columns:
        col_list.append(col.col_name)
        wildcard_list.append(col.get_wildcard())

    return ','.join(col_list), ','.join(wildcard_list)