from typing import Mapping, List, Iterable
from doltpy.etl.sql_sync.tools import TargetWriter, SourceReader
from mysql.connector import connection
import logging

logger = logging.getLogger(__name__)


def get_source_reader() -> SourceReader:
    raise NotImplemented()


def get_target_writer(conn: connection) -> TargetWriter:
    def inner(table_data_map: Mapping[str, Iterable[tuple]]):
        for table, data in table_data_map.items():
            data = list(data)
            logger.info('Writing {} rows to table {}'.format(table, len(data)))
            write_to_table(table, conn, data)

    return inner


def write_to_table(table: str, conn: connection, data: List[tuple]):
    # Get the column names in the target table
    columns = get_mysql_columns(table, conn)
    insert_query = _get_insert_query(table, columns)
    cursor = conn.cursor()
    cursor.executemany(insert_query, data)
    conn.commit()


def get_mysql_columns(table_name: str, mysql_conn) -> List[str]:
    get_cols_query = _get_columns_query(table_name).format(table_name=table_name)
    cursor = mysql_conn.cursor()
    cursor.execute(get_cols_query)
    cols = [tup[0] for tup in cursor]
    cols.sort()
    return cols


def _get_columns_query(table_name: str) -> str:
    query_template = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
    return query_template.format(table_name=table_name)


def _get_insert_query(table_name: str, columns: List[str]):
    query_template = '''
        INSERT IGNORE INTO {table_name} (
            {cols}
        ) VALUES ({col_value_wild_cards})
    '''.format(table_name=table_name,
               cols=_generate_col_list(columns),
               col_value_wild_cards=','.join(['%s' for _ in range(len(columns))]))
    return query_template


def _generate_col_list(columns: List[str]):
    if len(columns) == 1:
        return '`{}`'.format(columns[0])
    else:
        base = '\n'.join(['`{}`,'.format(col) for col in columns[:-1]])
        return base + '`{}`'.format(columns[-1])
