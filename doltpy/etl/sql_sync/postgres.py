from typing import List, Any, Callable
from doltpy.etl.sql_sync.db_tools import (TableMetadata,
                                          Column,
                                          DoltAsSourceWriter,
                                          DoltAsTargetReader,
                                          TableUpdate,
                                          build_target_writer,
                                          build_source_reader,
                                          get_table_reader,
                                          get_insertion_lists)
import logging

logger = logging.getLogger(__name__)


def get_target_writer(conn, update_on_duplicate: bool = True) -> DoltAsSourceWriter:
    """

    :param conn:
    :param update_on_duplicate:
    :return:
    """
    return build_target_writer(conn, get_table_metadata, get_insert_query, update_on_duplicate)


def get_source_reader(conn, reader: Callable[[str, Any], TableUpdate] = None) -> DoltAsTargetReader:
    """
    Given a connection and a reader provides a function that turns a set of tables in to a data structure containing
    the contents of each of the tables.
    :param conn:
    :param schema:
    :param reader:
    :return:
    """
    reader_function = reader or get_table_reader()
    return build_source_reader(conn, get_table_metadata, reader_function)


def get_tables(conn, schema: str = None):
    query = '''
        SELECT
            tablename
        FROM
            pg_catalog.pg_tables
        WHERE
            schemaname = '{schema}';
    '''.format(schema=schema or 'public')
    
    cursor = conn.cursor()
    cursor.execute(query)
    return [tup[0] for tup in cursor]


def get_table_metadata(table_name: str, conn) -> TableMetadata:
    query = '''
        SELECT
            column_name, data_type
        FROM
            information_schema.columns
        WHERE
            table_name = '{table}'
    '''.format(table=table_name)
    cursor = conn.cursor()
    cursor.execute(query)
    pks = _get_primary_key_cols(table_name, conn)
    columns = [Column(col_name, col_type, col_name in pks) for col_name, col_type in cursor]
    return TableMetadata(table_name, columns)


def _get_primary_key_cols(table_name: str, conn) -> List[str]:
    query = '''
    SELECT
        a.attname
    FROM
        pg_index i
    JOIN
        pg_attribute a ON a.attrelid = i.indrelid
        AND a.attnum = ANY(i.indkey)
    WHERE
        i.indrelid = '{table_name}'::regclass
        AND i.indisprimary
    '''.format(table_name=table_name)
    cursor = conn.cursor()
    cursor.execute(query)
    return [tup[0] for tup in cursor]


def get_insert_query(table_metadata: TableMetadata, update_on_duplicate: bool = True) -> str:
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
        ON CONFLICT ({pks}) DO UPDATE 
            SET {update_list}
        '''.format(pks=','.join(col.col_name for col in table_metadata.columns if col.key),
                   update_list=','.join(['{} = excluded.{}'.format(col, col) for col in col_list]))
        return base_query + update_clause
    else:
        return base_query

