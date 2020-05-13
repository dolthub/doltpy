from typing import List, Any, Callable, Tuple
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
    Given a psycopg2 connection returns a function that takes a map of tables names (optionally schema prefixed) to
    list of tuples and writes the list of tuples to the table in question. Each tuple must have the data in the order of
    the target tables columns sorted lexicographically.
    :param conn: database connection.
    :param update_on_duplicate: perform upserts instead of failing on duplicate primary keys
    :return:
    """
    return build_target_writer(conn, get_table_metadata, get_insert_query, update_on_duplicate)


def get_source_reader(conn, reader: Callable[[str, Any], TableUpdate] = None) -> DoltAsTargetReader:
    """
    Given a connection and a reader provides a function returns a mapping from table names as keys to lists of tuples
    representing the data in the table corresponding the key.
    :param conn:
    :param reader:
    :return:
    """
    reader_function = reader or get_table_reader()
    return build_source_reader(conn, get_table_metadata, reader_function)


def get_table_metadata(table_name: str, conn) -> TableMetadata:
    """
    Given a connection to a Postgres instance and table name
    :param table_name:
    :param conn:
    :return:
    """
    schema, table = _get_schema_and_table(table_name)
    query = '''
        SELECT
            column_name, data_type
        FROM
            information_schema.columns
        WHERE
            table_name = '{table}'
            AND table_schema = '{schema}'  
    '''.format(table=table, schema=schema)
    cursor = conn.cursor()
    cursor.execute(query)
    pks = _get_primary_key_cols(table_name, conn)
    columns = [Column(col_name, col_type, col_name in pks) for col_name, col_type in cursor]
    return TableMetadata(table_name, columns)


def _get_primary_key_cols(table_name: str, conn) -> List[str]:
    schema, table = _get_schema_and_table(table_name)
    query = '''
    SELECT
        a.attname
    FROM
        pg_index i
    JOIN
        pg_attribute a ON a.attrelid = i.indrelid
        AND a.attnum = ANY(i.indkey)
    WHERE
        i.indrelid = '{schema}.{table}'::regclass
        AND i.indisprimary
    '''.format(table=table, schema=schema)
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


def _get_schema_and_table(table_name: str) -> Tuple[str, str]:
    split = table_name.split('.')
    if len(split) == 1:
        return 'public', table_name
    elif len(split) == 2:
        return split[0], split[1]
    else:
        raise ValueError('Postgres tables must be referred by <schema>.<name> syntax')