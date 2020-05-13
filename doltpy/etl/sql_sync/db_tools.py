import logging
from typing import Tuple, Iterable, Mapping, Callable, List, Any

logger = logging.getLogger(__name__)

# Types that reflect the different nature of the syncs
DoltTableUpdate = Tuple[Iterable[tuple], Iterable[tuple]]
TableUpdate = Iterable[tuple]

# For using Dolt as the target
DoltAsTargetUpdate = Mapping[str, TableUpdate]
DoltAsTargetReader = Callable[[List[str]], DoltAsTargetUpdate]
DoltAsTargetWriter = Callable[[DoltAsTargetUpdate], None]

# For using Dolt as the source
DoltAsSourceUpdate = Mapping[str, DoltTableUpdate]
DoltAsSourceReader = Callable[[List[str]], DoltAsSourceUpdate]
DoltAsSourceWriter = Callable[[DoltAsSourceUpdate], None]


class Column:
    """
    A thin wrapper for database columns so we can access named attributes rather than tuples representing column
    metadata.
    """
    def __init__(self, col_name: str, col_type: str, key: bool = False ):
        self.col_name = col_name
        self.col_type = col_type
        self.key = key


class TableMetadata:
    """
    A thin wrapper for database tables so we can store a name and column rather than a tuple.
    """
    def __init__(self, name: str, columns: List[Column]):
        self.name = name
        self.columns = sorted(columns, key=lambda col: col.col_name)


def build_target_writer(conn,
                        table_meta_data_builder: Callable[[str, Any], TableMetadata],
                        insert_query_builder: Callable[[TableMetadata, bool], str],
                        update_on_duplicate: bool = True) -> DoltAsSourceWriter:
    """
    Given a connection, basically defined by having a cursor/transaction interface, executes a write to a target
    database that corresponds to the conn parameter.
    :param conn:
    :param table_meta_data_builder:
    :param insert_query_builder:
    :param update_on_duplicate:
    :return:
    """
    def inner(table_data_map: Mapping[str, Iterable[tuple]]):
        for table, table_update in table_data_map.items():
            table_metadata = table_meta_data_builder(table, conn)
            pks_to_drop, data = table_update
            pks_to_drop = list(pks_to_drop)
            logger.info('Dropping {} primary keys from {}'.format(len(pks_to_drop), table))
            drop_primary_keys(conn, table_metadata, pks_to_drop)
            data = list(data)
            logger.info('Writing {} rows to table {}'.format(len(data), table))
            write_to_table(conn, table_metadata, insert_query_builder, data, update_on_duplicate)

    return inner


def build_source_reader(conn,
                        table_meta_data_builder: Callable[[str, Any], TableMetadata],
                        reader: Callable[[str, Any], TableUpdate]) -> DoltAsTargetReader:
    """
    Given a connection and a reader provides a function that turns a set of tables in to a data structure containing
    the contents of each of the tables.
    :param conn:
    :param table_meta_data_builder:
    :param reader:
    :return:
    """
    def inner(tables: List[str]):
        result = {}

        for table in tables:
            logger.info('Reading tables {}'.format(table))
            table_metadata = table_meta_data_builder(table, conn)
            result[table] = reader(conn, table_metadata)

        return result

    return inner


def get_table_reader():
    """
    When syncing from a relational database, currently  MySQL or Postgres, the database has only a single concept of
    state, that is the current state. We simply capture this state by reading out all the data in the database.
    :return:
    """
    def inner(conn: Any, table_metadata: TableMetadata):
        query = '''
            SELECT
                {cols}
            FROM
                {table_name}
        '''.format(cols=','.join(col.col_name for col in table_metadata.columns), table_name=table_metadata.name)
        cursor = conn.cursor()
        cursor.execute(query)
        return [tup for tup in cursor]

    return inner


def write_to_table(conn,
                   table_metadata: TableMetadata,
                   insert_query_builder: Callable[[TableMetadata, bool], str],
                   data: List[tuple],
                   update_on_duplicate: bool = True):
    """
    Uses the standard cursor/transaction interface that connectors for both MySQL and Postgres provide to execute write.
    Takes a parameter for building an insert query since different database implementations have different syntax for
    performing upserts.
    :param conn:
    :param table_metadata:
    :param insert_query_builder:
    :param data:
    :param update_on_duplicate:
    :return:
    """
    insert_query = insert_query_builder(table_metadata, update_on_duplicate)
    cursor = conn.cursor()
    cursor.executemany(insert_query, data)
    conn.commit()
    cursor.close()


def drop_primary_keys(conn, table_metadata: TableMetadata, primary_key_values: List[tuple]):
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
    """
    Returns a set of filters for generic ANSII SQL queries.
    :param cols:
    :return:
    """
    if len(cols) == 1:
        delete_clause = '{col} = %s'.format(col=cols[0])
    else:
        base_delete_clause = '{first_col} = %s AND {rest_cols}'
        rest_cols = 'AND '.join(['{} = %s'.format(col) for col in cols[1:]])
        delete_clause = base_delete_clause.format(first_col=cols[0], rest_cols=rest_cols)

    return delete_clause


def get_insertion_lists(table_metadata: TableMetadata) -> Tuple[List[str], List[str]]:
    """
    Returns set of assignments for upserts in standard ANSII SQL update statements. Dolt tracks state, and when we want
    to sync an update to an existing primary key in a target relational database these provides the list of assignments
    from wildcard to table schema.
    :param table_metadata:
    :return:
    """
    col_list, wildcard_list = [], []

    for col in table_metadata.columns:
        col_list.append(col.col_name)
        wildcard_list.append('%s')

    return col_list, wildcard_list
