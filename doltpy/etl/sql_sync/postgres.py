from sqlalchemy.engine import Engine
from sqlalchemy import MetaData
from sqlalchemy.dialects.postgresql import insert
from typing import List, Any, Callable, Tuple
from doltpy.etl.sql_sync.db_tools import (DoltAsSourceUpdate,
                                          DoltAsSourceWriter,
                                          DoltAsTargetReader,
                                          TableUpdate,
                                          build_source_reader,
                                          get_table_reader)
import logging

logger = logging.getLogger(__name__)


def get_target_writer(engine: Engine, update_on_duplicate: bool = True) -> DoltAsSourceWriter:
    """
    Given a psycopg2 connection returns a function that takes a map of tables names (optionally schema prefixed) to
    list of tuples and writes the list of tuples to the table in question. Each tuple must have the data in the order of
    the target tables columns sorted lexicographically.
    :param engine: database connection.
    :param update_on_duplicate: perform upserts instead of failing on duplicate primary keys
    :return:
    """
    def inner(table_data_map: DoltAsSourceUpdate):
        metadata = MetaData(bind=engine, reflect=True)
        for table_name, table_update in table_data_map.items():
            table = metadata.tables[table_name]
            pks_to_drop, data = table_update

            # PKs to be dropped are provided as dicts, we drop them
            with engine.connect() as conn:
                conn.execute(table.delete(), list(pks_to_drop))

            # Need to do a bit more here
            insert_statement = insert(table).values(data)
            with engine.connect() as conn:

                if update_on_duplicate:
                    do_update_statement = insert_statement.on_conflict_do_update(
                        constraint=table.primary_key,
                        set_=dict(data=data)
                    )
                    conn.execute(do_update_statement)
                else:
                    do_nothing_statement = insert_statement.on_conflict_do_nothing()
                    conn.execute(do_nothing_statement)

    return inner


def get_source_reader(engine: Engine, reader: Callable[[str, Any], TableUpdate] = None) -> DoltAsTargetReader:
    """
    Given a connection and a reader provides a function returns a mapping from table names as keys to lists of tuples
    representing the data in the table corresponding the key.
    :param engine:
    :param reader:
    :return:
    """
    reader_function = reader or get_table_reader()
    return build_source_reader(engine, reader_function)
