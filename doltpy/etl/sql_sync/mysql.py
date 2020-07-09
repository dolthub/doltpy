from typing import Callable, Mapping, Iterable
from mysql.connector.connection import MySQLConnection
from sqlalchemy import MetaData, Column
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.mysql import insert
from doltpy.etl.sql_sync.db_tools import (DoltAsSourceUpdate,
                                          DoltAsSourceWriter,
                                          DoltAsTargetReader,
                                          TableUpdate,
                                          build_source_reader,
                                          get_table_reader)
from doltpy.core.system_helpers import get_logger

logger = get_logger(__name__)


def get_target_writer(engine: Engine, update_on_duplicate: bool = True) -> DoltAsSourceWriter:
    """
    Given a database connection returns a function that when passed a mapping from table names to TableUpdate will
    apply the table update. A table update consists of primary key values to drop, and data to insert/update.
    :param engine: a database connection
    :param update_on_duplicate: indicates whether to update values when encountering duplicate PK, default True
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

            # Now we can perform our inserts
            insert_statement = insert(table).values(data)
            with engine.connect() as conn:
                if update_on_duplicate:
                    on_duplicate_key_statement = insert_statement.on_duplicate_key_update(
                        data=insert_statement.data,
                        status='U'
                    )
                    conn.execute(on_duplicate_key_statement)
                else:
                    conn.execute(insert_statement)

    return inner


def get_source_reader(engine: Engine,
                      reader: Callable[[str, MySQLConnection], TableUpdate] = None) -> DoltAsTargetReader:
    """
    Given a connection and a reader provides a function that turns a set of tables in to a data structure containing
    the contents of each of the tables.
    :param engine:
    :param reader:
    :return:
    """
    reader_function = reader or get_table_reader()
    return build_source_reader(engine, reader_function)


