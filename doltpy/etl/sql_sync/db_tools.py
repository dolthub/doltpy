from typing import Tuple, Iterable, Mapping, Callable, List
from doltpy.core.system_helpers import get_logger
from sqlalchemy import MetaData, Table
from sqlalchemy.engine import Engine
from retry import retry

logger = get_logger(__name__)

# Types that reflect the different nature of the syncs
DoltTableUpdate = Tuple[Iterable[dict], Iterable[dict]]
TableUpdate = Iterable[dict]

# For using Dolt as the target
DoltAsTargetUpdate = Mapping[str, TableUpdate]
DoltAsTargetReader = Callable[[List[str]], DoltAsTargetUpdate]
DoltAsTargetWriter = Callable[[DoltAsTargetUpdate], None]

# For using Dolt as the source
DoltAsSourceUpdate = Mapping[str, DoltTableUpdate]
DoltAsSourceReader = Callable[[List[str]], DoltAsSourceUpdate]
DoltAsSourceWriter = Callable[[DoltAsSourceUpdate], None]


def get_source_reader(engine: Engine, reader: Callable[[Engine, Table], List[dict]] = None) -> DoltAsTargetReader:
    """
    Given a connection and a reader provides a function that turns a set of tables in to a data structure containing
    the contents of each of the tables.
    :param engine:
    :param reader:
    :return:
    """
    reader_function = reader or get_table_reader()
    return build_source_reader(engine, reader_function)


def build_source_reader(engine: Engine, reader: Callable[[Engine, Table], TableUpdate]) -> DoltAsTargetReader:
    """
    Given a connection and a reader provides a function that turns a set of tables in to a data structure containing
    the contents of each of the tables.
    :param engine:
    :param reader:
    :return:
    """
    def inner(tables: List[str]) -> DoltAsTargetUpdate:
        result = {}
        metadata = MetaData(bind=engine, reflect=True)

        for table in [table for table_name, table in metadata.tables.items() if table_name in tables]:
            logger.info('Reading tables {}'.format(table))
            result[table.name] = reader(engine, table)

        return result

    return inner


def get_table_reader() -> Callable[[Engine, Table], List[dict]]:
    """
    When syncing from a relational database, currently  MySQL or Postgres, the database has only a single concept of
    state, that is the current state. We simply capture this state by reading out all the data in the database.
    :return:
    """
    def inner(engine: Engine, table: Table) -> List[dict]:
        with engine.connect() as conn:
            result = conn.execute(table.select())
            return [dict(row) for row in result]

    return inner


# TODO this is flaky on Dolt, though not at all clear why
@retry(exceptions=Exception, delay=2, tries=10)
def get_table_metadata(engine: Engine, table_name: str) -> Table:
    metadata = MetaData(bind=engine, reflect=True)
    return metadata.tables[table_name]


def get_target_writer_helper(engine: Engine, get_upsert_statement, update_on_duplicate: bool) -> DoltAsSourceWriter:
    """
    Given a database connection returns a function that when passed a mapping from table names to TableUpdate will
    apply the table update. A table update consists of primary key values to drop, and data to insert/update.
    :param engine: a database connection
    :param get_upsert_statement:
    :param update_on_duplicate: indicates whether to update values when encountering duplicate PK, default True
    :return:
    """
    def inner(table_data_map: DoltAsSourceUpdate):
        metadata = MetaData(bind=engine, reflect=True)
        for table_name, table_update in table_data_map.items():
            table = metadata.tables[table_name]
            pks_to_drop, data = table_update

            # PKs to be dropped are provided as dicts, we drop them
            if pks_to_drop:
                drop_primary_keys(engine, table, pks_to_drop)

            # Now we can perform our inserts
            if data:
                with engine.connect() as conn:
                    if update_on_duplicate:
                        statement = get_upsert_statement(table, data)
                    else:
                        statement = table.insert().values(data)
                    conn.execute(statement)

    return inner


def drop_primary_keys(engine: Engine, table: Table, pks_to_drop: Iterable[dict]):
    with engine.connect() as conn:
        pks = [col.name for col in table.columns if col.primary_key]
        statement = table.delete()
        for pk in pks:
            statement = statement.where(table.c[pk].in_([pks_for_row[pk] for pks_for_row in pks_to_drop]))
        conn.execute(statement)


