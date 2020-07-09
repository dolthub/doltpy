from typing import Tuple, Iterable, Mapping, Callable, List, Any
from doltpy.core.system_helpers import get_logger
from sqlalchemy import MetaData, create_engine, Table, select
from sqlalchemy.engine import Engine

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


def build_source_reader(engine: Engine, reader: Callable[[Engine, str], TableUpdate]) -> DoltAsTargetReader:
    """
    Given a connection and a reader provides a function that turns a set of tables in to a data structure containing
    the contents of each of the tables.
    :param engine:
    :param reader:
    :return:
    """
    def inner(tables: List[str]):
        result = {}

        for table in tables:
            logger.info('Reading tables {}'.format(table))
            table_metadata = get_table_metadata(engine, table)
            result[table] = reader(engine, table_metadata)

        return result

    return inner


def get_table_reader():
    """
    When syncing from a relational database, currently  MySQL or Postgres, the database has only a single concept of
    state, that is the current state. We simply capture this state by reading out all the data in the database.
    :return:
    """
    def inner(engine: Engine, table: Table):
        with engine.connect() as conn:
            result = conn.execute(select[table])
            return result

    return inner


def get_table_metadata(engine: Engine, table_name: str) -> Table:
    metadata = MetaData(bind=engine, reflect=True)
    return metadata.tables[table_name]

