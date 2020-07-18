from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import Table
from doltpy.etl.sql_sync.db_tools import DoltAsSourceWriter, get_target_writer_helper
from doltpy.core.system_helpers import get_logger
from typing import List

logger = get_logger(__name__)


def get_target_writer(engine: Engine, update_on_duplicate: bool = True) -> DoltAsSourceWriter:
    """
    Given a psycopg2 connection returns a function that takes a map of tables names (optionally schema prefixed) to
    list of tuples and writes the list of tuples to the table in question. Each tuple must have the data in the order of
    the target tables columns sorted lexicographically.
    :param engine: database connection.
    :param update_on_duplicate: perform upserts instead of failing on duplicate primary keys
    :return:
    """
    return get_target_writer_helper(engine, upsert_helper, update_on_duplicate)


def upsert_helper(table: Table, data: List[dict]):
    # TODO this does not work yet
    insert_statement = insert(table).values(data)
    update = {col.name: col for col in insert_statement.excluded}
    upsert_statement = insert_statement.on_conflict_do_update(constraint=table.primary_key, set_=update)
    return upsert_statement
