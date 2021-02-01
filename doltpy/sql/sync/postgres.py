import logging
from typing import List

from sqlalchemy import Table  # type: ignore
from sqlalchemy.dialects import mysql, postgresql  # type: ignore
from sqlalchemy.dialects.postgresql import insert  # type: ignore
from sqlalchemy.engine import Engine  # type: ignore

from doltpy.sql.sync.db_tools import DoltAsSourceWriter, get_target_writer_helper

logger = logging.getLogger(__name__)


POSTGRES_TO_DOLT_TYPE_MAPPINGS = {
    postgresql.CIDR: mysql.VARCHAR(43),
    postgresql.INET: mysql.VARCHAR(43),
    postgresql.MACADDR: mysql.VARCHAR(43),
    postgresql.JSON: mysql.LONGTEXT,
    postgresql.JSONB: mysql.LONGTEXT,
    postgresql.ARRAY: mysql.LONGTEXT,
    postgresql.UUID: mysql.VARCHAR(43),
    postgresql.BYTEA: mysql.LONGTEXT,
}


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
