from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import Table, Column
from sqlalchemy.dialects import mysql, postgresql
from doltpy.etl.sql_sync.db_tools import DoltAsSourceWriter, get_target_writer_helper
from doltpy.core.system_helpers import get_logger
from typing import List

logger = get_logger(__name__)

# Rules for translating postgres to Dolt
# def clean_column(column):
#     # Postgres uses next_val in autoincrement, and mysql does not. If autoincrement
#     # and has a default, remove. This likely has other use cases this would not be
#     # desirable for, and should be adjusted as those are found.
#     if hasattr(column, "server_default") and column.autoincrement is True:
#         delattr(column, 'server_default')
#     # Postgres has native IP address types, that should be converted to VARCHAR(43)
#     if str(column.type) == 'CIDR':
#         column = Column(column.name, VARCHAR(length=43), autoincrement=column.autoincrement, nullable=column.nullable)
#     elif str(column.type) == 'INET':
#         column = Column(column.name, VARCHAR(length=43), autoincrement=column.autoincrement, nullable=column.nullable)
#     elif str(column.type) == 'MACADDR':
#         column = Column(column.name, VARCHAR(length=43), autoincrement=column.autoincrement, nullable=column.nullable)
#     elif str(column.type) == 'JSONB':
#         column = Column(column.name, mysql.LONGTEXT, autoincrement=column.autoincrement, nullable=column.nullable)
#     elif str(column.type) == 'ARRAY':
#         column = Column(column.name, mysql.LONGTEXT, autoincrement=column.autoincrement, nullable=column.nullable)
#     # Postgres can have an ARRAY of other types, such as SMALLINT[]
#     elif str(column.type).endswith('[]'):
#         column = Column(column.name, mysql.LONGTEXT, autoincrement=column.autoincrement, nullable=column.nullable)
#     elif str(column.type) == 'UUID':
#         column = Column(column.name, VARCHAR(length=36), autoincrement=column.autoincrement, nullable=column.nullable)
#     elif str(column.type) == 'BYTEA':
#         column = Column(column.name, mysql.LONGTEXT, autoincrement=column.autoincrement, nullable=column.nullable)
#     return column
POSTGRES_TO_DOLT_TYPE_MAPPINGS = {
    postgresql.CIDR: mysql.VARCHAR(43),
    postgresql.INET: mysql.VARCHAR(43),
    postgresql.MACADDR: mysql.VARCHAR(43),
    postgresql.JSON: mysql.LONGTEXT,
    postgresql.JSONB: mysql.LONGTEXT,
    postgresql.ARRAY: mysql.LONGTEXT,
    postgresql.UUID: mysql.VARCHAR(43),
    postgresql.BYTEA: mysql.LONGTEXT
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
