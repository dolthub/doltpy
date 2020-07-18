from sqlalchemy .engine import Engine
from sqlalchemy.dialects.mysql import insert
from sqlalchemy import Table
from doltpy.etl.sql_sync.db_tools import DoltAsSourceWriter, get_target_writer_helper
from doltpy.core.system_helpers import get_logger
from typing import List

logger = get_logger(__name__)


def get_target_writer(engine: Engine, update_on_duplicate: bool = True) -> DoltAsSourceWriter:
    """
    Given a database connection returns a function that when passed a mapping from table names to TableUpdate will
    apply the table update. A table update consists of primary key values to drop, and data to insert/update.
    :param engine: a database connection
    :param update_on_duplicate: indicates whether to update values when encountering duplicate PK, default True
    :return:
    """
    return get_target_writer_helper(engine, upsert_helper, update_on_duplicate)


def upsert_helper(table: Table, data: List[dict]):
    insert_statement = insert(table).values(data)
    update_dict = {el.name: el for el in insert_statement.inserted if not el.primary_key}
    on_duplicate_key_statement = insert_statement.on_duplicate_key_update(update_dict)
    return on_duplicate_key_statement

