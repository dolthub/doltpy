from sqlalchemy .engine import Engine
from sqlalchemy.dialects.mysql import insert
from sqlalchemy import Table, Column
from sqlalchemy.dialects import mysql
from doltpy.etl.sql_sync.db_tools import DoltAsSourceWriter, get_target_writer_helper
from doltpy.core.system_helpers import get_logger
from typing import List, Iterable
from datetime import datetime, date, time

logger = get_logger(__name__)

MYSQL_TO_DOLT_TYPE_MAPPINGS = {
    mysql.JSON: mysql.LONGTEXT
}


def get_target_writer(engine: Engine, update_on_duplicate: bool = True) -> DoltAsSourceWriter:
    """
    Given a database connection returns a function that when passed a mapping from table names to TableUpdate will
    apply the table update. A table update consists of primary key values to drop, and data to insert/update.
    :param engine: a database connection
    :param update_on_duplicate: indicates whether to update values when encountering duplicate PK, default True
    :return:
    """
    return get_target_writer_helper(engine, upsert_helper, update_on_duplicate, clean_types)


def upsert_helper(table: Table, data: List[dict]):
    insert_statement = insert(table).values(data)
    update_dict = {el.name: el for el in insert_statement.inserted if not el.primary_key}
    on_duplicate_key_statement = insert_statement.on_duplicate_key_update(update_dict)
    return on_duplicate_key_statement


def clean_types(data: Iterable[dict]) -> List[dict]:
    """
    MySQL does not support native array or JSON types, additionally mysql-connector-python does not support
    datetime.date (though that seems like a bug in the connector). This implements a very crude transformation of array
    types and coerces datetime.date values to equivalents. This is quite an experimental feature and is currently a way
    to transform array valued data read from Postgres to Dolt.
    :param data:
    :return:
    """
    data_copy = []
    for row in data:
        row_copy = {}
        for col, val in row.items():
            if type(val) == date:
                row_copy[col] = datetime.combine(val, time())
            elif type(val) == list:
                if not val:
                    row_copy[col] = None
                else:
                    row_copy[col] = ','.join(str(el) if el is not None else 'NULL' for el in val)
            elif type(val) == dict:
                row_copy[col] = str(val)
            else:
                row_copy[col] = val

        data_copy.append(row_copy)

    return data_copy
