import logging
from copy import deepcopy
from typing import List

from sqlalchemy import MetaData, Table, bindparam, select  # type: ignore
from sqlalchemy.engine import Engine  # type: ignore

from doltpy.sql.helpers import hash_row_els
from doltpy.sql.sync.db_tools import (
    DoltAsSourceUpdate,
    DoltAsSourceWriter,
    drop_primary_keys,
)

logger = logging.getLogger(__name__)


def get_target_writer(engine: Engine, update_on_duplicate: bool = True) -> DoltAsSourceWriter:
    """
    Given a database connection returns a function that when passed a mapping from table names to TableUpdate will
    apply the table update. A table update consists of primary key values to drop, and data to insert/update.
    :param engine: a database connection
    :param update_on_duplicate: indicates whether to update values when encountering duplicate PK, default True
    :return:
    """

    def inner(table_data_map: DoltAsSourceUpdate):
        metadata = MetaData(bind=engine)
        metadata.reflect()

        for table_name, table_update in table_data_map.items():
            table = metadata.tables[table_name]
            pks_to_drop, data = table_update
            clean_data = list(data)

            # PKs to be dropped are provided as dicts, we drop them
            if pks_to_drop:
                drop_primary_keys(engine, table, pks_to_drop)

            # Now we can perform our inserts
            if data:
                execute_updates_and_inserts(engine, table, clean_data, update_on_duplicate)

    return inner


def execute_updates_and_inserts(engine: Engine, table: Table, data: List[dict], update_on_duplicate: bool):
    # get the existing pks as dicts
    pk_cols = [col.name for col in table.columns if col.primary_key]
    non_pk_cols = [col.name for col in table.columns if not col.primary_key]

    with engine.connect() as conn:
        select_pks_statement = select([table.c[pk_col] for pk_col in pk_cols])
        existing_pks = conn.execute(select_pks_statement)
        existing_pk_lookup = {hash_row_els(dict(row), pk_cols) for row in existing_pks}

    updates, inserts = [], []
    for row in data:
        if hash_row_els(dict(row), pk_cols) in existing_pk_lookup:
            updates.append(row)
        else:
            inserts.append(row)

    _updates = deepcopy(updates)
    for dic in _updates:
        for col in list(dic.keys()):
            dic[f"_{col}"] = dic.pop(col)

    with engine.connect() as conn:
        for insert in inserts:
            insert_statement = table.insert().values(insert)
            conn.execute(insert_statement)
        if update_on_duplicate and _updates:
            update_statement = table.update()
            for pk_col in pk_cols:
                update_statement = update_statement.where(table.c[pk_col] == bindparam(f"_{pk_col}"))
            update_statement = update_statement.values({col: bindparam(f"_{col}") for col in non_pk_cols})
            conn.execute(update_statement, _updates)
