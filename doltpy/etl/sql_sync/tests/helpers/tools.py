from doltpy.etl.sql_sync.tests.helpers.data_helper import (TEST_TABLE_METADATA,
                                                           TEST_DATA_INITIAL,
                                                           TEST_DATA_APPEND_SINGLE_ROW,
                                                           TEST_DATA_APPEND_MULTIPLE_ROWS,
                                                           TEST_DATA_UPDATE_SINGLE_ROW,
                                                           TEST_DATA_APPEND_MULTIPLE_ROWS_WITH_DELETE,
                                                           get_data_for_comparison,
                                                           assert_rows_equal,
                                                           FIRST_UPDATE,
                                                           SECOND_UPDATE,
                                                           THIRD_UPDATE,
                                                           FOURTH_UPDATE,
                                                           get_expected_data)
from doltpy.etl.sql_sync.dolt import (get_source_reader as get_dolt_source_reader,
                                      get_target_writer as get_dolt_target_writer,
                                      get_table_reader_diffs as get_dolt_table_reader_diffs,
                                      get_table_reader as get_dolt_table_reader)
from doltpy.etl.sql_sync.sync_tools import sync_to_dolt, sync_from_dolt, DoltAsSourceWriter
from doltpy.etl.sql_sync.db_tools import get_table_metadata, drop_primary_keys
from typing import List, Callable
from sqlalchemy.engine import Engine
from sqlalchemy import Table


def validate_get_table_metadata(engine: Engine, table_name: str):
    """
    Verify that get_table_metadata correctly constructs the metadata associated with the test table. We manually build
    that metadata in helpers/data_helper.py to verify this.
    :param engine:
    :param table_name:
    :return:
    """
    result = get_table_metadata(engine, table_name)
    assert str(TEST_TABLE_METADATA.name) == str(result.name)
    assert len(TEST_TABLE_METADATA.columns) == len(result.columns)
    assert set(col.name for col in TEST_TABLE_METADATA.columns) == set(col.name for col in result.columns)


def validate_get_target_writer(engine: Engine,
                               table: Table,
                               get_target_writer: Callable[[Engine, bool], DoltAsSourceWriter]):
    """
    Ensure that writes using our write wrapper correctly show up in a relational database server.
    :param engine:
    :param table:
    :param get_target_writer:
    :return:
    """
    def _write_and_diff_helper(data: List[dict], update_num: int):
        get_target_writer(engine, True)({str(table.name): ([], data)})
        result = get_data_for_comparison(engine)
        _, expected_data = get_expected_data(update_num)
        assert_rows_equal(expected_data, result)

    _write_and_diff_helper(TEST_DATA_INITIAL, FIRST_UPDATE)
    _write_and_diff_helper(TEST_DATA_APPEND_SINGLE_ROW, SECOND_UPDATE)
    _write_and_diff_helper(TEST_DATA_APPEND_MULTIPLE_ROWS, THIRD_UPDATE)
    _write_and_diff_helper(TEST_DATA_UPDATE_SINGLE_ROW, FOURTH_UPDATE)


def validate_drop_primary_keys(engine: Engine, table: Table):
    """
    Verify that dropping a primary key from using drop_primary_keys leaves a relational database in the required state.
    :param engine:
    :param table:
    :return:
    """
    with engine.connect() as conn:
        conn.execute(table.insert(), TEST_DATA_APPEND_MULTIPLE_ROWS)

    pks_to_drop = [{'first_name': 'Stefanos', 'last_name': 'Tsitsipas'}]
    drop_primary_keys(engine, table, pks_to_drop)
    result = get_data_for_comparison(engine)
    assert_rows_equal(TEST_DATA_APPEND_MULTIPLE_ROWS_WITH_DELETE, result)


def validate_dolt_as_target(db_engine: Engine,
                            db_table: Table,
                            get_db_source_reader,
                            get_db_target_writer,
                            get_db_table_reader,
                            dolt_repo,
                            dolt_table):
    """
    Validates syncing from a relational database, so far MySQL and Postgres, to Dolt. Work by making a series of writes
    to the relational database (running in a Docker container provided by a fixture), executing a sync, and then
    validating the HEAD of master of the Dolt repo has the expected values. It also validates that the Dolt history is
    correct after every write. Finally validates that deletes flow through to Dolt.
    :param db_engine:
    :param db_table:
    :param get_db_source_reader:
    :param get_db_target_writer:
    :param get_db_table_reader:
    :param dolt_repo:
    :param dolt_table:
    :return:
    """
    def sync_to_dolt_helper():
        source_reader = get_db_source_reader(db_engine, get_db_table_reader())
        target_writer = get_dolt_target_writer(dolt_repo, commit=True)
        sync_to_dolt(source_reader, target_writer, {str(db_table.name): str(dolt_table.name)})

    def assertion_helper(commit: str, expected_diff: List[dict]):
        """
        Validates that both the HEAD of the current branch of the Dolt repo match MySQL, and that the diffs created by
        the write match what is expected.
        """
        _, dolt_data = get_dolt_table_reader(commit)(str(dolt_table.name), dolt_repo)
        db_table_metadata = get_table_metadata(db_engine, str(db_table.name))
        db_data = get_db_table_reader()(db_engine, db_table_metadata)
        assert_rows_equal(list(dolt_data), db_data)

        _, dolt_diff_data = get_dolt_table_reader_diffs(commit)(str(dolt_table.name), dolt_repo)
        assert_rows_equal(expected_diff, list(dolt_diff_data))

    update_sequence = [
        TEST_DATA_INITIAL,
        TEST_DATA_APPEND_MULTIPLE_ROWS,
        TEST_DATA_APPEND_SINGLE_ROW,
        TEST_DATA_UPDATE_SINGLE_ROW
    ]

    for update_data in update_sequence:
        get_db_target_writer(db_engine)({str(db_table.name): ([], update_data)})
        sync_to_dolt_helper()
        latest_commit = list(dolt_repo.log().keys())[0]
        assertion_helper(latest_commit, update_data)

    with db_engine.connect() as conn:
        conn.execute(db_table.delete().where(db_table.c.first_name == 'Novak'))

    sync_to_dolt_helper()
    latest_commit = list(dolt_repo.log().keys())[0]
    _, dolt_data = get_dolt_table_reader(latest_commit)(str(dolt_table.name), dolt_repo)
    db_data = get_db_table_reader()(db_engine, db_table)
    assert_rows_equal(list(dolt_data), db_data)
    dropped_pks, _ = get_dolt_table_reader_diffs(latest_commit)(str(dolt_table.name), dolt_repo)
    assert dropped_pks == [{'first_name': 'Novak', 'last_name': 'Djokovic'}]


def validate_dolt_as_source(db_conn, db_table, get_db_target_writer, dolt_repo, dolt_table):
    """
    Verifies that given a Dolt repository that has a a series of updates applied to it (defined in the fixture
    create_dolt_test_data_commits) that after syncing at each of the commits, the Dolt repository and the target
    relational database, so far MySQL or Postgres server instance contain the same data. Tests creates, updates, and
    deletes.
    :param db_conn:
    :param db_table:
    :param get_db_target_writer:
    :param dolt_repo:
    :param dolt_table:
    :return:
    """
    target_writer = get_db_target_writer(db_conn, True)
    table_mapping = {str(dolt_table.name): str(db_table.name)}

    commits = list(dolt_repo.log().keys())
    commits_to_check = [commits[0], commits[1], commits[2], commits[3], commits[4]]
    commits_to_check.reverse()

    for commit in commits_to_check:
        table_reader = get_dolt_table_reader_diffs(commit)
        sync_from_dolt(get_dolt_source_reader(dolt_repo, table_reader), target_writer, table_mapping)
        db_data = get_data_for_comparison(db_conn)
        _, dolt_data = get_dolt_table_reader(commit)(str(dolt_table.name), dolt_repo)
        assert assert_rows_equal(db_data, list(dolt_data))
