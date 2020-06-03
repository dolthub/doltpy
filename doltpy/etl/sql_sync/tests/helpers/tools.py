from doltpy.etl.sql_sync.tests.helpers.data_helper import (TEST_TABLE_COLUMNS,
                                                           TEST_TABLE_METADATA,
                                                           TEST_DATA_INITIAL,
                                                           TEST_DATA_APPEND_SINGLE_ROW,
                                                           TEST_DATA_APPEND_MULTIPLE_ROWS,
                                                           TEST_DATA_UPDATE_SINGLE_ROW,
                                                           TEST_DATA_APPEND_MULTIPLE_ROWS_WITH_DELETE,
                                                           get_data_for_comparison,
                                                           assert_tuple_array_equality,
                                                           FIRST_UPDATE,
                                                           SECOND_UPDATE,
                                                           THIRD_UPDATE,
                                                           FOURTH_UPDATE,
                                                           get_expected_data)
from doltpy.etl.sql_sync.db_tools import write_to_table, drop_primary_keys
from doltpy.etl.sql_sync.dolt import (get_source_reader as get_dolt_source_reader,
                                      get_target_writer as get_dolt_target_writer,
                                      get_table_reader_diffs as get_dolt_table_reader_diffs,
                                      get_table_reader as get_dolt_table_reader)
from doltpy.etl.sql_sync.sync_tools import sync_to_dolt, sync_from_dolt
from typing import List


def validate_get_table_metadata(db_conn, db_table, table_metadata_builder):
    """
    Verify that get_table_metadata correctly constructs the metadata associated with the test table. We manually build
    that metadata in helpers/data_helper.py to verify this.
    :param db_conn:
    :param db_table:
    :param table_metadata_builder:
    :return:
    """
    result = table_metadata_builder(db_table, db_conn)
    expected_columns = sorted(TEST_TABLE_COLUMNS, key=lambda col: col.col_name)
    assert TEST_TABLE_METADATA.name == result.name
    assert len(expected_columns) == len(result.columns)
    assert all(left.col_name == right.col_name for left, right in zip(expected_columns, result.columns))


def validate_write_to_table(db_conn, db_table, table_metadata_builder, insert_query_builder):
    """
    Ensure that writes using our write wrapper correctly show up in a relational database server.
    :param db_conn:
    :param db_table:
    :param table_metadata_builder:
    :param insert_query_builder:
    :return:
    """
    table_metadata = table_metadata_builder(db_table, db_conn)

    def _write_and_diff_helper(data, update_num):
        write_to_table(db_conn, table_metadata, insert_query_builder, data)
        result = get_data_for_comparison(db_conn)
        _, expected_data = get_expected_data(update_num)
        assert_tuple_array_equality(expected_data, result)

    _write_and_diff_helper(TEST_DATA_INITIAL, FIRST_UPDATE)
    _write_and_diff_helper(TEST_DATA_APPEND_SINGLE_ROW, SECOND_UPDATE)
    _write_and_diff_helper(TEST_DATA_APPEND_MULTIPLE_ROWS, THIRD_UPDATE)
    _write_and_diff_helper(TEST_DATA_UPDATE_SINGLE_ROW, FOURTH_UPDATE)


def validate_drop_primary_keys(db_conn, db_table, table_metadata_builder, insert_query_builder):
    """
    Verify that dropping a primary key from using drop_primary_keys leaves a relational database in the required state.
    :param db_conn:
    :param db_table:
    :param table_metadata_builder:
    :param insert_query_builder:
    :return:
    """
    table_metadata = table_metadata_builder(db_table, db_conn)

    write_to_table(db_conn, table_metadata, insert_query_builder, TEST_DATA_APPEND_MULTIPLE_ROWS)
    pks_to_drop = [('Stefanos', 'Tsitsipas')]
    drop_primary_keys(db_conn, table_metadata, pks_to_drop)
    result = get_data_for_comparison(db_conn)
    assert_tuple_array_equality(TEST_DATA_APPEND_MULTIPLE_ROWS_WITH_DELETE, result)


def validate_dolt_as_target(db_conn,
                            db_table,
                            get_db_table_metadata,
                            get_db_source_reader,
                            get_db_table_reader,
                            get_db_insert_query,
                            dolt_repo,
                            dolt_table):
    """
    Validates syncing from a relational database, so far MySQL and Postgres, to Dolt. Work by making a series of writes
    to the relational database (running in a Docker container provided by a fixture), executing a sync, and then
    validating the HEAD of master of the Dolt repo has the expected values. It also validates that the Dolt history is
    correct after every write. Finally validates that deletes flow through to Dolt.
    :param db_conn:
    :param db_table:
    :param get_db_table_metadata:
    :param get_db_source_reader:
    :param get_db_table_reader:
    :param get_db_insert_query:
    :param dolt_repo:
    :param dolt_table:
    :return:
    """

    db_table_metadata = get_db_table_metadata(db_table, db_conn)

    def sync_to_dolt_helper():
        source_reader = get_db_source_reader(db_conn, get_db_table_reader())
        target_writer = get_dolt_target_writer(dolt_repo, commit=True)
        sync_to_dolt(source_reader, target_writer, {db_table: dolt_table})

    def assertion_helper(commit: str, expected_diff: List[tuple]):
        """
        Validates that both the HEAD of the current branch of the Dolt repo match MySQL, and that the diffs created by
        the write match what is expected.
        """
        _, dolt_data = get_dolt_table_reader(commit)(dolt_table, dolt_repo)
        db_table_metadata = get_db_table_metadata(db_table, db_conn)
        db_data = get_db_table_reader()(db_conn, db_table_metadata)
        assert_tuple_array_equality(list(dolt_data), db_data)

        _, dolt_diff_data = get_dolt_table_reader_diffs(commit)(dolt_table, dolt_repo)
        assert_tuple_array_equality(expected_diff, list(dolt_diff_data))

    update_sequence = [
        TEST_DATA_INITIAL,
        TEST_DATA_APPEND_MULTIPLE_ROWS,
        TEST_DATA_APPEND_SINGLE_ROW,
        TEST_DATA_UPDATE_SINGLE_ROW
    ]

    for update_data in update_sequence:
        write_to_table(db_conn, db_table_metadata, get_db_insert_query, update_data)
        sync_to_dolt_helper()
        latest_commit = list(dolt_repo.log().keys())[0]
        assertion_helper(latest_commit, update_data)

    delete_query = '''
            DELETE FROM
                {table_name}
            WHERE
                first_name = 'Novak'
        '''.format(table_name=db_table)
    cursor = db_conn.cursor()
    cursor.execute(delete_query)
    db_conn.commit()
    sync_to_dolt_helper()
    latest_commit = list(dolt_repo.log().keys())[0]
    _, dolt_data = get_dolt_table_reader(latest_commit)(dolt_table, dolt_repo)
    db_data = get_db_table_reader()(db_conn, db_table_metadata)
    assert_tuple_array_equality(list(dolt_data), db_data)
    dropped_pks, _ = get_dolt_table_reader_diffs(latest_commit)(dolt_table, dolt_repo)
    assert dropped_pks == [('Novak', 'Djokovic')]


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
    table_mapping = {dolt_table: db_table}

    commits = list(dolt_repo.log().keys())
    commits_to_check = [commits[0], commits[1], commits[2], commits[3], commits[4]]
    commits_to_check.reverse()

    for commit in commits_to_check:
        table_reader = get_dolt_table_reader_diffs(commit)
        sync_from_dolt(get_dolt_source_reader(dolt_repo, table_reader), target_writer, table_mapping)
        db_data = get_data_for_comparison(db_conn)
        _, dolt_data = get_dolt_table_reader(commit)(dolt_table, dolt_repo)
        assert assert_tuple_array_equality(db_data, list(dolt_data))