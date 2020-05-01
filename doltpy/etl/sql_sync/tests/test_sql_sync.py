from doltpy.etl.sql_sync import sync_to_dolt, sync_from_dolt
import logging
from typing import List
from doltpy.etl.sql_sync.tests.helpers.data_helper import (get_data_for_comparison,
                                                           assert_tuple_array_equality,
                                                           TEST_DATA_INITIAL,
                                                           TEST_DATA_APPEND_MULTIPLE_ROWS,
                                                           TEST_DATA_APPEND_SINGLE_ROW,
                                                           TEST_DATA_UPDATE_SINGLE_ROW)
from doltpy.etl.sql_sync.mysql import (get_target_writer as get_mysql_target_writer,
                                       write_to_table as write_to_mysql_table,
                                       get_table_metadata,
                                       get_source_reader as get_mysql_source_reader,
                                       get_table_reader as get_mysql_table_reader)
from doltpy.etl.sql_sync.dolt import (get_source_reader as get_dolt_source_reader,
                                      get_target_writer as get_dolt_target_writer,
                                      get_table_reader_diffs as get_dolt_table_reader_diffs,
                                      get_table_reader as get_dolt_table_reader)


logger = logging.getLogger(__name__)


# TODO this needs to include record deletion to make sure we are capturing deletes
def test_dolt_to_mysql(mysql_with_table, create_dolt_test_data_commits):
    """
    Verifies that given a Dolt repository that has a a series of updates applied to it (defined in the fixture
    create_dolt_test_data_commits) that after syncing at each of the commits, the Dolt repository and the target
    MySQL server instance contain the same data. Tests creates, updates, and deletes.
    :param mysql_with_table:
    :param create_dolt_test_data_commits:
    :return:
    """
    mysql_conn, mysql_table = mysql_with_table
    dolt_repo, dolt_table = create_dolt_test_data_commits
    target_writer = get_mysql_target_writer(mysql_conn, True)
    table_mapping = {dolt_table: mysql_table}

    commits = list(dolt_repo.get_commits().keys())
    commits_to_check = [commits[0], commits[1], commits[2], commits[3], commits[4]]
    commits_to_check.reverse()

    for commit in commits_to_check:
        table_reader = get_dolt_table_reader_diffs(commit)
        sync_from_dolt(get_dolt_source_reader(dolt_repo, table_reader), target_writer, table_mapping)
        mysql_data = get_data_for_comparison(mysql_conn)
        _, dolt_data = get_dolt_table_reader(commit)(dolt_table, dolt_repo)
        assert assert_tuple_array_equality(mysql_data, list(dolt_data))


def test_mysql_to_dolt(mysql_with_table, repo_with_table):
    mysql_conn, mysql_table = mysql_with_table
    dolt_repo, dolt_table = repo_with_table

    mysql_table_metadata = get_table_metadata(mysql_table, mysql_conn)

    def sync_to_dolt_helper():
        source_reader = get_mysql_source_reader(mysql_conn, get_mysql_table_reader())
        target_writer = get_dolt_target_writer(dolt_repo, commit=True)
        sync_to_dolt(source_reader, target_writer, {mysql_table: dolt_table})

    def assertion_helper(commit: str, expected_diff: List[tuple]):
        """
        Validates that both the HEAD of the current branch of the Dolt repo match MySQL, and that the diffs created by
        the write match what is expected.
        """
        _, dolt_data = get_dolt_table_reader(commit)(dolt_table, dolt_repo)
        mysql_data = get_mysql_table_reader()(mysql_table, mysql_conn)
        assert_tuple_array_equality(list(dolt_data), mysql_data)

        _, dolt_diff_data = get_dolt_table_reader_diffs(commit)(dolt_table, dolt_repo)
        assert_tuple_array_equality(expected_diff, list(dolt_diff_data))

    update_sequence = [
        TEST_DATA_INITIAL,
        TEST_DATA_APPEND_MULTIPLE_ROWS,
        TEST_DATA_APPEND_SINGLE_ROW,
        TEST_DATA_UPDATE_SINGLE_ROW
    ]

    for update_data in update_sequence:
        write_to_mysql_table(mysql_table_metadata, mysql_conn, update_data)
        sync_to_dolt_helper()
        latest_commit = list(dolt_repo.get_commits().keys())[0]
        assertion_helper(latest_commit, update_data)

    delete_query = '''
        DELETE FROM
            {table_name}
        WHERE
            first_name = 'Novak'
    '''.format(table_name=mysql_table)
    cursor = mysql_conn.cursor()
    cursor.execute(delete_query)
    mysql_conn.commit()
    sync_to_dolt_helper()
    latest_commit = list(dolt_repo.get_commits().keys())[0]
    _, dolt_data = get_dolt_table_reader(latest_commit)(dolt_table, dolt_repo)
    mysql_data = get_mysql_table_reader()(mysql_table, mysql_conn)
    assert_tuple_array_equality(list(dolt_data), mysql_data)
    dropped_pks, _ = get_dolt_table_reader_diffs(latest_commit)(dolt_table, dolt_repo)
    assert dropped_pks == [('Novak', 'Djokovic')]
