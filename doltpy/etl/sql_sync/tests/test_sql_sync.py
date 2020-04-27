from doltpy.etl.sql_sync import DoltSync
import logging
from typing import Mapping
from doltpy.core import Dolt
from doltpy.etl.sql_sync.tests.helpers.data_helper import get_data_for_comparison, assert_tuple_array_equality
from doltpy.etl.sql_sync.mysql import get_target_writer as get_mysql_target_writer
from doltpy.etl.sql_sync.dolt import get_source_reader, get_table_reader_diffs, get_table_reader

logger = logging.getLogger(__name__)


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

    _test_sync_at_commits(dolt_repo, mysql_conn, target_writer, table_mapping)


def _test_sync_at_commits(dolt_repo: Dolt,
                          db_conn,
                          target_writer,
                          table_mapping: Mapping[str, str]):
    commits = list(dolt_repo.get_commits())
    commits_to_check = [commits[0], commits[1], commits[2], commits[3], commits[4]]
    commits_to_check.reverse()

    for source_table, target_table in table_mapping.items():
        for commit in commits_to_check:
            _build_sync_at_commit(dolt_repo, commit, target_writer, table_mapping).sync()
            _assert_sync_success(dolt_repo, source_table, commit, db_conn)


def _build_sync_at_commit(dolt_repo: Dolt,
                          commit_ref: str,
                          target_writer,
                          table_mapping: Mapping[str, str]) -> DoltSync:
    table_reader = get_table_reader_diffs(commit_ref)
    return DoltSync(get_source_reader(dolt_repo, table_reader), target_writer, table_mapping)


def _assert_sync_success(dolt_repo: Dolt, table: str, commit_ref: str, conn):
    mysql_data = get_data_for_comparison(conn)
    _, dolt_data = get_table_reader(commit_ref)(table, dolt_repo)
    assert assert_tuple_array_equality(mysql_data, list(dolt_data))
