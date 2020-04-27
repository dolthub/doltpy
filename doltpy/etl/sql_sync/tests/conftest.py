import pytest
from doltpy.core.tests.fixtures import init_empty_test_repo
from .fixtures.dolt import create_dolt_test_data_commits, repo_with_table
from .fixtures.mysql import mysql_connection, mysql_with_table, mysql_with_initial_data, mysql_service_def
from .fixtures.postgres import (postgres_connection,
                                postgres_service_def,
                                postgres_with_table,
                                postgres_with_initial_data)
from .fixtures.db_containers import docker_compose_file
