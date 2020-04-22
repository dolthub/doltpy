import pytest
from doltpy.core.tests.fixtures import init_empty_test_repo
from .fixtures.dolt import (repo_with_table as dolt_repo_with_table,
                            repo_with_initial_data as dolt_repo_with_initial_data)
from .fixtures.mysql import docker_compose_file, mysql_connection, mysql_with_table, mysql_with_initial_data
