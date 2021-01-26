from .sql import (DEFAULT_HOST,
                  DEFAULT_PORT,
                  DoltSQLContext,
                  DoltSQLServerContext,
                  DoltSQLEngineContext,
                  ServerConfig,
                  DoltCommit)
from doltpy.shared import register_cleanup

register_cleanup()
