from doltpy.cli import Dolt
from retry import retry
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine
from doltpy.shared import SQL_LOG_FILE
from subprocess import Popen, STDOUT
import os
from doltpy.shared import get_logger
import sqlalchemy

logger = get_logger(__name__)

DEFAULT_HOST, DEFAULT_PORT = '127.0.0.1', 3306


class ServerConfig:
    def __init__(self,
                 config: str = None,
                 host: str = DEFAULT_HOST,
                 port: int = DEFAULT_PORT,
                 user: str = None,
                 password: str = None,
                 timeout: int = None,
                 readonly: bool = None,
                 loglevel: str = None,
                 multi_db_dir: str = None,
                 no_auto_commit: bool = None,
                 echo: bool = False):
        self.config = config
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.timeout = timeout
        self.readonly = readonly
        self.loglevel = loglevel
        self.multi_db_dir = multi_db_dir
        self.no_auto_commit = no_auto_commit
        self.echo = echo


class DoltSQLServerManager:

    def __init__(self, dolt: Dolt, server_config: ServerConfig):
        self.dolt = dolt
        self.server_config = server_config
        self.engine = self._get_engine()
        self.server = None

    def __enter__(self):
        self.start_server()

    def __exit__(self):
        self.stop_server()

    def _get_engine(self) -> Engine:
        """
        Get a connection to ths server process that this repo is running, raise an exception if it is not running.
        :param echo:
        :return:
        """
        database = self.dolt.repo_name()
        user = self.server_config.user
        host = self.server_config.host
        port = self.server_config.port

        logger.info('Creating engine for Dolt SQL Server instance running on {}:{}'.format(host, port))

        def inner():
            return create_engine('mysql+mysqlconnector://{user}@{host}:{port}/{database}'.format(user=user,
                                                                                                 host=host,
                                                                                                 port=port,
                                                                                                 database=database),
                                 echo=self.server_config.echo)

        return inner()

    def start_server(self):
        """
        Start a MySQL Server process on local host using the parameters to configure behavior. The parameters are
        self-explanatory, but the config is a way to provide them as a YAML file rather than as function
        arguments.
        :return:
        """

        def inner(server_args):
            if self.server is not None:
                logger.warning('Server already running')

            log_file = SQL_LOG_FILE or os.path.join(self.repo_dir(), 'mysql_server.log')

            proc = Popen(args=['dolt'] + server_args,
                         cwd=self.repo_dir(),
                         stdout=open(log_file, 'w'),
                         stderr=STDOUT)

            self.server = proc

        args = ['sql-server']

        if self.server_config.config:
            args.extend(['--config', self.server_config.config])
        else:
            if self.server_config.host:
                args.extend(['--host', self.server_config.host])
            if self.server_config.port:
                args.extend(['--port', str(self.server_config.port)])
            if self.server_config.user:
                args.extend(['--user', self.server_config.user])
            if self.server_config.password:
                args.extend(['--password', self.server_config.password])
            if self.server_config.timeout:
                args.extend(['--timeout', str(self.server_config.timeout)])
            if self.server_config.readonly:
                args.extend(['--readonly'])
            if self.server_config.loglevel:
                args.extend(['--loglevel', self.server_config.loglevel])
            if self.server_config.multi_db_dir:
                args.extend(['--multi-db-dir', self.server_config.multi_db_dir])
            if self.server_config.no_auto_commit:
                args.extend(['--no-auto-commit'])

        inner(args)

    @retry(delay=2, tries=10, exceptions=(
            sqlalchemy.exc.OperationalError,
            sqlalchemy.exc.DatabaseError,
            sqlalchemy.exc.InterfaceError,
    ))
    def verify_connection(self) -> bool:
        with self.engine.connect() as conn:
            conn.close()
            return True

    def stop_server(self):
        """
        Stop the MySQL Server process this repo is running.
        :return:
        """
        if self.server is None:
            logger.warning("Server is not running")
            return

        self.server.kill()
        self.server = None
