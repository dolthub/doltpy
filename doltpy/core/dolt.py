import pandas as pd
from typing import List, Tuple, Callable, Mapping
from subprocess import Popen, PIPE, STDOUT
from datetime import datetime
import logging
from retry import retry
import tempfile
import io
import os
from mysql import connector
from collections import OrderedDict

logger = logging.getLogger(__name__)

CREATE, FORCE_CREATE, REPLACE, UPDATE = 'create', 'force_create', 'replace', 'update'
IMPORT_MODES_TO_FLAGS = {CREATE: ['-c'],
                         FORCE_CREATE: ['-f', '-c'],
                         REPLACE: ['-r'],
                         UPDATE: ['-u']}


class DoltException(Exception):
    """
    A class representing a Dolt exception.
    """
    def __init__(self, exec_args, stdout, stderr, exitcode):
        self.exec_args = exec_args
        self.stdout = stdout
        self.stderr = stderr
        self.exitcode = exitcode


def init_new_repo(repo_dir: str) -> 'Dolt':
    """
    Creates a new repository in the directory specified, creating the directory if `create_dir` is passed, and returns
    a `Dolt` object representing the newly created repo.
    :return:
    """
    logger.info("Initializing a new repository in {}".format(repo_dir))
    args = ["dolt", "init"]

    _execute(args=args, cwd=repo_dir)

    return Dolt(repo_dir)


# TODO we need to sort out where stuff gets cloned and ensure that clone actually takes an argument correctly. The
# function should return a Dolt object tied to the repo that was just cloned
def clone_repo(repo_url: str, repo_dir: str) -> 'Dolt':
    """
    Clones a repository into the repository specified, currently only supports DoltHub as a remote.
    :return:
    """
    args = ["dolt", "clone", repo_url, repo_dir]

    _execute(args=args, cwd='.')

    return Dolt(repo_dir)


class DoltCommitSummary:
    """
    Represents metadata about a commit, including a ref, timestamp, and author, to make it easier to sort and present
    to the user.
    """
    def __init__(self, ref: str, ts: datetime, author: str):
        self.hash = ref
        self.ts = ts
        self.author = author

    def __str__(self):
        return '{}: {} @ {}'.format(self.hash, self.author, self.ts)


# For now Doltpy works by dispatching into the shell. We will change this in an upcoming release, but these functions
# wrap calls to Popen, the main Python API for launching a subprocess.
def _execute(args, cwd):
    proc = Popen(args=args, cwd=cwd, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()
    exitcode = proc.returncode

    if exitcode != 0:
        raise DoltException(args, out, err, exitcode)

    return out.decode('utf-8')


def _execute_restart_serve_if_needed(dlt, args):
    was_serving = False
    if dlt.server is not None:
        was_serving = True
        dlt.stop_server()

    _execute(args=args, cwd=dlt.repo_dir())

    if was_serving:
        dlt.start_server()


class Dolt(object):
    """
    This the top level object for interacting with a Dolt database. A Dolt database lives at a path in the file system
    and there should be a 1:1 mapping between Dolt objects and repositories you want to interact with. There two pieces
    of state information this object stores are the repo path and a connection to the MySQL Server (if it is running).

    In the docstrings for the various functions "this repository" or "this database" refers to Dolt repo that exists at
    the directory returned by `self.repo_dir()`. All functions on an instance of the class will error out if called
    on an instance that does not correspond to an actual Dolt repo.

    Note, it is not reccomended to recycle objects, as this could lead to peculiar results. For example if you have Dolt
    databases in `~/db1` and `~/db2` the following will be strange:
    ```
    >>> from doltpy.core import Dolt
    >>> db = Dolt('~/db1')
    >>> db.start_server()
    >>> db._repo_dir = '~/db2'
    ```
    In this case calls to db that use the SQL server, for example `pandas_read_sql(...)` will reference the Dolt repo
    in `~/db1`, and calls that use the CLI will reference `~/db2`.

    Instead simply create a separate objects for each Dolt database to avoid this confusion.
    """
    def __init__(self, repo_dir):
        """

        :param repo_dir:
        """
        self._repo_dir = repo_dir
        self.server = None

    def repo_dir(self) -> str:
        return self._repo_dir

    def config(self, is_global: bool, user_name: str, user_email: str):
        """
        Exposes a way to set the user name and email to be associated with commit messages. Can be either global, or
        local to this repo.
        :param is_global:
        :param user_name:
        :param user_email:
        :return:
        """
        args = ["dolt", "config", "add"]
        if is_global:
            args.append("--global")

        name_args = args
        email_args = args.copy()

        name_args.extend(["user.name", user_name])
        email_args.extend(["user.email", user_email])

        if is_global:
            _execute(args=name_args, cwd=None)
            _execute(args=email_args, cwd=None)

        else:
            _execute(args=name_args, cwd=self.repo_dir())
            _execute(args=email_args, cwd=self.repo_dir())

    def create_branch(self, branch_name: str, commit_ref: str = None):
        """
        Creates a new branch in this repo with the name branch_name. If commit_ref is None the ref is the HEAD of the
        currently checked out branch.
        :param branch_name:
        :param commit_ref:
        :return:
        """
        args = ["dolt", "branch", branch_name]

        if commit_ref is not None:
            args.append(commit_ref)

        _execute(args=args, cwd=self.repo_dir())

    def checkout(self, branch_name: str):
        """
        Check out the repo in `self.repo_dir()` at the specified branch.
        :param branch_name: the branch to checkout at
        :return:
        """
        assert branch_name in self.get_branch_list(), 'Cannot checkout of non-existent branch {}'.format(branch_name)
        args = ["dolt", "checkout", branch_name]
        _execute_restart_serve_if_needed(self, args)

    def start_server(self):
        """
        Start a MySQL Server instance for the Dolt repo in `self.repo_dir()` running on port 3306. Note this function is
        very alpha, as it doesn't yet support specifying the port.
        """
        if self.server is not None:
            logger.warning('Server already running')

        args = ['dolt', 'sql-server', '-t', '0', '--loglevel', 'info']
        proc = Popen(args=args,
                     cwd=self.repo_dir(),
                     stdout=open(os.path.join(self.repo_dir(), 'mysql_server.log'), 'w'),
                     stderr=STDOUT)
        self.server = proc

    def stop_server(self):
        """
        Stop the MySQL Server instance running on port 3306, returns an error if the server is not running at on that
        port.
        :return:
        """
        if self.server is None:
            logger.warning("Server is not running")
            return

        self.server.kill()
        self.server = None

    @retry(exceptions=connector.errors.DatabaseError, delay=2, tries=10)
    def get_connection(self, host: str = None):
        database = str(self.repo_dir()).split('/')[-1]
        host = host or '127.0.0.1'
        if self.server is None:
            raise Exception('Server is not running, run repo.start_server() on your instance of Dolt')
        return connector.connect(host=host, user='root', database=database, port=3306)

    def repo_is_clean(self):
        """
        Returns true if the repo is clean, which is to say the working set has no changes, and false otherwise. This
        is directly analogous to the Git concept of "clean".
        :return:
        """
        res = _execute(['dolt', 'status'], self.repo_dir())
        return 'clean' in str(res)

    def query_server(self, query: str, connection: connector.connection):
        """
        Execute the specified query against the MySQL Server instance running on port 3306.
        :param query: the query to execute
        :param connection: connection to use
        :return:
        """
        if self.server is None:
            raise Exception("never started.")

        cursor = connection.cursor()
        cursor.execute(query)

        return cursor

    def execute_sql_stmt(self, stmt: str):
        """
        Execute the specified query via the `dolt sql -q` command line interface. This function will be deprecated in
        an upcoming release as the MySQL Server supports all statements that can be executed via the client.
        :param stmt: the
        :return:
        """
        logger.info('Executing the following SQL statement via CLI:\n{}\n'.format(stmt))
        _execute(['dolt', 'sql', '-q', stmt], cwd=self.repo_dir())

    def pandas_read_sql(self, query: str, connection: connector.connection):
        """
        Execute a SQL statement against the MySQL Server running on port 3306 and return the result as a Pandas
        `DataFrame` object. This is a higher level version of `query_server` where the object returned is the cursor
        associated with query executed.
        :param query:
        :param connection:
        :return:
        """
        if self.server is None:
            raise Exception("never started.")

        return pd.read_sql(query, con=connection)

    def read_table(self, table_name: str, delimiter: str = ',') -> pd.DataFrame:
        """
        Reads the contents of a table and returns it as a Pandas `DataFrame`. Under the hood this uses export and the
        filesystem, in short order we are likley to replace this with use of the MySQL Server.
        :param table_name:
        :param delimiter:
        :return:
        """
        fp = tempfile.NamedTemporaryFile(suffix='.csv')
        _execute(['dolt', 'table', 'export', table_name, fp.name, '-f'], self.repo_dir())
        result = pd.read_csv(fp.name, delimiter=delimiter)
        return result

    def import_df(self,
                  table_name: str,
                  data: pd.DataFrame,
                  primary_keys: List[str],
                  import_mode: str = None):
        """
        Imports the given DataFrame object to the specified table, dropping records that are duplicates on primary key
        (in order, preserving the first record, something we might want to allow the user to sepcify), subject to
        given import mode. Import mode defaults to CREATE if the table does not exist, and UPDATE otherwise.
        :param table_name:
        :param data:
        :param primary_keys:
        :param import_mode:
        :return:
        """
        def writer(filepath: str):
            clean = data.dropna(subset=primary_keys)
            clean.to_csv(filepath, index=False)

        self._import_helper(table_name, writer, primary_keys, import_mode)

    def bulk_import(self,
                    table_name: str,
                    data: io.StringIO,
                    primary_keys: List[str],
                    import_mode: str = None) -> None:
        """
        This takes a file like object representing a CSV and imports it to the table specified. Note that you must
        specify the primary key, and the import mode. The import mode is one of the keys of IMPORT_MODES_TO_FLAGS.
        Choosing the wrong import mode will throw an error, for example `CREATE` on an existing table. Import mode
        defaults to CREATE if the table does not exist, and UPDATE otherwise.
        :param table_name:
        :param data:
        :param primary_keys:
        :param import_mode:
        :return:
        """
        def writer(filepath: str):
            with open(filepath, 'w') as f:
                f.writelines(data.readlines())

        self._import_helper(table_name, writer, primary_keys, import_mode)

    def _import_helper(self,
                       table_name: str,
                       write_import_file: Callable[[str], None],
                       primary_keys: List[str],
                       import_mode: str) -> None:
        import_modes = IMPORT_MODES_TO_FLAGS.keys()
        if import_mode is not None:
            assert import_mode in import_modes, 'update_mode must be one of: {}'.format(import_modes)
        else:
            if table_name in self.get_existing_tables():
                logger.info('No import mode specified, table exists, using "{}"'.format(UPDATE))
                import_mode = UPDATE
            else:
                logger.info('No import mode specified, table exists, using "{}"'.format(CREATE))
                import_mode = CREATE

        import_flags = IMPORT_MODES_TO_FLAGS[import_mode]
        logger.info('Importing to table {} in dolt directory located in {}, import mode {}'.format(table_name,
                                                                                                   self.repo_dir(),
                                                                                                   import_mode))
        fp = tempfile.NamedTemporaryFile(suffix='.csv')
        write_import_file(fp.name)
        args = ['dolt', 'table', 'import', table_name, '--pk={}'.format(','.join(primary_keys))] + import_flags
        _execute(args + [fp.name], self.repo_dir())

    def add_table_to_next_commit(self, *table_names: str):
        """
        Stage the tables specified in table_names to be committed.
        :param table_names:
        :return:
        """
        _execute_restart_serve_if_needed(self, ["dolt", "add"] + list(table_names))

    def commit(self, commit_message):
        """
        Create a commit from the current working set the HEAD of the checked out branch to the value of the commit hash.
        :param commit_message:
        :return:
        """
        _execute_restart_serve_if_needed(self, ["dolt", "commit", "-m", commit_message])

    def push(self, remote: str, branch: str):
        """
        Push to the remote specified. If either the branch or the remote do not exist then an `AssertionError` will be
        thrown.
        :param remote:
        :param branch:
        :return:
        """
        def _assertion_helper(name: str, required: str, existing: List[str]):
            assert required in existing, 'cannot push to {} that does not exist, {} not in {}'.format(name,
                                                                                                      required,
                                                                                                      existing)
        _assertion_helper('branch', branch, self.get_branch_list())
        _assertion_helper('remote', remote, self.get_remote_list())
        _execute_restart_serve_if_needed(self, ['dolt', 'push', remote, branch])

    def pull(self, remote: str = 'origin'):
        _execute(['dolt', 'pull', remote], self.repo_dir())

    def get_commits(self) -> Mapping[str, DoltCommitSummary]:
        """
        Returns a list of `DoltCommitSummary`, representing the list of commits on the currently checked out branch,
        ordered by the timestamp associated with the commit.
        :return:
        """
        output = _execute(['dolt', 'log'], self.repo_dir()).split('\n')
        current_commit, author, date = None, None, None
        result = OrderedDict()
        for line in output:
            if line.startswith('commit'):
                current_commit = line.split(' ')[1]
            elif line.startswith('Author'):
                author = line.split(':')[1].lstrip()
            elif line.startswith('Date'):
                date = datetime.strptime(line.split(':', maxsplit=1)[1].lstrip(), '%a %b %d %H:%M:%S %z %Y')
            elif current_commit is not None:
                assert current_commit is not None and date is not None and author is not None
                result[current_commit] = DoltCommitSummary(current_commit, date, author)
                current_commit = None
            else:
                pass

        return result

    def get_dirty_tables(self) -> Tuple[Mapping[str, bool], Mapping[str, bool]]:
        """
        Returns a tuple of maps, the first element is a map keyed on the names of newly created tables, and a second
        is keyed on modified tables, with the values being boolean flags to indicate whether changes have been stage for
        commit.
        :return:
        """
        new_tables, changes = {}, {}

        if not self.repo_is_clean():
            output = [line.lstrip() for line in _execute(['dolt', 'status'], self.repo_dir()).split('\n')]
            staged = False
            for line in output:
                if line.startswith('Changes to be committed'):
                    staged = True
                elif line.startswith('Changes not staged for commit'):
                    staged = False
                elif line.startswith('Untracked files'):
                    staged = False
                elif line.startswith('modified'):
                    changes[line.split(':')[1].lstrip()] = staged
                elif line.startswith('new table'):
                    new_tables[line.split(':')[1].lstrip()] = staged
                else:
                    pass

        return new_tables, changes

    def clean_local(self) -> None:
        """
        Wipes out the un-commited tables in the working set, useful for scripting "all or nothing" data imports.
        :return:
        """
        new_tables, changes = self.get_dirty_tables()

        for table in [table for table, is_staged in list(new_tables.items()) + list(changes.items()) if is_staged]:
            logger.info('Resetting table {}'.format(table))
            _execute(['dolt', 'reset', table], self.repo_dir())

        for table in new_tables.keys():
            logger.info('Removing newly created table {}'.format(table))
            _execute(['dolt', 'table', 'rm', table], self.repo_dir())

        for table in changes.keys():
            logger.info('Discarding local changes to table {}'.format(table))
            _execute(['dolt', 'checkout', table], self.repo_dir())

        assert self.repo_is_clean(), 'Something went wrong, repo is not clean'

    def get_existing_tables(self) -> List[str]:
        """
        Get the list of tables in the the working set.
        :return:
        """
        return [line.lstrip() for line in _execute(['dolt', 'ls'], self.repo_dir()).split('\n')[1:] if line]

    def get_last_commit_time(self) -> datetime:
        """
        Returns the time stamp associated with the ref corresponding to HEAD on the currently checked out branch.
        :return:
        """
        return max([commit.ts for commit in self.get_commits()])

    def get_branch_list(self) -> List[str]:
        """
        Returns a list of branches in the repository in directory returned by `self.repo_dir()`
        :return:
        """
        return [line.replace('*', '').lstrip().rstrip()
                for line in _execute(['dolt', 'branch'], self.repo_dir()).split('\n') if line]

    def get_remote_list(self) -> List[str]:
        """
        Returns a list of remotes that have been added to the repository corresponding to self.
        :return: list of remotes
        """
        return [line.rstrip() for line in _execute(['dolt', 'remote'], self._repo_dir).split('\n') if line]

    def get_current_branch(self) -> str:
        """
        Returns the currently checked out branch of the Dolt repository corresping to self.
        :return: the checked out branch
        """
        for line in _execute(['dolt', 'branch'], self._repo_dir).split('\n'):
            if line.lstrip().startswith('*'):
                return line.replace('*', '').lstrip().rstrip()

    def schema_import_create(self, table: str, pks: List[str], path: str):
        """
        Surfaces a simple version of the schema import tool the CLI exposes for creating tables.
        :param table:
        :param pks:
        :param path:
        :return:
        """
        args = ['dolt', 'schema',  'import', '-c', '--pks', ','.join(pks), table, path]
        return _execute(args, self.repo_dir())
