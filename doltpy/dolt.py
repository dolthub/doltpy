import mysql.connector
import pandas as pd
import os
from typing import List, Tuple, Callable, Mapping
from subprocess import Popen, PIPE, STDOUT
from datetime import datetime
import logging
import pyarrow.csv as pacsv
from retry import retry
import tempfile
import io

logger = logging.getLogger(__name__)

CREATE, FORCE_CREATE, REPLACE, UPDATE = 'create', 'force_create', 'replace', 'update'
IMPORT_MODES_TO_FLAGS = {CREATE: ['-c'],
                         FORCE_CREATE: ['-f', '-c'],
                         REPLACE: ['-r'],
                         UPDATE: ['-u']}


class DoltException(Exception):
    def __init__(self, exec_args, stdout, stderr, exitcode):
        self.exec_args = exec_args
        self.stdout = stdout
        self.stderr = stderr
        self.exitcode = exitcode


class DoltCommitSummary:
    def __init__(self, hash: str, ts: datetime, author: str):
        self.hash = hash
        self.ts = ts
        self.author = author

    def __str__(self):
        return '{}: {} @ {}'.format(self.hash, self.author, self.ts)


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

    _execute(args=args, cwd=dlt.repo_dir)

    if was_serving:
        dlt.start_server()


class Dolt(object):
    def __init__(self, repo_dir):
        self.repo_dir = repo_dir
        self.dir_exists = os.path.exists(repo_dir)
        self.server = None
        self.cnx = None

    def config(self, is_global, user_name, user_email):
        args = ["dolt", "config", "add"]
        if is_global:
            args.append("--global")
        elif not self.dir_exists:
            raise Exception("{} does not exist.  Cannot configure local options without a valid directory.".format(self.repo_dir))

        name_args = args
        email_args = args.copy()

        name_args.append(["user.name", user_name])
        email_args.append(["user.email", user_email])

        if is_global:
            _execute(args=name_args, cwd=None)
            _execute(args=email_args, cwd=None)

        else:
            _execute(args=name_args, cwd=self.repo_dir)
            _execute(args=email_args, cwd=self.repo_dir)

    def clone(self, repo):
        # if self.dir_exists:
        #     raise Exception(self.repo_dir + " .")
        #
        # os.makedirs(self.repo_dir)
        # self.dir_exists = True

        args = ["dolt", "clone", repo, "./"]

        _execute(args=args, cwd=self.repo_dir)

    def init_new_repo(self):
        args = ["dolt", "init"]

        _execute(args=args, cwd=self.repo_dir)

    def create_branch(self, branch_name, commit_ref=None):
        args = ["dolt", "branch", branch_name]

        if commit_ref is not None:
            args.append(commit_ref)

        _execute(args=args, cwd=self.repo_dir)

    def checkout(self, branch_name):
        assert branch_name in self.get_branch_list(), 'Cannot checkout of non-existent branch {}'.format(branch_name)
        args = ["dolt", "checkout", branch_name]
        _execute_restart_serve_if_needed(self, args)

    def start_server(self):
        if self.server is not None:
            raise Exception("already running")

        args = ['dolt', 'sql-server', '-t', '0']
        proc = Popen(args=args, cwd=self.repo_dir, stdout=PIPE, stderr=STDOUT)

        # make sure the thread has started, this is a bit hacky
        @retry(exceptions=Exception, backoff=2)
        def get_connection():
            return mysql.connector.connect(user='root', host='127.0.0.1', port=3306, database='dolt')

        cnx = get_connection()

        self.server = proc
        self.cnx = cnx

    def repo_is_clean(self):
        res = _execute(['dolt', 'status'], self.repo_dir)
        return 'clean' in str(res)

    def stop_server(self):
        if self.server is None:
            raise Exception("never started.")

        self.cnx.close()
        self.cnx = None

        self.server.kill()
        self.server = None

    def query_server(self, query):
        if self.server is None:
            raise Exception("never started.")

        cursor = self.cnx.cursor()
        cursor.execute(query)

        return cursor

    def pandas_read_sql(self, query):
        if self.server is None:
            raise Exception("never started.")

        return pd.read_sql(query, con=self.cnx)

    def read_table(self, table_name: str, delimiter: str = ',') -> pd.DataFrame:
        fp = tempfile.NamedTemporaryFile(suffix='.csv')
        _execute(['dolt', 'table', 'export', table_name, fp.name, '-f'], self.repo_dir)
        result = pacsv.read_csv(fp.name, parse_options=pacsv.ParseOptions(delimiter=delimiter))
        return result.to_pandas()

    def import_df(self,
                  table_name: str,
                  data: pd.DataFrame,
                  primary_keys: List[str],
                  import_mode: str = CREATE):
        """
        Imports the given DataFrame object to the specified table, dropping records that are duplicates on primary key
        (in order, preserving the first record, something we might want to allow the user to sepcify), subject to
        given import mode.
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
                    import_mode: str):
        """
        This takes a file-handle like object and produces a
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
                       import_mode: str):
        import_modes = IMPORT_MODES_TO_FLAGS.keys()
        assert import_mode in import_modes, 'update_mode must be one of: {}'.format(import_modes)
        import_flags = IMPORT_MODES_TO_FLAGS[import_mode]

        logger.info('Importing to table {} in dolt directory located in {}, import mode {}'.format(table_name,
                                                                                                    self.repo_dir,
                                                                                                    import_mode))
        fp = tempfile.NamedTemporaryFile(suffix='.csv')
        write_import_file(fp.name)
        args = ['dolt', 'table', 'import', table_name, '--pk={}'.format(','.join(primary_keys))] + import_flags
        _execute(args + [fp.name], self.repo_dir)

    def put_row(self, table_name, row_data):
        args = ["dolt", "table", "put-row", table_name]
        key_value_pairs = [str(k) + ':' + str(v) for k, v in row_data.items()]
        args.extend(key_value_pairs)

        _execute_restart_serve_if_needed(self, args)

    def add_table_to_next_commit(self, table_name):
        _execute_restart_serve_if_needed(self, ["dolt", "add", table_name])

    def commit(self, commit_message):
        _execute_restart_serve_if_needed(self, ["dolt", "commit", "-m", commit_message])

    def push(self, remote: str, branch: str):
        def _assertion_helper(name: str, required: str, existing: List[str]):
            assert required in existing, 'cannot push to {} that does not exist, {} not in {}'.format(name,
                                                                                                      required,
                                                                                                      existing)
        _assertion_helper('branch', branch, self.get_branch_list())
        _assertion_helper('remote', remote, self.get_remote_list())
        _execute_restart_serve_if_needed(self, ['dolt', 'push', remote, branch])

    def get_commits(self) -> List[DoltCommitSummary]:
        output = _execute(['dolt', 'log'], self.repo_dir).split('\n')
        current_commit, author, date = None, None, None
        for line in output:
            if line.startswith('commit'):
                current_commit = line.split(' ')[1]
            elif line.startswith('Author'):
                author = line.split(':')[1].lstrip()
            elif line.startswith('Date'):
                date = datetime.strptime(line.split(':', maxsplit=1)[1].lstrip(), '%a %b %d %H:%M:%S %z %Y')
            elif current_commit is not None:
                assert current_commit is not None and date is not None and author is not None
                yield DoltCommitSummary(current_commit, date, author)
                current_commit = None
            else:
                pass

    def get_dirty_tables(self) -> Tuple[Mapping[str, bool], Mapping[str, bool]]:
        new_tables, changes = {}, {}

        if not self.repo_is_clean():
            output = [line.lstrip() for line in _execute(['dolt', 'status'], self.repo_dir).split('\n')]
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

    def clean_local(self):
        new_tables, changes = self.get_dirty_tables()

        for table in [table for table, is_staged in list(new_tables.items()) + list(changes.items()) if is_staged]:
            logger.info('Resetting table {}'.format(table))
            _execute(['dolt', 'reset', table], self.repo_dir)

        for table in new_tables.keys():
            logger.info('Removing newly created table {}'.format(table))
            _execute(['dolt', 'table', 'rm', table], self.repo_dir)

        for table in changes.keys():
            logger.info('Discarding local changes to table {}'.format(table))
            _execute(['dolt', 'checkout', table], self.repo_dir)

        assert self.repo_is_clean(), 'Something went wrong, repo is not clean'

    def get_existing_tables(self) -> List[str]:
        return [line.lstrip() for line in _execute(['dolt', 'ls'], self.repo_dir).split('\n')[1:] if line]

    def get_last_commit_time(self):
        return max([commit.ts for commit in self.get_commits()])

    def get_branch_list(self):
        return [line.replace('*', '').lstrip().rstrip()
                for line in _execute(['dolt', 'branch'], self.repo_dir).split('\n') if line]

    def get_remote_list(self):
        return [line.rstrip() for line in _execute(['dolt', 'remote'], self.repo_dir).split('\n') if line]

    def get_current_branch(self):
        for line in _execute(['dolt', 'branch'], self.repo_dir).split('\n'):
            if line.lstrip().startswith('*'):
                return line.replace('*', '').lstrip().rstrip()
