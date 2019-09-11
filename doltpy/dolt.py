import mysql.connector
import pandas as pd
import os
import uuid
from typing import List, Tuple, Callable, Mapping
from subprocess import Popen, PIPE, STDOUT
from datetime import datetime
import pyarrow
import pyarrow.csv as pacsv
from retry import retry


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


class DoltLoader:
    # Not clear what this should look like quite yet, but it should baiscally take a function that produces a commit
    # thus the function is paratrized by the unit that separates the work.
    pass


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
        if self.dir_exists:
            raise Exception(self.repo_dir + " .")

        os.makedirs(self.repo_dir)
        self.dir_exists = True

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

        # Need to replace this with Turbodbc
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

    def read_table(self, table_name: str, delimiter: str = ',') -> pyarrow.Table:
        # export table (hopefully soon we can do this with ODBC)
        path = '{}/{}_export_{}.csv'.format(self.repo_dir, table_name, str(uuid.uuid4()))
        _execute(['dolt', 'table', 'export', table_name, path], self.repo_dir)
        result = pacsv.read_csv(path, parse_options=pacsv.ParseOptions(delimiter=delimiter))
        os.remove(path)
        return result

    def create_derivded_table(self,
                              source_table: str,
                              target_table: str,
                              target_pk_cols: List[str],
                              transformer: Callable[[pd.DataFrame], pd.DataFrame]):
        self._transform_helper(source_table, target_table, target_pk_cols, transformer, create_target=True)

    def transform_to_existing_table(self,
                                    source_table: str,
                                    target_table: str,
                                    target_pk_cols: List[str],
                                    transformer: Callable[[pd.DataFrame], pd.DataFrame]):
        self._transform_helper(source_table, target_table, target_pk_cols, transformer, create_target=False)

    # TODO: for now this method needs the PK cols, though in theory it should not
    # TODO: there remains a question about what to do if the transformation drops records? Should they remain unchanged?
    def transform_table_inplace(self,
                                table: str,
                                pk_cols: List[str],
                                transformer: Callable[[pd.DataFrame], pd.DataFrame]):
        self._transform_helper(table, table, pk_cols, transformer, create_target=False)

    def _transform_helper(self,
                          source_table: str,
                          target_table: str,
                          target_pk_cols: List[str],
                          transformer: Callable[[pd.DataFrame], pd.DataFrame],
                          create_target: bool):
        existing = self.read_table(source_table).to_pandas()
        transformed = transformer(existing)
        assert all(col in transformed.columns for col in target_pk_cols), 'Result must have pk cols specified'
        self.import_df(target_table, transformed, target_pk_cols, create=create_target)

    def import_df(self,
                  table_name: str,
                  df: pd.DataFrame,
                  primary_keys: List[str],
                  create: bool = False,
                  force: bool = False):
        cur_dur = os.getcwd()
        if cur_dur != self.repo_dir:
            os.chdir(self.repo_dir)

        print('Importing {} rows to table {} in dolt directory located in {}'.format(len(df), table_name, os.getcwd()))
        base_args = ['dolt', 'table', 'import', table_name, '--pk={}'.format(','.join(primary_keys))]

        if create:
            create_switches = ['-c', '-f'] if force else ['-c']
            print('Creating table with force set to {}'.format(force))
            args = base_args + create_switches
        else:
            print('Updating existing table')
            args = base_args + ['-u']

        temp_location = '{}.csv'.format(str(uuid.uuid4()))

        # Primary key columns cannot be null
        clean = df.dropna(subset=primary_keys)
        print('Dropped {} records with null values in the primary key columsn {}'.format(len(df) - len(clean),
                                                                                         primary_keys))
        clean.to_csv(temp_location, index=False)

        try:
            _execute(args + [temp_location], self.repo_dir)
        except Exception as e:
            raise e
        finally:
            os.remove(temp_location)
            os.chdir(cur_dur)

    def put_row(self, table_name, row_data):
        args = ["dolt", "table", "put-row", table_name]
        key_value_pairs = [str(k) + ':' + str(v) for k, v in row_data.items()]
        args.extend(key_value_pairs)

        _execute_restart_serve_if_needed(self, args)

    def add_table_to_next_commit(self, table_name):
        args = ["dolt", "add", table_name]
        _execute_restart_serve_if_needed(self, args)

    def commit(self, commit_message):
        args = ["dolt", "commit", "-m", commit_message]
        _execute_restart_serve_if_needed(self, args)

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
            print('Resetting table {}'.format(table))
            _execute(['dolt', 'reset', table], self.repo_dir)

        for table in new_tables.keys():
            print('Removing newly created table {}'.format(table))
            _execute(['dolt', 'table', 'rm', table], self.repo_dir)

        for table in changes.keys():
            print('Discarding local changes to table {}'.format(table))
            _execute(['dolt', 'checkout', table], self.repo_dir)

        assert self.repo_is_clean(), 'Something went wrong, repo is not clean'

    def get_exisitng_tabels(self) -> List[str]:
        return [line.lstrip() for line in _execute(['dolt', 'ls'], self.repo_dir).split('\n')[1:] if line]

    def get_last_commit_time(self):
        return max([commit.ts for commit in self.get_commits()])

    def get_branch_list(self):
        return [line.replace('*', '') for line in _execute(['dolt', 'ls'], self.repo_dir).split('\n')]

    def get_current_branch(self):
        for line in _execute(['dolt', 'ls'], self.repo_dir).split('\n'):
            if line.startswith('*'):
                return line
