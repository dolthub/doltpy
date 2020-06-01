from typing import List, Union, Mapping, Tuple
from datetime import datetime
from subprocess import Popen, PIPE, STDOUT
import os
import logging
from collections import OrderedDict
from retry import retry
from mysql import connector

logger = logging.getLogger(__name__)


class DoltException(Exception):
    """
    A class representing a Dolt exception.
    """
    def __init__(self, exec_args, stdout, stderr, exitcode):
        self.exec_args = exec_args
        self.stdout = stdout
        self.stderr = stderr
        self.exitcode = exitcode


def _execute(args: List[str], cwd: str):
    _args = ['dolt'] + args
    proc = Popen(args=_args, cwd=cwd, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()
    exitcode = proc.returncode

    if exitcode != 0:
        raise DoltException(_args, out, err, exitcode)

    return out.decode('utf-8')


class DoltStatus:

    def __init__(self, is_clean: bool, modified_tables: Mapping[str, bool], added_tables: Mapping[str, bool]):
        self.is_clean = is_clean
        self.modified_tables = modified_tables
        self.added_tables = added_tables


class DoltTable:

    def __init__(self, name: str, table_hash: str = None, rows: int = None, system: bool = False):
        self.name = name
        self.table_hash = table_hash
        self.rows = rows
        self.system = system


class DoltCommit:
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


class DoltKeyPair:

    def __init__(self, public_key: str, key_id: str, active: bool):
        self.public_key = public_key
        self.key_id = key_id
        self.active = active


class DoltBranch:

    def __init__(self, name: str, commit_id: str):
        self.name = name
        self.commit_id = commit_id


class DoltRemote:

    def __init__(self, name: str, url: str):
        self.name = name
        self.url = url


class Dolt:
    """
    This class wraps the Dolt command line interface, mimicking functionality exactly to the extent that is possible.
    Some commands simply do not translate to Python, such as `dolt sql` (with no arguments) since that command
    launches an interactive shell.
    """

    def __init__(self, repo_dir: str):
        self._repo_dir = repo_dir
        self.server = None

    def repo_dir(self):
        return self._repo_dir

    def execute(self, args: List[str], print_output: bool = True) -> List[str]:
        output = _execute(args, self.repo_dir())

        if print_output:# TODO configure outpu
            print(output)

        return output.split('\n')

    @staticmethod
    def init(repo_dir: str = None) -> 'Dolt':
        """
        Creates a new repository in the directory specified, creating the directory if `create_dir` is passed, and returns
        a `Dolt` object representing the newly created repo.
        :return:
        """
        if not repo_dir:
            repo_dir = os.getcwd()

        if os.path.exists(repo_dir):
            logger.info('Dolt repo in existing dir {}'.format(repo_dir))
        else:
            try:
                logger.info('Creating directory {}'.format(repo_dir))
            except Exception as e:
                raise e

        output = _execute(['init'], cwd=repo_dir)
        print(output)
        return Dolt(repo_dir)

    def status(self) -> DoltStatus:
        new_tables, changes = {}, {}

        output = self.execute(['status'], print_output=False)

        if 'clean' in str('\n'.join(output)):
            return DoltStatus(True, changes, new_tables)
        else:
            staged = False
            for line in output:
                _line = line.lstrip()
                if _line.startswith('Changes to be committed'):
                    staged = True
                elif _line.startswith('Changes not staged for commit'):
                    staged = False
                elif _line.startswith('Untracked files'):
                    staged = False
                elif _line.startswith('modified'):
                    changes[_line.split(':')[1].lstrip()] = staged
                elif _line.startswith('new table'):
                    new_tables[_line.split(':')[1].lstrip()] = staged
                else:
                    pass

        return DoltStatus(False, changes, new_tables)

    def add(self, table_or_tables: Union[str, List[str]]):
        if type(table_or_tables) == str:
            to_add = [table_or_tables]
        else:
            to_add = table_or_tables
        self.execute(["add"] + to_add)
        return self.status()

    def reset(self, table_or_tables: Union[str, List[str]], hard: bool = False, soft: bool = False):
        if type(table_or_tables) == str:
            to_reset = [table_or_tables]
        else:
            to_reset = table_or_tables

        args = ['reset']

        assert not(hard and soft), 'Cannot reset hard and soft'

        if hard:
            args.append('--hard')
        if soft:
            args.append('--soft')

        self.execute(args + to_reset)

    def commit(self, message: str = None, allow_empty: bool = False, date: datetime = None):
        args = ['commit', '-m', message]

        if allow_empty:
            args.append('--allow-empty')

        if date:
            # TODO format properly
            args.extend(['--date', str(date)])

        self.execute(args)

    def sql(self,
            query: str = None,
            result_format: str = None,
            execute: str = False,
            save: str = None,
            message: str = None,
            list_saved: bool = False,
            batch: bool = False,
            multi_db_dir: str = None):
        args = ['sql']

        if list_saved:
            assert not any([query, result_format, save, message, batch, multi_db_dir])
            args.append('--list-saved')
            self.execute(args)

        if execute:
            assert not any([query, save, message, list_saved, batch, multi_db_dir])
            args.extend(['--execute', execute])

        if multi_db_dir:
            args.extend(['--multi-db-dir', multi_db_dir])

        if batch:
            args.append('--batch')

        if save:
            args.extend(['--save', save])
            if message:
                args.extend(['--message', message])

        args.extend(['--query', query])
        self.execute(args)

    def sql_server(self,
                   config: str = None,
                   host: str = None,
                   port: str = None,
                   user: str = None,
                   password: str = None,
                   timeout: int = None,
                   readonly: bool = False,
                   loglevel: str = 'info',
                   multi_db_dir: str = None,
                   no_auto_commit: str = None):
        def start_server(server_args):
            if self.server is not None:
                logger.warning('Server already running')

            proc = Popen(args=['dolt'] + server_args,
                         cwd=self.repo_dir(),
                         stdout=open(os.path.join(self.repo_dir(), 'mysql_server.log'), 'w'),
                         stderr=STDOUT)
            self.server = proc

        args = ['sql-server']

        if config:
            args.extend(['--config', config])
        else:
            if host:
                args.extend(['--host', host])
            if port:
                args.extend(['--port', port])
            if user:
                args.extend(['--user', user])
            if password:
                args.extend(['--password', password])
            if timeout:
                args.extend(['--timeout', timeout])
            if readonly:
                args.extend(['--readonly'])
            if loglevel:
                args.extend(['--loglevel', loglevel])
            if multi_db_dir:
                args.extend(['--multi-db-dir', multi_db_dir])
            if no_auto_commit:
                args.extend(['--no-auto-commit', no_auto_commit])

        start_server(args)

    @retry(exceptions=connector.errors.DatabaseError, delay=2, tries=10)
    def get_connection(self, host: str = None):
        database = str(self.repo_dir()).split('/')[-1]
        host = host or '127.0.0.1'
        if self.server is None:
            raise Exception('Server is not running, run repo.start_server() on your instance of Dolt')
        return connector.connect(host=host, user='root', database=database, port=3306)

    def sql_server_stop(self):
        if self.server is None:
            logger.warning("Server is not running")
            return

        self.server.kill()
        self.server = None

    def log(self, number: int = None, commit: str = None) -> OrderedDict:
        args = ['log']

        if number:
            args.extend(['--number', number])
        if commit:
            raise NotImplementedError()

        output = self.execute(args, print_output=False)
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
                result[current_commit] = DoltCommit(current_commit, date, author)
                current_commit = None
            else:
                pass

        return result

    # returns a table-like structure
    def diff(self,
             commit: str = None,
             other_commit: str = None,
             table_or_tables: Union[str, List[str]] = None,
             data: bool = False,
             schema: bool = False, # can we even support this?
             summary: bool = False,
             sql: bool = False,
             where: str = None,
             limit: int = None):
        switch_count = [el for el in [data, schema, summary] if el]
        assert len(switch_count) <= 1, 'At most one of delete, copy, move can be set to True'

        if type(table_or_tables) == str:
            tables = [table_or_tables]
        else:
            tables = table_or_tables

        args = ['diff']

        if data:
            if where:
                args.extend(['--where', where])
            if limit:
                args.extend(['--limit', limit])

        if summary:
            args.append('--summary')

        if schema:
            args.extend('--schema')

        if sql:
            args.append('--sql')

        if commit:
            args.append(commit)
        if other_commit:
            args.append(other_commit)

        if tables:
            args.append(' '.join(tables))

        self.execute(args)

    def blame(self, table_name: str, rev: str = None):
        args = ['blame']

        if rev:
            args.append(rev)

        args.append(table_name)
        self.execute(args)

    def branch(self,
               branch_name: str = None,
               start_point: str = None,
               new_branch: str = None,
               force: bool = False,
               delete: bool = False,
               copy: bool = False,
               move: bool = False):
        switch_count = [el for el in [delete, copy, move] if el]
        assert len(switch_count) <= 1, 'At most one of delete, copy, move can be set to True'

        if not any([branch_name, delete, copy, move]):
            assert not force, 'force is not valid without providing a new branch name, or copy, move, or delete being true'
            return self._get_branches()

        args = ['branch']
        if force:
            args.append('--force')

        if branch_name and not(delete and copy and move):
            args.append(branch_name)
            if start_point:
                args.append(start_point)
            _execute(args, self.repo_dir())
            return self._get_branches()

        if copy:
            assert new_branch, 'must provide new_branch when copying a branch'
            args.append('--copy')
            if branch_name:
                args.append(branch_name)
            args.extend(new_branch)
            self.execute(args)

        if delete:
            assert branch_name, 'must provide branch_name when deleting'
            args.extend(['--delete', branch_name])
            self.execute(args)

        if move:
            assert new_branch, 'must provide new_branch when moving a branch'
            args.append('--move')
            if branch_name:
                args.append(branch_name)
            args.extend(new_branch)
            self.execute(args)

        if branch_name:
            args.extend(branch_name)
            if start_point:
                args.append(start_point)
            self.execute(args)

        return self._get_branches()

    def _get_branches(self) -> Tuple[DoltBranch, List[DoltBranch]]:
        args = ['branch', '--list', '--verbose']
        output = self.execute(args)
        branches, active_branch = [], None
        for line in output:
            if not line:
                break
            elif line.startswith('*'):
                split = line.lstrip()[1:].split()
                branch, commit = split[0], split[1]
                active_branch = DoltBranch(branch, commit)
                branches.append(active_branch)
            else:
                split = line.lstrip().split()
                branch, commit = split[0], split[1]
                branches.append(DoltBranch(branch, commit))

        return active_branch, branches

    def checkout(self,
                 branch: str = None,
                 table_or_tables: Union[str, List[str]] = None,
                 checkout_branch: bool = False,
                 start_point: str = None):
        args = ['checkout']

        if type(table_or_tables) == str:
            tables = [table_or_tables]
        else:
            tables = table_or_tables

        if branch:
            assert not table_or_tables, 'No table_or_tables '
            if checkout_branch:
                args.append('-b')
                if start_point:
                    args.append(start_point)
            args.append(branch)

        if tables:
            assert not branch, 'Passing a branch not compatible with tables'
            args.append(' '.join(tables))

        self.execute(args)

    # TODO
    # esoteric options to add
    def remote(self, add: bool = False, name: str = None, url: str = None, remove: bool = None):
        args = ['remote', '--verbose']

        if not(add or remove):
            output = self.execute(args, print_output=False)

            remotes = []
            for line in output:
                if not line:
                    break

                split = line.lstrip().split()
                remotes.append(DoltRemote(split[0], split[1]))

            return remotes

        if remove:
            assert not add, 'add and remove are not comptaibe '
            assert name, 'Must provide the name of a remote to move'
            args.extend(['remove', name])

        if add:
            assert name and url, 'Must provide name and url to add'
            args.extend(['add', name, url])

        self.execute(args)

    def push(self, remote: str, refspec: str = None, set_upstream: str = None, force: bool = False):
        args = ['push']

        if set_upstream:
            args.append('--set-upstream')

        if force:
            args.append('--force')

        args.append(remote)
        if refspec:
            args.append(refspec)

        # just print the output
        output = _execute(args, self.repo_dir()).split('\n')
        self._print_output(output)

    def pull(self, remote: str):
        args = ['pull', remote]

        output = _execute(args, self.repo_dir()).split('\n')

        self._print_output(output)

    def fetch(self, remote: str = 'origin', refspec_or_refspecs: Union[str, List[str]] = None, force: bool = False):
        args = ['fetch']

        if type(refspec_or_refspecs) == str:
            refspecs = [refspec_or_refspecs]
        else:
            refspecs = refspec_or_refspecs

        if force:
            args.append('--force')
        if remote:
            args.append(remote)
        if refspec_or_refspecs:
            args.extend(refspecs)

        output = _execute(args, self.repo_dir()).split('\n')

        self._print_output(output)

    @staticmethod
    def clone(remote_url: str, new_dir: str = None, remote: str = None, branch: str = None):
        """
        Clones a repository into the repository specified, currently only supports DoltHub as a remote.
        :return:
        """
        args = ["dolt", "clone", remote_url]

        if remote:
            args.extend(['--remote', remote])

        if branch:
            args.extend(['--branch', branch])

        if not new_dir:
            split = remote_url.split('/')
            new_dir = os.path.join(os.getcwd(), split[-1])

        if new_dir:
            args.append(new_dir)

        _execute(args, cwd=new_dir)

        return Dolt(new_dir)

    def creds_new(self) -> bool:
        args = ['creds', 'new']

        output = _execute(args, self.repo_dir())

        if len(output) == 2:
            for out in output:
                logger.info(out)
        else:
            raise ValueError('Unexpected output: \n{}'.format('\n'.join(output)))

        return True

    def creds_rm(self, public_key: str) -> bool:
        args = ['creds', 'rm', public_key]

        output = _execute(args, self.repo_dir())

        if output[0].startswith('failed'):
            logger.error(output[0])
            raise DoltException('Tried to remove non-existent creds')

        return True

    def creds_ls(self) -> List[DoltKeyPair]:
        args = ['creds', 'ls', '--verbose']

        output = _execute(args, self.repo_dir())

        creds = []
        for line in output:
            if line.startswith('*'):
                active = True
                split = line[1:].lstrip().split(' ')
            else:
                active = False
                split = line.lstrip().splity(' ')

            creds.append(DoltKeyPair(split[0], split[1], active))

        return creds


    def creds_check(self, endpoint: str = None, creds: str = None) -> bool:
        args = ['dolt', 'creds', 'check']

        if endpoint:
            args.extend(['--endpoint', endpoint])
        if creds:
            args.extend(['--creds', creds])

        output = _execute(args, self.repo_dir())

        if output[3].startswith('error'):
            logger.error('\n'.join(output[3:]))
            return False

        return True

    def creds_use(self, public_key_id: str) -> bool:
        args = ['creds', 'use', public_key_id]

        output = _execute(args, self.repo_dir())

        if output[0].startswith('error'):
            logger.error('\n'.join(output[3:]))
            raise DoltException('Bad public key')

        return True

    def creds_import(self, jwk_filename: str, no_profile: str):
        raise NotImplementedError()

    def config(self,
               name: str = None,
               value: str = None,
               add: bool = False,
               list: bool = False,
               get: bool = False,
               unset: bool = False):
        switch_count = [el for el in [add, list, get, unset] if el]
        assert len(switch_count) == 1, 'Exactly one of add, list, get, unset must be True'

        args = ['config']

        if add:
            assert name and value, 'For add, name and value must be set'
            args.extend(['--add', '--name', name, '--value', value])
        if list:
            assert not(name or value), 'For list, no name and value provided'
            args.append('--list')
        if get:
            assert name and not value, 'For get, only name is provided'
            args.extend(['--get', '--name', name])
        if unset:
            assert name and not value, 'For get, only name is provided'
            args.extend(['--unset', '--name', name])

        # TODO something with the output
        output = _execute(args, self.repo_dir()).split('\n')
        self._print_output(output)

    def ls(self, system: bool = False, all: bool = False) -> List[DoltTable]:
        args = ['ls', '--verbose']

        if all:
            args.append('--all')

        if system:
            args.append('--system')

        output = self.execute(args, print_output=False)
        tables = []
        system_pos = None

        for i, line in enumerate(output):
            if line.startswith('Tables') or not line:
                pass
            elif line.startswith('System'):
                system_pos = i
                break
            else:
                if not line:
                    pass
                split = line.lstrip().split()
                tables.append(DoltTable(split[0], split[1], split[2]))

        if system_pos:
            for line in output[system_pos:]:
                if line.startswith('System'):
                    pass
                else:
                    tables.append(DoltTable(line.strip(), system=True))

        return tables

    def schema_export(self, table: str, filename: str = None):
        args = ['schema', 'export', table]

        if filename:
            args.extend(['--filename', filename])
            _execute(args, self.repo_dir())
            return True
        else:
            output = _execute(args, self.repo_dir())
            logger.info('\n'.join(output))
            return True

    def schema_import(self,
                      table: str,
                      filename: str,
                      create: bool = False,
                      update: bool = False,
                      replace: bool = False,
                      dry_run: bool = False,
                      keep_types: bool = False,
                      file_type: bool = False,
                      pks: List[str] = None,
                      map: str = None,
                      float_threshold: float = None,
                      delim: str = None):
        switch_count = [el for el in [create, update, replace] if el]
        assert len(switch_count) == 1, 'Exactly one of create, update, replace must be True'

        args = ['schema', 'import']

        if create:
            args.append('--create')
            assert pks, 'When create is set to True, pks must be provided'
        if update:
            args.append('--update')
        if replace:
            args.append('--replace')
            assert pks, 'When replace is set to True, pks must be provided'
        if dry_run:
            args.append('--dry-run')
        if keep_types:
            args.append('--keep-types')
        if file_type:
            args.extend(['--file_type', file_type])
        if pks:
            args.extend(['--pks', ','.join(pks)])
        if map:
            args.extend(['--map', map])
        if float_threshold:
            args.extend(['--float-threshold', float_threshold])
        if delim:
            args.extend(['--delim', delim])

        args.extend([table, filename])

        self.execute(args)

    def schema_show(self, table_or_tables: Union[str, List[str]], commit: str = None):
        if type(table_or_tables) == str:
            to_show = [table_or_tables]
        else:
            to_show = table_or_tables

        args = ['schema', 'show']

        if commit:
            args.append(commit)

        args.extend(to_show)

        self.execute(args)

    def table_rm(self, table_or_tables: Union[str, List[str]]):
        if type(table_or_tables) == str:
            tables = [table_or_tables]
        else:
            tables = table_or_tables

        self.execute(['rm', ' '.join(tables)])

    def table_import(self,
                     table: str,
                     filename: str,
                     create_table: bool = False,
                     update_table: bool = False,
                     force: bool = False,
                     mapping_file: str = None,
                     pk: List[str] = None,
                     replace_table: bool = False,
                     file_type: bool = None,
                     continue_importing: bool = False,
                     delim: bool = None):
        switch_count = [el for el in [create_table, update_table, replace_table] if el]
        assert len(switch_count) == 1, 'Exactly one of create, update, replace must be True'

        args = ['table', 'import']

        if create_table:
            args.append('--create')
            assert pk, 'When create is set to True, pks must be provided'
        if update_table:
            args.append('--update')
        if replace_table:
            args.append('--replace')
            assert pk, 'When replace is set to True, pks must be provided'
        if file_type:
            args.extend(['--file_type', file_type])
        if pk:
            args.extend(['--pks', ','.join(pk)])
        if mapping_file:
            args.extend(['--map', mapping_file])
        if delim:
            args.extend(['--delim', delim])
        if continue_importing:
            args.extend('--continue')

        args.extend([table, filename])
        self.execute(args)

    def table_export(self,
                     table: str,
                     filename: str,
                     force: bool = False,
                     schema: str = None,
                     mapping_file: str = None,
                     pk: List[str] = None,
                     file_type: str = None,
                     continue_exporting: bool = False):
        args = ['table', 'export']

        if force:
            args.append('--force')

        if continue_exporting:
            args.append('--continue')

        if schema:
            args.extend(['--schema', schema])

        if mapping_file:
            args.extend(['--map', mapping_file])

        if pk:
            args.extend(['--pk', ','.join(pk)])

        if file_type:
            args.extend(['--file-type', file_type])

        args.extend([table, filename])
        self.execute(args)


    def table_mv(self, old_table: str, new_table: str, force: bool = False):
        args = ['table', 'mv']

        if force:
            args.append('--force')

        args.extend([old_table, new_table])
        self.execute(args)

    def table_cp(self, old_table: str, new_table: str, commit: str = None, force: bool = False):
        args = ['table', 'cp']

        if force:
            args.append('--force')

        if commit:
            args.append(commit)

        args.extend([old_table, new_table])
        self.execute(args)
