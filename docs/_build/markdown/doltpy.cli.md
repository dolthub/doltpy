# doltpy.cli package

## Subpackages


* doltpy.cli.read package


    * Submodules


    * doltpy.cli.read.read module


    * Module contents


* doltpy.cli.write package


    * Submodules


    * doltpy.cli.write.write module


    * Module contents


## Submodules

## doltpy.cli.dolt module


### class doltpy.cli.dolt.Dolt(repo_dir: [str](https://docs.python.org/3/library/stdtypes.html#str))
Bases: `Generic`[`doltpy.types.dolt._T`]

This class wraps the Dolt command line interface, mimicking functionality exactly to the extent that is possible.
Some commands simply do not translate to Python, such as dolt sql (with no arguments) since that command
launches an interactive shell.


#### add(table_or_tables: Union[[str](https://docs.python.org/3/library/stdtypes.html#str), List[[str](https://docs.python.org/3/library/stdtypes.html#str)]])
Adds the table or list of tables in the working tree to staging.
:param table_or_tables:
:return:


#### blame(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), rev: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)
Executes a blame command that prints out a table that shows the authorship of the last change to a row.
:param table_name:
:param rev:
:return:


#### branch(branch_name: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, start_point: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, new_branch: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, force: [bool](https://docs.python.org/3/library/functions.html#bool) = False, delete: [bool](https://docs.python.org/3/library/functions.html#bool) = False, copy: [bool](https://docs.python.org/3/library/functions.html#bool) = False, move: [bool](https://docs.python.org/3/library/functions.html#bool) = False)
Checkout, create, delete, move, or copy, a branch. Only
:param branch_name:
:param start_point:
:param new_branch:
:param force:
:param delete:
:param copy:
:param move:
:return:


#### checkout(branch: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, table_or_tables: Optional[Union[[str](https://docs.python.org/3/library/stdtypes.html#str), List[[str](https://docs.python.org/3/library/stdtypes.html#str)]]] = None, checkout_branch: [bool](https://docs.python.org/3/library/functions.html#bool) = False, start_point: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)
Checkout an existing branch, or create a new one, optionally at a specified commit. Or, checkout a table or list
of tables.
:param branch: branch to checkout or create
:param table_or_tables: table or tables to checkout
:param checkout_branch: branch to checkout
:param start_point: tip of new branch
:return:


#### static clone(remote_url: [str](https://docs.python.org/3/library/stdtypes.html#str), new_dir: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, remote: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, branch: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)
Clones the specified DoltHub database into a new directory, or optionally an existing directory provided by the
user.
:param remote_url:
:param new_dir:
:param remote:
:param branch:
:return:


#### commit(message: [str](https://docs.python.org/3/library/stdtypes.html#str) = '', allow_empty: [bool](https://docs.python.org/3/library/functions.html#bool) = False, date: Optional[[datetime.datetime](https://docs.python.org/3/library/datetime.html#datetime.datetime)] = None)
Create a commit with the currents in the working set that are currently in staging.
:param message:
:param allow_empty:
:param date:
:return:


#### classmethod config_global(name: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, value: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, add: [bool](https://docs.python.org/3/library/functions.html#bool) = False, list: [bool](https://docs.python.org/3/library/functions.html#bool) = False, get: [bool](https://docs.python.org/3/library/functions.html#bool) = False, unset: [bool](https://docs.python.org/3/library/functions.html#bool) = False)
Class method for manipulating global configs.
:param name:
:param value:
:param add:
:param list:
:param get:
:param unset:
:return:


#### config_local(name: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, value: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, add: [bool](https://docs.python.org/3/library/functions.html#bool) = False, list: [bool](https://docs.python.org/3/library/functions.html#bool) = False, get: [bool](https://docs.python.org/3/library/functions.html#bool) = False, unset: [bool](https://docs.python.org/3/library/functions.html#bool) = False)
Instance method for manipulating configs local to a repository.
:param name:
:param value:
:param add:
:param list:
:param get:
:param unset:
:return:


#### creds_check(endpoint: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, creds: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)
Check that credentials authenticate with the specified endpoint, return True if authorized, False otherwise.
:param endpoint: the endpoint to check
:param creds: creds identified by public key ID
:return:


#### creds_import(jwk_filename: [str](https://docs.python.org/3/library/stdtypes.html#str), no_profile: [str](https://docs.python.org/3/library/stdtypes.html#str))
Not currently supported.
:param jwk_filename:
:param no_profile:
:return:


#### creds_ls()
Parse the set of keys this repo has into DoltKeyPair objects.
:return:


#### creds_new()
Create a new set of credentials for this Dolt repository.
:return:


#### creds_rm(public_key: [str](https://docs.python.org/3/library/stdtypes.html#str))
Remove the key pair identified by the specified public key ID.
:param public_key:
:return:


#### creds_use(public_key_id: [str](https://docs.python.org/3/library/stdtypes.html#str))
Use the credentials specified by the provided public keys ID.
:param public_key_id:
:return:


#### diff(commit: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, other_commit: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, table_or_tables: Optional[Union[[str](https://docs.python.org/3/library/stdtypes.html#str), List[[str](https://docs.python.org/3/library/stdtypes.html#str)]]] = None, data: [bool](https://docs.python.org/3/library/functions.html#bool) = False, schema: [bool](https://docs.python.org/3/library/functions.html#bool) = False, summary: [bool](https://docs.python.org/3/library/functions.html#bool) = False, sql: [bool](https://docs.python.org/3/library/functions.html#bool) = False, where: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, limit: Optional[[int](https://docs.python.org/3/library/functions.html#int)] = None)
Executes a diff command and prints the output. In the future we plan to create a diff object that will allow
for programmatic interactions.
:param commit: commit to diff against the tip of the current branch
:param other_commit: optionally specify two specific commits if desired
:param table_or_tables: table or list of tables to diff
:param data: diff only data
:param schema: diff only schema
:param summary: summarize the data changes shown, valid only with data
:param sql: show the diff in terms of SQL
:param where: apply a where clause to data diffs
:param limit: limit the number of rows shown in a data diff
:return:


#### execute(args: List[[str](https://docs.python.org/3/library/stdtypes.html#str)], print_output: [bool](https://docs.python.org/3/library/functions.html#bool) = True)
Manages executing a dolt command, pass all commands, sub-commands, and arguments as they would appear on the
command line.
:param args:
:param print_output:
:return:


#### fetch(remote: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'origin', refspec_or_refspecs: Optional[Union[[str](https://docs.python.org/3/library/stdtypes.html#str), List[[str](https://docs.python.org/3/library/stdtypes.html#str)]]] = None, force: [bool](https://docs.python.org/3/library/functions.html#bool) = False)
Fetch the specified branch or list of branches from the remote provided, defaults to origin.
:param remote: the reomte to fetch from
:param refspec_or_refspecs: branch or branches to fetch
:param force: whether to override local history with remote
:return:


#### static init(repo_dir: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)
Creates a new repository in the directory specified, creating the directory if create_dir is passed, and returns
a Dolt object representing the newly created repo.
:return:


#### log(number: Optional[[int](https://docs.python.org/3/library/functions.html#int)] = None, commit: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)
Parses the log created by running the log command into instances of DoltCommit that provide detail of the
commit, including timestamp and hash.
:param number:
:param commit:
:return:


#### ls(system: [bool](https://docs.python.org/3/library/functions.html#bool) = False, all: [bool](https://docs.python.org/3/library/functions.html#bool) = False)
List the tables in the working set, the system tables, or all. Parses the tables and their object hash into an
object that also provides row count.
:param system:
:param all:
:return:


#### merge(branch: [str](https://docs.python.org/3/library/stdtypes.html#str), message: [str](https://docs.python.org/3/library/stdtypes.html#str), squash: [bool](https://docs.python.org/3/library/functions.html#bool) = False)
Executes a merge operation. If conflicts result, the merge is aborted, as an interactive merge does not really
make sense in a scripting environment, or at least we have not figured out how to model it in a way that does.
:param branch:
:param message:
:param squash:
:return:


#### pull(remote: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'origin')
Pull the latest changes from the specified remote.
:param remote:
:return:


#### push(remote: [str](https://docs.python.org/3/library/stdtypes.html#str), refspec: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, set_upstream: [bool](https://docs.python.org/3/library/functions.html#bool) = False, force: [bool](https://docs.python.org/3/library/functions.html#bool) = False)
Push the to the specified remote. If set_upstream is provided will create an upstream reference of all branches
in a repo.
:param remote:
:param refspec: optionally specify a branch to push
:param set_upstream: add upstream reference for every branch successfully pushed
:param force: overwrite the history of the upstream with this repo’s history
:return:


#### static read_tables(remote_url: [str](https://docs.python.org/3/library/stdtypes.html#str), committish: [str](https://docs.python.org/3/library/stdtypes.html#str), table_or_tables: Optional[Union[[str](https://docs.python.org/3/library/stdtypes.html#str), List[[str](https://docs.python.org/3/library/stdtypes.html#str)]]] = None, new_dir: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)
Reads the specified tables, or all the tables, from the DoltHub database specified into a new local database,
at the commit or branch provided. Users can optionally provide an existing directory.
:param remote_url:
:param committish:
:param table_or_tables:
:param new_dir:
:return:


#### remote(add: [bool](https://docs.python.org/3/library/functions.html#bool) = False, name: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, url: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, remove: Optional[[bool](https://docs.python.org/3/library/functions.html#bool)] = None)
Add or remove remotes to this repository. Note we do not currently support some more esoteric options for using
AWS and GCP backends, but will do so in a future release.
:param add:
:param name:
:param url:
:param remove:
:return:


#### repo_dir()
The absolute path of the directory this repository represents.
:return:


#### property repo_name()

#### reset(table_or_tables: Union[[str](https://docs.python.org/3/library/stdtypes.html#str), List[[str](https://docs.python.org/3/library/stdtypes.html#str)]], hard: [bool](https://docs.python.org/3/library/functions.html#bool) = False, soft: [bool](https://docs.python.org/3/library/functions.html#bool) = False)
Reset a table or set of tables that have changes in the working set to their value at the tip of the current
branch.
:param table_or_tables:
:param hard:
:param soft:
:return:


#### schema_export(table: [str](https://docs.python.org/3/library/stdtypes.html#str), filename: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)
Export the scehma of the table specified to the file path specified.
:param table:
:param filename:
:return:


#### schema_import(table: [str](https://docs.python.org/3/library/stdtypes.html#str), filename: [str](https://docs.python.org/3/library/stdtypes.html#str), create: [bool](https://docs.python.org/3/library/functions.html#bool) = False, update: [bool](https://docs.python.org/3/library/functions.html#bool) = False, replace: [bool](https://docs.python.org/3/library/functions.html#bool) = False, dry_run: [bool](https://docs.python.org/3/library/functions.html#bool) = False, keep_types: [bool](https://docs.python.org/3/library/functions.html#bool) = False, file_type: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, pks: Optional[List[[str](https://docs.python.org/3/library/stdtypes.html#str)]] = None, map: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, float_threshold: Optional[[float](https://docs.python.org/3/library/functions.html#float)] = None, delim: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)
This implements schema import from Dolt, it works by inferring a schema from the file provided. It operates in
three modes: create, update, and replace. All require a table name. Create and replace require a primary key, as
they replace an existing table with a new one with a newly inferred schema.


* **Parameters**

    
    * **table** – name of the table to create or update


    * **filename** – file to infer schema from


    * **create** – create a table


    * **update** – update a table


    * **replace** – replace a table


    * **dry_run** – output the SQL to run, do not execute it


    * **keep_types** – when a column already exists, use its current type


    * **file_type** – type of file used for schema inference


    * **pks** – the list of primary keys


    * **map** – mapping file mapping column name to new value


    * **float_threshold** – minimum value fractional component must have to be float


    * **delim** – the delimeter used in the file being inferred from



* **Returns**

    


#### schema_show(table_or_tables: Union[[str](https://docs.python.org/3/library/stdtypes.html#str), List[[str](https://docs.python.org/3/library/stdtypes.html#str)]], commit: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)
Dislay the schema of the specified table or tables at the (optionally) specified commit, defaulting to the tip
of master on the current branch.
:param table_or_tables:
:param commit:
:return:


#### sql(query: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, result_format: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, execute: [bool](https://docs.python.org/3/library/functions.html#bool) = False, save: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, message: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, list_saved: [bool](https://docs.python.org/3/library/functions.html#bool) = False, batch: [bool](https://docs.python.org/3/library/functions.html#bool) = False, multi_db_dir: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)
Execute a SQL query, using the options to dictate how it is executed, and where the output goes.
:param query: query to be executed
:param result_format: the file format of the
:param execute: execute a saved query, not valid with other parameters
:param save: use the name provided to save the value of query
:param message: the message associated with the saved query, if any
:param list_saved: print out a list of saved queries
:param batch: execute in batch mode, one statement after the other delimited by ;
:param multi_db_dir: use a directory of Dolt repos, each one treated as a database
:return:


#### status()
Parses the status of this repository into a DoltStatus object.
:return:


#### table_cp(old_table: [str](https://docs.python.org/3/library/stdtypes.html#str), new_table: [str](https://docs.python.org/3/library/stdtypes.html#str), commit: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, force: [bool](https://docs.python.org/3/library/functions.html#bool) = False)
Copy an existing table to a new table, optionally at a specified commit.
:param old_table: existing table name
:param new_table: new table name
:param commit: commit at which to read old_table
:param force: override changes in the working set
:return:


#### table_export(table: [str](https://docs.python.org/3/library/stdtypes.html#str), filename: [str](https://docs.python.org/3/library/stdtypes.html#str), force: [bool](https://docs.python.org/3/library/functions.html#bool) = False, schema: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, mapping_file: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, pk: Optional[List[[str](https://docs.python.org/3/library/stdtypes.html#str)]] = None, file_type: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, continue_exporting: [bool](https://docs.python.org/3/library/functions.html#bool) = False)

* **Parameters**

    
    * **table** – 


    * **filename** – 


    * **force** – 


    * **schema** – 


    * **mapping_file** – 


    * **pk** – 


    * **file_type** – 


    * **continue_exporting** – 



* **Returns**

    


#### table_import(table: [str](https://docs.python.org/3/library/stdtypes.html#str), filename: [str](https://docs.python.org/3/library/stdtypes.html#str), create_table: [bool](https://docs.python.org/3/library/functions.html#bool) = False, update_table: [bool](https://docs.python.org/3/library/functions.html#bool) = False, force: [bool](https://docs.python.org/3/library/functions.html#bool) = False, mapping_file: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, pk: Optional[List[[str](https://docs.python.org/3/library/stdtypes.html#str)]] = None, replace_table: [bool](https://docs.python.org/3/library/functions.html#bool) = False, file_type: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, continue_importing: [bool](https://docs.python.org/3/library/functions.html#bool) = False, delim: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)
Import a table from a filename, inferring the schema from the file. Operates in two possible modes, update,
create, or replace. If creating must provide a primary key.
:param table: the table to be created or updated
:param filename: the data file to import
:param create_table: create a table
:param update_table: update a table
:param force: force the import to overwrite existing data
:param mapping_file: file mapping column names in file to new names
:param pk: columns from which to build a primary key
:param replace_table: replace existing tables
:param file_type: the type of the file being imported
:param continue_importing:
:param delim:
:return:


#### table_mv(old_table: [str](https://docs.python.org/3/library/stdtypes.html#str), new_table: [str](https://docs.python.org/3/library/stdtypes.html#str), force: [bool](https://docs.python.org/3/library/functions.html#bool) = False)
Rename a table from name old_table to name new_table.
:param old_table: existing table
:param new_table: new table name
:param force: override changes in the working set
:return:


#### table_rm(table_or_tables: Union[[str](https://docs.python.org/3/library/stdtypes.html#str), List[[str](https://docs.python.org/3/library/stdtypes.html#str)]])
Remove the table or list of tables provided from the working set.
:param table_or_tables:
:return:


#### static version()

### class doltpy.cli.dolt.DoltBranch(name: [str](https://docs.python.org/3/library/stdtypes.html#str), commit_id: [str](https://docs.python.org/3/library/stdtypes.html#str))
Bases: [`object`](https://docs.python.org/3/library/functions.html#object)

Represents a branch, along with the commit it points to.


### class doltpy.cli.dolt.DoltCommit(ref: [str](https://docs.python.org/3/library/stdtypes.html#str), ts: [datetime.datetime](https://docs.python.org/3/library/datetime.html#datetime.datetime), author: [str](https://docs.python.org/3/library/stdtypes.html#str), message: [str](https://docs.python.org/3/library/stdtypes.html#str), merge: Optional[Tuple[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]] = None)
Bases: [`object`](https://docs.python.org/3/library/functions.html#object)

Represents metadata about a commit, including a ref, timestamp, and author, to make it easier to sort and present
to the user.


### exception doltpy.cli.dolt.DoltDirectoryException(message)
Bases: [`Exception`](https://docs.python.org/3/library/exceptions.html#Exception)


### exception doltpy.cli.dolt.DoltException(exec_args, stdout, stderr, exitcode)
Bases: [`Exception`](https://docs.python.org/3/library/exceptions.html#Exception)

A class representing a Dolt exception.


### class doltpy.cli.dolt.DoltHubContext(db_path: [str](https://docs.python.org/3/library/stdtypes.html#str), path: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, remote: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'origin', tables_to_read: Optional[List[[str](https://docs.python.org/3/library/stdtypes.html#str)]] = None)
Bases: [`object`](https://docs.python.org/3/library/functions.html#object)


### class doltpy.cli.dolt.DoltKeyPair(public_key: [str](https://docs.python.org/3/library/stdtypes.html#str), key_id: [str](https://docs.python.org/3/library/stdtypes.html#str), active: [bool](https://docs.python.org/3/library/functions.html#bool))
Bases: [`object`](https://docs.python.org/3/library/functions.html#object)

Represents a key pair generated by Dolt for authentication with remotes.


### class doltpy.cli.dolt.DoltRemote(name: [str](https://docs.python.org/3/library/stdtypes.html#str), url: [str](https://docs.python.org/3/library/stdtypes.html#str))
Bases: [`object`](https://docs.python.org/3/library/functions.html#object)

Represents a remote, effectively a name and URL pair.


### exception doltpy.cli.dolt.DoltServerNotRunningException(message)
Bases: [`Exception`](https://docs.python.org/3/library/exceptions.html#Exception)


### class doltpy.cli.dolt.DoltStatus(is_clean: [bool](https://docs.python.org/3/library/functions.html#bool), modified_tables: Dict[[str](https://docs.python.org/3/library/stdtypes.html#str), [bool](https://docs.python.org/3/library/functions.html#bool)], added_tables: Dict[[str](https://docs.python.org/3/library/stdtypes.html#str), [bool](https://docs.python.org/3/library/functions.html#bool)])
Bases: [`object`](https://docs.python.org/3/library/functions.html#object)

Represents the current status of a Dolt repo, summarized by the is_clean field which is True if the wokring set is
clean, and false otherwise. If the working set is not clean, then the changes are stored in maps, one for added
tables, and one for modifications, each name maps to a flag indicating whether the change is staged.


### class doltpy.cli.dolt.DoltTable(name: [str](https://docs.python.org/3/library/stdtypes.html#str), table_hash: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, rows: Optional[[int](https://docs.python.org/3/library/functions.html#int)] = None, system: [bool](https://docs.python.org/3/library/functions.html#bool) = False)
Bases: [`object`](https://docs.python.org/3/library/functions.html#object)

Represents a Dolt table in the working set.


### exception doltpy.cli.dolt.DoltWrongServerException(message)
Bases: [`Exception`](https://docs.python.org/3/library/exceptions.html#Exception)

## Module contents
