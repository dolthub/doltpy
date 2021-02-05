# doltpy.types package

## Submodules

## doltpy.types.dolt module


### class doltpy.types.dolt.DoltT(\*args, \*\*kwds)
Bases: `Generic`[`doltpy.types.dolt._T`]


#### abstract add(table_or_tables: Union[[str](https://docs.python.org/3/library/stdtypes.html#str), List[[str](https://docs.python.org/3/library/stdtypes.html#str)]])

#### abstract blame(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), rev: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)

#### abstract branch(branch_name: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, start_point: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, new_branch: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, force: [bool](https://docs.python.org/3/library/functions.html#bool) = False, delete: [bool](https://docs.python.org/3/library/functions.html#bool) = False, copy: [bool](https://docs.python.org/3/library/functions.html#bool) = False, move: [bool](https://docs.python.org/3/library/functions.html#bool) = False)

#### abstract checkout(branch: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, table_or_tables: Optional[Union[[str](https://docs.python.org/3/library/stdtypes.html#str), List[[str](https://docs.python.org/3/library/stdtypes.html#str)]]] = None, checkout_branch: [bool](https://docs.python.org/3/library/functions.html#bool) = False, start_point: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)

#### abstract static clone(remote_url: [str](https://docs.python.org/3/library/stdtypes.html#str), new_dir: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, remote: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, branch: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)

#### abstract commit(message: [str](https://docs.python.org/3/library/stdtypes.html#str) = '', allow_empty: [bool](https://docs.python.org/3/library/functions.html#bool) = False, date: Optional[[datetime.datetime](https://docs.python.org/3/library/datetime.html#datetime.datetime)] = None)

#### abstract classmethod config_global(name: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, value: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, add: [bool](https://docs.python.org/3/library/functions.html#bool) = False, list: [bool](https://docs.python.org/3/library/functions.html#bool) = False, get: [bool](https://docs.python.org/3/library/functions.html#bool) = False, unset: [bool](https://docs.python.org/3/library/functions.html#bool) = False)

#### abstract config_local(name: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, value: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, add: [bool](https://docs.python.org/3/library/functions.html#bool) = False, list: [bool](https://docs.python.org/3/library/functions.html#bool) = False, get: [bool](https://docs.python.org/3/library/functions.html#bool) = False, unset: [bool](https://docs.python.org/3/library/functions.html#bool) = False)

#### abstract creds_check(endpoint: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, creds: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)

#### abstract creds_import(jwk_filename: [str](https://docs.python.org/3/library/stdtypes.html#str), no_profile: [str](https://docs.python.org/3/library/stdtypes.html#str))

#### abstract creds_ls()

#### abstract creds_new()

#### abstract creds_rm(public_key: [str](https://docs.python.org/3/library/stdtypes.html#str))

#### abstract creds_use(public_key_id: [str](https://docs.python.org/3/library/stdtypes.html#str))

#### abstract diff(commit: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, other_commit: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, table_or_tables: Optional[Union[[str](https://docs.python.org/3/library/stdtypes.html#str), List[[str](https://docs.python.org/3/library/stdtypes.html#str)]]] = None, data: [bool](https://docs.python.org/3/library/functions.html#bool) = False, schema: [bool](https://docs.python.org/3/library/functions.html#bool) = False, summary: [bool](https://docs.python.org/3/library/functions.html#bool) = False, sql: [bool](https://docs.python.org/3/library/functions.html#bool) = False, where: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, limit: Optional[[int](https://docs.python.org/3/library/functions.html#int)] = None)

#### abstract execute(args: List[[str](https://docs.python.org/3/library/stdtypes.html#str)], print_output: [bool](https://docs.python.org/3/library/functions.html#bool) = True)

#### abstract fetch(remote: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'origin', refspec_or_refspecs: Optional[Union[[str](https://docs.python.org/3/library/stdtypes.html#str), List[[str](https://docs.python.org/3/library/stdtypes.html#str)]]] = None, force: [bool](https://docs.python.org/3/library/functions.html#bool) = False)

#### abstract static init(repo_dir: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)

#### abstract log(number: Optional[[int](https://docs.python.org/3/library/functions.html#int)] = None, commit: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)

#### abstract ls(system: [bool](https://docs.python.org/3/library/functions.html#bool) = False, all: [bool](https://docs.python.org/3/library/functions.html#bool) = False)

#### abstract merge(branch: [str](https://docs.python.org/3/library/stdtypes.html#str), message: [str](https://docs.python.org/3/library/stdtypes.html#str), squash: [bool](https://docs.python.org/3/library/functions.html#bool) = False)

#### abstract pull(remote: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'origin')

#### abstract push(remote: [str](https://docs.python.org/3/library/stdtypes.html#str), refspec: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, set_upstream: [bool](https://docs.python.org/3/library/functions.html#bool) = False, force: [bool](https://docs.python.org/3/library/functions.html#bool) = False)

#### abstract static read_tables(remote_url: [str](https://docs.python.org/3/library/stdtypes.html#str), committish: [str](https://docs.python.org/3/library/stdtypes.html#str), table_or_tables: Optional[Union[[str](https://docs.python.org/3/library/stdtypes.html#str), List[[str](https://docs.python.org/3/library/stdtypes.html#str)]]] = None, new_dir: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)

#### abstract remote(add: [bool](https://docs.python.org/3/library/functions.html#bool) = False, name: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, url: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, remove: Optional[[bool](https://docs.python.org/3/library/functions.html#bool)] = None)

#### abstract repo_dir()

#### abstract property repo_name()

#### abstract reset(table_or_tables: Union[[str](https://docs.python.org/3/library/stdtypes.html#str), List[[str](https://docs.python.org/3/library/stdtypes.html#str)]], hard: [bool](https://docs.python.org/3/library/functions.html#bool) = False, soft: [bool](https://docs.python.org/3/library/functions.html#bool) = False)

#### abstract schema_export(table: [str](https://docs.python.org/3/library/stdtypes.html#str), filename: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)

#### abstract schema_import(table: [str](https://docs.python.org/3/library/stdtypes.html#str), filename: [str](https://docs.python.org/3/library/stdtypes.html#str), create: [bool](https://docs.python.org/3/library/functions.html#bool) = False, update: [bool](https://docs.python.org/3/library/functions.html#bool) = False, replace: [bool](https://docs.python.org/3/library/functions.html#bool) = False, dry_run: [bool](https://docs.python.org/3/library/functions.html#bool) = False, keep_types: [bool](https://docs.python.org/3/library/functions.html#bool) = False, file_type: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, pks: Optional[List[[str](https://docs.python.org/3/library/stdtypes.html#str)]] = None, map: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, float_threshold: Optional[[float](https://docs.python.org/3/library/functions.html#float)] = None, delim: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)

#### abstract schema_show(table_or_tables: Union[[str](https://docs.python.org/3/library/stdtypes.html#str), List[[str](https://docs.python.org/3/library/stdtypes.html#str)]], commit: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)

#### abstract sql(query: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, result_format: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, execute: [bool](https://docs.python.org/3/library/functions.html#bool) = False, save: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, message: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, list_saved: [bool](https://docs.python.org/3/library/functions.html#bool) = False, batch: [bool](https://docs.python.org/3/library/functions.html#bool) = False, multi_db_dir: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)

#### abstract status()

#### abstract table_cp(old_table: [str](https://docs.python.org/3/library/stdtypes.html#str), new_table: [str](https://docs.python.org/3/library/stdtypes.html#str), commit: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, force: [bool](https://docs.python.org/3/library/functions.html#bool) = False)

#### abstract table_export(table: [str](https://docs.python.org/3/library/stdtypes.html#str), filename: [str](https://docs.python.org/3/library/stdtypes.html#str), force: [bool](https://docs.python.org/3/library/functions.html#bool) = False, schema: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, mapping_file: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, pk: Optional[List[[str](https://docs.python.org/3/library/stdtypes.html#str)]] = None, file_type: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, continue_exporting: [bool](https://docs.python.org/3/library/functions.html#bool) = False)

#### abstract table_import(table: [str](https://docs.python.org/3/library/stdtypes.html#str), filename: [str](https://docs.python.org/3/library/stdtypes.html#str), create_table: [bool](https://docs.python.org/3/library/functions.html#bool) = False, update_table: [bool](https://docs.python.org/3/library/functions.html#bool) = False, force: [bool](https://docs.python.org/3/library/functions.html#bool) = False, mapping_file: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, pk: Optional[List[[str](https://docs.python.org/3/library/stdtypes.html#str)]] = None, replace_table: [bool](https://docs.python.org/3/library/functions.html#bool) = False, file_type: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, continue_importing: [bool](https://docs.python.org/3/library/functions.html#bool) = False, delim: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)

#### abstract table_mv(old_table: [str](https://docs.python.org/3/library/stdtypes.html#str), new_table: [str](https://docs.python.org/3/library/stdtypes.html#str), force: [bool](https://docs.python.org/3/library/functions.html#bool) = False)

#### abstract table_rm(table_or_tables: Union[[str](https://docs.python.org/3/library/stdtypes.html#str), List[[str](https://docs.python.org/3/library/stdtypes.html#str)]])

#### abstract static version()
## Module contents
