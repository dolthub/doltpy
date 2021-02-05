# doltpy.sql package

## Subpackages


* doltpy.sql.sync package


    * Submodules


    * doltpy.sql.sync.db_tools module


    * doltpy.sql.sync.dolt module


    * doltpy.sql.sync.mysql module


    * doltpy.sql.sync.oracle module


    * doltpy.sql.sync.postgres module


    * doltpy.sql.sync.sync_tools module


    * Module contents


## Submodules

## doltpy.sql.helpers module


### doltpy.sql.helpers.clean_types(data: Iterable[[dict](https://docs.python.org/3/library/stdtypes.html#dict)])
MySQL does not support native array or JSON types, additionally mysql-connector-python does not support
datetime.date (though that seems like a bug in the connector). This implements a very crude transformation of array
types and coerces datetime.date values to equivalents. This is quite an experimental feature and is currently a way
to transform array valued data read from Postgres to Dolt.
:param data:
:return:


### doltpy.sql.helpers.get_existing_pks(engine: sqlalchemy.engine.base.Engine, table: sqlalchemy.sql.schema.Table)
Creates an index of hashes of the values of the primary keys in the table provided.
:param engine:
:param table:
:return:


### doltpy.sql.helpers.get_inserts_and_updates(engine: sqlalchemy.engine.base.Engine, table: sqlalchemy.sql.schema.Table, data: List[[dict](https://docs.python.org/3/library/stdtypes.html#dict)])

### doltpy.sql.helpers.hash_row_els(row: [dict](https://docs.python.org/3/library/stdtypes.html#dict), cols: List[[str](https://docs.python.org/3/library/stdtypes.html#str)])

### doltpy.sql.helpers.infer_table_schema(metadata: sqlalchemy.sql.schema.MetaData, table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), rows: Iterable[[dict](https://docs.python.org/3/library/stdtypes.html#dict)], primary_key: List[[str](https://docs.python.org/3/library/stdtypes.html#str)])
## doltpy.sql.sql module


### class doltpy.sql.sql.DoltSQLContext()
Bases: [`object`](https://docs.python.org/3/library/functions.html#object)


#### commit_tables(commit_message: [str](https://docs.python.org/3/library/stdtypes.html#str), table_or_tables: Optional[Union[[str](https://docs.python.org/3/library/stdtypes.html#str), List[[str](https://docs.python.org/3/library/stdtypes.html#str)]]] = None, allow_emtpy: [bool](https://docs.python.org/3/library/functions.html#bool) = False)

#### dolt(: doltpy.cli.dolt.Dolt)

#### engine(: sqlalchemy.engine.base.Engine)

#### execute(sql: [str](https://docs.python.org/3/library/stdtypes.html#str), commit: [bool](https://docs.python.org/3/library/functions.html#bool) = False, commit_message: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, allow_emtpy: [bool](https://docs.python.org/3/library/functions.html#bool) = False)

#### read_columns(table: [str](https://docs.python.org/3/library/stdtypes.html#str), as_of: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)

#### read_columns_sql(sql: [str](https://docs.python.org/3/library/stdtypes.html#str))

#### read_pandas(table: [str](https://docs.python.org/3/library/stdtypes.html#str), as_of: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)

#### read_pandas_sql(sql: [str](https://docs.python.org/3/library/stdtypes.html#str))

#### read_rows(table: [str](https://docs.python.org/3/library/stdtypes.html#str), as_of: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)

#### read_rows_sql(sql: [str](https://docs.python.org/3/library/stdtypes.html#str))

#### server_config(: doltpy.sql.sql.ServerConfig)

#### verify_connection()

#### write_batch(table: sqlalchemy.sql.schema.Table, rows: List[[dict](https://docs.python.org/3/library/stdtypes.html#dict)], on_duplicate_key_update: [bool](https://docs.python.org/3/library/functions.html#bool))

#### write_columns(table: [str](https://docs.python.org/3/library/stdtypes.html#str), columns: Mapping[[str](https://docs.python.org/3/library/stdtypes.html#str), List[Any]], on_duplicate_key_update: [bool](https://docs.python.org/3/library/functions.html#bool) = True, create_if_not_exists: [bool](https://docs.python.org/3/library/functions.html#bool) = False, primary_key: Optional[List[[str](https://docs.python.org/3/library/stdtypes.html#str)]] = None, commit: [bool](https://docs.python.org/3/library/functions.html#bool) = True, commit_message: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, commit_date: Optional[[datetime.datetime](https://docs.python.org/3/library/datetime.html#datetime.datetime)] = None, allow_empty: [bool](https://docs.python.org/3/library/functions.html#bool) = False, batch_size: [int](https://docs.python.org/3/library/functions.html#int) = 100000)

#### write_file(table: [str](https://docs.python.org/3/library/stdtypes.html#str), file_path: [str](https://docs.python.org/3/library/stdtypes.html#str), on_duplicate_key_update: [bool](https://docs.python.org/3/library/functions.html#bool) = True, create_if_not_exists: [bool](https://docs.python.org/3/library/functions.html#bool) = False, primary_key: Optional[List[[str](https://docs.python.org/3/library/stdtypes.html#str)]] = None, commit: [bool](https://docs.python.org/3/library/functions.html#bool) = True, commit_message: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, commit_date: Optional[[datetime.datetime](https://docs.python.org/3/library/datetime.html#datetime.datetime)] = None, allow_empty: [bool](https://docs.python.org/3/library/functions.html#bool) = False, batch_size: [int](https://docs.python.org/3/library/functions.html#int) = 100000)

#### write_pandas(table: [str](https://docs.python.org/3/library/stdtypes.html#str), df: pandas.core.frame.DataFrame, on_duplicate_key_update: [bool](https://docs.python.org/3/library/functions.html#bool) = True, create_if_not_exists: [bool](https://docs.python.org/3/library/functions.html#bool) = False, primary_key: Optional[List[[str](https://docs.python.org/3/library/stdtypes.html#str)]] = None, commit: [bool](https://docs.python.org/3/library/functions.html#bool) = False, commit_message: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, commit_date: Optional[[datetime.datetime](https://docs.python.org/3/library/datetime.html#datetime.datetime)] = None, allow_empty: [bool](https://docs.python.org/3/library/functions.html#bool) = False, batch_size: [int](https://docs.python.org/3/library/functions.html#int) = 100000)

#### write_rows(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), rows: Iterable[[dict](https://docs.python.org/3/library/stdtypes.html#dict)], on_duplicate_key_update: [bool](https://docs.python.org/3/library/functions.html#bool) = True, create_if_not_exists: [bool](https://docs.python.org/3/library/functions.html#bool) = False, primary_key: Optional[List[[str](https://docs.python.org/3/library/stdtypes.html#str)]] = None, commit: [bool](https://docs.python.org/3/library/functions.html#bool) = False, commit_message: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, commit_date: Optional[[datetime.datetime](https://docs.python.org/3/library/datetime.html#datetime.datetime)] = None, allow_empty: [bool](https://docs.python.org/3/library/functions.html#bool) = False, batch_size: [int](https://docs.python.org/3/library/functions.html#int) = 100000)

### class doltpy.sql.sql.DoltSQLEngineContext(dolt: doltpy.cli.dolt.Dolt, server_config: doltpy.sql.sql.ServerConfig)
Bases: `doltpy.sql.sql.DoltSQLContext`


#### dolt(: doltpy.cli.dolt.Dolt)

#### engine(: sqlalchemy.engine.base.Engine)

#### server_config(: doltpy.sql.sql.ServerConfig)

### class doltpy.sql.sql.DoltSQLServerContext(dolt: doltpy.cli.dolt.Dolt, server_config: doltpy.sql.sql.ServerConfig)
Bases: `doltpy.sql.sql.DoltSQLContext`


#### dolt(: doltpy.cli.dolt.Dolt)

#### engine(: sqlalchemy.engine.base.Engine)

#### server_config(: doltpy.sql.sql.ServerConfig)

#### start_server()
Start a MySQL Server process on local host using the parameters to configure behavior. The parameters are
self-explanatory, but the config is a way to provide them as a YAML file rather than as function
arguments.
:return:


#### stop_server()
Stop the MySQL Server process this repo is running.
:return:


### class doltpy.sql.sql.ServerConfig(branch: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, config: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, host: [str](https://docs.python.org/3/library/stdtypes.html#str) = '127.0.0.1', port: [int](https://docs.python.org/3/library/functions.html#int) = 3306, user: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, password: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, timeout: Optional[[int](https://docs.python.org/3/library/functions.html#int)] = None, readonly: Optional[[bool](https://docs.python.org/3/library/functions.html#bool)] = None, loglevel: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, multi_db_dir: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, no_auto_commit: Optional[[bool](https://docs.python.org/3/library/functions.html#bool)] = None, echo: [bool](https://docs.python.org/3/library/functions.html#bool) = False)
Bases: [`object`](https://docs.python.org/3/library/functions.html#object)

## Module contents
