# doltpy.sql.sync package

## Submodules

## doltpy.sql.sync.db_tools module


### doltpy.sql.sync.db_tools.build_source_reader(engine: sqlalchemy.engine.base.Engine, reader: Callable[[sqlalchemy.engine.base.Engine, sqlalchemy.sql.schema.Table], Iterable[[dict](https://docs.python.org/3/library/stdtypes.html#dict)]])
Given a connection and a reader provides a function that turns a set of tables in to a data structure containing
the contents of each of the tables.
:param engine:
:param reader:
:return:


### doltpy.sql.sync.db_tools.drop_primary_keys(engine: sqlalchemy.engine.base.Engine, table: sqlalchemy.sql.schema.Table, pks_to_drop: Iterable[[dict](https://docs.python.org/3/library/stdtypes.html#dict)])

### doltpy.sql.sync.db_tools.get_source_reader(engine: sqlalchemy.engine.base.Engine, reader: Optional[Callable[[sqlalchemy.engine.base.Engine, sqlalchemy.sql.schema.Table], List[[dict](https://docs.python.org/3/library/stdtypes.html#dict)]]] = None)
Given a connection and a reader provides a function that turns a set of tables in to a data structure containing
the contents of each of the tables.
:param engine:
:param reader:
:return:


### doltpy.sql.sync.db_tools.get_table_metadata(engine: sqlalchemy.engine.base.Engine, table_name: [str](https://docs.python.org/3/library/stdtypes.html#str))

### doltpy.sql.sync.db_tools.get_table_reader()
When syncing from a relational database, currently  MySQL or Postgres, the database has only a single concept of
state, that is the current state. We simply capture this state by reading out all the data in the database.
:return:


### doltpy.sql.sync.db_tools.get_target_writer_helper(engine: sqlalchemy.engine.base.Engine, get_upsert_statement, update_on_duplicate: [bool](https://docs.python.org/3/library/functions.html#bool), clean_types: Optional[Callable[[Iterable[[dict](https://docs.python.org/3/library/stdtypes.html#dict)]], List[[dict](https://docs.python.org/3/library/stdtypes.html#dict)]]] = None)
Given a database connection returns a function that when passed a mapping from table names to TableUpdate will
apply the table update. A table update consists of primary key values to drop, and data to insert/update.
:param engine: a database connection
:param get_upsert_statement:
:param update_on_duplicate: indicates whether to update values when encountering duplicate PK, default True
:param clean_types: an optional function to clean up the types being written
:return:

## doltpy.sql.sync.dolt module


### doltpy.sql.sync.dolt.drop_missing_pks(engine: sqlalchemy.engine.base.Engine, table: sqlalchemy.sql.schema.Table, data: List[[dict](https://docs.python.org/3/library/stdtypes.html#dict)])
This a very basic n-squared implementation for dropping the primary keys present in Dolt that have been dropped in
the target database.
:param engine:
:param table:
:param data:
:return:


### doltpy.sql.sync.dolt.get_dropped_pks(engine: sqlalchemy.engine.base.Engine, table: sqlalchemy.sql.schema.Table, from_commit: [str](https://docs.python.org/3/library/stdtypes.html#str), to_commit: [str](https://docs.python.org/3/library/stdtypes.html#str))
Given table_metadata, a connection, and a pair of commits, will return the list of pks that were dropped between
the two commits.
:param engine:
:param table:
:param from_commit:
:param to_commit:
:return:


### doltpy.sql.sync.dolt.get_from_commit_to_commit(dsc: doltpy.sql.sql.DoltSQLContext, commit_ref: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)
Given a repo and commit it returns the commit and its parent, if no commit is provided the head and the parent of
head are returned.
:param dsc:
:param commit_ref:
:return:


### doltpy.sql.sync.dolt.get_source_reader(dsc: doltpy.sql.sql.DoltSQLContext, reader: Callable[[[str](https://docs.python.org/3/library/stdtypes.html#str), doltpy.sql.sql.DoltSQLContext], Tuple[Iterable[[dict](https://docs.python.org/3/library/stdtypes.html#dict)], Iterable[[dict](https://docs.python.org/3/library/stdtypes.html#dict)]]])
Returns a function that takes a list of tables and returns a mapping from the table name to the data returned by
the passed reader. The reader is generally one of get_table_reader_diffs or get_table_reader, but it would
be easy enough to provide some other kind of function if neither of these meet your needs.
:param dsc:
:param reader:
:return:


### doltpy.sql.sync.dolt.get_table_reader(commit_ref: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, branch: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)
Returns a function that reads the entire table at a commit and/or branch, and returns the data.
:param commit_ref:
:param branch:
:return:


### doltpy.sql.sync.dolt.get_table_reader_diffs(commit_ref: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, branch: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)
Returns a function that reads the diff from a commit and/or branch, defaults to the HEAD of the current branch if
neither are provided.
:param commit_ref:
:param branch:
:return:


### doltpy.sql.sync.dolt.get_target_writer(dsc: doltpy.sql.sql.DoltSQLContext, branch: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, commit: [bool](https://docs.python.org/3/library/functions.html#bool) = True, message: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)
Given a repo, writes to the specified branch (defaults to current), and optionally commits with the provided
message or generates a standard one.
:param dssc:
:param branch:
:param commit:
:param message:
:return:

## doltpy.sql.sync.mysql module


### doltpy.sql.sync.mysql.get_target_writer(engine: sqlalchemy.engine.base.Engine, update_on_duplicate: [bool](https://docs.python.org/3/library/functions.html#bool) = True)
Given a database connection returns a function that when passed a mapping from table names to TableUpdate will
apply the table update. A table update consists of primary key values to drop, and data to insert/update.
:param engine: a database connection
:param update_on_duplicate: indicates whether to update values when encountering duplicate PK, default True
:return:


### doltpy.sql.sync.mysql.upsert_helper(table: sqlalchemy.sql.schema.Table, data: List[[dict](https://docs.python.org/3/library/stdtypes.html#dict)])
## doltpy.sql.sync.oracle module


### doltpy.sql.sync.oracle.execute_updates_and_inserts(engine: sqlalchemy.engine.base.Engine, table: sqlalchemy.sql.schema.Table, data: List[[dict](https://docs.python.org/3/library/stdtypes.html#dict)], update_on_duplicate: [bool](https://docs.python.org/3/library/functions.html#bool))

### doltpy.sql.sync.oracle.get_target_writer(engine: sqlalchemy.engine.base.Engine, update_on_duplicate: [bool](https://docs.python.org/3/library/functions.html#bool) = True)
Given a database connection returns a function that when passed a mapping from table names to TableUpdate will
apply the table update. A table update consists of primary key values to drop, and data to insert/update.
:param engine: a database connection
:param update_on_duplicate: indicates whether to update values when encountering duplicate PK, default True
:return:

## doltpy.sql.sync.postgres module


### doltpy.sql.sync.postgres.get_target_writer(engine: sqlalchemy.engine.base.Engine, update_on_duplicate: [bool](https://docs.python.org/3/library/functions.html#bool) = True)
Given a psycopg2 connection returns a function that takes a map of tables names (optionally schema prefixed) to
list of tuples and writes the list of tuples to the table in question. Each tuple must have the data in the order of
the target tables columns sorted lexicographically.
:param engine: database connection.
:param update_on_duplicate: perform upserts instead of failing on duplicate primary keys
:return:


### doltpy.sql.sync.postgres.upsert_helper(table: sqlalchemy.sql.schema.Table, data: List[[dict](https://docs.python.org/3/library/stdtypes.html#dict)])
## doltpy.sql.sync.sync_tools module


### doltpy.sql.sync.sync_tools.coerce_column_to_dolt(column: sqlalchemy.sql.schema.Column, type_mapping: [dict](https://docs.python.org/3/library/stdtypes.html#dict))
Defines how we map MySQL types to Dolt types, and removes unsupported column level constraints. Eventually this
function should be trivial since we aim to faithfully support MySQL.
:param column:
:param type_mapping:
:return:


### doltpy.sql.sync.sync_tools.coerce_schema_to_dolt(target_table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), table: sqlalchemy.sql.schema.Table, type_mapping: [dict](https://docs.python.org/3/library/stdtypes.html#dict))

### doltpy.sql.sync.sync_tools.sync_from_dolt(source_reader: Callable[[List[[str](https://docs.python.org/3/library/stdtypes.html#str)]], Mapping[[str](https://docs.python.org/3/library/stdtypes.html#str), Tuple[Iterable[[dict](https://docs.python.org/3/library/stdtypes.html#dict)], Iterable[[dict](https://docs.python.org/3/library/stdtypes.html#dict)]]]], target_writer: Callable[[Mapping[[str](https://docs.python.org/3/library/stdtypes.html#str), Tuple[Iterable[[dict](https://docs.python.org/3/library/stdtypes.html#dict)], Iterable[[dict](https://docs.python.org/3/library/stdtypes.html#dict)]]]], [None](https://docs.python.org/3/library/constants.html#None)], table_map: Mapping[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)])
Executes a sync from Dolt to another database (currently only MySQL). Works by taking source_reader that reads from
Dolt. Various implementations are provided in doltpy.etl.sql_sync.dolt that offer different semantics. For example,
one might want to choose whether to sync the state of table at HEAD of master, or take only incremental diffs.
The writer implementations are more straightforward, and found in doltpy.etl.sql_sync.mysql. They offer the user the
ability to configure what to do on primary key duplicates.

Of course one can easily implement their own reads and writers, as they conform to the relevant type interfaces at
the top of the file.
:param source_reader:
:param target_writer:
:param table_map:
:return:


### doltpy.sql.sync.sync_tools.sync_schema_to_dolt(source_engine: sqlalchemy.engine.base.Engine, target_engine: sqlalchemy.engine.base.Engine, table_map: Mapping[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)], type_mapping: [dict](https://docs.python.org/3/library/stdtypes.html#dict))

* **Parameters**

    
    * **source_engine** – 


    * **target_engine** – 


    * **table_map** – 


    * **type_mapping** – 



* **Returns**

    


### doltpy.sql.sync.sync_tools.sync_to_dolt(source_reader: Callable[[List[[str](https://docs.python.org/3/library/stdtypes.html#str)]], Mapping[[str](https://docs.python.org/3/library/stdtypes.html#str), Iterable[[dict](https://docs.python.org/3/library/stdtypes.html#dict)]]], target_writer: Callable[[Mapping[[str](https://docs.python.org/3/library/stdtypes.html#str), Iterable[[dict](https://docs.python.org/3/library/stdtypes.html#dict)]]], [None](https://docs.python.org/3/library/constants.html#None)], table_map: Mapping[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)])
Executes a sync from another database (currently on MySQL) to Dolt. Since generally other databases have a single
notion of state, we merely seek to capture that notion of state. The source_reader function in
doltpy.etl.sql_sync.mysql captures the state of the table, and then the target_writer in doltpy.etl.sql_sync.dolt
merely writes that data to a commit.

One could imagine a more complex implementation were the source_reader captures only rows that have updated_at
field that is greater than the last sync timestamp. They would be easy enough to implement and we will provide
examples in future documentation.
:param source_reader:
:param target_writer:
:param table_map:
:return:

## Module contents
