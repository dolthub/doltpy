# doltpy.etl package

## Submodules

## doltpy.etl.loaders module


### doltpy.etl.loaders.create_table_from_schema_import(repo: doltpy.cli.dolt.Dolt, table: [str](https://docs.python.org/3/library/stdtypes.html#str), pks: List[[str](https://docs.python.org/3/library/stdtypes.html#str)], path: [str](https://docs.python.org/3/library/stdtypes.html#str), commit: [bool](https://docs.python.org/3/library/functions.html#bool) = True, commit_message: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)
Execute Dolt.schema_import_create(…) against a file with a specified set of primary key columns, and optionally
commit the created table.
:param repo:
:param table:
:param pks:
:param path:
:param commit:
:param commit_message:
:return:


### doltpy.etl.loaders.create_table_from_schema_import_unique_key(repo: doltpy.cli.dolt.Dolt, table: [str](https://docs.python.org/3/library/stdtypes.html#str), path: [str](https://docs.python.org/3/library/stdtypes.html#str), commit: [bool](https://docs.python.org/3/library/functions.html#bool) = True, commit_message: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)
Execute Dolt.schema_import_create(…) against a file where we will use insert_unique_key(…) to create a unique
key on this data. The standard “hash_id” column name will be used for the unique key.
:param repo:
:param table:
:param path:
:param commit:
:param commit_message:
:return:


### doltpy.etl.loaders.get_branch_creator(new_branch_name: [str](https://docs.python.org/3/library/stdtypes.html#str), refspec: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None)
Returns a function that creates a branch at the specified refspec.
:param new_branch_name:
:param refspec:
:return:


### doltpy.etl.loaders.get_bulk_table_writer(table: [str](https://docs.python.org/3/library/stdtypes.html#str), get_data: Callable[], _io.StringIO], pk_cols: List[[str](https://docs.python.org/3/library/stdtypes.html#str)], import_mode: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, transformers: Optional[List[Callable[[_io.StringIO], _io.StringIO]]] = None)
Returns a function that takes a Dolt repository object and writes the contents of the file like object returned by
the function parameter get_data to the table specified using the primary keys passed. Optionally toggle the import
mode and apply a list of transformers to do some data cleaning operations. For example, we might apply a transformer
that converts some date strings to proper datetime objects.
:param table:
:param get_data:
:param pk_cols:
:param import_mode:
:param transformers:
:return:


### doltpy.etl.loaders.get_df_table_writer(table: [str](https://docs.python.org/3/library/stdtypes.html#str), get_data: Callable[], pandas.core.frame.DataFrame], pk_cols: List[[str](https://docs.python.org/3/library/stdtypes.html#str)], import_mode: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, transformers: Optional[List[Callable[[pandas.core.frame.DataFrame], pandas.core.frame.DataFrame]]] = None)
Returns a function that takes a Dolt repository object and writes the Pandas DataFrame returned by the function
parameter get_data to the table specified using the primary keys passed. Optionally toggle the import mode and
apply a list of transformers to do some data cleaning operations. For example, we might apply a transformer that
converts some date strings to proper datetime objects.
:param table:
:param get_data:
:param pk_cols:
:param import_mode:
:param transformers:
:return:


### doltpy.etl.loaders.get_dolt_loader(writer_or_writers: Union[Callable[[doltpy.cli.dolt.Dolt], [str](https://docs.python.org/3/library/stdtypes.html#str)], List[Callable[[doltpy.cli.dolt.Dolt], [str](https://docs.python.org/3/library/stdtypes.html#str)]]], commit: [bool](https://docs.python.org/3/library/functions.html#bool), message: [str](https://docs.python.org/3/library/stdtypes.html#str), branch: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'master', transaction_mode: Optional[[bool](https://docs.python.org/3/library/functions.html#bool)] = None)
Given a repo and a set of table loaders, run the table loaders and conditionally commit the results with the
specified message on the specified branch. If transaction_mode is true then ensure all loaders/transformers are
successful, or all are rolled back.
:param writer_or_writers:
:param commit:
:param message:
:param branch:
:param transaction_mode:
:return: the branch written to


### doltpy.etl.loaders.get_table_transformer(get_data: Callable[[doltpy.cli.dolt.Dolt], pandas.core.frame.DataFrame], target_table: [str](https://docs.python.org/3/library/stdtypes.html#str), transformer: Callable[[pandas.core.frame.DataFrame], pandas.core.frame.DataFrame], target_pk_cols: Optional[List[[str](https://docs.python.org/3/library/stdtypes.html#str)]] = None, import_mode: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'update')
A version of get_df_table writer where the input is a Dolt repository. This is used for transforming raw data into
derived tables.
:param get_data:
:param target_table:
:param target_pk_cols:
:param transformer:
:param import_mode:
:return:


### doltpy.etl.loaders.get_unique_key_table_writer(table: [str](https://docs.python.org/3/library/stdtypes.html#str), get_data: Callable[], pandas.core.frame.DataFrame], import_mode: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'update', transformers: Optional[List[Callable[[pandas.core.frame.DataFrame], pandas.core.frame.DataFrame]]] = None)
This is a convenience function wrapping for loading data when using the insert_primary_key transformer to
generate a unique key.
:param table:
:param get_data:
:param import_mode:
:param transformers:
:return:


### doltpy.etl.loaders.insert_unique_key(df: pandas.core.frame.DataFrame)
This function takes Pandas DataFrame and inserts a unique hash to each row created from the row itself, along
with a count of how many rows produce the same hash. The idea is to provide some rudimentary tools for writing data
with unique keys.
:param df:
:return:

## doltpy.etl.wrappers module


### doltpy.etl.wrappers.load_to_dolt(loader_or_loaders: Union[Callable[[doltpy.cli.dolt.Dolt], [str](https://docs.python.org/3/library/stdtypes.html#str)], List[Callable[[doltpy.cli.dolt.Dolt], [str](https://docs.python.org/3/library/stdtypes.html#str)]]], dolt_dir: [str](https://docs.python.org/3/library/stdtypes.html#str), dry_run: [bool](https://docs.python.org/3/library/functions.html#bool))
This function takes a DoltLoaderBuilder, repo and remote settings, and attempts to execute the loaders returned
by the builder.
:param loader_or_loaders:
:param dolt_dir:
:param dry_run:
:return:


### doltpy.etl.wrappers.load_to_dolthub(loader_or_loaders: Union[Callable[[doltpy.cli.dolt.Dolt], [str](https://docs.python.org/3/library/stdtypes.html#str)], List[Callable[[doltpy.cli.dolt.Dolt], [str](https://docs.python.org/3/library/stdtypes.html#str)]]], clone: [bool](https://docs.python.org/3/library/functions.html#bool), push: [bool](https://docs.python.org/3/library/functions.html#bool), remote_name: [str](https://docs.python.org/3/library/stdtypes.html#str), remote_url: [str](https://docs.python.org/3/library/stdtypes.html#str), dolt_dir: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, dry_run: [bool](https://docs.python.org/3/library/functions.html#bool) = False)
This function takes a DoltLoaderBuilder, repo and remote settings, and attempts to execute the loaders returned
by the builder.
:param loader_or_loaders:
:param dolt_dir:
:param clone:
:param push:
:param remote_name:
:param dry_run:
:param remote_url:
:return:

## Module contents
