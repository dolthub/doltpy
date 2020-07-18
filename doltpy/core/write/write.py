from typing import List, Callable, Any, Mapping
import pandas as pd
from doltpy.core.dolt import Dolt, DEFAULT_HOST, DEFAULT_PORT
import logging
import io
import tempfile
from datetime import datetime
from sqlalchemy import String, DateTime, Integer, Float, Table, MetaData, Column
import math


logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 300000
CREATE, FORCE_CREATE, REPLACE, UPDATE = 'create', 'force_create', 'replace', 'update'
IMPORT_MODES_TO_FLAGS = {CREATE: ['-c'],
                         FORCE_CREATE: ['-f', '-c'],
                         REPLACE: ['-r'],
                         UPDATE: ['-u']}


def import_df(repo: Dolt,
              table_name: str,
              data: pd.DataFrame,
              primary_keys: List[str],
              import_mode: str = None):
    """
    Imports the given DataFrame object to the specified table, dropping records that are duplicates on primary key
    (in order, preserving the first record, something we might want to allow the user to sepcify), subject to
    given import mode. Import mode defaults to CREATE if the table does not exist, and UPDATE otherwise.
    :param repo:
    :param table_name:
    :param data:
    :param primary_keys:
    :param import_mode:
    :return:
    """
    def writer(filepath: str):
        clean = data.dropna(subset=primary_keys)
        clean.to_csv(filepath, index=False)

    _import_helper(repo, table_name, writer, primary_keys, import_mode)


def bulk_import(repo: Dolt,
                table_name: str,
                data: io.StringIO,
                primary_keys: List[str],
                import_mode: str = None) -> None:
    """
    This takes a file like object representing a CSV and imports it to the table specified. Note that you must
    specify the primary key, and the import mode. The import mode is one of the keys of IMPORT_MODES_TO_FLAGS.
    Choosing the wrong import mode will throw an error, for example `CREATE` on an existing table. Import mode
    defaults to CREATE if the table does not exist, and UPDATE otherwise.
    :param repo:
    :param table_name:
    :param data:
    :param primary_keys:
    :param import_mode:
    :return:
    """
    def writer(filepath: str):
        with open(filepath, 'w') as f:
            f.writelines(data.readlines())

    _import_helper(repo, table_name, writer, primary_keys, import_mode)


def _import_helper(repo: Dolt,
                   table_name: str,
                   write_import_file: Callable[[str], None],
                   primary_keys: List[str],
                   import_mode: str) -> None:
    import_modes = IMPORT_MODES_TO_FLAGS.keys()
    if import_mode is not None:
        assert import_mode in import_modes, 'update_mode must be one of: {}'.format(import_modes)
    else:
        if table_name in [table.name for table in repo.ls()]:
            logger.info('No import mode specified, table exists, using "{}"'.format(UPDATE))
            import_mode = UPDATE
        else:
            logger.info('No import mode specified, table exists, using "{}"'.format(CREATE))
            import_mode = CREATE

    import_flags = IMPORT_MODES_TO_FLAGS[import_mode]
    logger.info('Importing to table {} in dolt directory located in {}, import mode {}'.format(table_name,
                                                                                               repo.repo_dir(),
                                                                                               import_mode))
    fp = tempfile.NamedTemporaryFile(suffix='.csv')
    write_import_file(fp.name)
    args = ['table', 'import', table_name, '--pk={}'.format(','.join(primary_keys))] + import_flags
    repo.execute(args + [fp.name])


def import_dict(repo: Dolt,
                table_name: str,
                data: Mapping[str, List[Any]],
                primary_keys: List[str] = None,
                import_mode: str = None,
                batch_size: int = DEFAULT_BATCH_SIZE):
    """
    Provides a column major interface for writing Python data structures to Dolt, specifically data should be a dict
    where the keys are column names and the values are equal length lists of values to be written to Dolt. The lists
    must consist of:
        - values that match the type of the table in the schema of the table being written to
        - values of the same type that can be coalesced to a Python type by the (very limited) type inference logic
          for generating a schema from a data structure

    Note it is necessary for all list to be of the same length since we must coalesce the lists into rows, and that
    doesn't really make sense when the lists are not of the same length.

    Let's proceed with the example of creating a simple table and showing how to write some data structures:
        CREATE TABLE players (id INT, name VARCHAR(16), PRIMARY KEY (id))

    Now write in update mode:
    >>> dict_of_lists = {'id': [1, 2], 'name': ['Roger', 'Rafael']}
    >>> import_dict(repo, 'players', dict_of_lists, import_mode='update')

    Alternatively we can let the Python code infer a schema:
    >>> import_dict(repo, 'players', dict_of_lists, ['id'], import_mode='create')

    Assertions:
        - all list values are of equal length
        - when inferring a schema each list value has elements of a type that can be mapped to a SQL type, the logic is
          currently very limited
        - when inferring a schema

    This function requires the Dolt SQL server to be running on the host and port provided, defaulting to
    127.0.0.1:3306.

    :param repo:
    :param table_name:
    :param data:
    :param primary_keys:
    :param import_mode:
    :param batch_size:
    :return:
    """
    assert import_mode in [UPDATE, CREATE]

    # Grab some basic information about the data
    assert data, 'Cannot provide an empty dictionary'
    row_count = len(list(data.values())[0])
    assert row_count > 0, 'Must provide at least a single row'
    assert all(len(val_list) == row_count for val_list in data.values()), 'Must provide value lists of uniform length'

    # Get an Engine object

    # If the table does not exist, create it using type inference to build a create statement
    if import_mode == CREATE:
        assert primary_keys, 'primary_keys need to be provided when inferring a schema'
        _create_table_inferred(repo, table_name, data, primary_keys)

    rows = []
    for i in range(row_count):
        rows.append({col: data[col][i] for col in data.keys()})

    logger.info('Inserting {row_count} rows into table {table_name}'.format(row_count=row_count,
                                                                            table_name=table_name))
    table = MetaData(bind=repo.engine, reflect=True).tables[table_name]
    for i in range(max(1, math.ceil(len(rows) / batch_size))):
        batch_start = i * batch_size
        batch_end = min((i+1) * batch_size, len(rows))
        batch = rows[batch_start:batch_end]
        logger.info('Writing records {} through {} of {} rows to Dolt'.format(batch_start, batch_end, len(rows)))
        with repo.engine.connect() as conn:
            conn.execute(table.insert(), batch)


def _create_table_inferred(repo: Dolt, table_name: str, data: Mapping[str, List[Any]], primary_keys: List[str]):
    # generate and execute a create table statement
    cols_to_types = {}
    for col_name, list_of_values in data.items():
        # Just take the first value to by the type
        first_non_null = None
        for val in list_of_values:
            if val is not None:
                first_non_null = val
                break
            raise ValueError('Cannot provide an empty list, types cannot be inferred')
        cols_to_types[col_name] = _get_col_type(first_non_null, list_of_values)

    metadata = MetaData(bind=repo.engine)
    table = _get_table_def(metadata, table_name, cols_to_types, primary_keys)
    table.create()


def _get_col_type(sample_value: Any, values: Any):
    if type(sample_value) == str:
        return String(2 * max(len(val) for val in values))
    elif type(sample_value) == int:
        return Integer
    elif type(sample_value) == float:
        return Float
    elif type(sample_value) == datetime:
        return DateTime
    else:
        raise ValueError('Value of type {} is unsupported'.format(type(sample_value)))


def _get_table_def(metadata, table_name: str, cols_with_types: Mapping[str, str], pks: List[str]):
    columns = [Column(col_name, col_type, primary_key=col_name in pks)
               for col_name, col_type in cols_with_types.items()]
    return Table(table_name, metadata, *columns)


def import_list(repo: Dolt,
                table_name: str,
                data: List[Mapping[str, Any]],
                primary_keys: List[str] = None,
                import_mode: str = None,
                batch_size: int = DEFAULT_BATCH_SIZE):
    """
    This provides a write interface for writing row major Python data structures to Dolt. The data parameter should be a
    list of dicts, where each dict represents a row. Each dict must have the same columns, and:
        - values that match the type of the table in the schema of the table being written to
        - values of the same type that can be coalesced to a Python type by the (very limited) type inference logic
          for generating a schema from a data structure.

    Let's proceed with the example of creating a simple table and showing how to write some data structures:
        CREATE TABLE players (id INT, name VARCHAR(16), PRIMARY KEY (id))

    Now write in update mode:
    >>> list_of_dicts = [{'id': 1, 'name': 'Roger'}, {'id': 2, 'name': 'Rafael'}]
    >>> import_list(repo, 'players', list_of_dicts, import_mode='update')

    Alternatively we can let the Python code infer a schema
    >>> import_list(repo, 'players', list_of_dicts, ['id'], import_mode='create')

    Note some restrictions (which we should loosen in a future release):
        - all dicts must have the same set of columns, and they must be a strict subset of the table's columns
        - when inferring a schema the type inference is very limited, and all values that correspond to a given key
          must be of the same type
        - when inferring a schema we cannot have a column of null values since no schema can be inferred

    This function requires the Dolt SQL server to be running on the host and port provided, defaulting to
    127.0.0.1:3306.

    :param repo:
    :param table_name:
    :param data:
    :param primary_keys:
    :param import_mode:
    :param batch_size:
    :return:
    """
    assert data, 'Cannot provide empty dict'

    reformatted = {}
    cols = set(data[0].keys())

    logger.info('Reshaping data into columns')
    for row in data:
        assert set(row.keys()) == cols, 'Two rows with different keys found'

        for col_name, value in row.items():
            if col_name in reformatted:
                reformatted[col_name].append(value)
            else:
                reformatted[col_name] = [value]

    import_dict(repo, table_name, reformatted, primary_keys, import_mode, batch_size)
