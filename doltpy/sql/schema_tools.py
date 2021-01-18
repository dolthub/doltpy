
from sqlalchemy import String, DateTime, Date, Integer, Float, Table, MetaData, Column
from typing import List, Mapping, Any, Iterable
from datetime import datetime, date
from doltpy.shared import rows_to_columns

# todo validation
# assert data, 'Cannot provide an empty dictionary'
#         row_count = len(list(data.values())[0])
#         assert row_count > 0, 'Must provide at least a single row'
#         assert all(
#             len(val_list) == row_count for val_list in data.values()), 'Must provide value lists of uniform length'


def infer_table_schema(metadata: MetaData, table_name: str, rows: Iterable[dict], primary_key: List[str]):
    # generate and execute a create table statement
    cols_to_types = {}
    columns = rows_to_columns(rows)
    for col_name, list_of_values in columns.items():
        # Just take the first value to by the type
        first_non_null = None
        for val in list_of_values:
            if val is not None:
                first_non_null = val
                break
            raise ValueError('Cannot provide an empty list, types cannot be inferred')
        cols_to_types[col_name] = _get_col_type(first_non_null, list_of_values)

    table = _get_table_def(metadata, table_name, cols_to_types, primary_key)
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
    elif type(sample_value) == date:
        return Date
    else:
        raise ValueError('Value of type {} is unsupported'.format(type(sample_value)))


def _get_table_def(metadata, table_name: str, cols_with_types: Mapping[str, str], primary_key: List[str] = None):
    _primary_key = primary_key or []
    columns = [Column(col_name, col_type, primary_key=col_name in _primary_key)
               for col_name, col_type in cols_with_types.items()]
    return Table(table_name, metadata, *columns)
