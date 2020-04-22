from typing import List, Mapping
import logging
from typing import Iterable, Callable
import re

logger = logging.getLogger(__name__)

SourceReader = Callable[[List[str]], Mapping[str, Iterable[tuple]]]
TargetWriter = Callable[[Mapping[str, Iterable[tuple]]], None]


class DoltSync:
    """
    This object defines a sync operation between a sourcea and a target, one of which is a Dolt repository.
    """
    def __init__(self, source_reader: SourceReader, target_writer: TargetWriter, table_map: Mapping[str, str]):
        """
        Get an instance of DoltSync
        :param source_reader: the reader that returns a mapping from table name to tuples given a list of tables
        :param target_writer: the writer that takes a mapping from table to tuples and writes the tuples to the table
        :param table_map: the mapping from tables names from source to target
        """
        self.source_reader = source_reader
        self.target_writer = target_writer
        self.table_map = table_map

    def sync(self):
        """
        Executes a sync using function parameters provided to the constructor
        :return:
        """
        to_sync = self.source_reader(list(self.table_map.keys()))
        remapped = {self.table_map[source_table]: source_data for source_table, source_data in to_sync.items()}
        self.target_writer(remapped)


class Column:

    def __init__(self, col_name: str, col_type: str, key: str = None):
        """

        :param col_name:
        :param col_type:
        :param key:
        """
        self.col_name = col_name
        self.col_type = col_type
        self.key = key

    def get_wildcard(self):
        # TODO we should implement an is_numeric function on the column object, and rename it ColumnMetadata
        # if 'decimal' in self.col_type:
        #     return '%d'
        # else:
        #     return '%s'
        return '%s'


class TableMetadata:

    def __init__(self, name: str, columns: List[Column]):
        self.name = name
        self.columns = sorted(columns, key=lambda col: col.col_name)

