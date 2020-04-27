from typing import List, Mapping, Tuple
import logging
from typing import Iterable, Callable

logger = logging.getLogger(__name__)

TableUpdate = Tuple[Iterable[tuple], Iterable[tuple]]
DatabaseUpdate = Mapping[str, TableUpdate]
SourceReader = Callable[[List[str]], DatabaseUpdate]
TargetWriter = Callable[[DatabaseUpdate], None]


class DoltSync:
    """
    This object defines a sync operation between a source and a target, one of which is a Dolt repository. It is
    parametrized by functions that follow the interfaces defined above, and upon execution delegates it's behavior to
    the functions provided. We currently provide functions for reading from Dolt, and writing to MySQL.
    """
    def __init__(self, source_reader: SourceReader, target_writer: TargetWriter, table_map: Mapping[str, str]):
        """
        Crates an instance of DoltSync.
        :param source_reader: the reader that returns a mapping from table name to tuples given a list of tables
        :param target_writer: the writer that takes a mapping from table to tuples and writes the tuples to the table
        :param table_map: the mapping from tables names from source to target
        """
        self.source_reader = source_reader
        self.target_writer = target_writer
        self.table_map = table_map

    def sync(self):
        """
        Executes a sync using function parameters provided to the constructor. Data to sync is obtained by executing
        self.source_reader, and written using self.target_writer.
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


class TableMetadata:
    def __init__(self, name: str, columns: List[Column]):
        self.name = name
        self.columns = sorted(columns, key=lambda col: col.col_name)

