from typing import List, Mapping, Tuple
import logging
from typing import Iterable, Callable

logger = logging.getLogger(__name__)

# Types that reflect the different nature of the syncs
DoltTableUpdate = Tuple[Iterable[tuple], Iterable[tuple]]
TableUpdate = Iterable[tuple]

# For using Dolt as the target
DoltAsTargetUpdate = Mapping[str, TableUpdate]
DoltAsTargetReader = Callable[[List[str]], DoltAsTargetUpdate]
DoltAsTargetWriter = Callable[[DoltAsTargetUpdate], None]

# For using Dolt as the source
DoltAsSourceUpdate = Mapping[str, DoltTableUpdate]
DoltAsSourceReader = Callable[[List[str]], DoltAsSourceUpdate]
DoltAsSourceWriter = Callable[[DoltAsSourceUpdate], None]


def sync_to_dolt(source_reader: DoltAsTargetReader, target_writer: DoltAsTargetWriter, table_map: Mapping[str, str]):
    """
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
    """
    _sync_helper(source_reader, target_writer, table_map)


def sync_from_dolt(source_reader: DoltAsSourceReader, target_writer: DoltAsSourceWriter, table_map: Mapping[str, str]):
    """
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
    """
    _sync_helper(source_reader, target_writer, table_map)


def _sync_helper(source_reader, target_writer, table_map: Mapping[str, str]):
    to_sync = source_reader(list(table_map.keys()))
    remapped = {table_map[source_table]: source_data for source_table, source_data in to_sync.items()}
    target_writer(remapped)


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

