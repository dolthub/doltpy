from sqlalchemy.engine import Engine
from doltpy.core.dolt import Dolt
import pandas as pd
import tempfile
from doltpy.core.system_helpers import get_logger

logger = get_logger(__name__)


def read_table(repo: Dolt, table_name: str, delimiter: str = ',') -> pd.DataFrame:
    """
    Reads the contents of a table and returns it as a Pandas `DataFrame`. Under the hood this uses export and the
    filesystem, in short order we are likley to replace this with use of the MySQL Server.
    :param repo:
    :param table_name:
    :param delimiter:
    :return:
    """
    fp = tempfile.NamedTemporaryFile(suffix='.csv')
    repo.execute(['table', 'export', table_name, fp.name, '-f'])
    result = pd.read_csv(fp.name, delimiter=delimiter)
    return result


def pandas_read_sql(query: str, engine: Engine) -> pd.DataFrame:
    """
    Execute a SQL statement against the MySQL Server running on port 3306 and return the result as a Pandas
    `DataFrame` object. This is a higher level version of `query_server` where the object returned is the cursor
    associated with query executed.
    :param query:
    :param engine:
    :return:
    """
    return pd.read_sql(query, con=engine)
