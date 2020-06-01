from typing import List, Callable
import pandas as pd
from doltpy.core.dolt import Dolt
import logging
import io
import tempfile

logger = logging.getLogger(__name__)

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
