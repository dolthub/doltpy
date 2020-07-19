from typing import Callable, List, Union
import io
from doltpy.core.dolt import Dolt
from doltpy.core.write import import_df, bulk_import, UPDATE
from doltpy.core.read import read_table
from doltpy.core.system_helpers import get_logger
import pandas as pd
import hashlib
import itertools
import tempfile

DoltTableWriter = Callable[[Dolt], str]
DoltLoader = Callable[[Dolt], str]
DataframeTransformer = Callable[[pd.DataFrame], pd.DataFrame]
FileTransformer = Callable[[io.StringIO], io.StringIO]

logger = get_logger(__name__)
INSERTED_ROW_HASH_COL = 'hash_id'
INSERTED_COUNT_COL = 'count'


def _apply_df_transformers(data: pd.DataFrame, transformers: List[DataframeTransformer]) -> pd.DataFrame:
    if not transformers:
        return data
    temp = data.copy()
    for transformer in transformers:
        temp = transformer(data)
    return temp


def _apply_file_transformers(data: io.StringIO, transformers: List[FileTransformer]) -> io.StringIO:
    data.seek(0)
    if not transformers:
        return data
    temp = transformers[0](data)
    for transformer in transformers[1:]:
        temp = transformer(temp)

    return temp


def get_bulk_table_writer(table: str,
                          get_data: Callable[[], io.StringIO],
                          pk_cols: List[str],
                          import_mode: str = None,
                          transformers: List[FileTransformer] = None) -> DoltTableWriter:
    """
    Returns a function that takes a Dolt repository object and writes the contents of the file like object returned by
    the function parameter `get_data` to the table specified using the primary keys passed. Optionally toggle the import
    mode and apply a list of transformers to do some data cleaning operations. For example, we might apply a transformer
    that converts some date strings to proper datetime objects.
    :param table:
    :param get_data:
    :param pk_cols:
    :param import_mode:
    :param transformers:
    :return:
    """
    def inner(repo: Dolt):
        _import_mode = import_mode or ('create' if table not in [t.name for t in repo.ls()] else 'update')
        data_to_load = _apply_file_transformers(get_data(), transformers)
        bulk_import(repo, table, data_to_load, pk_cols, import_mode=_import_mode)
        return table

    return inner


def get_df_table_writer(table: str,
                        get_data: Callable[[], pd.DataFrame],
                        pk_cols: List[str],
                        import_mode: str = None,
                        transformers: List[DataframeTransformer] = None) -> DoltTableWriter:
    """
    Returns a function that takes a Dolt repository object and writes the Pandas DataFrame returned by the function
    parameter `get_data` to the table specified using the primary keys passed. Optionally toggle the import mode and
    apply a list of transformers to do some data cleaning operations. For example, we might apply a transformer that
    converts some date strings to proper datetime objects.
    :param table:
    :param get_data:
    :param pk_cols:
    :param import_mode:
    :param transformers:
    :return:
    """
    def inner(repo: Dolt):
        _import_mode = import_mode or ('create' if table not in [t.name for t in repo.ls()] else 'update')
        data_to_load = _apply_df_transformers(get_data(), transformers)
        import_df(repo, table, data_to_load, pk_cols, import_mode=_import_mode)
        return table

    return inner


def get_table_transformer(get_data: Callable[[Dolt], pd.DataFrame],
                          target_table: str,
                          target_pk_cols: List[str],
                          transformer: DataframeTransformer,
                          import_mode: str = UPDATE) -> DoltTableWriter:
    """
    A version of get_df_table writer where the input is a Dolt repository. This is used for transforming raw data into
    derived tables.
    :param get_data:
    :param target_table:
    :param target_pk_cols:
    :param transformer:
    :param import_mode:
    :return:
    """
    def inner(repo: Dolt):
        input_data = get_data(repo)
        transformed_data = transformer(input_data)
        import_df(repo, target_table, transformed_data, target_pk_cols, import_mode=import_mode)
        return target_table

    return inner


def get_unique_key_table_writer(table: str,
                                get_data: Callable[[], pd.DataFrame],
                                import_mode: str = UPDATE,
                                transformers: List[DataframeTransformer] = None) -> DoltTableWriter:
    """
    This is a convenience function wrapping for loading data when using the `insert_primary_key` transformer to
    generate a unique key.
    :param table:
    :param get_data:
    :param import_mode:
    :param transformers:
    :return:
    """
    _transformers = transformers + [insert_unique_key] if transformers else [insert_unique_key]
    create = get_df_table_writer(table, get_data, [INSERTED_ROW_HASH_COL], import_mode, _transformers)
    update = _get_unique_key_update_writer(table, get_data, transformers)
    return create if import_mode != UPDATE else update


def insert_unique_key(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function takes Pandas `DataFrame` and inserts a unique hash to each row created from the row itself, along
    with a count of how many rows produce the same hash. The idea is to provide some rudimentary tools for writing data
    with unique keys.
    :param df:
    :return:
    """
    assert INSERTED_ROW_HASH_COL not in df.columns and INSERTED_COUNT_COL not in df.columns, 'Require hash_id and count not in df'
    ids = df.apply(lambda r: hashlib.md5(','.join([str(el) for el in r]).encode('utf-8')).hexdigest(), axis=1)
    with_id = df.assign(hash_id=ids).set_index(INSERTED_ROW_HASH_COL)
    count_by_id = with_id.groupby(INSERTED_ROW_HASH_COL).size()
    with_id.loc[:, INSERTED_COUNT_COL] = count_by_id
    unique = with_id.reset_index().drop_duplicates(subset=[INSERTED_ROW_HASH_COL])
    return unique


def _get_unique_key_update_writer(table: str,
                                  get_data: Callable[[], pd.DataFrame],
                                  transformers: List[DataframeTransformer] = None) -> DoltTableWriter:
    def inner(repo: Dolt):
        _transformers = transformers + [insert_unique_key] if transformers else [insert_unique_key]
        data = _apply_df_transformers(get_data(), _transformers)
        if table not in [t.name for t in repo.ls()]:
            raise ValueError('Missing table')

        # Get existing PKs
        existing = read_table(repo, table)
        existing_pks = existing[INSERTED_ROW_HASH_COL].to_list()

        # Get proposed PKs
        proposed_pks = data[INSERTED_ROW_HASH_COL].to_list()
        to_drop = [existing for existing in existing_pks if existing not in proposed_pks]

        if to_drop:
            iterator = iter(to_drop)
            while iterator:
                batch = list(itertools.islice(iterator, 30000))
                if len(batch) == 0:
                    break

            logger.info('Dropping batch of {} IDs from table {}'.format(len(batch), table))
            drop_statement = '''
            DELETE FROM {table} WHERE {pk} in ("{pks_to_drop}")
            '''.format(table=table, pk=INSERTED_ROW_HASH_COL, pks_to_drop='","'.join(batch))
            repo.sql(query=drop_statement)

        new_data = data[~(data[INSERTED_ROW_HASH_COL].isin(existing_pks))]
        if not new_data.empty:
            logger.info('Importing {} records'.format(len(new_data)))
            import_df(repo, table, new_data, [INSERTED_ROW_HASH_COL], 'update')

        return table

    return inner


def get_dolt_loader(writer_or_writers: Union[DoltTableWriter, List[DoltTableWriter]],
                    commit: bool,
                    message: str,
                    branch: str = 'master',
                    transaction_mode: bool = None) -> DoltLoader:
    """
    Given a repo and a set of table loaders, run the table loaders and conditionally commit the results with the
    specified message on the specified branch. If transaction_mode is true then ensure all loaders/transformers are
    successful, or all are rolled back.
    :param writer_or_writers:
    :param commit:
    :param message:
    :param branch:
    :param transaction_mode:
    :return: the branch written to
    """
    if type(writer_or_writers) == list:
        writers = writer_or_writers
    else:
        writers = [writer_or_writers]

    def inner(repo: Dolt):
        current_branch, current_branch_list = repo.branch()
        original_branch = current_branch.name

        if branch != original_branch and not commit:
            raise ValueError('If writes are to another branch, and commit is not True, writes will be lost')

        if current_branch.name != branch:
            logger.info('Current branch is {}, checking out {}'.format(current_branch.name, branch))
            if branch not in [b.name for b in current_branch_list]:
                logger.info('{} does not exist, creating'.format(branch))
                repo.branch(branch_name=branch)
            repo.checkout(branch)

        if transaction_mode:
            raise NotImplementedError('transaction_mode is not yet implemented')

        tables_updated = [writer(repo) for writer in writers]

        if commit:
            if not repo.status().is_clean:
                logger.info('Committing to repo located in {} for tables:\n{}'.format(repo.repo_dir, tables_updated))
                for table in tables_updated:
                    repo.add(table)
                repo.commit(message)

            else:
                logger.warning('No changes to repo in:\n{}'.format(repo.repo_dir))

        current_branch, branches = repo.branch()
        if original_branch != current_branch.name:
            logger.info('Checked out {} from {}, checking out {} to restore state'.format([b.name for b in branches],
                                                                                          original_branch,
                                                                                          original_branch))
            repo.checkout(original_branch)

        return branch

    return inner


def get_branch_creator(new_branch_name: str, refspec: str = None):
    """
    Returns a function that creates a branch at the specified refspec.
    :param new_branch_name:
    :param refspec:
    :return:
    """
    def inner(repo: Dolt):
        _, current_branches = repo.branch()
        branches = [branch.name for branch in current_branches]
        assert new_branch_name not in branches, 'Branch {} already exists'.format(new_branch_name)
        logger.info('Creating new branch on repo in {} named {} at refspec {}'.format(repo.repo_dir,
                                                                                      new_branch_name,
                                                                                      refspec))
        repo.branch(new_branch_name)

        return new_branch_name

    return inner


def create_table_from_schema_import(repo: Dolt,
                                    table: str,
                                    pks: List[str],
                                    path: str,
                                    commit: bool = True,
                                    commit_message: str = None):
    """
    Execute Dolt.schema_import_create(...) against a file with a specified set of primary key columns, and optionally
    commit the created table.
    :param repo:
    :param table:
    :param pks:
    :param path:
    :param commit:
    :param commit_message:
    :return:
    """
    _create_table_from_schema_import_helper(repo, table, pks, path, commit=commit, commit_message=commit_message)


def create_table_from_schema_import_unique_key(repo: Dolt,
                                               table: str,
                                               path: str,
                                               commit: bool = True,
                                               commit_message: str = None):
    """
    Execute Dolt.schema_import_create(...) against a file where we will use insert_unique_key(...) to create a unique
    key on this data. The standard "hash_id" column name will be used for the unique key.
    :param repo:
    :param table:
    :param path:
    :param commit:
    :param commit_message:
    :return:
    """
    _create_table_from_schema_import_helper(repo,
                                            table,
                                            [INSERTED_ROW_HASH_COL],
                                            path,
                                            [insert_unique_key],
                                            commit,
                                            commit_message)


def _create_table_from_schema_import_helper(repo: Dolt,
                                            table: str,
                                            pks: List[str],
                                            path: str,
                                            transformers: List[DataframeTransformer] = None,
                                            commit: bool = True,
                                            commit_message: str = None):
    if transformers:
        fp = tempfile.NamedTemporaryFile(suffix='.csv')
        temp = pd.read_csv(path)
        transformed = _apply_df_transformers(temp, transformers)
        transformed.to_csv(fp.name, index=False)
        path = fp.name

    repo.schema_import(table=table, pks=pks, filename=path, create=True)

    if commit:
        message = commit_message or 'Creating table {}'.format(table)
        repo.add(table)
        repo.commit(message)
