from doltpy.dolt import Dolt
import pandas as pd
import hashlib
from typing import Callable, List

Transformer = Callable[[pd.DataFrame], pd.DataFrame]


# TODO this currently requires a commit per dataset, might need a grouping
# TODO this currently has no concept of branch, but it probably should
class Dataset:

    def __init__(self,
                 dolt_table_name: str,
                 get_data: Callable[[], pd.DataFrame],
                 pk_cols: List[str],
                 transformers: List[Transformer] = None):
        self.dolt_table_name = dolt_table_name
        self.get_data = get_data
        self.transformers = transformers or []
        self.pk_cols = pk_cols

    def _apply_transformers(self, data: pd.DataFrame) -> pd.DataFrame:
        if not self.transformers:
            return data
        temp = data.copy()
        for transformer in self.transformers:
            temp = transformer(data)
        return temp

    def load_data(self, repo: Dolt):
        data_to_load = self._apply_transformers(self.get_data())
        repo.import_df(self.dolt_table_name,
                       data_to_load,
                       self.pk_cols,
                       create=self.dolt_table_name not in repo.get_exisitng_tabels())


class ETLWorkload:

    def __init__(self, repo: Dolt, datasets: List[Dataset]):
        self.repo = repo
        self.datasets = datasets

    def load_to_dolt(self, commit: bool, message: str = None):
        assert not commit or message is not None, 'When commit is True, must provide message'
        print('Loading Dolt repo at {}'.format(self.repo.repo_dir))
        for dataset in self.datasets:
            print('Loading data to table {} with primary keys {}'.format(dataset.dolt_table_name, dataset.pk_cols))
            dataset.load_data(self.repo)

        new_tables, changes = self.repo.get_dirty_tables()

        if not (new_tables or changes):
            print('No changes detected in upstream data sources, all done')
        else:
            for table in [table for table, staged in list(new_tables.items()) + list(changes.items()) if not staged]:
                print("Adding {}".format(table))
                self.repo.add_table_to_next_commit(table)

            if commit:
                print('Committing changes')
                self.repo.commit(message)


def insert_unique_key(df: pd.DataFrame) -> pd.DataFrame:
    assert 'hash_id' not in df.columns and 'count' not in df.columns, 'Require hash_id and count not in df'
    ids = df.apply(lambda r: hashlib.md5(','.join([str(el) for el in r]).encode('utf-8')).hexdigest(), axis=1)
    with_id = df.assign(hash_id=ids).set_index('hash_id')
    count_by_id = with_id.groupby('hash_id').size()
    with_id.loc[:, 'count'] = count_by_id
    return with_id.reset_index()
