import abc
import datetime
from typing import Generic, List, Mapping, Dict, Tuple, TypeVar, Union, Optional

__all__ = ["DoltT"]

_T = TypeVar("_T")


class DoltT(Generic[_T]):

    _repo_dir: str

    @abc.abstractmethod
    def repo_dir(self):
        ...

    @property
    @abc.abstractmethod
    def repo_name(self):
        ...

    @abc.abstractmethod
    def execute(self, args: List[str], print_output: bool = True) -> List[str]:
        ...

    @staticmethod
    @abc.abstractmethod
    def init(repo_dir: Optional[str] = None) -> "Dolt":  # type: ignore
        ...

    @staticmethod
    @abc.abstractmethod
    def version():
        ...

    @abc.abstractmethod
    def status(self) -> "DoltStatus":  # type: ignore
        ...

    @abc.abstractmethod
    def add(self, table_or_tables: Union[str, List[str]]):
        ...

    @abc.abstractmethod
    def reset(
        self,
        table_or_tables: Union[str, List[str]],
        hard: bool = False,
        soft: bool = False,
    ):
        ...

    @abc.abstractmethod
    def commit(
        self,
        message: str = "",
        allow_empty: bool = False,
        date: Optional[datetime.datetime] = None,
    ):
        ...

    @abc.abstractmethod
    def merge(self, branch: str, message: str, squash: bool = False):
        ...

    @abc.abstractmethod
    def sql(
        self,
        query: Optional[str] = None,
        result_format: Optional[str] = None,
        execute: bool = False,
        save: Optional[str] = None,
        message: Optional[str] = None,
        list_saved: bool = False,
        batch: bool = False,
        multi_db_dir: Optional[str] = None,
    ):
        ...

    @abc.abstractmethod
    def _parse_tabluar_output_to_dict(self, args: List[str]):
        ...

    @abc.abstractmethod
    def log(self, number: Optional[int] = None, commit: Optional[str] = None) -> Dict:
        ...

    @abc.abstractmethod
    def diff(
        self,
        commit: Optional[str] = None,
        other_commit: Optional[str] = None,
        table_or_tables: Optional[Union[str, List[str]]] = None,
        data: bool = False,
        schema: bool = False,  # can we even support this?
        summary: bool = False,
        sql: bool = False,
        where: Optional[str] = None,
        limit: Optional[int] = None,
    ):
        ...

    @abc.abstractmethod
    def blame(self, table_name: str, rev: Optional[str] = None):
        ...

    @abc.abstractmethod
    def branch(
        self,
        branch_name: Optional[str] = None,
        start_point: Optional[str] = None,
        new_branch: Optional[str] = None,
        force: bool = False,
        delete: bool = False,
        copy: bool = False,
        move: bool = False,
    ):
        ...

    @abc.abstractmethod
    def _get_branches(self) -> Tuple["DoltBranch", List["DoltBranch"]]:  # type: ignore
        ...

    @abc.abstractmethod
    def checkout(
        self,
        branch: Optional[str] = None,
        table_or_tables: Optional[Union[str, List[str]]] = None,
        checkout_branch: bool = False,
        start_point: Optional[str] = None,
    ):
        ...

    @abc.abstractmethod
    def remote(
        self,
        add: bool = False,
        name: Optional[str] = None,
        url: Optional[str] = None,
        remove: Optional[bool] = None,
    ):
        ...

    @abc.abstractmethod
    def push(
        self,
        remote: str,
        refspec: Optional[str] = None,
        set_upstream: bool = False,
        force: bool = False,
    ):
        ...

    @abc.abstractmethod
    def pull(self, remote: str = "origin"):
        ...

    @abc.abstractmethod
    def fetch(
        self,
        remote: str = "origin",
        refspec_or_refspecs: Optional[Union[str, List[str]]] = None,
        force: bool = False,
    ):
        ...

    @staticmethod
    @abc.abstractmethod
    def clone(remote_url: str, new_dir: Optional[str] = None, remote: Optional[str] = None, branch: Optional[str] = None) -> "Dolt":  # type: ignore
        ...

    @classmethod
    @abc.abstractmethod
    def _new_dir_helper(cls, new_dir: str, remote_url: str):
        ...

    @staticmethod
    @abc.abstractmethod
    def read_tables(
        remote_url: str,
        committish: str,
        table_or_tables: Optional[Union[str, List[str]]] = None,
        new_dir: Optional[str] = None,
    ) -> "Dolt":  # type: ignore
        ...

    @abc.abstractmethod
    def creds_new(self) -> bool:
        ...

    @abc.abstractmethod
    def creds_rm(self, public_key: str) -> bool:
        ...

    @abc.abstractmethod
    def creds_ls(self) -> List["DoltKeyPair"]:  # type: ignore
        ...

    @abc.abstractmethod
    def creds_check(self, endpoint: Optional[str] = None, creds: Optional[str] = None) -> bool:
        ...

    @abc.abstractmethod
    def creds_use(self, public_key_id: str) -> bool:
        ...

    @abc.abstractmethod
    def creds_import(self, jwk_filename: str, no_profile: str):
        ...

    @classmethod
    @abc.abstractmethod
    def config_global(
        cls,
        name: Optional[str] = None,
        value: Optional[str] = None,
        add: bool = False,
        list: bool = False,
        get: bool = False,
        unset: bool = False,
    ) -> Mapping[str, str]:
        ...

    @abc.abstractmethod
    def config_local(
        self,
        name: Optional[str] = None,
        value: Optional[str] = None,
        add: bool = False,
        list: bool = False,
        get: bool = False,
        unset: bool = False,
    ) -> Mapping[str, str]:
        ...

    @classmethod
    @abc.abstractmethod
    def _config_helper(
        cls,
        global_config: bool = False,
        local_config: bool = False,
        cwd: Optional[str] = None,
        name: Optional[str] = None,
        value: Optional[str] = None,
        add: bool = False,
        list: bool = False,
        get: bool = False,
        unset: bool = False,
    ) -> Mapping[str, str]:
        ...

    @abc.abstractmethod
    def ls(self, system: bool = False, all: bool = False) -> List["DoltTable"]:  # type: ignore
        ...

    @abc.abstractmethod
    def schema_export(self, table: str, filename: Optional[str] = None):
        ...

    @abc.abstractmethod
    def schema_import(
        self,
        table: str,
        filename: str,
        create: bool = False,
        update: bool = False,
        replace: bool = False,
        dry_run: bool = False,
        keep_types: bool = False,
        file_type: Optional[str] = None,
        pks: Optional[List[str]] = None,
        map: Optional[str] = None,
        float_threshold: Optional[float] = None,
        delim: Optional[str] = None,
    ):
        ...

    @abc.abstractmethod
    def schema_show(self, table_or_tables: Union[str, List[str]], commit: Optional[str] = None):
        ...

    @abc.abstractmethod
    def table_rm(self, table_or_tables: Union[str, List[str]]):
        ...

    @abc.abstractmethod
    def table_import(
        self,
        table: str,
        filename: str,
        create_table: bool = False,
        update_table: bool = False,
        force: bool = False,
        mapping_file: Optional[str] = None,
        pk: Optional[List[str]] = None,
        replace_table: bool = False,
        file_type: Optional[str] = None,
        continue_importing: bool = False,
        delim: Optional[str] = None,
    ):
        ...

    @abc.abstractmethod
    def table_export(
        self,
        table: str,
        filename: str,
        force: bool = False,
        schema: Optional[str] = None,
        mapping_file: Optional[str] = None,
        pk: Optional[List[str]] = None,
        file_type: Optional[str] = None,
        continue_exporting: bool = False,
    ):
        ...

    @abc.abstractmethod
    def table_mv(self, old_table: str, new_table: str, force: bool = False):
        ...

    @abc.abstractmethod
    def table_cp(
        self,
        old_table: str,
        new_table: str,
        commit: Optional[str] = None,
        force: bool = False,
    ):
        ...
