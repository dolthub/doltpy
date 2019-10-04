import importlib
from doltpy import Dolt, DoltException
from doltpy_etl.tools import ETLWorkload


def load_to_dolt(repo: Dolt, dolt_load_module: str, commit: bool, message: str, dry_run: bool, branch: str):
    if branch not in repo.get_branch_list():
        raise DoltException('Trying to update branch {} that does not exist, branches are:\n {}'.format(branch,
                                                                                                        repo.get_branch_list()))
    if repo.get_current_branch() != branch:
        print('Current branch is {}, checking out {}'.format(repo.get_current_branch(), branch))
        repo.checkout(branch)
    retrieved_module = importlib.import_module(dolt_load_module)
    if hasattr(retrieved_module, 'get_dolt_datasets'):
        datasets = retrieved_module.get_dolt_datasets()

        if not dry_run:
            ETLWorkload(repo, datasets).load_to_dolt(commit=commit, message=message)
        else:
            print('Doing nothing, in dry run mode...')

    else:
        raise AttributeError('Missing required method get_dolt_datasets on module {}'.format(dolt_load_module))

