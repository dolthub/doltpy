import argparse
from doltpy.dolt import Dolt
import os
import tempfile
from doltpy_etl.helpers import load_to_dolt


def loader(dolt_load_module: str,
           dolt_dir: str,
           clone: bool,
           branch: str,
           commit: bool,
           push: bool,
           remote: str,
           message: str,
           dry_run: bool):
    if clone:
        assert remote, 'If clone is True then remote must be passed'
        temp_dir = tempfile.mkdtemp()
        print('Clone is set to true, so ignoring dolt_dir')
        repo = Dolt(temp_dir)
        if clone:
            print('Clone set to True, cloning remote {}'.format(remote))
        repo.clone(remote)
    else:
        assert os.path.exists(os.path.join(dolt_dir, '.dolt')), 'Repo must exist locally if not cloned'
        repo = Dolt(dolt_dir)

    print(
        '''Commencing to load to DoltHub with the following options, and the following options
                        - module    {dolt_load_module}
                        - dolt_dir  {dolt_dir}
                        - commit    {commit}
                        - branch    {branch}
                        - clone     {clone}
                        - remote    {remote}
                        - push      {push}
        '''.format(dolt_load_module=dolt_load_module,
                   dolt_dir=repo.repo_dir,
                   commit=commit,
                   branch=branch,
                   push=push,
                   clone=clone,
                   remote=remote))

    load_to_dolt(repo, dolt_load_module, commit, message, dry_run, branch)

    if push:
        print('Pushing changes to remote {} on branch {}'.format(remote, branch))
        repo.push(remote, branch)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('dolt_load_module', )
    parser.add_argument('-c', '--commit', action='store_true')
    parser.add_argument('-d', '--dolt_dir', type=str, help='The dolt dir')
    parser.add_argument('-p', '--push', action='store_true', help='Push changes to remote, must sepcify arg --remote')
    parser.add_argument('-m', '--message', type=str, help='Commit message associated with the commit')
    parser.add_argument('-r', '--remote_url', type=str, help='DoltHub remote being used', required=True)
    parser.add_argument('--clone', action='store_true', help='Clone the remote to the local machine')
    parser.add_argument('-b', '--branch', type=str, help='Branch to write to, default is master', default='master')
    parser.add_argument('--dry_run', action='store_true')
    args = parser.parse_args()
    loader(dolt_load_module=args.dolt_load_module,
           dolt_dir=args.dolt_dir,
           clone=args.clone,
           commit=args.commit,
           push=args.push,
           remote=args.remote_url,
           message=args.message,
           dry_run=args.dry_run,
           branch=args.branch)


if __name__ == '__main__':
    main()
