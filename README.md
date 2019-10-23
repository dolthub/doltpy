## DoltPy
This is DoltPy, the Python API for Dolt. Python is the language of choice for data science and data engineering, and thus we thought it would be wise to publish an API for building automated workflows on top of Dolt. 

### Installation
`doltpy` is published on PyPi, and thus can be easily installed using `pip`:
```
pip install doltpy
```

### Layout
The top level package is organized into two subpackages, `core` and `etl`. `etl` depends on `core`, but not vice versa. 

### Usage
You will then have two commands at your disposal:
```bash
$ dolt-load --help
usage: dolt-load [-h] --dolt-dir DOLT_DIR [--commit] [--message MESSAGE]
                 [--branch BRANCH] [--dry-run]
                 dolt_load_module

positional arguments:
  dolt_load_module     Fully qualified path to a module providing a set of
                       loaders

optional arguments:
  -h, --help           show this help message and exit
  --dolt-dir DOLT_DIR  The directory of the Dolt repo being loaded to
  --commit
  --message MESSAGE    Commit message to assciate created commit (requires
                       --commit)
  --branch BRANCH      Branch to write to, default is master
  --dry-run            Print out parameters, but don't do anything
```
And then `dolthub-load` has an expanded set of tools for dealing with remotes:
```
$ dolthub-load --help
usage: dolthub-load [-h] [--dolt-dir DOLT_DIR] [--commit] [--message MESSAGE]
                    [--branch BRANCH] [--clone] --remote-url REMOTE_URL
                    [--remote-name REMOTE_NAME] [--push] [--dry-run]
                    dolt_load_module

positional arguments:
  dolt_load_module      Fully qualified path to a module providing a set of
                        loaders

optional arguments:
  -h, --help            show this help message and exit
  --dolt-dir DOLT_DIR   The directory of the Dolt repo being loaded to
  --commit
  --message MESSAGE     Commit message to assciate created commit (requires
                        --commit)
  --branch BRANCH       Branch to write to, default is master
  --clone               Clone the remote to the local machine
  --remote-url REMOTE_URL
                        DoltHub remote being used
  --remote-name REMOTE_NAME
                        Alias for remote, default is origin
  --push                Push changes to remote, must sepcify arg --remote
  --dry-run             Print out parameters, but don't do anything
```
Which will allow you to start using these scripts to load data into Dolt.

### Examples
See the [liquidata-etl-jobs](https://github.com/liquidata-inc/liquidata-etl-jobs) repo for examples of using the package as a dependency rather than a via the command line.
