## DoltPy
This is DoltPy, the Python API for Dolt. Dolt is an open source relational database that Git-like version control semantics. You can read more about Dolt on its GitHub [page](https://github.com/liquidata-inc/dolt). DoltHub is a platform for hosting Dolt databases that provides an additional set of collaboration tools for Dolt users allowing them to collabroatively build datasets. You can read more about how to get started with DoltHub in this getting started [blog post](https://www.dolthub.com/blog/2020-02-03-dolt-and-dolthub-getting-started/). 

Returning to Python, it is the language of choice for data science and data engineering, and thus we thought it would be wise to publish an API for building automated workflows on top of Dolt.

### Installation
`doltpy` is published on PyPi, and thus can be easily installed using `pip`:
```
pip install doltpy
```

### Layout
The top level package is organized into two subpackages, `core` and `etl`. `etl` depends on `core`, but not vice versa. 

### Shell Usage
The package installs two shim commands, shell scripts that wrap Python code. They are utilities for moving data into Dolt and DoltHub, and have help menus describing their parameters.
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
`dolthub-load` does something similar, and has an expanded set of tools for dealing with remotes hosted on DoltHub:
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

### Python Usage
The fundamental object of interest in Dolt is the repo, terminology borrowed from Git. The first decision to make when using Git is what directory you would like to version. Dolt is similar, and Doltpy uses a `Dolt` object in `doltpy.core` package to represent this mapping:
```python
from doltpy.core imoprt Dolt
repo = Dolt('some_dir')
repo.init_new_repo()
```
This creates a new repo in `some_dir`, which you can now perform basic operations on, such as:
```
import pandas as pd
df = pd.read_csv('my_csv.csv')
repo.import_df(test_table', df, ['c1', 'c2'])
repo.add_table_to_next_commit('test_table')
repo.commit('Created test table')
```
Thus we have created a repo, created a table, written some data to it, and created a commit. These the fundamental operations the brief tutorial on the Dolt GitHub page covers, but executed via Python. We also exposed use of Dolt's MySQL Server implementation:
```
repo.start_server()
df = pandas_read_sql(self, 'select * from test_table')
```
And thus we have now used Dolt's SQL interface and and ODBC connection to connect and execute a SQL query, which we in turn rendered as a `pd.DataFrame`.

This concludes a brief tour of `doltpy.core`, see the [liquidata-etl-jobs](https://github.com/liquidata-inc/liquidata-etl-jobs) repo for examples of using the `doltpy.etl` package, where we have implemented a number of ETL jobs to push data to DoltHub.
