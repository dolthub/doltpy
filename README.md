## DoltPy
This is DoltPy, the Python API for [Dolt](https://github.com/liquidata-inc/dolt). Python is the language of choice for data science and data engineering, and thus we thought it would be wise to publish an API for building automated workflows on top of Dolt and [DoltHub](https://www.dolthub.com/), a collaboration platform for Dolt databases.

## Installation
You need to install Dolt, which is documented [here](https://www.dolthub.com/docs/tutorials/installation/). It's easy for *nix users:
```
$ sudo bash -c 'curl -L https://github.com/liquidata-inc/dolt/releases/latest/download/install.sh | sudo bash'
```
We also distribute Dolt as a Homebrew formula:
```
$ brew install dolt
```
Finally, for Windows users our release page has `.zip` and `.msi` files.

Once Dolt is installed you can install Doltpy using `pip`:
```
$ pip install doltpy
```

## Overview
Doltpy is broken up into modules. 

### `doltpy.core`
This is the most important module. It effectively wraps the Dolt command-line-interface (CLI) that is exposed by the Go binary. The CLI is exposed more or less exactly as it is implemented, returning wrapper objects where appropriate.

### `doltpy.core.read` and `doltpy.core.write`
These modules provide basic read and write interfaces for reading and writing a variety of tabular data formats, including:
- CSV
- `pandas.DataFrame`
- Python dictionaries of lists, i.e. `{'col': [...vals...], ...}`
- Python lists of dictionaries, i.e. `[{'col': val, ...}, ...]`

### `doltpy.etl`
This module provides a set of tools for scripting ETL/ELT workflows. At Liquidata we use it internally to push datasets onto DoltHub.

### `doltpy.etl.sql_sync`
Provides a set of tools for syncing between Dolt and supported relational databases, currently MySQL and Postgres. There is guide on our [documentation site](https://www.dolthub.com/docs/guides/sql-sync/).


## More Information
As alluded to above, you can find a more detailed description of Doltpy [here](https://www.dolthub.com/docs/reference/python/).
