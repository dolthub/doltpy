# Current State - Deprecated

DoltPy was created in an era where Dolt was a command-line tool. We've worked very hard to make Dolt a MySQL compatible database. To use Dolt with Python, start a Dolt SQL server using `dolt sql-server` and connect with [any Python MySQL client](https://docs.dolthub.com/sql-reference/supported-clients/clients#python). Use the exposed Dolt [stored procedures](https://docs.dolthub.com/sql-reference/version-control/dolt-sql-procedures) or [system tables](https://docs.dolthub.com/sql-reference/version-control/dolt-system-tables) to access version control functionality. Popular libraries like Pandas all support MySQL connectivity.

Dolt MySQL client support works in any language, not just Python. Thus, it is more time efficient for us to focus on that interface.

## DoltPy
This is DoltPy, the Python API for [Dolt](https://github.com/dolthub/dolt). Python is the language of choice for data science and data engineering, and thus we thought it would be wise to publish an API for building automated workflows on top of Dolt and [DoltHub](https://www.dolthub.com/), a collaboration platform for Dolt databases.

## Installation
You need to install Dolt, which is documented [here](https://docs.dolthub.com/introduction/installation). It's easy for *nix users:
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

### `doltpy.cli`
This is the most important module. It effectively wraps the Dolt command-line-interface (CLI) that is exposed by the Go binary. The CLI is exposed more or less exactly as it is implemented, returning wrapper objects where appropriate.

It's implementation has moved to a separate repository [here](https://github.com/dolthub/doltcli)

#### `doltpy.cli.read` and `doltpy.cli.write`
These modules provide basic read and write interfaces for reading and writing a variety of tabular data formats, including:
- CSV files
- `pandas.DataFrame`
- columns, that is dictionaries of lists, i.e. `{'col': [...vals...], ...}`
- rows, that is lists of dictionaries, i.e. `[{'col': val, ...}, ...]`

### `doltpy.sql`
This module provides tools for interacting with Dolt via a Python based SQL connector. The most important class is `DoltSQLContext`, which has concrete subclasses `DoltSQLServerContext` and `DoltSQLEngineContext`. `DoltSQLServerContext` is for users that want to write Python scripts that use and manage the Dolt SQL Server instance as a child process. `DoltSQLEngineContext` is for users who want to interact with a remote Dolt SQL Server.

These classes have equivalents of the read and write functions in `doltpy.cli.read` and `doltpy.cli.write` for writing CSV files, `pandas.DataFrame` objects, rows, and columns.

#### `doltpy.sql.sql_sync`
This package provides tools for syncing data to and from Dolt, and other relational databases. Currently there is support for MySQL, Postgres, and Oracle. You can find a more detailed description of how to use SQL Sync tools [here](https://docs.dolthub.com/guides/sql-sync).

### `doltpy.etl`
This module provides a set of tools for scripting ETL/ELT workflows. At Liquidata we use it internally to push datasets onto DoltHub.

## More Information
As alluded to above, you can find a more detailed description of Doltpy [here](https://docs.dolthub.com/guides/python/doltpy).
