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
Doltpy is broken up into modules. The most important modules, `doltpy.core`, effectively wraps the Dolt command-line-interface (CLI) that is exposed by the Go binary. 

### `doltpy.core`


### `doltpy.core.read` and `doltpy.core.write`


### `doltpy.etl`


### `doltpy.etl.sql_sync`



## More Information
As alluded to above, you can find a more detailed description of Doltpy [here](https://www.dolthub.com/docs/reference/python/).
