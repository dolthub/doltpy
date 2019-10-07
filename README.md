## DoltPy
This is DoltPy, the Python API for Dolt. Python is the language of choice for data science and data engineering, and thus we thought it would be wise to publish an API for building automated workflows on top of Dolt. 

### Installation
We are yet to publish this API to PyPi, but you can install from source using `pip` by cloning this repo into a directory, let's assume you used `doltpy`, then:
```bash
[doltpy] $ pip install .
```
Which will use `setup.py` to make the appropriate installation using a fresh build of the local copy of the source files.

### Usage
You will then have two commands at your disposal:
```bash
$ dolt-load
usage: dolt-load [-h] [-c] -d DOLT_DIR [-m MESSAGE] [-b BRANCH] [--dry_run]
                 dolt_load_module
dolt-load: error: the following arguments are required: dolt_load_module, -d/--dolt_dir
$ dolthub-load
usage: dolthub-load [-h] [-c] [-d DOLT_DIR] [-p] [-m MESSAGE] -r REMOTE_URL
                    [--clone] [-b BRANCH] [--dry_run]
                    dolt_load_module
dolthub-load: error: the following arguments are required: dolt_load_module, -r/--remote_url
```
Which will allow you to start using these scripts to load data into Dolt.

### Examples
See the [liquidata-etl-jobs](https://github.com/liquidata-inc/liquidata-etl-jobs) repo for examples.
