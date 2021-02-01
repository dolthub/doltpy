#!/bin/bash

set -xeou pipefail

DIR=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
BASE=$DIR/..

if [ -x poetry ] ; then
    echo "Install poetry"
    echo "curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python3 - --version=1.1.0b2"
    exit 1
fi

cd $BASE
poetry install
poetry run sphinx-apidoc doltpy -o docs/

cd $BASE/docs
poetry run make html
poetry run make markdown
