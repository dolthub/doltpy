#!/bin/bash

set -xeou pipefail

DIR=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
BASE=$DIR/..

REPO_NAME=
PYPI_URL=
USERNAME=
PASSWORD=

poetry config repositories.$REPO_NAME http://$PYPI_URL
poetry config http-basic.$REPO_NAME $USERNAME $PASSWORD

cd $BASE
poetry build
poetry publish -r doltpy $@
