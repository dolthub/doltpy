from distutils.dir_util import copy_tree
import os
import pathlib
import shutil
import tempfile

import nox


@nox.session(python=["3.8", "3.7", "3.6"])
# @nox.parametrize("numpy", ["1.17.5"])
# @nox.parametrize("pillow", ["^5.4.0"])
def tests(session):
    with tempfile.TemporaryDirectory() as tmp_dir:
        base_dir = pathlib.Path(os.path.abspath("."))
        copy_tree(base_dir, tmp_dir)
        os.chdir(tmp_dir)

        # args = session.posargs or ["--cov=term"]
        args = ["-m", '"not sql_sync"']
        # session.run("poetry", "add", f"numpy@{numpy}", f"pillow@{pillow}", external=True)
        session.run("poetry", "install", external=True)
        session.run("pytest", *args)
