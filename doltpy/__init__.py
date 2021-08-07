import shutil

if not shutil.which("dolt"):
    install_dolt = """`dolt` not found in `PATH`

Reference: https://docs.dolthub.com/getting-started/installation
to install:

> sudo bash -c 'curl -L https://github.com/dolthub/dolt/releases/latest/download/install.sh | sudo bash'
or
> brew install dolt

and configure:

> dolt config --global --add user.name <your name>
> dolt config --global --add user.email <your email>
"""
    raise Exception(install_dolt)
