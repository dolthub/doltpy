#!/bin/bash

# Note that the user name for this is oscarbatori, see me for password
version=$1
message="Are you sure you want to release doltpy version $version using commit $(git rev-parse HEAD)? [Y/y to proceed]"

read -p "$message" -n 1 -r
echo    # (optional) move to a new line
if [[ $REPLY =~ ^[Yy]$ ]]
then
  printf "Building building distro dist/doltpy-$version.tar.gz\n"
  python setup.py sdist

  printf "Uploading to PyPi servers\n"
  twine upload dist/doltpy-$version.tar.gz
else
  printf "Exiting, nothing to do...\n"
fi
