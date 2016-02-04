#!/bin/bash

fail() {
	echo "$1" > /dev/stderr
	exit 1
}

# Make sure we're standing where alongside this script
# in order to properly execute the rest of the stuff
this=$0
if [ -h $0 ]
then
	this=$(readlink -f $0)
fi
cd $(dirname $this)

# And now...
cd src

# Build the C autotools-based module
# If we're using a virtualenv install it there
prefix=
if [ -z "$VIRTUAL_ENV" ]
then
	prefix="--prefix=\"$VIRTUAL_ENV\""
fi

cd ngamsCClient
./bootstrap || fail "Failed to bootstrap ngamsCClient module"
./configure $prefix || fail "Failed to ./configure $cModule"
make clean all install || fail "Failed to compile $cModule"
cd ..

# Build python setup.py-based modules
# The ngamsPlugIns module eventually requires numpy which we need to install
# manually outside the setuptools world
pip install numpy
for pyModule in pcc ngamsCore ngamsPClient ngamsServer ngamsPlugIns
do
	prevDir=$(pwd -P)
	cd $pyModule
	python setup.py install || fail "Failed to setup.py $pyModule"
	cd "$prevDir"
done