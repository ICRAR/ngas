#!/bin/bash

fail() {
	echo "$1" > /dev/stderr
	exit 1
}

# Check that we're using a virtualenv
if test -z "$VIRTUAL_ENV"
then
	fail "No VIRTUAL_ENV variable defined, make sure you have the correct environment loaded"
fi

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
cd ngamsCClient
./bootstrap || fail "Failed to bootstrap ngamsCClient module"
./configure --prefix="$VIRTUAL_ENV" || fail "Failed to ./configure $cModule"
make clean all install || fail "Failed to compile $cModule"
cd ..

# Build python setup.py-based modules
for pyModule in pcc ngamsCore ngamsPClient ngamsServer ngamsPlugIns
do
	prevDir=$(pwd -P)
	cd $pyModule
	python setup.py install || fail "Failed to setup.py $pyModule"
	cd "$prevDir"
done