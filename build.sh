#!/bin/bash

fail() {
	echo "$1" > /dev/stderr
	exit 1
}

if test -z "$VIRTUAL_ENV"
then
	fail "No VIRTUAL_ENV variable defined, make sure you have the correct environment loaded"
fi

cd src

# Create the configure script for the ngamsCClient module
cd ngamsCClient
./bootstrap || fail "Failed to bootstrap ngamsCClient module"
cd ..

# Uncompress sqlite from its sources
tar xf ../clib_tars/sqlite-autoconf-3070400.tar.gz || fail "Failed to uncompress the sqlite module"

# Build autotools-based modules
for cModule in sqlite-autoconf-3070400 ngamsCClient
do
	prevDir=$(pwd -P)
	cd $cModule
	./configure --prefix="$VIRTUAL_ENV" || fail "Failed to ./configure $cModule"
	make clean all install || fail "Failed to compile $cModule"
	cd "$prevDir"
done

# Build python setup.py-based modules
for pyModule in pcc ngamsCore ngamsPClient ngamsServer ngamsPlugIns
do
	prevDir=$(pwd -P)
	cd $pyModule
	python setup.py install || fail "Failed to setup.py $pyModule"
	cd "$prevDir"
done
