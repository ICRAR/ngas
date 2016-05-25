#!/bin/bash

function print_usage {
	echo "$0 [-h?] [-c]"
	echo
	echo "-h, -?: Show this help"
	echo "-c: Include the C client compilation"
}

# Command-line option parsing
BUILD_CCLIENT=

while getopts "ch?" opt
do
	case "$opt" in
		c)
			BUILD_CCLIENT=yes
			;;
		[h?])
			print_usage
			exit 0
			;;
		:)
			print_usage
			exit 1
	esac
done

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
cd "$(dirname $this)"

# And now...
cd src

# If we're using a virtualenv install it there
prefix=
if [ -n "$VIRTUAL_ENV" ]
then
	prefix="--prefix=$VIRTUAL_ENV"
	echo "Will install NGAS under $VIRTUAL_ENV"
fi

# Build the C autotools-based module
if [ -n "$BUILD_CCLIENT" ]
then
	cd ngamsCClient
	./bootstrap || fail "Failed to bootstrap ngamsCClient module"
	./configure "$prefix" || fail "Failed to ./configure ngamsCCLient"
	make clean all install || fail "Failed to compile ngamsCClient"
	cd ..
fi

# Build python setup.py-based modules
# The ngamsPlugIns module eventually requires numpy which we need to install
# manually outside the setuptools world
pip install numpy
for pyModule in pcc crc32c ngamsCore ngamsPClient ngamsServer ngamsPlugIns
do
	prevDir=$(pwd -P)
	cd "$pyModule"
	python setup.py install || fail "Failed to setup.py $pyModule"
	cd "$prevDir"
done
