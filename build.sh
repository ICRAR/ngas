#!/bin/bash
#
# Build script for the NGAS software
#
# ICRAR - International Centre for Radio Astronomy Research
# (c) UWA - The University of Western Australia, 2016
# Copyright by UWA (in the framework of the ICRAR)
# All rights reserved
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston,
# MA 02111-1307  USA
#

function print_usage {
	echo "$0 [-h?] [-cd]"
	echo
	echo "-h, -?: Show this help"
	echo "-c: Include the C client compilation"
	echo "-d: Install Python eggs as development eggs"
}

# Command-line option parsing
BUILD_CCLIENT=
SETUP_ACTION=install

while getopts "cdh?" opt
do
	case "$opt" in
		c)
			BUILD_CCLIENT=yes
			;;
		d)
			SETUP_ACTION=develop
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

function fail {
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
for pyModule in crc32c ngamsCore ngamsPClient ngamsServer ngamsPlugIns
do
	prevDir=$(pwd -P)
	cd "$pyModule"
	python setup.py $SETUP_ACTION || fail "Failed to setup.py $pyModule"
	cd "$prevDir"
done
