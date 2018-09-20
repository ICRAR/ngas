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
	echo "$0 [-h?] [-cdD]"
	echo
	echo "-h, -?: Show this help"
	echo "-c: Include the C client compilation"
	echo "-d: Install Python eggs as development eggs"
	echo "-D: Install Python packages needed to build the docs"
	echo
	echo "Also, set the NGAS_NO_CRC32C environment variable to any value to skip crc32c"
	echo "installation (if you are not planning to use it, and therefore don't want to"
	echo "depend on it)"
}

error() {
	echo "$1" 1>&2
	exit 1
}

warning() {
	echo "WARNING: $1" 1>&2
}

# Command-line option parsing
BUILD_CCLIENT=
INSTALL_DOC_DEPS=
SETUP_ACTION=install

while getopts "cdDh?" opt
do
	case "$opt" in
		c)
			BUILD_CCLIENT=yes
			;;
		d)
			SETUP_ACTION=develop
			;;
		D)
			INSTALL_DOC_DEPS=yes
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

# Build the C autotools-based module
if [ -n "$BUILD_CCLIENT" ]
then

	# If we're using a virtualenv install it there
	prefix=
	if [ -n "$VIRTUAL_ENV" ]
	then
		prefix="--prefix=$VIRTUAL_ENV"
		echo "Will install NGAS C client under $VIRTUAL_ENV"
	fi

	cd ngamsCClient
	./bootstrap || error "Failed to bootstrap ngamsCClient module"
	./configure "$prefix" || error "Failed to ./configure ngamsCCLient"
	make all install || error "Failed to compile ngamsCClient"
	cd ..
fi

# Build python setup.py-based modules
# The ngamsPlugIns module eventually requires astropy,
# which is much faster to install using pip
# The gleam plug-ins require numpy, but we leave that
# out of the core dependencies of NGAS
pip install 'astropy' || warning "Failed to install astropy via pip"

# It would be ideal to install the NGAS modules via pip,
# but their setup.py currently references the VERSION File
# at the root of this repository. pip on the other hand makes
# a copy of the sources onto some temporary directory to build
# the modules, and therefore they fail to build due to the missing
# file.
#
# We have two solutions for this:
# * Stop organizing the code as separate modules
# * Have each module maintain its own version
#
# I haven't decided on either yet, but I think the first one
# makes more sense given that nobody is really using the client only,
# and most of the dependencies are on the ngamsCore package anyway
for pyModule in ngamsCore ngamsPClient ngamsServer ngamsPlugIns
do
	prevDir=$(pwd -P)
	cd "$pyModule"
	python setup.py $SETUP_ACTION || error "Failed to setup.py $pyModule"
	cd "$prevDir"
done

# Install additional dependencies needed to build the docs
if [ -n "${INSTALL_DOC_DEPS}" ]
then
	pip install sphinx sphinx-rtd-theme || warning "Failed to install sphinx packages (needed to build docs)"
fi
