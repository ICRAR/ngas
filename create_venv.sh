#!/bin/bash
#
# Script to create a virtual environment suitable for
# developing/installing NGAS, or to use the fabric scripts
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

#
# This script creates a virtual environment.
# This new venv can be then used both to install NGAS on it
# (either normally, or in development mode)
# or to locally support the fabric-based remote installation procedure.
#

error() {
	echo "ERROR: $1" 1>&2
	exit 1
}

if [[ -z "$1" ]]
then
	error "Usage: $0 <virtualenv-directory>"
fi
veDir="$1"

if [[ -d "$veDir" ]]
then
	error "$veDir already exists"
fi

# First things first, check that we have python installed
if [[ -z "$(which python 2> /dev/null)" ]]
then
	error "No Python found in this system, install Python 2.7"
fi

# Check that the python version is correct
pythonVersion=$(python -V 2>&1)
if [[ ! "$pythonVersion" == *"2.7"* ]]
then
	error "Python 2.7 needed, found: $pythonVersion"
fi

# Check if we already have virtualenv
# If not download one and untar it
veCommand="virtualenv"
sourceCommand="source $veDir/bin/activate"
if [[ -z "$(which virtualenv 2> /dev/null)" ]]
then
	VIRTUALENV_URL='https://pypi.python.org/packages/8b/2c/c0d3e47709d0458816167002e1aa3d64d03bdeb2a9d57c5bd18448fd24cd/virtualenv-15.0.3.tar.gz#md5=a5a061ad8a37d973d27eb197d05d99bf'
	if [[ ! -z "$(which wget 2> /dev/null)" ]]
	then
		wget "$VIRTUALENV_URL" | error "Failed to download virtualenv"
	elif [[ ! -z "$(which curl 2> /dev/null)" ]]
	then
		curl "$VIRTUALENV_URL" | error "Failed to download virtualenv"
	else
		error "Can't find a download tool (tried wget and curl), cannot download virtualenv"
	fi

	tar xf virtualenv-15.0.3.tar.gz  | error "Failed to untar virtualenv"
	veCommand="python virtualenv-15.0.3/virtualenv.py"
	removeVE="rm -rf virtualenv-15.0.3"
fi

# Create a virtual environment for the NGAMS installation procedure to begin
# and source it
$veCommand $veDir
if [[ ! -z "$removeVE" ]]
then
	$removeVE
fi

# Install initial packages into the new venv
# Fabric is needed to allow using the fab scripts in the first place.
# pycrypto is needed by the SSH pubkey-related bits in the fab scripts.
# boto is needed to support the aws-related fab tasks.
$sourceCommand
pip install boto Fabric pycrypto

echo
echo
echo "----------------------------------------------------------------------------"
echo "Virtual Environment successfully created!"
echo "Now run the following command on your shell to load the virtual environment:"
echo
echo "$sourceCommand"
echo
echo "You can now use this virtual environment to either locally install NGAS"
echo "(normally or in development mode, see ./build.sh -h), or to run the remote"
echo "installation procedures via fabric scripts (run fab -l for more information)"
echo "----------------------------------------------------------------------------"
echo
