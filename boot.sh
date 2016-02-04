#!/bin/bash
#
# Script to bootstrap the installation process of NGAMs
#
# NGAMs is installed in a virtualenv environment, where all its (python) dependencies are also installed.
# The installation procedure is driven by the machine-setup/fabfile.py file, which is a Fabric file.
# This bootstrap script creates a virtual environment for installing NGAS on it and installes the minimum
# dependencies on it to run the Fabric-based installation procedure

error() {
	echo "ERROR: $1" > /dev/stderr
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
	VIRTUALENV_URL='https://pypi.python.org/packages/source/v/virtualenv/virtualenv-12.0.7.tar.gz'
	if [[ ! -z "$(which wget 2> /dev/null)" ]]
	then
		wget "$VIRTUALENV_URL" | error "Failed to download virtualenv"
	elif [[ ! -z "$(which curl 2> /dev/null)" ]]
	then
		curl "$VIRTUALENV_URL" | error "Failed to download virtualenv"
	else
		error "Can't find a download tool (tried wget and curl), cannot download virtualenv"
	fi

	tar xf virtualenv-12.0.7.tar.gz  | error "Failed to untar virtualenv"
	veCommand="python virtualenv-12.0.7/virtualenv.py"
	removeVE="rm -rf virtualenv-12.0.7"
fi

# Create a virtual environment for the NGAMS installation procedure to begin
# and source it
$veCommand $veDir
$sourceCommand
if [[ ! -z "$removeVE" ]]
then
	$removeVE
fi

# Install Fabric and Boto
for pkg in boto Fabric
do
	pip install $pkg
done

echo
echo
echo    "----------------------------------------------------------------------------"
echo -e "Now run the following command on your shell to load the virtual environment:\n$sourceCommand"
echo
