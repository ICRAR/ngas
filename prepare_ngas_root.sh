#!/bin/bash
#
# Utility script to create a new NGAS root
#
# ICRAR - International Centre for Radio Astronomy Research
# (c) UWA - The University of Western Australia, 2018
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
	echo "Creates and prepares a new NGAS root directory"
	echo
	echo "$0 [-h | -?] [-f] [-D] [-C] <NGAS root directory>"
	echo
	echo "-h, -?: Show this help"
	echo "-f: Force creation of NGAS root, even if directory exists"
	echo "-D: Do *not* create a configuration file"
	echo "-C: Do *not* create an SQLite3 database"
	echo
}

error() {
	retcode=1
	if [ $# = 2 ]
	then
		retcode=$2
	fi

	echo "ERROR: $1" 1>&2
	exit $retcode
}

warning() {
	echo "WARNING: $1" 1>&2
}

# Command-line option parsing
FORCE=
CREATE_DB=yes
CREATE_CFG=yes

while getopts "fDCh?" opt
do
	case "$opt" in
		f)
			FORCE=yes
			;;
		D)
			CREATE_DB=no
			;;
		C)
			CREATE_CFG=no
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

# One optional argument is required
if [ $(($# - $OPTIND)) -lt 0 ]
then
	print_usage 1>&2
	exit 1
fi

root_dir="${@:$OPTIND:1}"

if [ -d "${root_dir}" -a "$FORCE" != 'yes' ]
then
	error "${root_dir} already exists, will not overwrite (use -f for that)" 2
fi

# Make sure we're standing where alongside this script
# in order to properly execute the rest of the stuff
this=$0
if [ -h $0 ]
then
	this=$(readlink -f $0)
fi
cd "$(dirname $this)"

# Get the absolute name of root_dir, which is what will go
# to the configuration file
head=$(dirname "$root_dir")
head=$(cd "$head" && pwd)
root_dir=$head/$(basename "${root_dir}")

# Calculate now the rest of the files that depend on root_dir
cfg_file="${root_dir}/cfg/ngamsServer.conf"
db_file="${root_dir}/ngas.sqlite"

# Create and populate the root directory
echo "Creating NGAS root directory"
mkdir -p "${root_dir}" || error "Failed to create NGAS root directory"
cp -R NGAS/* "${root_dir}" || error "Failed to populate NGAS root with initial contents"

# Copy sample configuration file and adjust it to use an sqlite3 database
# located in the NGAS root directory (if required)
if [ "${CREATE_CFG}" = "yes" ]
then

	echo "Creating and preparing initial configuration file"
	cp cfg/sample_server_config.xml "${cfg_file}" || error "Failed to create initial configuration file"
	sed -E -i "s@RootDirectory=\"[^\"]+\"@RootDirectory=\"${root_dir}\"@g" "${cfg_file}" || error "Failed to set RootDirectory setting"

	if [ "${CREATE_DB}" = "yes" ]
	then
		sed -E -i "s@database=\"[^\"]+\"@database=\"${db_file}\"@g" "${cfg_file}" || error "Failed to set Db.database setting"
	fi
fi

# Initialize the SQlite database
if [ "${CREATE_DB}" = "yes" ]
then
	echo "Creating initial database"
	sqlite3 "${db_file}" < src/ngamsCore/ngamsSql/ngamsCreateTables-SQLite.sql || error "Failed to create SQLite database file"
fi

echo
echo
echo "----------------------------------------------------------------------------"
echo "Successfully setup ${root_dir} as an NGAS root directory"
echo
if [ "${CREATE_CFG}" = "yes" ]
then
	echo "A working configuration file has been created at ${cfg_file}."
	if [ "${CREATE_DB}" = "yes" ]
	then
		echo "The configuration points to a working SQLite3 database created at ${db_file}."
	fi
	echo
elif [ "${CREATE_DB}" = "yes" ]
then
	echo "A working SQLite3 database has been created at ${db_file}."
	echo
fi
echo "To try out your new NGAS root, you can start an NGAS server with the following command:"
echo
echo "$> ngamsServer -cfg ${cfg_file} -v 4 -autoonline"
echo
echo "Then, hit Ctrl-C to stop the server."
echo "----------------------------------------------------------------------------"
echo
