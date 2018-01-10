#!/bin/bash
#
# NGAS build script to Travis CI
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

# We know the following variables:
#
# DB                the database used in this run (sqlite3, mysql, postgresql)
# TRAVIS_BUILD_DIR  the directory where the sources are located
# TRAVIS_OS_NAME    the OS under which we are running (linux, osx)
#

cd ${TRAVIS_BUILD_DIR}

fail() {
	echo $1 1>&2
	exit 1
}

# In OSX we need to brew install some things
#
# Most notably, Travis doesn't support python builds in OSX,
# but the brew packages that come preinstalled in the virtual machines
# include python 2.7. We sill need to get ourselves a
# virtualenv though and manually source it whenever we use it.
if [ "${TRAVIS_OS_NAME}" = "osx" ]
then
	brew install berkeley-db || fail "Failed to brew install packages"

	# Now create ourselves a virtualenv please and go in there
	./create_venv.sh ./osx_venv
	source ./osx_venv/bin/activate
fi

EUSER="Failed to create database user ngas"
EPASS="Failed to change password"
EDB="Failed to create database ngas"
EPERM="Failed to grant priviledges to user ngas on database ngas"
ECREAT="Failed to create the ngas database schema on database ngas"
EPIP="Failed to install pip packages"

# The python packages we need to install either because NGAS itself needs them,
# or because it's what we need to communicate with our database engine
# In the case of NGAS dependencies, if these weren't here then they would be
# pulled out automatically during package installation, but in source-code form,
# which in some cases, like astropy, can take some time to build and doesn't get
# cached.
PIP_PACKAGES="bsddb3 python-daemon astropy"

# We need to prepare the database for what's to come later on, and to install
# the corresponding python module so NGAS can talk to the database
if [[ "$DB" == "mysql" ]]; then

	# Create ngas database, we keep using the "travis" user
	# (who might have already all priviledges over its newly created database)
	mysql_cmd="mysql -uroot -e"
	$mysql_cmd "CREATE USER 'ngas'@'%' IDENTIFIED BY 'ngas';" || fail "$EUSER"
	$mysql_cmd "CREATE DATABASE ngas;" || fail "$EDB"
	$mysql_cmd "GRANT ALL ON ngas.* TO 'ngas'@'%';" || fail "$EPERM"

	# Create ngas database schema
	mysql -ungas -D ngas -h 127.0.0.1 -pngas \
	    < src/ngamsCore/ngamsSql/ngamsCreateTables-mySQL.sql \
		 || fail "$ECREAT"

	# Python packages needed
	PIP_PACKAGES+=" MySQL-python"

elif [[ "$DB" == "postgresql" ]]; then

	# Create database and user
	psql_cmd="psql -U postgres -c"
	$psql_cmd "CREATE USER ngas WITH PASSWORD 'ngas';" || fail "$EUSER"
	$psql_cmd 'CREATE DATABASE ngas;' || fail "$EDB"
	$psql_cmd 'GRANT ALL PRIVILEGES ON DATABASE ngas TO ngas;' || fail "$EPERM"

	# Create ngas database schema
	PGPASSWORD=ngas psql -U ngas -d ngas -h localhost \
	    < src/ngamsCore/ngamsSql/ngamsCreateTables-PostgreSQL.sql \
		 || fail "$ECREAT"

	# Python packages needed
	PIP_PACKAGES+=" psycopg2"

fi
# sqlite3 we doesn't require preparation or any extra modules

# MacOS needs again a bit more of preparation
if [ "${TRAVIS_OS_NAME}" = "osx" ]
then
	cellar_dir="`brew --cellar`"
	db_dir="${cellar_dir}/berkeley-db/`ls -tr1 ${cellar_dir}/berkeley-db | tail --lines 1`"

	export YES_I_HAVE_THE_RIGHT_TO_USE_THIS_BERKELEY_DB_VERSION=1
	export BERKELEYDB_DIR="${db_dir}"
	export CFLAGS="$CFLAGS -I${db_dir}/include"
	export LDFLAGS="$LDFLAGS -L${db_dir}/lib"
fi

pip install $PIP_PACKAGES || fail "$EPIP"
./build.sh -d -c
