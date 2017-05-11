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
#

cd ${TRAVIS_BUILD_DIR}

fail() {
	echo $1 1>&2
	exit 1
}

EUSER="Failed to create database user ngas"
EDB="Failed to create database ngas"
EPERM="Failed to grant priviledges to user ngas on database ngas"
ECREAT="Failed to create the ngas database schema on database ngas"

# The python packages we need to install either because NGAS itself eeds them,
# or because it's what we need to communicate with our database engine
PIP_PACKAGES="bsddb3"

# We need to prepare the database for what's to come later on, and to install
# the corresponding python module so NGAS can talk to the database
if [[ "$DB" == "mysql" ]]; then

	# Create ngas database, we keep using the "travis" user
	# (who might have already all priviledges over its newly created database)
	mysql_cmd="mysql -e"
	$mysql_cmd "CREATE DATABASE ngas;" || fail "$EDB"
	#$mysql_cmd "GRANT ALL ON ngas.* TO 'travis'@'localhost';" || fail "$EPERM"

	# Create ngas database schema
	mysql -u travis -D ngas -h 127.0.0.1 -p'' \
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

pip install $PIP_PACKAGES
./build.sh -c
