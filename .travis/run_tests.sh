#!/bin/bash
#
# NGAS test runner script to Travis CI
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

cd ${TRAVIS_BUILD_DIR}/src/ngamsTest/ngamsTest

NGAS_TESTDB=

# These are the user/dbname/passwd that we created on run_build
if [[ "$DB" == "mysql" ]]; then
	NGAS_TESTDB='<Db Id="blah" Snapshot="1" Interface="MySQLdb" host="127.0.0.1" db="ngas" user="ngas" passwd="ngas"/>' python ngamsServerTest.py
elif [[ "$DB" == "postgresql" ]]; then
	NGAS_TESTDB='<Db Id="blah" Snapshot="1" Interface="psycopg2" host="127.0.0.1" dbname="ngas" user="ngas" password="ngas"/>' python ngamsServerTest.py
else
	NGAS_TESTDB=${NGAS_TESTDB} python ngamsTest.py
fi
# sqlite3 is the default so it needs no special attention

