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
# TRAVIS_OS_NAME    the OS under which we are running (linux, osx)
#

fail() {
	echo $1 1>&2
	exit 1
}

# coverage version pinned to avoid issues with latest releases
# (see https://github.com/nedbat/coveragepy/issues/916, but possibly
# our problem has not even been reported)
#
# pytest version pinned to avoid issues with python2-based builds,
# which require an old version of astropy, which in turn requires an old
# version of pytest, and pip now complains about this.
#
# pytest-cov is pinned to work against pytest<3.7.
pip install psutil 'coverage<5' 'pytest<3.7' 'pytest-cov<2.10' coveralls trustme || fail "Failed to install unit test dependencies"

# Try to simply import the plugin modules
# This increases our coverage by a not-too-small amount
import_statements=''
for f in src/ngamsPlugIns/ngamsPlugIns/*.py; do
	f=`basename $f`
	if [[ $f == __init__.py ]]; then
		continue
	fi
	import_statements+="import ngamsPlugIns.${f%%.py}; "
done

# Try to simply import the utility modules
# This increases our coverage by a not-too-small amount
for f in src/ngamsUtils/ngamsUtils/*.py; do
	f=`basename $f`
	if [[ $f == __init__.py ]]; then
		continue
	fi
	import_statements+="import ngamsUtils.${f%%.py}; "
done

echo "Starting import of plugins and utilities code"
coverage run -p <(echo $import_statements) || fail "Importing plugins failed"
coverage combine || fail "Failed to combine coverage information"

# These are the user/dbname/passwd that we created on run_build
# sqlite3 is the default so it needs no special attention
NGAS_TESTDB=
if [[ "$DB" == "mysql" ]]; then
	NGAS_TESTDB='<Db Id="blah" Snapshot="0" Interface="MySQLdb" host="127.0.0.1" db="ngas" user="ngas" passwd="ngas"/>'
elif [[ "$DB" == "postgresql" ]]; then
	NGAS_TESTDB='<Db Id="blah" Snapshot="0" Interface="psycopg2" host="127.0.0.1" dbname="ngas" user="ngas" password="ngas"/>'
fi
export NGAS_TESTDB

py.test -v --cov --cov-append
