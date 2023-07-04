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

if [ -z "${SKIP_BBCP}" ]; then

# Make ourselves resolvable
# NGAS itself doesn't need this because it always internally resolves
# `hostname` to 127.0.0.1 to avoid hitting the network. Other programs
# don't do this though. In particular, bbcp (which we need to test the BBCPARC
# command) fails.
if [ "${TRAVIS_OS_NAME}" = "osx" ]; then
	sudo -h localhost sed -i '' "s/127\\.0\\.0\\.1.*/& `hostname`/" /etc/hosts
else
	sudo -h localhost sed -i "s/127\\.0\\.1\\.1.*/& `hostname`/" /etc/hosts
fi

# Enable passwordless localhost ssh self-connectivity
# Again, this is not needed by NGAS itself, but by bbcp.
# NGAS forces bbcp to use key-based authentication because interactive
# password-based authentication is not viable.
if [ "${TRAVIS_OS_NAME}" = "osx" ]; then
	sudo systemsetup -setremotelogin on
else
	sudo start ssh || sudo systemctl start ssh || echo "couldn't start ssh" 1>&2
fi
ssh-keygen -t rsa -f ~/.ssh/id_rsa -N "" -q || fail "Failed to create RSA key"
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys || fail "Failed to add public key to authorized keys"
ssh-keyscan localhost >> ~/.ssh/known_hosts || fail "Failed to import localhost's keys"
chmod 600 ~/.ssh/authorized_keys ~/.ssh/known_hosts
cat << EOF >> ~/.ssh/config
Host *
     IdentityFile ~/.ssh/id_rsa
     NoHostAuthenticationForLocalhost yes
EOF
ssh localhost ls || fail "Testing ssh localhost failed"
ssh 127.0.0.1 ls || fail "Testing ssh 127.0.0.1 failed"
ssh `hostname` ls || fail "Testing ssh `hostname` failed"

# Install bbcp
# After compilation we put it in the PATH, then go back to where we were
cd ../
git clone https://www.slac.stanford.edu/~abh/bbcp/bbcp.git
if [ $? -eq 0 ]; then
	cd bbcp/src
	make all || fail "Failed to build bbcp"
	sudo cp $PWD/../bin/`../MakeSname`/bbcp /usr/local/bin || fail "Failed to copy bbcp to /usr/local/bin"
	bbcp --help > /dev/null || fail "bbcp failed to run with --help"

	# In MacOS /usr/local/bin is not seen by newly created environments
	# (e.g., when ssh-ing into localhost, which bbcp does), so let's
	# do this ourselves
	echo "export PATH=/usr/local/bin:\$PATH" | cat - ~/.bashrc > ~/.bashrc.mod
	mv ~/.bashrc.mod ~/.bashrc
else
	echo "Failed to clone bbcp, testing proceeding without bbcp" 1>&2
fi
cd ${TRAVIS_BUILD_DIR}

fi

# In OSX we need to brew install some things
#
# Most notably, Travis doesn't support python builds in OSX,
# but the brew packages that come preinstalled in the virtual machines
# include python 2.7
if [ "${TRAVIS_OS_NAME}" = "osx" ]
then
	# update-reset seems to be the solution for transient homebrew LoadErrors we have had
	#  https://stackoverflow.com/questions/54888582/ruby-cannot-load-such-file-active-support-core-ext-object-blank
	brew update-reset || true

	# Avoid slow autoupdates and autocleanups when brew installing stuff
	export HOMEBREW_NO_AUTO_UPDATE=1
	export HOMEBREW_NO_INSTALL_CLEANUP=1

	brew unlink python || fail "Failed to brew unlink python"
	brew ls --version python@2 || brew install python@2 || fail "Failed to brew install python@2"
	brew install berkeley-db@4 || fail "Failed to brew install berkeley-db@4"

	# Aggresively changing names now...
	sudo scutil --set HostName `hostname`

	# Let's check what hostnames are reported in MacOS...
	for n in HostName LocalHostName ComputerName; do
		echo -n "scutil --get $n: "
		scutil --get $n
	done
fi

# Let's check what hostnames are reported in MacOS...
fqdn=`hostname --fqdn`
echo "FQDN: '$fqdn'"
cat /etc/hosts
sudo sed -i "s/\\([^ ]\\+\\) .*$fqdn.*/\\1 my-ngas-host my-ngas-host.localdomain/" /etc/hosts
cat /etc/hosts
sudo hostname my-ngas-host
sudo domainname localdomain
echo -n "hostname: "
hostname
echo -n "domainname: "
domainname
for m in "getfqdn()" \
         "gethostname()" \
         "gethostbyname(socket.gethostname())" \
         "gethostbyname(\"localhost\")" \
         "gethostbyname_ex(socket.gethostname())" \
         "gethostbyname_ex(\"localhost\")" \
         "gethostbyaddr(\"127.0.0.1\")" \
         "gethostbyaddr(socket.gethostbyname(socket.gethostname()))" \
         "gethostbyaddr(socket.gethostbyname(\"localhost\"))" ; do
	echo -n "socket.$m: "
	python -c "import socket; print(socket.$m)"
done

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
# bsddb3 is constrained because 6.2.8 fails to install in python 2.7,
# and the python installation in Travis doesn't come with the bsddb built-in module.
# Additionally, in later python versions installing C extensiosn
# seems to yield unloadable modules (e.g., netifaces.AF_INET could not be loaded).
PIP_PACKAGES="bsddb3!=6.2.8 python-daemon<=2.3.0 astropy netifaces certifi==2020.4.5.1"

# We need to prepare the database for what's to come later on, and to install
# the corresponding python module so NGAS can talk to the database
DB_HOST=${DB_HOST:-127.0.0.1}
if [[ "$DB" == "mysql" ]]; then

	# Create ngas database, we keep using the "travis" user
	# (who might have already all priviledges over its newly created database)
	mysql_cmd="mysql -h ${DB_HOST} -u root --password=mysql -e"
	$mysql_cmd "CREATE USER 'ngas'@'%' IDENTIFIED BY 'ngas';" || fail "$EUSER"
	$mysql_cmd "CREATE DATABASE ngas;" || fail "$EDB"
	$mysql_cmd "GRANT ALL ON ngas.* TO 'ngas'@'%';" || fail "$EPERM"

	# Create ngas database schema
	mysql -ungas -D ngas -h ${DB_HOST} -pngas \
	    < src/ngamsCore/ngamsSql/ngamsCreateTables-mySQL.sql \
		 || fail "$ECREAT"

	# Python packages needed
	PIP_PACKAGES+=" mysqlclient"

elif [[ "$DB" == "postgresql" ]]; then

	# Create database and user
	export PGPASSWORD=postgres
	psql_cmd="psql -W -U postgres -h ${DB_HOST} -c"
	$psql_cmd "CREATE USER ngas WITH PASSWORD 'ngas';" || fail "$EUSER"
	$psql_cmd 'CREATE DATABASE ngas;' || fail "$EDB"
	$psql_cmd 'GRANT ALL PRIVILEGES ON DATABASE ngas TO ngas;' || fail "$EPERM"
	$psql_cmd 'GRANT ALL ON SCHEMA public TO ngas;' -d ngas || fail "$EPERM"

	# Create ngas database schema
	PGPASSWORD=ngas psql -U ngas -d ngas -h ${DB_HOST} \
	    < src/ngamsCore/ngamsSql/ngamsCreateTables-PostgreSQL.sql \
		 || fail "$ECREAT"

	# Python packages needed
	PIP_PACKAGES+=" psycopg2-binary"

fi
# sqlite3 we doesn't require preparation or any extra modules

# MacOS needs again a bit more of preparation
if [ "${TRAVIS_OS_NAME}" = "osx" ]
then
	cellar_dir="`brew --cellar`"
	db_dir="${cellar_dir}/berkeley-db@4/`ls -tr1 ${cellar_dir}/berkeley-db@4 | tail -n 1`"

	export YES_I_HAVE_THE_RIGHT_TO_USE_THIS_BERKELEY_DB_VERSION=1
	export BERKELEYDB_DIR="${db_dir}"
	export CFLAGS="$CFLAGS -I${db_dir}/include"
	export LDFLAGS="$LDFLAGS -L${db_dir}/lib"

	# setuptools is not present in homebrew python installations.
	# It previously got automatically pulled by our dependencies,
	# but now that they've removed python 2.7 support we need
	# to explicitly install a previous version
	PIP_PACKAGES="setuptools<45 $PIP_PACKAGES"

	# Avoid issue in Travis with MacOS 10.13 where the MacOS SDK
	# doesn't exist (or the wrong one gets picked up), so no headers
	# are found. Still don't know exactly what's going on, but this
	# seems to fix it
	export CFLAGS="$CFLAGS -isysroot /"
fi

# Install latest setuptools in Xenial,
# as python 3.6/3.7 fails otherwise
if [ "${TRAVIS_OS_NAME}" = linux ]; then
	pip install -U setuptools || fail "Can't install latest setuptools"
	export BERKELEYDB_DIR=/usr
fi

pip install $PIP_PACKAGES || fail "$EPIP"
./build.sh -d -c
