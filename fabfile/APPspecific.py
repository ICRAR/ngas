#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2016
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#
"""
Main module where application-specific tasks are defined. The main procedure
is dependent on the fabfileTemplate module.
"""
import os
from fabric.state import env
from fabric.colors import red
from fabric.operations import local
from fabric.decorators import task
from fabric.context_managers import settings, cd
from fabric.contrib.files import exists, sed
from fabric.utils import abort
# import urllib2

# >>> All the settings below are kept in the special fabric environment
# >>> dictionary called env. Don't change the names, only adjust the
# >>> values if necessary. The most important one is env.APP.

# The following variable will define the Application name as well as directory
# structure and a number of other application specific names.
env.APP_NAME = 'NGAS'

# The username to use by default on remote hosts where APP is being installed
# This user might be different from the initial username used to connect to the
# remote host, in which case it will be created first
env.APP_USER = env.APP_NAME.lower()

# Name of the directory where APP sources will be expanded on the target host
# This is relative to the APP_USER home directory
env.APP_SRC_DIR_NAME = env.APP_NAME.lower() + '_src'

# Name of the directory where APP root directory will be created
# This is relative to the APP_USER home directory
env.APP_ROOT_DIR_NAME = env.APP_NAME.upper()

# Name of the directory where a virtualenv will be created to host the APP
# software installation, plus the installation of all its related software
# This is relative to the APP_USER home directory
env.APP_INSTALL_DIR_NAME = env.APP_NAME.lower() + '_rt'

# Version of Python required for the Application
env.APP_PYTHON_VERSION = '3.7'

# URL to download the correct Python version
env.APP_PYTHON_URL = 'https://www.python.org/ftp/python/3.7.4/Python-3.7.4.tgz'

env.APP_DATAFILES = ['NGAS']

# >>> The following settings are only used within this APPspecific file, but may be
# >>> passed in through the fab command line as well, which will overwrite the 
# >>> defaults below.

defaults = {
# Do not compile C Client
'NGAS_NO_CLIENT': False,

# The type of server to configure after installation
# Values are 'archive' and 'cache'
'NGAS_SERVER_TYPE': 'archive',

# Do not install CRC32C module
'NGAS_NO_CRC32C': True,

# Is this a development installation
'NGAS_DEVELOP': False,

# Compile the NGAS docs
'NGAS_DOC_DEPENDENCIES': False,

# Overwrite existing ROOT directory
'NGAS_OVERWRITE_ROOT': False,

# Overwrite existing installation directory
'APP_OVERWRITE_INSTALLATION': False
}

# Boto specific settings
env.AWS_PROFILE = 'NGAS'
env.AWS_REGION = 'us-east-1'
env.AWS_AMI_NAME = 'Amazon'
env.AWS_INSTANCES = 1
env.AWS_INSTANCE_TYPE = 't1.micro'
env.AWS_KEY_NAME = 'icrar_{0}'.format(env.APP_USER)
env.AWS_SEC_GROUP = 'NGAS' # Security group allows SSH and other ports
env.AWS_SEC_GROUP_PORTS = [22, 80, 7777, 8888] # ports to open
env.AWS_SUDO_USER = 'ec2-user' # required to install init scripts.

# NOTE: Make sure to modify the following lists to meet the requirements for
# the application.
# Alpha-sorted packages to be installed per supported package manager
env.pkgs = {
            'YUM_PACKAGES' : [
                'autoconf',
                'bzip2-devel',
                'cfitsio-devel',
                'db4-devel',
                'gcc',
                'gdbm-devel',
                'git',
                'libdb-devel',
                'libtool',
                'make',
                'openssl-devel',
                'patch',
                'postfix',
                'postgresql-devel',
                'python36-devel',
                'python-devel',
                'readline-devel',
                'sqlite-devel',
                'tar',
                'wget',
                'zlib-devel',
            ],
            'APT_PACKAGES' : [
                'autoconf',
                'libcfitsio-dev',
                'libdb5.3-dev',
                'libdb-dev',
                'libgdbm-dev',
                'libreadline-dev',
                'libsqlite3-dev',
                'libssl-dev',
                'libtool',
                'libzlcore-dev',
                'make',
                'patch',
                'postgresql-client',
                'python-dev',
                'python-setuptools',
                'tar',
                'sqlite3',
                'wget',
                'zlib1g-dbg',
                'zlib1g-dev',
            ],
            'SLES_PACKAGES' : [
                'autoconf',
                'automake',
                'gcc',
                'gdbm-devel',
                'git',
                'libdb-4_5',
                'libdb-4_5-devel',
                'libtool',
                'make',
                'openssl',
                'patch',
                'python-devel',
                'python-html5lib',
                'python-pyOpenSSL',
                'python-xml',
                'postfix',
                'postgresql-devel',
                'sqlite3-devel',
                'wget',
                'zlib',
                'zlib-devel',
            ],
            'BREW_PACKAGES' : [
                'autoconf',
                'automake',
                'berkeley-db@4',
                'libtool',
                'wget',
            ],
            'PORT_PACKAGES' : [
                'autoconf',
                'automake',
                'db60',
                'libtool',
                'wget',
            ]
       }

# This dictionary defines the visible tasks available to fab.
__all__ = [
    'start_APP_and_check_status',
    'sysinitstart_NGAS_and_check_status'
]

# Set the rpository root to be relative to the location of this file.
env.APP_repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# >>> The following lines need to be after the definitions above!!!
from fabfileTemplate.utils import sudo, info, success, default_if_empty, home, run
from fabfileTemplate.utils import overwrite_defaults, failure
from fabfileTemplate.system import check_command, get_linux_flavor, MACPORT_DIR
from fabfileTemplate.APPcommon import virtualenv, APP_doc_dependencies, APP_source_dir
from fabfileTemplate.APPcommon import APP_root_dir, extra_python_packages, APP_user, build
from fabfileTemplate.pkgmgr import check_brew_port, check_brew_cellar

# get the settings from the fab environment if set on command line
settings = overwrite_defaults(defaults)

def APP_build_cmd():

    # The installation of the bsddb package (needed by ngamsCore) is in
    # particular difficult because it requires some flags to be passed on
    # (particularly if using MacOSX's port
    build_cmd = []
    linux_flavor = get_linux_flavor()
    if linux_flavor == 'Darwin':
        pkgmgr = check_brew_port()
        if pkgmgr == 'brew':
            cellardir = check_brew_cellar()
            db_version = run('ls -tr1 {0}/berkeley-db@4'.format(cellardir)).split()[-1]
            db_dir = '{0}/berkeley-db@4/{1}'.format(cellardir, db_version)
            build_cmd.append('BERKELEYDB_DIR={0}'.format(db_dir))
            if not settings['NGAS_NO_CLIENT']:
                build_cmd.append('CFLAGS=-I{0}/include'.format(db_dir))
                build_cmd.append('LDFLAGS=-L{0}/lib'.format(db_dir))
        else:
            incdir = MACPORT_DIR + '/include/db60'
            libdir = MACPORT_DIR + '/lib/db60'
            build_cmd.append('BERKELEYDB_INCDIR=' + incdir)
            build_cmd.append('BERKELEYDB_LIBDIR=' + libdir)
            if not settings['NGAS_NO_CLIENT']:
                build_cmd.append('CFLAGS=-I' + incdir)
                build_cmd.append('LDFLAGS=-L' + libdir)
        build_cmd.append('YES_I_HAVE_THE_RIGHT_TO_USE_THIS_BERKELEY_DB_VERSION=1')

    if settings['NGAS_NO_CRC32C']:
        build_cmd.append('NGAS_NO_CRC32C=1')
    build_cmd.append('./build.sh')
    if not settings['NGAS_NO_CLIENT']:
        build_cmd.append("-c")
    if settings['NGAS_DEVELOP']:
        build_cmd.append("-d")
    if not settings['NGAS_DOC_DEPENDENCIES']:
        build_cmd.append('-D')

    return ' '.join(build_cmd)


def install_sysv_init_script(nsd, nuser, cfgfile):
    """
    Install the NGAS init script for an operational deployment.
    The init script is an old System V init system.
    In the presence of a systemd-enabled system we use the update-rc.d tool
    to enable the script as part of systemd (instead of the System V chkconfig
    tool which we use instead). The script is prepared to deal with both tools.
    """

    # Different distros place it in different directories
    # The init script is prepared for both
    opt_file = '/etc/sysconfig/ngas'
    if get_linux_flavor() in ('Ubuntu', 'Debian'):
        opt_file = '/etc/default/ngas'

    # Script file installation
    sudo('cp %s/fabfile/init/sysv/ngas-server /etc/init.d/' % (nsd,))
    sudo('chmod 755 /etc/init.d/ngas-server')

    # Options file installation and edition
    ntype = settings['NGAS_SERVER_TYPE']
    sudo('cp %s/fabfile/init/sysv/ngas-server.options %s' % (nsd, opt_file))
    sudo('chmod 644 %s' % (opt_file,))
    sed(opt_file, '^USER=.*', 'USER=%s' % (nuser,), use_sudo=True, backup='')
    sed(opt_file, '^CFGFILE=.*', 'CFGFILE=%s' % (cfgfile,), use_sudo=True, backup='')
    if ntype == 'cache':
        sed(opt_file, '^CACHE=.*', 'CACHE=YES', use_sudo=True, backup='')
    elif ntype == 'data-mover':
        sed(opt_file, '^DATA_MOVER=.*', 'DATA_MOVER=YES', use_sudo=True, backup='')

    # Enabling init file on boot
    if check_command('update-rc.d'):
        sudo('update-rc.d ngas-server defaults')
    else:
        sudo('chkconfig --add ngas-server')

    success("NGAS init script installed")


@task
def start_APP_and_check_status():
    """
    Starts the ngamsDaemon process and checks that the server is up and running.
    Then it shuts down the server
    """

    # We sleep 2 here as it was found on Mac deployment to docker container that the
    # shell would exit before the ngasDaemon could detach, thus resulting in no startup.
    virtualenv('ngamsDaemon start -cfg {0} && sleep 2'.format(env.tgt_cfg))
    try:
        res = virtualenv('ngamsDaemon status -cfg {0}'.format(env.tgt_cfg), warn_only=True)
        if res.failed:
            failure("Couldn't contact NGAS server after starting it. "
                    "Check log files under %s/log/ to find out what went wrong" % APP_source_dir(),
                    with_stars=False)
        else:
            success('NGAS server started correctly :)')
    finally:
        info("Shutting NGAS server down now")
        virtualenv("ngamsDaemon stop -cfg {0}".format(env.tgt_cfg))

@task
def sysinitstart_NGAS_and_check_status():
    """
    Starts the ngamsDaemon process and checks that the server is up and running.
    Then it shuts down the server
    """

    # We sleep 2 here as it was found on Mac deployment to docker container that the
    # shell would exit before the ngasDaemon could detach, thus resulting in no startup.
    sudo('service ngas-server start && sleep 2')
    try:
        res = sudo('service ngas-server status', warn_only=True)
        print(res)
        if res.failed:
            failure("Couldn't contact NGAS server after starting it. "
                    "Check log files under %s/log/ to find out what went wrong" % APP_source_dir(),
                    with_stars=False)
        else:
            success('NGAS server started correctly :)')
    finally:
        info("Shutting NGAS server down now")
        sudo("service ngas-server stop ")


def prepare_ngas_data_dir():
    """Creates a new NGAS root directory"""

    info('Preparing NGAS root directory')
    nrd = APP_root_dir()
    tgt_cfg = os.path.join(nrd, 'cfg', 'ngamsServer.conf')
    with cd(APP_source_dir()):

        cmd = ['./prepare_ngas_root.sh']
        if 'NGAS_OVERWRITE_ROOT' in env and env.NGAS_OVERWRITE_ROOT:
            cmd.append('-f')
        cmd.append(nrd)
        res = run(' '.join(cmd), quiet=True)
        if res.succeeded:
            success("NGAS data directory ready")
            env.tgt_cfg = tgt_cfg
            return tgt_cfg

    # Deal with the errors here
    error = 'NGAS root directory preparation under {0} failed.\n'.format(nrd)
    if res.return_code == 2:
        error = (nrd + " already exists. Specify NGAS_OVERWRITE_ROOT to overwrite, "
                 "or a different NGAS_ROOT_DIR location")
    else:
        error = res
    abort(error)

def install_docker_compose():
    pass


env.build_cmd = APP_build_cmd
env.APP_init_install_function = install_sysv_init_script
env.APP_start_check_function = start_APP_and_check_status
env.sysinitAPP_start_check_function = sysinitstart_NGAS_and_check_status
env.prepare_APP_data_dir = prepare_ngas_data_dir
env.APP_extra_sudo_function = install_docker_compose

