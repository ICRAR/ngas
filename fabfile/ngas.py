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
Main module where NGAS-specific tasks are carried out, like copying its sources,
installing it and making sure it works after starting it.
"""
import contextlib
import functools
import httplib
import os
import tempfile
import time
import urllib2

from fabric.colors import green, red
from fabric.context_managers import settings, cd
from fabric.contrib.files import exists, sed
from fabric.decorators import task, parallel
from fabric.operations import local, put
from fabric.state import env
from fabric.utils import puts, abort

from pkgmgr import install_system_packages, check_brew_port, check_brew_cellar
from system import check_dir, download, check_command, \
    postfix_config, create_user, get_linux_flavor, python_setup, check_python, \
    MACPORT_DIR
from utils import to_boolean, is_localhost, home, default_if_empty, sudo, run

# Don't re-export the tasks imported from other modules, only ours
__all__ = [
    'start_ngas_and_check_status',
    'test_ngas_status',
    'virtualenv_setup',
    'install_user_profile',
    'ngas_buildout',
    'ngas_full_buildout',
    'install',
    'copy_sources',
    'install_and_check'
]

# The username to use by default on remote hosts where NGAS is being installed
# This user might be different from the initial username used to connect to the
# remote host, in which case it will be created first
NGAS_USER = 'ngas'

# Name of the directory where NGAS sources will be expanded on the target host
# This is relative to the NGAS_USER home directory
NGAS_SRC_DIR_NAME = 'ngas_src'

# Name of the directory where NGAS root directory will be created
# This is relative to the NGAS_USER home directory
NGAS_ROOT_DIR_NAME = 'NGAS'

# Name of the directory where a virtualenv will be created to host the NGAS
# software installation, plus the installation of all its related software
# This is relative to the NGAS_USER home directory
NGAS_INSTALL_DIR_NAME = 'ngas_rt'

VIRTUALENV_URL = 'https://pypi.python.org/packages/source/v/virtualenv/virtualenv-12.0.7.tar.gz'

def ngas_user(check_localhost=True):
    key = 'NGAS_USER'
    if key not in env:
        # Sensible defaults
        if check_localhost and is_localhost():
            env[key] = env.user
        else:
            env[key] = NGAS_USER
    return env.NGAS_USER

def ngas_install_dir():
    key = 'NGAS_INSTALL_DIR'
    if key not in env:
        env[key] = os.path.abspath(os.path.join(home(), NGAS_INSTALL_DIR_NAME))
    return env[key]

def ngas_overwrite_installation():
    key = 'NGAS_OVERWRITE_INSTALLATION'
    return key in env

def ngas_root_dir():
    key = 'NGAS_ROOT_DIR'
    if key not in env:
        env[key] = os.path.abspath(os.path.join(home(), NGAS_ROOT_DIR_NAME))
    return env[key]

def ngas_source_dir():
    key = 'NGAS_SRC_DIR'
    if key not in env:
        env[key] = os.path.abspath(os.path.join(home(), NGAS_SRC_DIR_NAME))
    return env[key]

def ngas_no_client():
    key = 'NGAS_NO_CLIENT'
    return key in env

def ngas_develop():
    key = 'NGAS_DEVELOP'
    return key in env

def has_local_git_repo():
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    return os.path.exists(os.path.join(repo_root, '.git'))

def default_ngas_revision():
    if has_local_git_repo():
        return local('git rev-parse --abbrev-ref HEAD', capture=True)
    return 'local'

def ngas_revision():
    default_if_empty(env, 'NGAS_REV', default_ngas_revision)
    return env.NGAS_REV

def virtualenv(command, **kwargs):
    """
    Just a helper function to execute commands in the NGAS virtualenv
    """
    nid = ngas_install_dir()
    run('source {0}/bin/activate && {1}'.format(nid, command), **kwargs)

def initName(typ='archive'):
    """
    Helper function to set the name of the link to the config file.
    """
    if typ == 'archive':
        initFile = 'ngamsServer.init.sh'
        NGAS_DEF_CFG = 'NgamsCfg.SQLite.mini.xml'
        NGAS_LINK_CFG = 'ngamsServer.conf'
    elif typ == 'cache':
        initFile = 'ngamsCache.init.sh'
        NGAS_DEF_CFG = 'NgamsCfg.SQLite.cache.xml'
        NGAS_LINK_CFG = 'ngamsCacheServer.conf'
    return (initFile, initFile.split('.')[0], NGAS_DEF_CFG, NGAS_LINK_CFG)

@task
def start_ngas_and_check_status():
    """
    Starts the ngamsDaemon process and checks that the server is up and running
    """
    # We sleep 2 here as it was found on Mac deployment to docker container that the
    # shell would exit before the ngasDaemon could detach, thus resulting in no startup.
    virtualenv('ngamsDaemon start && sleep 2')

    # Give it a few seconds to make sure it started
    puts(green("\n******** SERVER STARTED!********\n"))
    time.sleep(3)

    try:
        test_ngas_status()
        puts(green("\n>>>>> SERVER STATUS CHECKED <<<<<<<<<<<\n"))
    except:
        puts(red("\n>>>>>>> SERVER STATUS NOT OK <<<<<<<<<<<<\n"))

@task
@parallel
def test_ngas_status():
    """
    Execute the STATUS command against the NGAS server on the host fabric is
    currently pointing at
    """
    try:
        serv = urllib2.urlopen('http://{0}:7777/STATUS'.format(env.host), timeout=5)
    except IOError:
        puts(red('Problem connecting to server {0}'.format(env.host)))
        raise

    response = serv.read()
    serv.close()
    if response.find('Status="SUCCESS"') == -1:
        puts(red('Problem with response from {0}, not SUCESS as expected'.format(env.host)))
        raise ValueError(response)
    else:
        puts(green('Response from {0} OK'.format(env.host)))

@task
def virtualenv_setup(python_install):
    """
    Creates a new virtualenv that will hold the NGAS installation
    """
    ngasInstallDir = ngas_install_dir()
    if check_dir(ngasInstallDir):
        overwrite = ngas_overwrite_installation()
        if not overwrite:
            msg = "%s exists already. Specify NGAS_OVERWRITE_INSTALLATION to overwrite"
            abort(msg % (ngasInstallDir,))
        run("rm -rf %s" % (ngasInstallDir,))

    # Check which python will be bound to the virtualenv
    ppath = check_python()
    if not ppath or str(python_install) == 'True':
        ppath = python_setup(os.path.join(home(),'python'))

    # Get virtualenv if necessary and create the new NGAS virtualenv,
    # making sure the new virtualenv ends up using the python executable
    # we gave as argument (or the default one)
    if check_command('virtualenv'):
        run('virtualenv {0} -p {1}'.format(ngasInstallDir, ppath))
    else:
        with cd('/tmp'):
            f = download(VIRTUALENV_URL)
            vbase = f.split('.tar.gz')[0]
            run('tar -xzf {0}.tar.gz'.format(vbase))
            with cd(vbase):
                run('{1} virtualenv.py -p {1} {0}'.format(ngasInstallDir, ppath))
            run('rm -rf virtualenv*')

    # Download this particular certifcate; otherwise pip complains
    # in some platforms
    if not(check_dir('~/.pip')):
        run('mkdir ~/.pip');
        with cd('~/.pip'):
            download('http://curl.haxx.se/ca/cacert.pem')
    run('echo "[global]" > ~/.pip/pip.conf; echo "cert = {0}/.pip/cacert.pem" >> ~/.pip/pip.conf;'.format(home()))

    # Update pip and install wheel; this way we can install binary wheels from
    # PyPI if available (like numpy)
    # TODO: setuptools and python-daemon are here only because
    #       python-daemon 2.1.2 is having a problem to install via setuptools
    #       but not via pip (see https://pagure.io/python-daemon/issue/2 and
    #       https://pagure.io/python-daemon/issue/3).
    #       When this problem is fixed we'll fix our dependency on python-daemo
    #       to avoid this issue entirely
    virtualenv('pip install -U pip wheel setuptools python-daemon')

    puts(green("\n******** VIRTUALENV SETUP COMPLETED!********\n"))

@task
def install_user_profile():
    """
    Put the activation of the virtualenv into the login profile of the user
    unless the NGAS_DONT_MODIFY_BASHPROFILE environment variable is defined

    NOTE: This will be executed for the user running NGAS.
    """
    if run('echo $NGAS_DONT_MODIFY_BASHPROFILE') or \
       'NGAS_NO_BASH_PROFILE' in env:
        return

    nid = ngas_install_dir()
    nrd = ngas_root_dir()
    with cd("~"):
        if not exists(".bash_profile_orig"):
            run('cp .bash_profile .bash_profile_orig', warn_only=True)
        else:
            run('cp .bash_profile_orig .bash_profile')

        script = ('if [ -f "{0}/bin/activate" ]'.format(nid),
                  'then',
                  '   source "{0}/bin/activate"'.format(nid),
                  'fi',
                  'export NGAS_PREFIX="{0}"'.format(nrd))

        run("echo '{0}' >> .bash_profile".format('\n'.join(script)))

def ngas_build_cmd(no_client, develop):
    # The installation of the bsddb package (needed by ngamsCore) is in
    # particular difficult because it requires some flags to be passed on
    # (particularly if using MacOSX's port
    build_cmd = []
    linux_flavor = get_linux_flavor()
    if linux_flavor == 'Darwin':
        pkgmgr = check_brew_port()
        if pkgmgr == 'brew':
            cellardir = check_brew_cellar()
            db_version = run('ls -tr1 {0}/berkeley-db'.format(cellardir)).split()[-1]
            build_cmd.append('BERKELEYDB_DIR={0}/berkeley-db/{1}'.format(cellardir, db_version))
        else:
            incdir = MACPORT_DIR + '/include/db60'
            libdir = MACPORT_DIR + '/lib/db60'
            build_cmd.append('BERKELEYDB_INCDIR=' + incdir)
            build_cmd.append('BERKELEYDB_LIBDIR=' + libdir)
            if not no_client:
                build_cmd.append('CFLAGS=-I' + incdir)
                build_cmd.append('LDFLAGS=-L' + libdir)
        build_cmd.append('YES_I_HAVE_THE_RIGHT_TO_USE_THIS_BERKELEY_DB_VERSION=1')
    build_cmd.append('./build.sh')
    if not no_client:
        build_cmd.append("-c")
    if develop:
        build_cmd.append("-d")
    return ' '.join(build_cmd)

@task
def ngas_buildout(typ='archive'):
    """
    Perform just the buildout and virtualenv config

    if env.standalone is not 0 then the eggs from the additional_tars
    will be installed to avoid accessing the internet.
    """
    nrd = ngas_root_dir()
    nsd = ngas_source_dir()
    no_client = ngas_no_client()
    develop = ngas_develop()

    with cd(nsd):

        # Main NGAMs compilation routine
        build_cmd = ngas_build_cmd(no_client, develop)
        virtualenv(build_cmd)

        # Installing and initializing an NGAS_ROOT directory
        _,_,cfg,lcfg,  = initName(typ=typ)
        ngasTargetCfg = os.path.join(nrd, 'cfg', lcfg)
        run('mkdir -p {0}'.format(nrd))
        run('cp -R NGAS/* {0}'.format(nrd))
        run('cp cfg/{0} {1}'.format(cfg, ngasTargetCfg))
        sed(ngasTargetCfg, '\*replaceRoot\*', nrd)

        # Initialize the SQlite database
        sql = "src/ngamsCore/ngamsSql/ngamsCreateTables-SQLite.sql"
        run('sqlite3 {0}/ngas.sqlite < {1}'.format(nrd, sql))


    puts(green("\n******** NGAS_BUILDOUT COMPLETED!********\n"))


@task
def ngas_full_buildout(typ='archive'):
    """
    Perform the full install and buildout
    """

    copy_sources()
    ngas_buildout(typ=typ)
    install_user_profile()

@task
def install(sys_install=True, user_install=True,
            init_install=True, typ='archive',
            python_install=False, postfix=False):
    """
    Install NGAS users and NGAS software on the host this task is being run on
    """

    user = ngas_user()

    # Prepare the system before doing anything
    if to_boolean(sys_install):
        install_system_packages()
    if to_boolean(postfix):
        postfix_config()
    if to_boolean(user_install):
        create_user(user)

    # Switch to the NGAS_USER for the rest of the installation procedure
    # This is possible because during create_user() we copy the public SSH
    # key we are using to the authorized_keys file of NGAS_USER
    with settings(user=user):
        virtualenv_setup(python_install)
        ngas_full_buildout(typ=typ)

        nsd = ngas_source_dir()
        nid = ngas_install_dir()

    # The NGAS_USER probably doesn't have sudo access, so we need to run this
    # bit using our original user
    if to_boolean(init_install):
        init_deploy(nsd, nid)

    puts(green("\n******** INSTALLATION COMPLETED!********\n"))


def init_deploy(nsd, nid, typ = 'archive'):
    """
    Install the NGAS init script for an operational deployment
    """

    initFile, initLinkName, _, _ = initName(typ = typ)
    initLinkAbs = '/etc/init.d/' + initLinkName

    sudo('cp {0}/src/ngamsStartup/{1} {2}'.format(nsd, initFile, initLinkAbs))
    sudo("sed -i 's/NGAS_USER=\"ngas\"/NGAS_USER=\"{0}\"/g' {1}".format(env.NGAS_USER, initLinkAbs))
    sudo("sed -i 's/NGAS_ROOT=\"\/home\/$NGAS_USER\/ngas_rt\"/NGAS_ROOT=\"{0}\"/g' {1}".\
         format(nid.replace('/', '\/'), initLinkAbs))
    sudo('chmod a+x {0}'.format(initLinkAbs))
    if (get_linux_flavor() in ['Ubuntu', 'SUSE', 'Suse']):
        sudo('chkconfig --add {0}'.format(initLinkName))
    else:
        sudo('chkconfig --add '.format(initLinkAbs))


def create_sources_tarball(tarball_filename):
    # Make sure we are git-archivin'ing from the root of the repository,
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if has_local_git_repo():
        local('cd {0}; git archive -o {1} {2}'.format(repo_root, tarball_filename, ngas_revision()))
    else:
        local('cd {0}; tar czf {1} .'.format(repo_root, tarball_filename))


@task
def copy_sources():
    """
    Creates a copy of the NGAS sources in the target host.
    """

    # We still don't open the git repository to the world, so for the time
    # being we always make a tarball from our repository and copy it over
    # ssh to the remote host, where we expand it back

    nsd = ngas_source_dir()

    # Because this could be happening in parallel in various machines
    # we generate a tmpfile locally, but the target file is the same
    local_file = tempfile.mktemp(".tar.gz")
    create_sources_tarball(local_file)

    # transfer the tar file if not local
    if not is_localhost():
        target_tarfile = '/tmp/ngas_tmp.tar'
        put(local_file, target_tarfile)
    else:
        target_tarfile = local_file

    # unpack the tar file into the ngas_src_dir
    # (mind the "p", to preserve permissions)
    run('mkdir -p {0}'.format(nsd))
    with cd(nsd):
        run('tar xpf {0}'.format(target_tarfile))
        if not is_localhost():
            run('rm {0}'.format(target_tarfile))

    # Cleaning up now
    local('rm {0}'.format(local_file))

@task
@parallel
def install_and_check(sys_install, user_install, init_install, typ):
    """
    Runs the full installation procedure and checks that the NGAS server is up
    and running after finishing
    """
    install(sys_install=sys_install, user_install=user_install, init_install=init_install, typ=typ)
    with settings(user=ngas_user()):
        start_ngas_and_check_status()

def upload_to(host, filename, port=7777):
    """
    Simple method to upload a file into NGAS
    """
    with contextlib.closing(httplib.HTTPConnection(host, port)) as conn:
        conn.putrequest('POST', '/QARCHIVE?filename=%s' % (urllib2.quote(os.path.basename(filename)),) )
        conn.putheader('Content-Length', os.stat(filename).st_size)
        conn.endheaders()
        with open(filename) as f:
            for data in iter(functools.partial(f.read, 4096), ''):
                conn.send(data)
        r = conn.getresponse()
        if r.status != httplib.OK:
            raise Exception("Error while QARCHIVE-ing %s to %s:%d:\nStatus: %d\n%s\n\n%s" % (filename, conn.host, conn.port, r.status, r.msg, r.read()))