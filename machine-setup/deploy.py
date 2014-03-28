"""
Fabric file for installing NGAS servers

Test deployment on EC2 is simple as it only runs on one server
fab test_deploy

The tasks can be used individually and thus allow installations in very
diverse situations.

For a full deployment use the command

fab --set postfix=False -f machine-setup/deploy.py test_deploy

For a local installation under a normal user without sudo access

fab -u `whoami` -H <IP address> -f machine-setup/deploy.py user_deploy
"""
import glob

import boto
import os
import time

from fabric.api import put, env, require, local, task
from fabric.api import run as frun
from fabric.api import sudo as fsudo
from fabric.context_managers import cd, hide, settings
from fabric.contrib.console import confirm
from fabric.contrib.files import append, sed, comment
from fabric.contrib.project import rsync_project
from fabric.decorators import task, serial
from fabric.operations import prompt
from fabric.utils import puts, abort, fastprint

FILTER = 'The cray-mpich2 module is now deprecated and will be removed in a future release.\r\r\nPlease use the cray-mpich module.'

def run(*args, **kwargs):
    res = frun(*args, **kwargs)
    res = res.replace(FILTER,'')
    res = res.replace('\n','')
    res = res.replace('\r','')
    return res

def sudo(*args, **kwargs):
    res = fsudo(*args, **kwargs)
    res = res.replace(FILTER, '')
    res = res.replace('\n','')
    res = res.replace('\r','')
    return res

#Defaults
thisDir = os.path.dirname(os.path.realpath(__file__))

BRANCH = 'master'    # this is controlling which branch is used in git clone
USERNAME = 'ec2-user'
POSTFIX = False
AMI_IDs = {'CentOS':'ami-aecd60c7', 'SLES':'ami-e8084981'}
AMI_ID = AMI_IDs['CentOS']
INSTANCE_NAME = 'NGAS_{0}'.format(BRANCH)
INSTANCE_TYPE = 't1.micro'
INSTANCES_FILE = os.path.expanduser('~/.aws/aws_instances')
AWS_KEY = os.path.expanduser('~/.ssh/icrar_ngas.pem')
KEY_NAME = 'icrar_ngas'
ELASTIC_IP = 'False'
SECURITY_GROUPS = ['NGAS'] # Security group allows SSH
NGAS_USERS = ['ngas','ngasmgr']
NGAS_PYTHON_VERSION = '2.7'
NGAS_PYTHON_URL = 'http://www.python.org/ftp/python/2.7.6/Python-2.7.6.tgz'
NGAS_DIR = 'ngas_rt' #NGAS runtime directory
NGAS_DEF_CFG = 'NgamsCfg.SQLite.mini.xml'
GITUSER = 'icrargit'
GITREPO = 'gitsrv.icrar.org:ngas'

YUM_PACKAGES = [
   'python27-devel',
   'git',
   'autoconf',
   'libtool',
   'zlib-devel',
   'db4-devel',
   'gdbm-devel',
   'readline-devel',
   'sqlite-devel',
   'make',
   'gcc',
   'java-1.6.0-openjdk-devel.x86_64',
   'postfix',
   'openssl-devel.x86_64',
   'wget.x86_64',
   'postgresql-devel.x86_64',
]

APT_PACKAGES = [
        'libtool',
        'autoconf',
        'zlib1g-dbg',
        'libzlcore-dev',
        'libdb4.8-dev',
        'libgdbm-dev',
        'openjdk-6-jdk',
        'libreadline-dev',
        'sqlite3',
        'libsqlite3-dev',
        'postgresql-client',
        ]

SLES_PACKAGES = [
                 'git',
                 'automake',
                 'autoconf',
                 'libtool',
                 'zlib',
                 'zlib-devel',
                 'gdbm-devel',
                 'readline-devel',
                 'sqlite3-devel',
                 'make',
                 'postfix',
                 'openssl-devel',
                 'wget',
                 'java-1_7_0-ibm-devel',
                 'libdb-4_5',
                 'libdb-4_5-devel',
                 'gcc',
                 'postgresql-devel',
                 ]

PYTHON_PACKAGES = [
        'zc.buildout',
        'pycrypto',
        'paramiko',
        'Fabric',
        'boto',
        'markup',
        'egenix-mx-base',
        'bsddb3',
        'bottle',
        ]


PUBLIC_KEYS = os.path.expanduser('~/.ssh')
# WEB_HOST = 0
# UPLOAD_HOST = 1
# DOWNLOAD_HOST = 2

def set_env():
    # set environment to default for EC2, if not specified on command line.

    # puts(env)
    if not env.has_key('GITUSER') or not env.GITUSER:
        env.GITUSER = GITUSER
    if not env.has_key('GITREPO') or not env.GITREPO:
        env.GITREPO = GITREPO
    if not env.has_key('postfix') or not env.postfix:
        env.postfix = POSTFIX
    if not env.has_key('user') or not env.user:
        env.user = USERNAME
    if not env.has_key('NGAS_USERS') or not env.NGAS_USERS:
        env.NGAS_USERS = NGAS_USERS
    elif type(env.NGAS_USERS) == type(''): # if its just a string
        print "NGAS_USERS preset to {0}".format(env.NGAS_USERS)
        env.NGAS_USERS = [env.NGAS_USERS] # change the type
    if not env.has_key('HOME') or env.HOME[0] == '~' or not env.HOME:
        env.HOME = run("echo ~{0}".format(NGAS_USERS[0]))
    if not env.has_key('src_dir') or not env.src_dir:
        env.src_dir = ''
    require('hosts', provided_by=[test_env])
    if not env.has_key('HOME') or env.HOME[0] == '~' or not env.HOME:
        env.HOME = run("echo ~{0}".format(NGAS_USERS[0]))
    if not env.has_key('PREFIX') or env.PREFIX[0] == '~' or not env.PREFIX:
        env.PREFIX = env.HOME
    if not env.has_key('NGAS_DIR_ABS') or env.NGAS_DIR_ABS[0] == '~' \
    or not env.NGAS_DIR_ABS:
        env.NGAS_DIR_ABS = '{0}/{1}'.format(env.PREFIX, NGAS_DIR)
        env.NGAS_DIR = NGAS_DIR
    else:
        env.NGAS_DIR = env.NGAS_DIR_ABS.split('/')[-1]
    if not env.has_key('force') or not env.force:
        env.force = 0
    if not env.has_key('ami_name') or not env.ami_name:
        env.ami_name = 'CentOS'
    env.AMI_ID = AMI_IDs[env.ami_name]
    if env.ami_name == 'SLES':
        env.user = 'root'
    get_linux_flavor()
    puts("""Environment:
            USER:              {0};
            Key file:          {1};
            hosts:             {2};
            host_string:       {3};
            postfix:           {4};
            HOME:              {8};
            NGAS_DIR_ABS:      {5};
            NGAS_DIR:          {6};
            NGAS_USERS:        {7};
            """.\
            format(env.user, env.key_filename, env.hosts,
                   env.host_string, env.postfix, env.NGAS_DIR_ABS,
                   env.NGAS_DIR, env.NGAS_USERS, env.HOME))


@task
def create_instance(names, use_elastic_ip, public_ips):
    """Create the EC2 instance

    :param names: the name to be used for this instance
    :type names: list of strings
    :param boolean use_elastic_ip: is this instance to use an Elastic IP address

    :rtype: string
    :return: The public host name of the AWS instance
    """

    puts('Creating instances {0} [{1}:{2}]'.format(names, use_elastic_ip, public_ips))
    number_instances = len(names)
    if number_instances != len(public_ips):
        abort('The lists do not match in length')

    # This relies on a ~/.boto file holding the '<aws access key>', '<aws secret key>'
    conn = boto.connect_ec2()

    if use_elastic_ip:
        # Disassociate the public IP
        for public_ip in public_ips:
            if not conn.disassociate_address(public_ip=public_ip):
                abort('Could not disassociate the IP {0}'.format(public_ip))

    reservations = conn.run_instances(env.AMI_ID, instance_type=INSTANCE_TYPE, \
                                    key_name=KEY_NAME, security_groups=SECURITY_GROUPS,\
                                    min_count=number_instances, max_count=number_instances)
    instances = reservations.instances
    # Sleep so Amazon recognizes the new instance
    for i in range(4):
        fastprint('.')
        time.sleep(5)

    # Are we running yet?
    for i in range(number_instances):
        while not instances[i].update() == 'running':
            fastprint('.')
            time.sleep(5)

    # Sleep a bit more Amazon recognizes the new instance
    for i in range(4):
        fastprint('.')
        time.sleep(5)
    puts('.')

    # Tag the instance
    for i in range(number_instances):
        conn.create_tags([instances[i].id], {'Name': names[i]})

    # Associate the IP if needed
    if use_elastic_ip:
        for i in range(number_instances):
            puts('Current DNS name is {0}. About to associate the Elastic IP'.format(instances[i].dns_name))
            if not conn.associate_address(instance_id=instances[i].id, public_ip=public_ips[i]):
                abort('Could not associate the IP {0} to the instance {1}'.format(public_ips[i], instances[i].id))

    # Give AWS time to switch everything over
    time.sleep(10)

    # Load the new instance data as the dns_name may have changed
    host_names = []
    for i in range(number_instances):
        instances[i].update(True)
        puts('Current DNS name is {0} after associating the Elastic IP'.format(instances[i].dns_name))
        host_names.append(str(instances[i].dns_name))


    # The instance is started, but not useable (yet)
    puts('Started the instance(s) now waiting for the SSH daemon to start.')
    for i in range(12):
        fastprint('.')
        time.sleep(5)
    puts('.')

    return host_names


def to_boolean(choice, default=False):
    """Convert the yes/no to true/false

    :param choice: the text string input
    :type choice: string
    """
    valid = {"yes":True,   "y":True,  "ye":True,
             "no":False,     "n":False}
    choice_lower = choice.lower()
    if choice_lower in valid:
        return valid[choice_lower]
    return default

def check_command(command):
    """
    Check existence of command remotely

    INPUT:
    command:  string

    OUTPUT:
    Boolean
    """
    res = run('if command -v {0} &> /dev/null ;then command -v {0};else echo ;fi'.format(command))
    return res

def check_dir(directory):
    """
    Check existence of remote directory
    """
    res = run("""if [ -d {0} ]; then echo 1; else echo ; fi""".format(directory))
    return res


def check_path(path):
    """
    Check existence of remote path
    """
    res = run('if [ -e {0} ]; then echo 1; else echo ; fi'.format(path))
    return res


def check_python():
    """
    Check for the existence of correct version of python

    INPUT:
    None

    OUTPUT:
    path to python binary    string, could be empty string
    """
    # Try whether there is already a local python installation for this user
    ppath = env.NGAS_DIR_ABS+'/../python'
    ppath = check_command('{0}/bin/python{1}'.format(ppath, NGAS_PYTHON_VERSION))
    if ppath:
        env.PYTHON = ppath
        return ppath
    # Try python2.7 first
    ppath = check_command('python{0}'.format(NGAS_PYTHON_VERSION))
    if ppath:
        env.PYTHON = ppath
        return ppath

    # don't check for any other python, since we need to run
    # all the stuff with a version number.
#    elif check_command('python'):
#        res = run('python -V')
#        if res.find(NGAS_PYTHON_VERSION) >= 0:
#            return check_command('python')
#        else:
#            return ''
#    else:
#        return ''

def install_yum(package):
    """
    Install a package using YUM
    """
    errmsg = sudo('yum --assumeyes --quiet install {0}'.format(package),\
                   combine_stderr=True, warn_only=True)
    processCentOSErrMsg(errmsg)


def install_zypper(package):
    """
    Install a package using zypper (SLES)
    """
    sudo('zypper --non-interactive install {0}'.format(package),\
                   combine_stderr=True, warn_only=True)



def install_apt(package):
    """
    Install a package using APT

    NOTE: This requires sudo access
    """
    sudo('apt-get -qq -y install {0}'.format(package))


def check_yum(package):
    """
    Check whether package is installed or not

    NOTE: requires sudo access to machine
    """
    with hide('stdout','running','stderr'):
        res = sudo('yum --assumeyes --quiet list installed {0}'.format(package), \
             combine_stderr=True, warn_only=True)
    #print res
    if res.find(package) > 0:
        print "Installed package {0}".format(package)
        return True
    else:
        print "NOT installed package {0}".format(package)
        return False


def check_apt(package):
    """
    Check whether package is installed using APT

    NOTE: This requires sudo access
    """
    # TODO
    with hide('stdout','running'):
        res = sudo('dpkg -L | grep {0}'.format(package))
    if res.find(package) > -1:
        print "Installed package {0}".format(package)
        return True
    else:
        print "NOT installed package {0}".format(package)
        return False


def copy_public_keys():
    """
    Copy the public keys to the remote servers
    """
    env.list_of_users = []
    for file in glob.glob(PUBLIC_KEYS + '/*.pub'):
        filename = '.ssh/{0}'.format(os.path.basename(file))
        user, ext = os.path.splitext(filename)
        env.list_of_users.append(user)
        put(file, filename)

def virtualenv(command):
    """
    Just a helper function to execute commands in the virtualenv
    """
    env.activate = 'source {0}/bin/activate'.format(env.NGAS_DIR_ABS)
    with cd(env.NGAS_DIR_ABS):
        run(env.activate + '&&' + command)

def git_pull():
    """
    Updates the repository.
    TODO: This does not work outside iVEC. The current implementation
    is thus using a tar-file, copied over from the calling machine.
    """
    with cd(env.NGAS_DIR_ABS):
        sudo('git pull', user=env.user)

def git_clone():
    """
    Clones the NGAS repository.
    """
    copy_public_keys()
    with cd(env.NGAS_DIR_ABS):
        run('git clone {0}@{1} -b {2}'.format(env.GITUSER, env.GITREPO, BRANCH))


@task
def git_clone_tar(standalone=0):
    """
    Clones the repository into /tmp and packs it into a tar file

    TODO: This does not work outside iVEC. The current implementation
    is thus using a tar-file, copied over from the calling machine.
    """
    set_env()
    if not env.src_dir:
        local('cd /tmp && git clone {0}@{1} -b {2} {2}'.format(env.GITUSER, env.GITREPO, BRANCH))
        local('cd /tmp && mv {0} {1}'.format(BRANCH, env.NGAS_DIR))
        tar_dir = '/tmp/{0}'.format(env.NGAS_DIR)
    else:
        tar_dir = '{0}/..'.format(env.src_dir)
        local('cd {0} && mv {1} {2}'.format(tar_dir, os.path.basename(env.src_dir), env.NGAS_DIR))
        tdir = tar_dir.split('/')[1:-2]
        print tdir
        tdir = '/' + '/'.join(tdir)
        tar_dir = tdir+'/'+env.NGAS_DIR
    if standalone:
        local('cd {0}/.. && tar -cjf {1}.tar.bz2 --exclude BIG_FILES \
            --exclude .git {1}'.format(tar_dir, env.NGAS_DIR))
    else:
        local('cd {0}/.. && tar -cjf {1}.tar.bz2 --exclude BIG_FILES \
            --exclude .git --exclude eggs.tar.gz {1}'.format(tar_dir, env.NGAS_DIR))
    tarfile = '{0}.tar.bz2'.format(env.NGAS_DIR)
    put('{0}/../{1}'.format(tar_dir,tarfile), '{1}/../{0}'.format(tarfile, env.NGAS_DIR_ABS))
    local('rm -rf /tmp/{0}'.format(env.NGAS_DIR))  # cleanup local git clone dir
    with cd(env.NGAS_DIR_ABS+'/..'):
        run('tar -xjf {0} && cp {0} /tmp'.format(tarfile))


@task
def ngas_minimal_tar():
    """
    This function packs the minimal required parts of the NGAS source tree
    into a tar file and copies it to the remote site.
    """
    set_env()
    parts = ['src',
             'cfg',
             'NGAS',
             'COPYRIGHT',
             'README',
             'INSTALL',
             'LICENSE',
             'VERSION',
             'bootstrap.py',
             'buildout.cfg',
             'doc',
             'hooks',
             'machine_setup',
             'setup.py',
             ]
    excludes = ['.git',
                ]
    exclude = ' --exclude ' + ' --exclude '.join(excludes)
    local('cd {0}/../.. && tar {1} -czf /tmp/ngas_src.tar.gz ngas'.format(thisDir, exclude))
    put('/tmp/ngas_src.tar.gz','/tmp/ngas.tar.gz')
    run('cd {0} && tar --strip-components 1 -xzf /tmp/ngas.tar.gz'.format(env.NGAS_DIR_ABS))

def processCentOSErrMsg(errmsg):
    if (errmsg == None or len(errmsg) == 0):
        return
    if (errmsg == 'Error: Nothing to do'):
        return
    firstKey = errmsg.split()[0]
    if (firstKey == 'Error:'):
        abort(errmsg)

@task
def get_linux_flavor():
    """
    Obtain and set the env variable linux_flavor
    """
    if (check_path('/etc/issue')):
        re = run('cat /etc/issue')
        linux_flavor = re.split()
        if (len(linux_flavor) > 0):
            if linux_flavor[0] == 'CentOS' or linux_flavor[0] == 'Ubuntu' \
               or linux_flavor[0] == 'Debian':
                linux_flavor = linux_flavor[0]
            elif linux_flavor[0] == 'Amazon':
                linux_flavor = ' '.join(linux_flavor[:2])
            elif linux_flavor[2] == 'SUSE':
                linux_flavor = linux_flavor[2]
    else:
        linux_flavor = run('uname -s')

    print "Remote machine running %s" % linux_flavor
    env.linux_flavor = linux_flavor
    return linux_flavor

@task
def system_install_f():
    """
    Perform the system installation part.

    NOTE: Most of this requires sudo access on the machine(s)
    """
    set_env()

    # Install required packages
    linux_flavor = get_linux_flavor()
    if (linux_flavor in ['CentOS','Amazon Linux']):
         # Update the machine completely
        errmsg = sudo('yum --assumeyes --quiet update', combine_stderr=True, warn_only=True)
        processCentOSErrMsg(errmsg)
        for package in YUM_PACKAGES:
            install_yum(package)

    elif (linux_flavor in ['Ubuntu', 'Debian']):
        errmsg = sudo('apt-get -qq -y update', combine_stderr=True, warn_only=True)
        for package in APT_PACKAGES:
            install_apt(package)
    elif linux_flavor == 'SUSE':
        errmsg = sudo('zypper -n -q patch', combine_stderr=True, warn_only=True)
        for package in SLES_PACKAGES:
            install_zypper(package)
    else:
        abort("Unknown linux flavor detected: {0}".format(re))


@task
def system_check():
    """
    Check for existence of system level packages

    NOTE: This requires sudo access on the machine(s)
    """
    with hide('running','stderr','stdout'):
        set_env()

        re = run('cat /etc/issue')
    linux_flavor = re.split()
    if (len(linux_flavor) > 0):
        if linux_flavor[0] == 'CentOS':
            linux_flavor = linux_flavor[0]
        elif linux_flavor[0] == 'Amazon':
            linux_flavor = ' '.join(linux_flavor[:2])

    summary = True
    if (linux_flavor in ['CentOS','Amazon Linux']):
        for package in YUM_PACKAGES:
            if not check_yum(package):
                summary = False
    elif (linux_flavor == 'Ubuntu'):
        for package in APT_PACKAGE:
            if not check_apt(package):
                summary = False
    else:
        abort("Unknown linux flavor detected: {0}".format(re))
    if summary:
        print "\n\nAll required packages are installed."
    else:
        print "\n\nAt least one package is missing!"


@task
def postfix_config():
    """
    Setup the e-mail system for the NGAS
    notifications. It requires access to an SMTP server.
    """

    if 'gmail_account' not in env:
        prompt('GMail Account:', 'gmail_account')
    if 'gmail_password' not in env:
        prompt('GMail Password:', 'gmail_password')

    # Setup postfix
    sudo('service sendmail stop')
    sudo('service postfix stop')
    sudo('chkconfig sendmail off')
    sudo('chkconfig sendmail --del')

    sudo('chkconfig postfix --add')
    sudo('chkconfig postfix on')

    sudo('service postfix start')

    sudo('''echo "relayhost = [smtp.gmail.com]:587
smtp_sasl_auth_enable = yes
smtp_sasl_password_maps = hash:/etc/postfix/sasl_passwd
smtp_sasl_security_options = noanonymous
smtp_tls_CAfile = /etc/postfix/cacert.pem
smtp_use_tls = yes

# smtp_generic_maps
smtp_generic_maps = hash:/etc/postfix/generic
default_destination_concurrency_limit = 1" >> /etc/postfix/main.cf''')

    sudo('echo "[smtp.gmail.com]:587 {0}@gmail.com:{1}" > /etc/postfix/sasl_passwd'.format(env.gmail_account, env.gmail_password))
    sudo('chmod 400 /etc/postfix/sasl_passwd')
    sudo('postmap /etc/postfix/sasl_passwd')

@task
def user_setup():
    """
    setup ngas users.

    TODO: sort out the ssh keys
    """

    set_env()
    if not env.user:
        env.user = USERNAME # defaults to ec2-user
    group = 'ngas'
    sudo('groupadd ngas', warn_only=True)
    for user in env.NGAS_USERS:
        sudo('useradd -g {0} -m -s /bin/bash {1}'.format(group, user), warn_only=True)
        sudo('mkdir /home/{0}/.ssh'.format(user), warn_only=True)
        sudo('chmod 700 /home/{0}/.ssh'.format(user))
        sudo('chown -R {0}:{1} /home/{0}/.ssh'.format(user,group))
        home = run('echo $HOME')
        sudo('cp {0}/.ssh/authorized_keys /home/{1}/.ssh/authorized_keys'.format(home, user))
        sudo('chmod 600 /home/{0}/.ssh/authorized_keys'.format(user))
        sudo('chown {0}:{1} /home/{0}/.ssh/authorized_keys'.format(user, group))


@task
def python_setup():
    """
    Ensure that there is the right version of python available
    If not install it from scratch in user directory.

    INPUT:
    None

    OUTPUT:
    None
    """
    set_env()

    with cd('/tmp'):
        run('wget --no-check-certificate -q {0}'.format(NGAS_PYTHON_URL))
        base = os.path.basename(NGAS_PYTHON_URL)
        pdir = os.path.splitext(base)[0]
        run('tar -xzf {0}'.format(base))
    ppath = env.NGAS_DIR_ABS + '/../python'
    with cd('/tmp/{0}'.format(pdir)):
        run('./configure --prefix {0};make;make install'.format(ppath))
        ppath = '{0}/bin/python{1}'.format(ppath,NGAS_PYTHON_VERSION)
    env.PYTHON = ppath
    print "\n\n******** PYTHON INSTALLATION COMPLETED!********\n\n"


@task
def virtualenv_setup():
    """
    setup virtualenv with the detected or newly installed python
    """
    set_env()
    check_python()
    print "CHECK_DIR: {0}".format(env.NGAS_DIR_ABS+'/src')
    if check_dir(env.NGAS_DIR_ABS+'/src') and not env.force:
        abort('ngas_rt directory exists already')

    with cd('/tmp'):
        put('{0}/../clib_tars/virtualenv-1.10.tar.gz'.format(thisDir), 'virtualenv-1.10.tar.gz')
        run('tar -xzf virtualenv-1.10.tar.gz')
        run('cd virtualenv-1.10; {0} virtualenv.py {1}'.format(env.PYTHON, env.NGAS_DIR_ABS))
    print "\n\n******** VIRTUALENV SETUP COMPLETED!********\n\n"



@task
def ngas_buildout(standalone=0, typ='archive'):
    """
    Perform just the buildout and virtualenv config

    if standalone is not 0 then the eggs from the additional_tars
    will be installed to avoid accessing the internet.
    """
    set_env()

    with cd(env.NGAS_DIR_ABS):
        put('data/common.py.patch', '{0}/.'.\
            format(env.NGAS_DIR_ABS))
        if (standalone):
            put('{0}/../additional_tars/eggs.tar.gz'.format(thisDir), '{0}/eggs.tar.gz'.format(env.NGAS_DIR_ABS))
            run('tar -xzf eggs.tar.gz')
            run('patch eggs/minitage.recipe.common-1.90-py2.7.egg/minitage/recipe/common/common.py common.py.patch')
            virtualenv('buildout -Nvo')
        else:
            run('patch eggs/minitage.recipe.common-1.90-py2.7.egg/minitage/recipe/common/common.py common.py.patch')
            virtualenv('buildout')
        run('cp -R {0}/NGAS {0}/../NGAS'.format(env.NGAS_DIR_ABS))
        run('ln -s {0}/cfg/{1} {0}/../NGAS/cfg/{2}'.format(\
              env.NGAS_DIR_ABS, initName(typ=typ)[2], initName(typ=typ)[3]))
        nda = '\/'+'\/'.join(env.NGAS_DIR_ABS.split('/')[1:-1])+'\/NGAS'
        run("""sed -i '' 's/RootDirectory="\/home\/ngas\/NGAS"/RootDirectory="{0}"/' cfg/NgamsCfg.SQLite.mini.xml""".format(nda))
        with cd('../NGAS'):
            with settings(warn_only=True):
                run('sqlite3 -init {0}/src/ngamsSql/ngamsCreateTables-SQLite.sql ngas.sqlite <<< $(echo ".quit")'\
                    .format(env.NGAS_DIR_ABS))
                run('cp ngas.sqlite {0}/src/ngamsTest/src/ngas_Sqlite_db_template'.format(env.NGAS_DIR_ABS))


    print "\n\n******** NGAS_BUILDOUT COMPLETED!********\n\n"


@task
def ngas_full_buildout(standalone=0, typ='archive'):
    """
    Perform the full install and buildout
    """
    set_env()
    # First get the sources
    #
    if (standalone):
        ngas_minimal_tar()
    elif not check_path('{0}/bootstrap.py'.format(env.NGAS_DIR_ABS)):
        git_clone_tar(standalone=standalone)

    with cd(env.NGAS_DIR_ABS):
        virtualenv('pip install clib_tars/zc.buildout-2.2.1.tar.gz')
        virtualenv('pip install clib_tars/pycrypto-2.6.tar.gz')
        virtualenv('pip install clib_tars/paramiko-1.11.0.tar.gz')
        # make this installation self consistent
        virtualenv('pip install clib_tars/Fabric-1.7.0.tar.gz')
        virtualenv('pip install clib_tars/boto-2.13.0.tar.gz')
        virtualenv('pip install clib_tars/markup-1.9.tar.gz')
        virtualenv('pip install additional_tars/egenix-mx-base-3.2.6.tar.gz')
        #The following will only work if the Berkeley DB had been installed already
        virtualenv('pip install additional_tars/bsddb3-6.0.0.tar.gz')
        virtualenv('pip install additional_tars/bottle-0.11.6.tar.gz')

        # run bootstrap with correct python version (explicit)
        run('if [ -a bin/python ] ; then rm bin/python ; fi') # avoid the 'busy' error message
        virtualenv('python{0} bootstrap.py'.format(NGAS_PYTHON_VERSION))

    ngas_buildout(standalone=standalone, typ=typ)

    # put the activation of the virtualenv into the login profile of the user
    if not check_path('.bash_profile_orig'):
        run('cp .bash_profile .bash_profile_orig')
    else:
        run('cp .bash_profile_orig .bash_profile')
    run('echo "export NGAS_PREFIX={0}\n" >> .bash_profile'.format(env.PREFIX))
    run('echo "source {0}/bin/activate\n" >> .bash_profile'.format(env.NGAS_DIR_ABS))

    print "\n\n******** NGAS_FULL_BUILDOUT COMPLETED!********\n\n"



@task
@serial
def test_env():
    """Configure the test environment on EC2

    Ask a series of questions before deploying to the cloud.

    Allow the user to select if a Elastic IP address is to be used
    """
    if not env.has_key('instance_name') or not env.instance_name:
        env.instance_name = INSTANCE_NAME
    if not env.has_key('use_elastic_ip') or not env.use_elastic_ip:
        env.use_elastic_ip = ELASTIC_IP
    if not env.has_key('key_filename') or not env.key_filename:
        env.key_filename = AWS_KEY
    if not env.has_key('ami_name') or not env.ami_name:
        env.ami_name = 'CentOS'
    env.AMI_ID = AMI_IDs[env.ami_name]
    env.instance_name = INSTANCE_NAME
    env.use_elastic_ip = ELASTIC_IP
    if 'use_elastic_ip' in env:
        use_elastic_ip = to_boolean(env.use_elastic_ip)
    else:
        use_elastic_ip = confirm('Do you want to assign an Elastic IP to this instance: ', False)

    public_ip = None
    if use_elastic_ip:
        if 'public_ip' in env:
            public_ip = env.public_ip
        else:
            public_ip = prompt('What is the public IP address: ', 'public_ip')

    if 'instance_name' not in env:
        prompt('AWS Instance name: ', 'instance_name')

    # Create the instance in AWS
    host_names = create_instance([env.instance_name], use_elastic_ip, [public_ip])
    env.hosts = host_names
    if not env.host_string:
        env.host_string = env.hosts[0]
    env.user = USERNAME
    if env.ami_name == 'SLES':
        env.user = 'root'

    env.key_filename = AWS_KEY
    env.roledefs = {
        'ngasmgr' : host_names,
        'ngas' : host_names,
    }
    print "\n\n******** EC2 ENVIRONMENT SETUP!********\n\n"



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
def user_deploy(typ='archive', standalone=0):
    """
    Deploy the system as a normal user without sudo access
    NOTE: The parameter can be passed from the command line by using

    fab -f deploy.py user_deploy:typ='cache'
    """
    env.HOME = run("echo ~{0}".format(env.user))
    set_env()
    ppath = check_python()
    if not ppath:
        python_setup()
    else:
        env.PYTHON = ppath
    virtualenv_setup()
    ngas_full_buildout(standalone=standalone, typ=typ)

    print "\n\n******** INSTALLATION COMPLETED!********\n\n"


@task
def init_deploy(typ='archive'):
    """
    Install the NGAS init script for an operational deployment
    """
    (initFile, initLink, cfg, lcfg) = initName(typ=typ)

    set_env()

    sudo('cp {0}/src/ngamsStartup/{1} /etc/init.d/{2}'.\
         format(env.NGAS_DIR_ABS, initFile, initLink))
    sudo('chmod a+x /etc/init.d/{0}'.format(initLink))
    sudo('chkconfig --add /etc/init.d/{0}'.format(initLink))
    # on ubuntu, this should be
    # sudo('chkconfig --add {0}'.format(initLink))
    print "\n\n******** CONFIGURED INIT SCRIPTS!********\n\n"


@task
@serial
def operations_deploy(system_install=True, user_install=True, typ='archive', standalone=0):
    """
    ** MAIN TASK **: Deploy the full NGAS operational environment.
    In order to install NGAS on an operational host go to any host
    where NGAS is already running or where you have git-cloned the
    NGAS software and issue the command:

    fab -u <super-user> -H <host> -f machine_setup/deploy.py operations_deploy

    where <super-user> is a user on the target machine with root priviledges
    and <host> is either the DNS resolvable name of the target machine or
    its IP address.

    NOTE: The parameter can be passed from the command line by using

    fab -f deploy.py operations_deploy:typ='cache'
    """

    if not env.user:
        env.user = 'root'
    # set environment to default, if not specified otherwise.
    set_env()
    if system_install: system_install_f()
    if env.postfix:
        postfix_config()
    if user_install: user_setup()
    install()
    print "\n\n******** OPERATIONS_DEPLOY COMPLETED!********\n\n"


@task
@serial
def test_deploy():
    """
    ** MAIN TASK **: Deploy the full NGAS EC2 test environment.
    """

    test_env()
    # set environment to default for EC2, if not specified otherwise.
    set_env()
    system_install_f()
    if env.postfix:
        postfix_config()
    install()
    with settings(user='ngas'):
        run('ngamsDaemon start')
    print "\n\n******** SERVER STARTED!********\n\n"



@task
def install():
    """
    Install NGAS users and NGAS software on existing machine.
    Note: Requires root permissions!
    """
    set_env()
    user_setup()
    with settings(user='ngas'):
        ppath = check_python()
        if not ppath:
            python_setup()
    if env.PREFIX != env.HOME: # generate non-standard ngas_rt directory
        sudo('mkdir -p {0}'.format(env.PREFIX))
        sudo('chown -R {0}:ngas {1}'.format(env.NGAS_USERS[0], env.PREFIX))
    with settings(user='ngas'):
        virtualenv_setup()
        ngas_full_buildout()
    init_deploy()
    print "\n\n******** INSTALLATION COMPLETED!********\n\n"

@task
def uninstall():
    """
    Uninstall NGAS, NGAS users and init script.
    """
    set_env()
    sudo('userdel -r ngasmgr', warn_only=True)
    sudo('userdel -r ngas', warn_only=True)
    sudo('groupdel ngas', warn_only=True)
    sudo('rm /etc/ngamsServer.conf', warn_only=True)
    sudo('rm /etc/init.d/ngamsServer', warn_only=True)
    sudo('rm -rf {0}'.format(env.PREFIX), warn_only=True)
    print "\n\n******** UNINSTALL COMPLETED!********\n\n"

@task
def upgrade():
    """
    Upgrade the NGAS software on a target host using rsync.

    NOTE: This does NOT perform a new buildout, i.e. all the binaries and libraries are untouched.

    use --set src_dir=your/local/directory
    to point to the top-level NGAS soruce tree directory.
    """
    # use the PREFIX from the command line or try to set it from
    # the remote environment. If both fails bail-out.
    if not env.has_key('PREFIX') or not env.PREFIX:
        env.PREFIX = run('echo $NGAS_PREFIX')
    if not env.PREFIX:
        print 'Unable to identify location of NGAS installation!'
        print 'Please set the environment variable NGAS_PREFIX in .bash_profile.'
        print 'of the user running NGAS on the remote host.'
        abort('\n\n******** UPGRADE ABORTED!********\n\n')
    if not env.has_key('src_dir') or not env.src_dir:
        print 'Please specify the local source directory of the NGAS software'
        print 'on the command line using --set src_dir=your/local/directory'
        abort('\n\n******** UPGRADE ABORTED!********\n\n')
    else: # check whether the source directory setting is likely to be correct
        res = local('grep "The Next Generation Archive System" {0}/README'.format(env.src_dir), \
                    capture=True)
        if not res:
            abort('src_dir does not point to a valid NGAS source directory!!')
    set_env()
    run('$NGAS_PREFIX/ngas_rt/bin/ngamsDaemon stop')
    rsync_project(local_dir=env.src_dir+'/src', remote_dir=env.NGAS_DIR_ABS+'/src', exclude=".git")
    #git_clone_tar()
    run('$NGAS_PREFIX/ngas_rt/bin/ngamsDaemon start')
    print "\n\n******** UPGRADE COMPLETED!********\n\n"

