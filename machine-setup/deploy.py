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

For a remote installation under non-default user ngas-user using a
non-default source directory for the installation you can use. This
installation is using a different (sudo) user on the target machine
to run the installation.

fab -u sudo_user -H <IP address> -f machine-setup/deploy.py user_deploy --set APP_USERS=ngas-user,src_dir=/tmp/ngas_test
"""
import glob

import boto, boto.ec2
import os
import time, urllib, inspect

from fabric.api import put, env, require, local, task
from fabric.api import run as frun
from fabric.api import sudo as fsudo
from fabric.context_managers import cd, hide, settings, warn_only
from fabric.contrib.console import confirm
from fabric.contrib.files import append, sed, comment
from fabric.contrib.project import rsync_project
from fabric.decorators import task, serial
from fabric.operations import prompt
from fabric.utils import puts, abort, fastprint
from fabric.exceptions import NetworkError
from fabric.colors import *

# FILTER = 'The cray-mpich2 module is now deprecated and will be removed in a future release.\r\r\nPlease use the cray-mpich module.'

def run(*args, **kwargs):
    FILTER = frun('echo')  # This should not return anything
    res = frun(*args, **kwargs)
    res = res.replace(FILTER,'')
#     res = res.replace('\n','')
#     res = res.replace('\r','')
    return res

def sudo(*args, **kwargs):
    FILTER = frun('echo')  # This should not return anything
    res = fsudo(*args, **kwargs)
    res = res.replace(FILTER, '')
    res = res.replace('\n','')
    res = res.replace('\r','')
    return res

#Defaults
thisDir = os.path.dirname(os.path.realpath(__file__))

#### This should be replaced by another key and security group
AWS_REGION = 'us-east-1'
AWS_PROFILE = 'NGAS'
KEY_NAME = 'icrar_ngas'
AWS_KEY = os.path.expanduser('~/.ssh/{0}.pem'.format(KEY_NAME))
SECURITY_GROUPS = ['NGAS'] # Security group allows SSH and other ports


BRANCH = 'master'    # this is controlling which branch is used in git clone
USERNAME = 'ec2-user'
POSTFIX = False
AMI_IDs = {
           'CentOS':'ami-7c807d14', 
           'Old_CentOS':'ami-aecd60c7', 
           'SLES-SP2':'ami-e8084981',
           'SLES-SP3':'ami-c08fcba8'
           }
AMI_NAME = 'CentOS'
AMI_ID = AMI_IDs[AMI_NAME]
INSTANCE_NAME = 'NGAS_{0}'.format(BRANCH)
INSTANCE_TYPE = 't1.micro'
INSTANCES_FILE = os.path.expanduser('~/.aws/aws_instances')
ELASTIC_IP = 'False'
USERS = ['ngas']
APP_PYTHON_VERSION = '2.7'
APP_PYTHON_URL = 'https://www.python.org/ftp/python/2.7.9/Python-2.7.9.tgz'
APP_DIR = 'ngas_rt' #NGAS runtime directory
INIT_SRC_T = '{0}/src/ngamsStartup/ngamsServer.init.sh' # Template for init source file.
INIT_TRG = '/etc/init.d/ngamsServer'
APP_CONF = 'ngamsServer.conf'
GITUSER = 'icrargit'
GITREPO = 'gitsrv.icrar.org:ngas'

SUPPORTED_OS = [
                'Amazon Linux',
                'Amazon',
                'CentOS', 
                'Ubuntu', 
                'Debian', 
                'Suse',
                'SUSE',
                'SLES-SP2',
                'SLES-SP3',
                'Darwin',
                ]

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
   'postfix',
   'openssl-devel.x86_64',
   'wget.x86_64',
   'postgresql-devel.x86_64',
   'patch',
]

APT_PACKAGES = [
        'libtool',
        'autoconf',
        'zlib1g-dbg',
        'libzlcore-dev',
        'libdb-dev',
        'libgdbm-dev',
        'libreadline-dev',
        'sqlite3',
        'libsqlite3-dev',
        'postgresql-client',
        'patch',
        'python-dev',
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
                 'libdb-4_5',
                 'libdb-4_5-devel',
                 'gcc',
                 'postgresql-devel',
                 'patch'
                 ]

BREW_PACKAGES = [
                 'berkeley-db',
                 'libtool',
                 'automake',
                 'autoconf'
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
    env.keepalive = 15
    env.connection_attempts = 5
    if not env.has_key('GITUSER') or not env.GITUSER:
        env.GITUSER = GITUSER
    if not env.has_key('GITREPO') or not env.GITREPO:
        env.GITREPO = GITREPO
    if not env.has_key('postfix') or not env.postfix:
        env.postfix = POSTFIX
    if not env.has_key('user') or not env.user:
        env.user = USERNAME
    if not env.has_key('APP_USERS') or not env.APP_USERS:
        env.APP_USERS = USERS
    if type(env.APP_USERS) == type(''): # if its just a string
        print "USERS preset to {0}".format(env.APP_USERS)
        env.APP_USERS = [env.APP_USERS] # change the type
    if not env.has_key('HOME') or env.HOME[0] == '~' or not env.HOME:
        env.HOME = run("echo ~{0}".format(env.APP_USERS[0]))
    if not env.has_key('src_dir') or not env.src_dir:
        env.src_dir = thisDir + '/../'
    if not env.has_key('hosts') or env.hosts:
        env.hosts = [env.host_string]
    if not env.has_key('HOME') or env.HOME[0] == '~' or not env.HOME:
        env.HOME = run("echo ~{0}".format(USERS[0]))
    if not env.has_key('PREFIX') or env.PREFIX[0] == '~' or not env.PREFIX:
        env.PREFIX = env.HOME
    if not env.has_key('APP_CONF') or not env.APP_CONF:
        env.APP_CONF = APP_CONF
    if not env.has_key('APP_DIR_ABS') or env.APP_DIR_ABS[0] == '~' \
    or not env.APP_DIR_ABS:
        env.APP_DIR_ABS = '{0}/{1}'.format(env.PREFIX, APP_DIR)
        env.APP_DIR = APP_DIR
    else:
        env.APP_DIR = env.APP_DIR_ABS.split('/')[-1]
    if not env.has_key('INIT_SRC') or not env.INIT_SRC:
        env.INIT_SRC = INIT_SRC_T.format(env.APP_DIR_ABS)
    if not env.has_key('AWS_PROFILE') or not env.AWS_PROFILE:
        env.AWS_PROFILE = AWS_PROFILE
    if not env.has_key('INIT_TRG') or not env.INIT_TRG:
        env.INIT_TRG = INIT_TRG
    if not env.has_key('force') or not env.force:
        env.force = 0
    if not env.has_key('standalone') or not env.standalone:
        env.standalone = 0
    if not env.has_key('AMI_NAME') or not env.AMI_NAME:
        env.AMI_NAME = 'CentOS'
    if env.AMI_NAME == 'SLES':
        env.user = 'root'
    get_linux_flavor()
    puts("""Environment:
            USER:              {0};
            Key file:          {1};
            hosts:             {2};
            host_string:       {3};
            postfix:           {4};
            HOME:              {8};
            APP_DIR_ABS:      {5};
            APP_DIR:          {6};
            USERS:        {7};
            PREFIX:            {9};
            SRC_DIR:           {10};
            """.\
            format(env.user, env.key_filename, env.hosts,
                   env.host_string, env.postfix, env.APP_DIR_ABS,
                   env.APP_DIR, env.APP_USERS, env.HOME, env.PREFIX, 
                   env.src_dir))


@task
def whatsmyip():
    """
    Returns the external IP address of the host running fab.
    
    NOTE: This is only used for EC2 setups, thus it is assumed
    that the host is on-line.
    """
    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))
    whatismyip = 'http://bot.whatismyipaddress.com/'
    myip = urllib.urlopen(whatismyip).readlines()[0]

    return myip

@task
def check_ssh():
    """
    Check availability of SSH on HOST
    """
    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))

    if not env.has_key('key_filename') or not env.key_filename:
        env.key_filename = AWS_KEY
    else:
        puts(red("SSH key_filename: {0}".format(env.key_filename)))        
    if not env.has_key('user') or not env.user:
        env.user = USERNAME
    else:
        puts(red("SSH user name: {0}".format(env.user)))                

    ssh_available = False
    ntries = 10
    tries = 0
    t_sleep = 30
    while tries < ntries and not ssh_available:
        try:
            run("echo 'Is SSH working?'", combine_stderr=True)
            ssh_available = True
            puts(green("SSH is working!"))
        except NetworkError:
            puts(red("SSH is NOT working after {0} seconds!".format(str(tries*t_sleep))))
            tries += 1
            time.sleep(t_sleep)


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
    conn = boto.ec2.connect_to_region(AWS_REGION, profile_name=env.AWS_PROFILE)

    if use_elastic_ip:
        # Disassociate the public IP
        for public_ip in public_ips:
            if not conn.disassociate_address(public_ip=public_ip):
                abort('Could not disassociate the IP {0}'.format(public_ip))

    reservations = conn.run_instances(AMI_IDs[env.AMI_NAME], instance_type=INSTANCE_TYPE, \
                                    key_name=KEY_NAME, security_groups=SECURITY_GROUPS,\
                                    min_count=number_instances, max_count=number_instances)
    instances = reservations.instances
    # Sleep so Amazon recognizes the new instance
    for i in range(4):
        fastprint('.')
        time.sleep(5)

    # Are we running yet?
    iid = []
    for i in range(number_instances):
        iid.append(instances[i].id)

    stat = conn.get_all_instance_status(iid)
    running = [x.state_name=='running' for x in stat]
    puts('\nWaiting for instances to be fully available:\n')
    while sum(running) != number_instances:
        fastprint('.')
        time.sleep(5)
        stat = conn.get_all_instance_status(iid)
        running = [x.state_name=='running' for x in stat]
    puts('.') #enforce the line-end

    # Local user and host
    userAThost = os.environ['USER'] + '@' + whatsmyip()

    # Tag the instance
    for i in range(number_instances):
        conn.create_tags([instances[i].id], {'Name': names[i], 
                                             'Created By':userAThost,
                                             })

    # Associate the IP if needed
    if use_elastic_ip:
        for i in range(number_instances):
            puts('Current DNS name is {0}. About to associate the Elastic IP'.format(instances[i].dns_name))
            if not conn.associate_address(instance_id=instances[i].id, public_ip=public_ips[i]):
                abort('Could not associate the IP {0} to the instance {1}'.format(public_ips[i], instances[i].id))

    # Load the new instance data as the dns_name may have changed
    host_names = []
    for i in range(number_instances):
        instances[i].update(True)
        puts('Current DNS name is {0} after associating the Elastic IP'.format(instances[i].dns_name))
        puts('Instance ID is {0}'.format(instances[i].id))
        print blue('In order to terminate this instance you can call:')
        print blue('fab -f machine-setup/deploy.py terminate:instance_id={0}'.format(instances[i].id))
        host_names.append(str(instances[i].dns_name))

    # The instance is started, but not useable (yet)
    puts('Started the instance(s) now waiting for the SSH daemon to start.')
    env.host_string = host_names[0]
    check_ssh()
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
    res = run('if [ -e {0} ]; then echo 1; else echo 0; fi'.format(path))
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
    ppath = env.APP_DIR_ABS+'/../python'
    ppath = check_command('{0}/bin/python{1}'.format(ppath, APP_PYTHON_VERSION))
    if ppath:
        env.PYTHON = ppath
        return ppath
    # Try python2.7 first
    ppath = check_command('python{0}'.format(APP_PYTHON_VERSION))
    if ppath:
        env.PYTHON = ppath
        return ppath

    # don't check for any other python, since we need to run
    # all the stuff with a version number.
#    elif check_command('python'):
#        res = run('python -V')
#        if res.find(APP_PYTHON_VERSION) >= 0:
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


def install_brew(package):
    """
    Install a package using homebrew (Mac OSX)
    """
    with settings(warn_only=True):
        run('export HOMEBREW_NO_EMOJI=1; brew install {0} | grep -v "\%"'.format(package))


@task    
def install_homebrew():
    """
    Task to install homebrew on Mac OSX.
    
    NOTE: This should not be done if macports is installed already.
    """
    lf = get_linux_flavor()
    if lf != 'Darwin':
        puts(red("Potentially this is not a Mac OSX installation: {0}".format(lf)))
        raise(ValueError)
    if check_command('port'):
        puts(red('MacPorts is installed and it is not recommended to mix it with homebrew!!'))
        puts(red('Bailing out!'))
        raise(ValueError)
        return
    if not check_command('brew'):
        run('ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"')
    else:
        puts(red('Homebrew is installed already! New installation not required.'))
    

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

def check_brew_cellar():
    """
    Find the brewing cellar (Mac OSX)
    """
    with hide('output'):
        cellar = run('brew config | grep HOMEBREW_CELLAR')
    return cellar.split(':')[1].strip()



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

def virtualenv(command, **kwargs):
    """
    Just a helper function to execute commands in the virtualenv
    """
    env.activate = 'source {0}/bin/activate'.format(env.APP_DIR_ABS)
    with cd(env.APP_DIR_ABS):
        run(env.activate + '&&' + command, **kwargs)

def git_pull():
    """
    Updates the repository.
    TODO: This does not work outside iVEC. The current implementation
    is thus using a tar-file, copied over from the calling machine.
    """
    with cd(env.APP_DIR_ABS):
        sudo('git pull', user=env.user)

def git_clone():
    """
    Clones the APP repository.
    """
    copy_public_keys()
    with cd(env.APP_DIR_ABS):
        run('git clone {0}@{1} -b {2}'.format(env.GITUSER, env.GITREPO, BRANCH))


@task
def git_clone_tar(unpack=True):
    """
    Clones the repository into /tmp and packs it into a tar file

    TODO: This does not work outside iVEC. The current implementation
    is thus using a tar-file, copied over from the calling machine.
    """
    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))
    set_env()
    egg_excl = ' '
    if not env.src_dir:
        local('cd /tmp && git clone {0}@{1} -b {2} {2}'.format(env.GITUSER, env.GITREPO, BRANCH))
        local('cd /tmp && mv {0} {1}'.format(BRANCH, env.APP_DIR))
        tar_dir = '/tmp/{0}'.format(env.APP_DIR)
        sdir = '/tmp'
    else:
        tar_dir = '/tmp/'
        sdir = tar_dir
        local('cd {0} && ln -s {1} {2}'.format(tar_dir, env.src_dir, env.APP_DIR))
        tar_dir = tar_dir+'/'+env.APP_DIR+'/.'
    if not env.standalone:
        egg_excl = ' --exclude eggs.tar.gz '

    # create the tar
    local('cd {0} && tar -cjf {1}.tar.bz2 --exclude BIG_FILES \
            --exclude .git --exclude .s* --exclude .e* {2} {1}/.'.format(sdir, env.APP_DIR, egg_excl))
    tarfile = '{0}.tar.bz2'.format(env.APP_DIR)

    # transfer the tar file if not local
    if not env.host_string in ['localhost','127.0.0.1',whatsmyip()]:
        put('{0}/{1}'.format(sdir,tarfile), '/tmp/{0}'.format(tarfile, env.APP_DIR_ABS))
        local('rm -rf /tmp/{0}'.format(env.APP_DIR))  # cleanup local git clone dir

    if unpack:
        # unpack the tar file remotely
        with cd(env.APP_DIR_ABS+'/..'):
            run('tar -xjf /tmp/{0}'.format(tarfile))


@task
def ngas_minimal_tar(transfer=True):
    """
    This function packs the minimal required parts of the NGAS source tree
    into a tar file and copies it to the remote site.
    """
    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))
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
    excludes = ['.git', '.s*', 
                ]
    exclude = ' --exclude ' + ' --exclude '.join(excludes)
    local('cd {0}/.. && tar -czf /tmp/ngas_src.tar.gz {1} ngas'.format(env.src_dir, exclude))
    if transfer:
        put('/tmp/ngas_src.tar.gz','/tmp/ngas.tar.gz')
        run('cd {0} && tar --strip-components 1 -xzf /tmp/ngas.tar.gz'.format(env.APP_DIR_ABS))

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
    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))
    if not env.has_key('linux_flavor'):
        if (check_path('/etc/issue') == '1'):
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
    else:
        linux_flavor = env.linux_flavor
    
    if type(linux_flavor) == type([]):
        linux_flavor = linux_flavor[0]
    if linux_flavor not in SUPPORTED_OS:
        puts('>>>>>>>>>>')
        puts('Target machine is running an unsupported or unkown Linux flavor:{0}.'\
             .format(linux_flavor))
        puts('If you know better, please enter it below.')
        puts('Must be one of:')
        puts(' '.join(SUPPORTED_OS))
        linux_flavor = prompt('LINUX flavor: ')

    print "Remote machine running %s" % linux_flavor
    env.linux_flavor = linux_flavor
    return linux_flavor

@task
def system_install():
    """
    Perform the system installation part.

    NOTE: Most of this requires sudo access on the machine(s)
    """
    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))
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
    elif linux_flavor in ['SUSE','SLES-SP2', 'SLES-SP3', 'SLES']:
        errmsg = sudo('zypper -n -q patch', combine_stderr=True, warn_only=True)
        for package in SLES_PACKAGES:
            install_zypper(package)
    elif linux_flavor == 'Darwin':
        install_homebrew()
        for package in BREW_PACKAGES:
            install_brew(package)        
    else:
        abort("Unsupported linux flavor detected: {0}".format(linux_flavor))
    puts(green("\n\n******** System packages installation COMPLETED!********\n\n"))


@task
def system_check():
    """
    Check for existence of system level packages

    NOTE: This requires sudo access on the machine(s)
    """
    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))
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
    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))

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
    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))

    set_env()
    if not env.user:
        env.user = USERNAME # defaults to ec2-user
    group = 'ngas'
    sudo('groupadd ngas', warn_only=True)
    for user in env.APP_USERS:
        sudo('useradd -g {0} -m -s /bin/bash {1}'.format(group, user), warn_only=True)
        sudo('mkdir /home/{0}/.ssh'.format(user), warn_only=True)
        sudo('chmod 700 /home/{0}/.ssh'.format(user))
        sudo('chown -R {0}:{1} /home/{0}/.ssh'.format(user,group))
        home = run('echo $HOME')
        put('{0}machine-setup/authorized_keys'.format(env.src_dir),
                '/tmp/authorized_keys')
        sudo('mv /tmp/authorized_keys /home/{0}/.ssh/authorized_keys'.format(user))
        sudo('chmod 600 /home/{0}/.ssh/authorized_keys'.format(user))
        sudo('chown {0}:{1} /home/{0}/.ssh/authorized_keys'.format(user, group))
        
    # create NGAS directories and chown to correct user and group
    sudo('mkdir -p {0}'.format(env.APP_DIR_ABS))
    sudo('chown {0}:{1} {2}'.format(env.APP_USERS[0], group, env.APP_DIR_ABS))
    sudo('mkdir -p {0}/../NGAS'.format(env.APP_DIR_ABS))
    sudo('chown {0}:{1} {2}/../NGAS'.format(env.APP_USERS[0], group, env.APP_DIR_ABS))
    puts(green("\n\n******** USER SETUP COMPLETED!********\n\n"))


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
    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))
    set_env()

    with cd('/tmp'):
        if not env.standalone:
            run('wget --no-check-certificate -q {0}'.format(APP_PYTHON_URL))
        else:
            put('{0}/additional_tars/Python-2.7.8.tgz'.format(env.src_dir), 'Python-2.7.8.tgz')
        base = os.path.basename(APP_PYTHON_URL)
        pdir = os.path.splitext(base)[0]
        run('tar -xzf {0}'.format(base))
    ppath = env.APP_DIR_ABS + '/../python'
    with cd('/tmp/{0}'.format(pdir)):
        run('./configure --prefix {0};make;make install'.format(ppath))
        ppath = '{0}/bin/python{1}'.format(ppath,APP_PYTHON_VERSION)
    env.PYTHON = ppath
    puts(green("\n\n******** PYTHON INSTALLATION COMPLETED!********\n\n"))


@task
def virtualenv_setup():
    """
    setup virtualenv with the detected or newly installed python
    """
    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))
    set_env()
    check_python()
    print "CHECK_DIR: {0}".format(env.APP_DIR_ABS+'/src')
    if check_dir(env.APP_DIR_ABS+'/src') and not env.force:
        abort('ngas_rt directory exists already')

    with cd('/tmp'):
        put('{0}/clib_tars/virtualenv-12.0.7.tar.gz'.format(env.src_dir), 'virtualenv-12.0.7.tar.gz')
        run('tar -xzf virtualenv-12.0.7.tar.gz')
        with settings(user=env.APP_USERS[0]):
            run('cd virtualenv-12.0.7; {0} virtualenv.py {1}'.format(env.PYTHON, env.APP_DIR_ABS))
            run('mkdir ~/.pip; cd ~/.pip; wget http://curl.haxx.se/ca/cacert.pem')
            run('echo "[global]" > ~/.pip/pip.conf; echo "cert = {0}/.pip/cacert.pem" >> ~/.pip/pip.conf;'.format(env.HOME))

    puts(green("\n\n******** VIRTUALENV SETUP COMPLETED!********\n\n"))



@task
def ngas_buildout(typ='archive'):
    """
    Perform just the buildout and virtualenv config

    if env.standalone is not 0 then the eggs from the additional_tars
    will be installed to avoid accessing the internet.
    """
    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))
    set_env()
    run('if [ -a bin/python ] ; then rm bin/python ; fi') # avoid the 'busy' error message

    with cd(env.APP_DIR_ABS):
        if (env.standalone):
            put('{0}/additional_tars/eggs.tar.gz'.format(env.src_dir), '{0}/eggs.tar.gz'.format(env.APP_DIR_ABS))
            run('tar -xzf eggs.tar.gz')
            if env.linux_flavor == 'Darwin':
                put('{0}/data/common.py.patch'.format(env.src_dir), '.')
                run('patch eggs/minitage.recipe.common-1.90-py2.7.egg/minitage/recipe/common/common.py common.py.patch')
            run('find . -name "._*" -exec rm -rf {} \;') # get rid of stupid stuff left over from MacOSX
            virtualenv('buildout -Nvo')
        else:
            run('find . -name "._*" -exec rm -rf {} \;')
            virtualenv('buildout')
        with settings(warn_only=True):
                run('mkdir -p {0}/../NGAS'.format(env.APP_DIR_ABS))
        run('cp -R {0}/NGAS/* {0}/../NGAS/.'.format(env.APP_DIR_ABS))
        with settings(warn_only=True):
            run('cp {0}/cfg/{1} {0}/../NGAS/cfg/{2}'.format(\
              env.APP_DIR_ABS, initName(typ=typ)[2], initName(typ=typ)[3]))
        nda = '\/'+'\/'.join(env.APP_DIR_ABS.split('/')[1:-1])+'\/NGAS'
        if env.linux_flavor == 'Darwin': # capture stupid difference in sed on Mac OSX
            run("""sed -i '' 's/\*replaceRoot\*/{0}/g' {0}/cfg/{1}""".
                format(nda, initName(typ=typ)[3]))
        else:
            run("""sed -i 's/\*replaceRoot\*/{0}/g' {0}/cfg/{1}""".
                format(nda, initName(typ=typ)[3]))

        with cd('../NGAS'):
            with settings(warn_only=True):
                run('sqlite3 -init {0}/src/ngamsSql/ngamsCreateTables-SQLite.sql ngas.sqlite <<< $(echo ".quit")'\
                    .format(env.APP_DIR_ABS))
                run('cp ngas.sqlite {0}/src/ngamsTest/src/ngas_Sqlite_db_template'.format(env.APP_DIR_ABS))


    puts(green("\n\n******** NGAS_BUILDOUT COMPLETED!********\n\n"))

@task
def install_user_profile():
    """
    Put the activation of the virtualenv into the login profile of the user
    
    NOTE: This will be executed for the user running NGAS.
    """
    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))
    set_env()
    nuser = env.APP_USERS[0]
    if env.user != nuser:
        with cd(env.HOME):
            res = sudo('if [ -e {0}/.bash_profile_orig ]; then echo 1; else echo ; fi'.format(env.HOME))
            if not res:
                sudo('sudo -u {0} cp .bash_profile .bash_profile_orig'.format(nuser),
                     warn_only=True)
            else:
                sudo('sudo -u {0} cp .bash_profile_orig .bash_profile'.format(nuser))
            sudo('sudo -u {0} echo "\nexport NGAS_PREFIX={1}\n" >> .bash_profile'.\
                format(nuser, env.APP_DIR_ABS))
            sudo('sudo -u {0} echo "source {1}/bin/activate\n" >> .bash_profile'.\
                 format(nuser, env.APP_DIR_ABS))
    else:
        with cd(env.HOME):
            res = run('if [ -e {0}/.bash_profile_orig ]; then echo 1; else echo ; fi'.format(env.HOME))
            if not res:
                run('cp .bash_profile .bash_profile_orig'.format(nuser), warn_only=True)
            else:
                run('cp .bash_profile_orig .bash_profile'.format(nuser))
            run('echo "export NGAS_PREFIX={1}\n" >> .bash_profile'.\
                format(nuser, env.APP_DIR_ABS))
            run('echo "source {1}/bin/activate\n" >> .bash_profile'.\
                 format(nuser, env.APP_DIR_ABS))

    puts(green("\n\n******** .bash_profile updated!********\n\n"))



@task
def ngas_full_buildout(typ='archive'):
    """
    Perform the full install and buildout
    """
    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))
    set_env()

    # First get the sources
    #
    if (env.standalone):
        ngas_minimal_tar()
    elif check_path('{0}/bootstrap.py'.format(env.APP_DIR_ABS)) == '0':
        git_clone_tar()

    with cd(env.APP_DIR_ABS):
        virtualenv('pip install clib_tars/zc.buildout-2.3.1.tar.gz')
        virtualenv('pip install clib_tars/pycrypto-2.6.tar.gz')
        virtualenv('pip install clib_tars/paramiko-1.11.0.tar.gz')
        # make this installation self consistent
        virtualenv('pip install clib_tars/Fabric-1.10.1.tar.gz')
        virtualenv('pip install clib_tars/boto-2.36.0.tar.gz')
        virtualenv('pip install clib_tars/markup-1.9.tar.gz')
        virtualenv('pip install additional_tars/egenix-mx-base-3.2.6.tar.gz')
        #The following will only work if the Berkeley DB had been installed already
        if env.linux_flavor == 'Darwin':
            puts('>>>> Installing Berkeley DB')
            system_install()
            cellar_dir = check_brew_cellar()
            db_version = run('ls -tr1 {0}/berkeley-db'.format(cellar_dir)).split()[-1]
            virtualenv('cd /tmp; tar -xzf {0}/additional_tars/bsddb3-6.1.0.tar.gz'.format(env.APP_DIR_ABS))
            virtualenv('cd /tmp/bsddb3-6.1.0; ' + \
                       'export YES_I_HAVE_THE_RIGHT_TO_USE_THIS_BERKELEY_DB_VERSION=1; ' +\
                       'python setup.py --berkeley-db=/usr/local/Cellar/berkeley-db/{0} build'.format(db_version))
            virtualenv('cd /tmp/bsddb3-6.1.0; ' + \
                       'export YES_I_HAVE_THE_RIGHT_TO_USE_THIS_BERKELEY_DB_VERSION=1; ' +\
                       'python setup.py --berkeley-db=/usr/local/Cellar/berkeley-db/{0} install'.format(db_version))
        else:
            virtualenv('pip install additional_tars/bsddb3-6.1.0.tar.gz')
        virtualenv('pip install additional_tars/bottle-0.11.6.tar.gz')

        # run bootstrap with correct python version (explicit)
        run('if [ -a bin/python ] ; then rm bin/python ; fi') # avoid the 'busy' error message
        virtualenv('python{0} bootstrap.py -v 2.3.1'.format(APP_PYTHON_VERSION))

    ngas_buildout(typ=typ)
    install_user_profile()

    puts(green("\n\n******** NGAS_FULL_BUILDOUT COMPLETED!********\n\n"))




@task
@serial
def test_env():
    """Configure the test environment on EC2

    Ask a series of questions before deploying to the cloud.

    Allow the user to select if a Elastic IP address is to be used
    """
    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))
    if not env.has_key('AWS_PROFILE') or not env.AWS_PROFILE:
        env.AWS_PROFILE = AWS_PROFILE
    if not env.has_key('instance_name') or not env.instance_name:
        env.instance_name = INSTANCE_NAME
    if not env.has_key('use_elastic_ip') or not env.use_elastic_ip:
        env.use_elastic_ip = ELASTIC_IP
    if not env.has_key('key_filename') or not env.key_filename:
        env.key_filename = AWS_KEY
    if not env.has_key('AMI_NAME') or not env.AMI_NAME:
        env.AMI_NAME = 'CentOS'
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

    env.user = USERNAME
    if env.AMI_NAME == 'SLES':
        env.user = 'root'
    # Create the instance in AWS
    host_names = create_instance([env.instance_name], use_elastic_ip, [public_ip])
    env.hosts = host_names
    if not env.host_string:
        env.host_string = env.hosts[0]

    env.key_filename = AWS_KEY
    env.roledefs = {
        'ngasmgr' : host_names,
        'ngas' : host_names,
    }
    puts(green("\n\n******** EC2 INSTANCE SETUP COMPLETE!********\n\n"))



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
def user_deploy(typ='archive'):
    """
    Deploy the system as a normal user without sudo access
    NOTE: The parameter can be passed from the command line by using

    fab -f deploy.py user_deploy:typ='cache'
    """
    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))
    if not env.has_key('APP_USERS') or not env.APP_USERS:
        # if not defined on the command line use the current user
        env.APP_USERS = os.environ['HOME'].split('/')[-1]

    install(sys_install=False, user_install=False, 
            init_install=False, typ=typ)
    puts(green("\n\n******** USER INSTALLATION COMPLETED!********\n\n"))


@task
def init_deploy(typ='archive'):
    """
    Install the NGAS init script for an operational deployment

    Typical usage:
    
    fab -f machine-setup/deploy.py init_deploy -H <host> -i <ssh-key-file> -u <sudo_user>
    """
    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))
    (initFile, initLink, cfg, lcfg) = initName(typ=typ)

    set_env()

    sudo('cp {0}/src/ngamsStartup/{1} /etc/init.d/{2}'.\
         format(env.APP_DIR_ABS, initFile, initLink))
    sudo("sed -i 's/NGAS_USER=\"ngas\"/NGAS_USER=\"{0}\"/g' /etc/init.d/{1}".\
         format(env.APP_USERS[0], initLink))
    sudo("sed -i 's/NGAS_ROOT=\"\/home\/$NGAS_USER\/ngas_rt\"/NGAS_ROOT=\"{0}\"/g' /etc/init.d/{1}".\
         format(env.APP_DIR_ABS.replace('/','\/'), initLink))
    sudo('chmod a+x /etc/init.d/{0}'.format(initLink))
    if (get_linux_flavor() in ['Ubuntu','SUSE', 'Suse']):
        sudo('chkconfig --add {0}'.format(initLink))
    else:
        sudo('chkconfig --add /etc/init.d/{0}'.format(initLink))
    puts(green("\n\n******** CONFIGURED INIT SCRIPTS!********\n\n"))


@task
@serial
def operations_deploy(sys_install=True, user_install=True, typ='archive'):
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
    
    NOTE: This task is now merely an alias for install.
    """

    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))
    install(sys_install=sys_install, user_install=user_install, 
            init_install=True, typ=typ)
    
    puts(green("\n\n******** OPERATIONS_DEPLOY COMPLETED!********\n\n"))
    puts(green("\n\nThe server could be started now using the sqlite backend."))
    puts(green("In most cases this is not reflecting the operational requirements though."))
    puts(green("Thus some local adjustments of the NGAS configuration is most probably"))
    puts(green("required. This includes the DB backend config as well as the configuration"))
    puts(green("of the data volumes.\n\n"))


@task
@serial
def test_deploy():
    """
    ** MAIN TASK **: Deploy the full NGAS EC2 test environment.

    Typical usage:
    
    fab -f machine-setup/deploy.py test_deploy
    """

    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))
    test_env()
    # set environment to default for EC2, if not specified otherwise.
    set_env()
    install(sys_install=True, user_install=True, init_install=True)
    with settings(user=env.APP_USERS[0]):
        run('ngamsDaemon start')
    puts(green("\n\n******** SERVER STARTED!********\n\n"))
    if test_status():
        puts(green("\n\n>>>>> SERVER STATUS CHECKED <<<<<<<<<<<\n\n"))
    else:
        puts(red("\n\n>>>>>>> SERVER STATUS NOT OK <<<<<<<<<<<<\n\n"))
    
    puts(green("\n\n******** TEST_DEPLOY COMPLETED on AWS host: {0} ********\n\n".format(env.host_string)))

@task
def test_status():
    """
    Execute the STATUS command against a running NGAS server
    """
    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))
    try:
        serv = urllib.urlopen('http://{0}:7777/STATUS'.format(env.host_string))
    except IOError:
        puts(red('Problem connecting to server !!!'))
        return False
        
    response = serv.read()
    serv.close()
    if response.find('Status="SUCCESS"') == -1:
        puts(red('Problem with server response!!!'))
        puts(red(response))
        return False
    else:
        puts(green('STATUS="SUCCESS"'))
        return True
    
    


@task
def archiveSource():
    """
    Archive the NGAS source package on a NGAS server
    
    Typical usage:
    
    fab -f machine-setup/deploy.py archiveSource -H ngas.ddns.net --set src_dir=.
    
    NOTE: The ngamsPClient module must be on the python path for fab.
    """
    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))
    import ngamsPClient
    if not env.has_key('src_dir') or not env.src_dir:
        print 'Please specify the local source directory of the NGAS software'
        print 'on the command line using --set src_dir=your/local/directory'
        abort(red('\n\n******** ARCHIVE ABORTED!********\n\n'))
    else: # check whether the source directory setting is likely to be correct
        res = local('grep "The Next Generation Archive System" {0}/README'.format(env.src_dir), \
                    capture=True)
        if not res:
            abort('src_dir does not point to a valid NGAS source directory!!')
    #set_env()
    client=ngamsPClient.ngamsPClient(host=env.host_string, port=7777)
    ngas_minimal_tar(transfer=False)
    stat = client.archive(fileUri='/tmp/ngas_src.tar.gz',mimeType='application/octet-stream')
    if stat.getStatus() != 'SUCCESS':
        puts(">>>> Problem archiving source package!")
    puts(green(stat.getMessage()))



@task
def install(sys_install=True, user_install=True, 
            init_install=True, typ='archive'):
    """
    Install NGAS users and NGAS software on existing machine.
    Note: Requires root permissions!
    """
    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))
    set_env()
    if sys_install and sys_install != 'False': system_install()
    if env.postfix:
        postfix_config()
    if user_install and user_install != 'False': user_setup()

    with settings(user=env.APP_USERS[0]):
        ppath = check_python()
        if not ppath:
            python_setup()
    if env.PREFIX != env.HOME: # generate non-standard ngas_rt directory
        sudo('mkdir -p {0}'.format(env.PREFIX))
        sudo('chown -R {0}:ngas {1}'.format(env.APP_USERS[0], env.PREFIX))
    with settings(user=env.APP_USERS[0]):
        virtualenv_setup()
        ngas_full_buildout(typ=typ)
        cleanup_tmp()
    if init_install and init_install != 'False': init_deploy()
    puts(green("\n\n******** INSTALLATION COMPLETED!********\n\n"))


@task
def uninstall(clean_system=False):
    """
    Uninstall the NGAS software 
    NGAS users and init script will only be removed if clean_system is True
    
    NOTE: This can only be used with a sudo user. Does not uninstall
          system packages.
    """
    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))
    set_env()
    with settings(user = env.APP_USERS[0]):
        if env.PREFIX != env.HOME: # avoid removing the home directory
            run('rm -rf {0}'.format(env.PREFIX), warn_only=True)
        run('rm -rf {0}'.format(env.APP_DIR_ABS), warn_only=True)
        run('mv .bash_profile_orig .bash_profile', warn_only=True)
    
    if clean_system and clean_system != 'False': # don't delete the users and system settings by default.
        for u in env.APP_USERS:
            sudo('userdel -r {0}'.format(u), warn_only=True)
        sudo('groupdel ngas', warn_only=True)
        sudo('rm /etc/ngamsServer.conf', warn_only=True)
        sudo('rm /etc/init.d/ngamsServer', warn_only=True)

    puts(green("\n\n******** UNINSTALL COMPLETED!********\n\n"))

@task
def upgrade():
    """
    Upgrade the NGAS software on a target host using rsync.

    NOTE: This does NOT perform a new buildout, i.e. all the binaries and libraries are untouched.
    
    Typical command line:
    fab -H ngas.ddns.net -i ~/.ssh/icrar_ngas.pem -u ngas -f machine-setup/deploy.py upgrade --set src_dir=.
    """
    # use the PREFIX from the command line or try to set it from
    # the remote environment. If both fails bail-out.
    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))
    if not env.has_key('PREFIX') or not env.PREFIX:
        env.PREFIX = run('echo $NGAS_PREFIX/..')
        env.APP_DIR_ABS = run('echo $NGAS_PREFIX')
    if not env.PREFIX:
        print 'Unable to identify location of NGAS installation!'
        print 'Please set the environment variable NGAS_PREFIX in .bash_profile.'
        print 'of the user running NGAS on the remote host.'
        abort(red('\n\n******** UPGRADE ABORTED!********\n\n'))
    if not env.has_key('src_dir') or not env.src_dir:
        print 'Please specify the local source directory of the NGAS software'
        print 'on the command line using --set src_dir=your/local/directory'
        abort(red('\n\n******** UPGRADE ABORTED!********\n\n'))
    else: # check whether the source directory setting is likely to be correct
        res = local('grep "The Next Generation Archive System" {0}/README'.format(env.src_dir), \
                    capture=True)
        if not res:
            abort(red('src_dir does not point to a valid NGAS source directory!!'))
    set_env()
    run('$NGAS_PREFIX/bin/ngamsDaemon stop')
    rsync_project(local_dir=env.src_dir+'/src', remote_dir=env.APP_DIR_ABS, exclude=".git")
    #git_clone_tar()
    run('$NGAS_PREFIX/bin/ngamsDaemon start')
    puts(green("\n\n******** UPGRADE COMPLETED!********\n\n"))

    
@task
def assign_ddns():
    """
    This task installs the noip ddns client to the specified host.
    After the installation the configuration step is executed and that
    requires some manual input. Then the noip2 client is started in background.
    
    NOTE: Obviously this should only be carried out for one NGAS deployment!!
    """
    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))
    sudo('yum-config-manager --enable epel')
    sudo('yum install -y noip')
    sudo('sudo noip2 -C')
    sudo('chkconfig noip on')
    sudo('service noip start')
    puts(green("\n\n***** Dynamic IP address assigned ******\n\n"))

@task
def connect():
    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))
    if not env.has_key('AWS_PROFILE') or not env.AWS_PROFILE:
        env.AWS_PROFILE = AWS_PROFILE
    if not env.has_key('key_filename') or not env.key_filename:
        env.key_filename = AWS_KEY

    conn = boto.ec2.connect_to_region(AWS_REGION, profile_name=env.AWS_PROFILE)
    return conn

@task
def list_instances():
    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))
    conn = connect()
    res = conn.get_all_instances()
    for r in res:
        inst_id = r.instances[0].id
        puts('Instance {0} tags:'.format(inst_id))
        tagdict = r.instances[0].tags
        for k in tagdict:
            print '{0}: {1}'.format(k,tagdict[k]),
        print
        print

@task
def terminate(instance_id):
    """
    Task to terminate the boto instances
    """
    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))
    userAThost = os.environ['USER'] + '@' + whatsmyip()
    if not(instance_id):
        puts('No instance ID specified. Please provide one.')
        return
    conn = connect()
    inst = conn.get_all_instances(instance_ids=[instance_id])
    puts('Instance {0} tags:'.format(instance_id))
    tagdict = inst[0].instances[0].tags
    for k in tagdict:
        print '{0}: {1}'.format(k,tagdict[k]),
    print
    if tagdict['Created By'] != userAThost:
        puts('******************************************************')
        puts('WARNING: This instances has not been created by you!!!')
        puts('******************************************************')
    if confirm("Do you really want to terminate this instance?"):
        puts('Teminating instance {0}'.format(instance_id))
        conn.terminate_instances(instance_ids=[instance_id])
    else:
        puts(red('Instance NOT terminated!'))
    return

@task
def cleanup_tmp():
    """
    Task to cleanup temporary files left-over from an installation.
    This task runs as the login user, not sudo.
    """
    tmp_items = [
                 'virtualenv*',
                 'db-6*',
                 'ngas_rt*',
                 ]
    for item in tmp_items:
        run('rm -rf /tmp/{0}'.format(item))
