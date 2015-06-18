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
    with hide('running'):
        FILTER = frun('echo')  # This should not return anything
    com = list(args)[0]
    com = 'unset PYTHONPATH; {0}'.format(com)
    res = frun(com, **kwargs)
    res = res.replace(FILTER,'')
#     res = res.replace('\n','')
#     res = res.replace('\r','')
    return res

def sudo(*args, **kwargs):
    with hide('running'):
        FILTER = frun('echo')  # This should not return anything
    com = list(args)[0]
    com = 'unset PYTHONPATH; {0}'.format(com)
    res = fsudo(com, **kwargs)
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
           'Amazon':'ami-7c807d14', 
           'CentOS': 'ami-8997afe0',
           'Old_CentOS':'ami-aecd60c7', 
           'SLES-SP2':'ami-e8084981',
           'SLES-SP3':'ami-c08fcba8'
           }
AMI_NAME = 'Amazon'
AMI_ID = AMI_IDs[AMI_NAME]
INSTANCE_NAME = 'NGAS_{0}'
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
MACPORT_DIR = '/opt/local' # The directory under which 'port' installs stuff

# the following can be set on the command line in order to clone from git.
GITUSER = '' 
GITREPO = ''
# GITUSER = 'icrargit'
# GITREPO = 'gitsrv.icrar.org:ngas'

SUPPORTED_OS_LINUX = [
                      'Amazon Linux',
                      'Amazon',
                      'CentOS',
                      'Ubuntu',
                      'Debian',
                      'Suse',
                      'SUSE',
                      'SLES-SP2',
                      'SLES-SP3'
]

SUPPORTED_OS_MAC = [
                    'Darwin',
]

SUPPORTED_OS = []
SUPPORTED_OS.extend(SUPPORTED_OS_LINUX)
SUPPORTED_OS.extend(SUPPORTED_OS_MAC)

YUM_PACKAGES = [
   'python27-devel',
   'python-devel',
   'git',
   'autoconf',
   'libtool',
   'zlib-devel',
   'db4-devel',
   'libdb-devel',
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
        'libssl-dev',
        'sqlite3',
        'libsqlite3-dev',
        'postgresql-client',
        'patch',
        'python-dev',
        'libdb5.3-dev',
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
                 'wget',
                 'berkeley-db',
                 'libtool',
                 'automake',
                 'autoconf'
                 ]

PORT_PACKAGES = [
                 'wget',
                 'db60',
                 'libtool',
                 'automake',
                 'autoconf',
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

    # Avoid multiple calls taking effect, one is enough
    if env.has_key('environment_already_set'): 
        if check_command('wget'): # first time set_env is called wget might not yet be available
            get_instance_id()
        else:
            env.instance_id = 'UNKNOWN'
        return  # already done

    # set environment to default for EC2, if not specified on command line.

    # puts(env)
    env.keepalive = 15
    env.connection_attempts = 5
    if not env.has_key('GITUSER') or not env.GITUSER:
        env.GITUSER = GITUSER
    if not env.has_key('GITREPO') or not env.GITREPO:
        env.GITREPO = GITREPO
    if not env.has_key('BRANCH') or not env.BRANCH:
        env.BRANCH = BRANCH
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
        with settings(user = env.APP_USERS[0]):
            env.HOME = run("echo $HOME") # always set to $HOME of APP_USERS[0]
                    
    if not env.has_key('src_dir') or not env.src_dir:
        env.src_dir = thisDir + '/../'
    if not env.has_key('hosts') or env.hosts:
        env.hosts = [env.host_string]
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
        env.AMI_NAME = 'Amazon'
    if env.AMI_NAME in ['CentOS', 'SLES']:
        env.user = 'root'
    get_linux_flavor()

    env.nprocs = 1
    if env.linux_flavor in SUPPORTED_OS_LINUX:
        env.nprocs = int(run('grep -c processor /proc/cpuinfo'))

    puts("""Environment:
            USER:              {0};
            Key file:          {1};
            hosts:             {2};
            host_string:       {3};
            postfix:           {4};
            HOME:              {8};
            APP_DIR_ABS:       {5};
            APP_DIR:           {6};
            USERS:             {7};
            PREFIX:            {9};
            SRC_DIR:          {10};
            BRANCH:           {11};
            instance_id:      {12};
            """.\
            format(env.user, env.key_filename, env.hosts,
                   env.host_string, env.postfix, env.APP_DIR_ABS,
                   env.APP_DIR, env.APP_USERS, env.HOME, env.PREFIX,
                   env.src_dir, env.BRANCH, env.instance_id))



@task
def whatsmyip():
    """
    Returns the external IP address of the host running fab.
    
    NOTE: This is only used for EC2 setups, thus it is assumed
    that the host is on-line.
    """
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))
    whatismyip = 'http://bot.whatismyipaddress.com/'
    myip = urllib.urlopen(whatismyip).readlines()[0]
    puts(green('IpAddress = "{0}"'.format(myip)))

    return myip

@task
def check_ssh():
    """
    Check availability of SSH on HOST
    """
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))

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

@task
def create_key_pair():
    """
    Create the AWS_KEY if it does not exist already and copies it into ~/.ssh
    """
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))
    if not env.has_key('AWS_PROFILE') or not env.AWS_PROFILE:
        env.AWS_PROFILE = AWS_PROFILE
    conn = boto.ec2.connect_to_region(AWS_REGION, profile_name=env.AWS_PROFILE)
    kp = conn.get_key_pair(KEY_NAME)
    if not kp:
        kp = conn.create_key_pair(KEY_NAME)
        kp.save('~/.ssh/')
        puts(green("\n******** KEY_PAIR created and written!********\n"))
    else:
        puts(green("\n******** KEY_PAIR retrieved********\n"))
        keyFile = os.path.expanduser(AWS_KEY)
        if not os.path.exists(keyFile):
            puts("Key file doesn't exist, creating new key in the server")
            conn.delete_key_pair(KEY_NAME)
            kp = conn.create_key_pair(KEY_NAME)
            kp.save('~/.ssh/')
            puts(green("\n******** KEY_PAIR created and written!********\n"))

    puts(green("\n******** Task {0} finished!********\n".\
        format(inspect.stack()[0][3])))
    conn.close()
    return



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
        print_instance(instances[i])
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


@task
def check_command(command):
    """
    Check existence of command remotely

    INPUT:
    command:  string

    OUTPUT:
    Boolean
    """
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))
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

@task
def check_python():
    """
    Check for the existence of correct version of python

    INPUT:
    None

    OUTPUT:
    path to python binary    string, could be empty string
    """
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))
    # Try whether there is already a local python installation for this user
    set_env()
    ppath = env.APP_DIR_ABS.split(env.APP_DIR)[0] + '/python' # make sure this is an absolute path
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


def install_port(package):
    """
    Install a package using macports (Mac OSX)
    """
    with settings(warn_only=True):
        run('sudo port install {0}'.format(package))


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

def check_brew_port():
    """
    Check for existence of homebrew or macports
    
    RETRUNS: string containing the installed package manager or None
    """
    if check_command('brew'):
        return 'brew'
    elif check_command('port'):
        return 'port'
    else:
        return None


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
        run('git clone {0}@{1} -b {2}'.format(env.GITUSER, env.GITREPO, env.BRANCH))


@task
def git_clone_tar(unpack=True):
    """
    Clones the repository into /tmp and packs it into a tar file

    TODO: This does not work outside iVEC. The current implementation
    is thus using a tar-file, copied over from the calling machine.
    """
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))
    set_env()
    egg_excl = ' '
    if env.GITREPO and env.GITUSER:
        local('cd /tmp && git clone {0}@{1} -b {2} {2}'.format(env.GITUSER, env.GITREPO, env.BRANCH))
        local('cd /tmp && mv {0} {1}'.format(env.BRANCH, env.APP_DIR))
        tar_dir = '/tmp/{0}'.format(env.APP_DIR)
        sdir = '/tmp'
    else:
        env.BRANCH = local('git rev-parse --abbrev-ref HEAD') # TODO: Fix the instance name!!
        tar_dir = '/tmp/'
        sdir = tar_dir
        local('cd {0} && ln -s {1} {2}'.format(tar_dir, env.src_dir, env.APP_DIR))
        tar_dir = tar_dir+'/'+env.APP_DIR+'/.'
    if not env.standalone:
        egg_excl = ' --exclude eggs.tar.gz '

    # create the tar
    local('cd {0} && tar -cjf ngas_tmp.tar.bz2 --exclude BIG_FILES \
            --exclude .git --exclude .s* --exclude .e* {2} {1}/.'.format(sdir, env.APP_DIR, egg_excl))
    tarfile = '{0}.tar.bz2'.format(env.APP_DIR)

    # transfer the tar file if not local
    if env.standalone != 0:
        testlist = ['localhost','127.0.0.1']
    else:
        testlist = ['localhost','127.0.0.1',whatsmyip()]
    if not env.host_string in testlist:
        put('{0}/ngas_tmp.tar.bz2'.format(sdir), '/tmp/{0}'.format(tarfile, env.APP_DIR_ABS))
        local('rm -rf /tmp/{0}'.format(env.APP_DIR))  # cleanup local git clone dir
    else: # if this is all local
        tarfile = 'ngas_tmp.tar.bz2'

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
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))
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
    src_dir_rel = os.path.split(env.src_dir)[-1]
    local('cd {0}/.. && tar -czf /tmp/ngas_src.tar.gz {1} {2}'.format(env.src_dir, exclude, src_dir_rel))
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
def get_info():
    """
    Show login and termination info
    """
    set_env()
    puts(green('To login run:'))
    puts(red('$ ssh -i {0} {1}@{2}'.
        format(AWS_KEY, env.APP_USERS[0], env.host_string)))
    puts(green('To terminate this instance run: '))
    puts(red('$ fab terminate:{0}'.format(env.instance_id)))


@task
def get_linux_flavor():
    """
    Obtain and set the env variable linux_flavor
    """

    # Already ran through this method
    if env.has_key('linux_flavor'):
        return env.linux_flavor

    linux_flavor = None
    # Try lsb_release
    if check_command('lsb_release'):
        distributionId = run('lsb_release -i')
        if distributionId and distributionId.find(':') != -1:
            linux_flavor = distributionId.split(':')[1].strip()

    # Try python
    if not linux_flavor and check_command('python'):
        lf = run("python -c 'import platform; print platform.linux_distribution()[0]'")
        if lf:
            linux_flavor = lf.split()[0]

    # Try /etc/issue
    if not linux_flavor and check_path('/etc/issue') == '1':
        re = run('cat /etc/issue')
        issue = re.split()
        if issue:
            if issue[0] == 'CentOS' or issue[0] == 'Ubuntu' \
               or issue[0] == 'Debian':
                linux_flavor = issue[0]
            elif issue[0] == 'Amazon':
                linux_flavor = ' '.join(issue[:2])
            elif issue[2] == 'SUSE':
                linux_flavor = issue[2]

    # Try uname -s
    if not linux_flavor:
        linux_flavor = run('uname -s')

    # Sanitize
    if linux_flavor and type(linux_flavor) == type([]):
        linux_flavor = linux_flavor[0]

    # Final check
    if not linux_flavor or linux_flavor not in SUPPORTED_OS:
        puts('>>>>>>>>>>')
        puts('Target machine is running an unsupported or unkown Linux flavor: {0}.'.format(linux_flavor))
        puts('If you know better, please enter it below.')
        puts('Must be one of:')
        puts(' '.join(SUPPORTED_OS))
        linux_flavor = prompt('LINUX flavor: ')

    print "Remote machine running %s" % linux_flavor
    env.linux_flavor = linux_flavor
    return linux_flavor

@task
def get_instance_id():
    """
    Tasks retrieves the instance_id of the current instance internally.
    It alos sets the variable env.instance_id
    """
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))
    env.instance_id = run("wget -qO- http://169.254.169.254/latest/meta-data/instance-id")
    puts(green("\n******** Task {0} finished!********\n".\
        format(inspect.stack()[0][3])))
    return env.instance_id


@task
def system_install():
    """
    Perform the system installation part.

    NOTE: Most of this requires sudo access on the machine(s)
    """
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))
    set_env()

    # Install required packages
    linux_flavor = get_linux_flavor()
    if (linux_flavor in ['CentOS','Amazon Linux']):
         # Update the machine completely
        errmsg = sudo('yum --assumeyes --quiet update', combine_stderr=True, warn_only=True)
        processCentOSErrMsg(errmsg)
        for package in YUM_PACKAGES:
            install_yum(package)
        if linux_flavor == 'CentOS':
            sudo('/etc/init.d/iptables stop') # CentOS firewall blocks NGAS port!
    elif (linux_flavor in ['Ubuntu', 'Debian']):
        errmsg = sudo('apt-get -qq -y update', combine_stderr=True, warn_only=True)
        for package in APT_PACKAGES:
            install_apt(package)
    elif linux_flavor in ['SUSE','SLES-SP2', 'SLES-SP3', 'SLES']:
        errmsg = sudo('zypper -n -q patch', combine_stderr=True, warn_only=True)
        for package in SLES_PACKAGES:
            install_zypper(package)
    elif linux_flavor == 'Darwin':
        pkg_mgr = pkg_mgr_ensure()
        if pkg_mgr == 'brew':
            for package in BREW_PACKAGES:
                install_brew(package)
        elif pkg_mgr == 'port':
            for package in PORT_PACKAGES:
                install_port(package)
    else:
        abort("Unsupported linux flavor detected: {0}".format(linux_flavor))
    puts(green("\n******** System packages installation COMPLETED!********\n"))

@task
def pkg_mgr_ensure():
    """
    Checks if either brew or port is installed. If none is it installs brew
    It then returns the package manager currently installed on the system
    """
    pkg_mgr = check_brew_port()
    if pkg_mgr == None:
        install_homebrew()
        pkg_mgr = 'brew'
    return pkg_mgr

@task
def system_check():
    """
    Check for existence of system level packages

    NOTE: This requires sudo access on the machine(s)
    """
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))
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
        print "\nAll required packages are installed."
    else:
        print "\nAt least one package is missing!"


@task
def postfix_config():
    """
    Setup the e-mail system for the NGAS
    notifications. It requires access to an SMTP server.
    """
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))

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
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))

    set_env()
    if not env.user:
        env.user = USERNAME # defaults to ec2-user
    group = env.user # defaults to the same as the user name
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
    puts(green("\n******** USER SETUP COMPLETED!********\n"))


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
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))
    set_env()
    if check_python():
        puts(green("\n******** Valid Python found {0}!********\n".format(env.PYTHON)))
        return
        
    with cd('/tmp'):
        run('wget --no-check-certificate -q {0}'.format(APP_PYTHON_URL))
        base = os.path.basename(APP_PYTHON_URL)
        pdir = os.path.splitext(base)[0]
        run('tar -xzf {0}'.format(base))
    ppath = run('echo $PWD') + '/python'
    with cd('/tmp/{0}'.format(pdir)):
        puts('Python BUILD log-file can be found in: /tmp/py_install.log')
        puts(green('Configuring Python.....'))
        run('./configure --prefix {0} > /tmp/py_install.log 2>&1;'.format(ppath))
        puts(green('Building Python.....'))
        run('make >> /tmp/py_install.log 2>&1;')
        puts(green('Installing Python.....'))
        run('make install >> /tmp/py_install.log 2>&1')
        ppath = '{0}/bin/python{1}'.format(ppath,APP_PYTHON_VERSION)
    env.PYTHON = ppath
    puts(green("\n******** PYTHON SETUP COMPLETED!********\n"))


@task
def virtualenv_setup():
    """
    setup virtualenv with the detected or newly installed python
    """
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))
    set_env()
    check_python()
    print "CHECK_DIR: {0}".format(env.APP_DIR_ABS+'/src')
    if check_dir(env.APP_DIR_ABS+'/src') and not env.force:
        abort('{0} directory exists already'.format(env.APP_DIR_ABS))

    with cd('/tmp'):
        put('{0}/clib_tars/virtualenv-12.0.7.tar.gz'.format(env.src_dir), 'virtualenv-12.0.7.tar.gz')
        run('tar -xzf virtualenv-12.0.7.tar.gz')
        with settings(user=env.APP_USERS[0]):
            run('cd virtualenv-12.0.7; {0} virtualenv.py {1}'.format(env.PYTHON, env.APP_DIR_ABS))
            if not(check_dir('~/.pip')):
                run('mkdir ~/.pip; cd ~/.pip; wget http://curl.haxx.se/ca/cacert.pem')
            run('echo "[global]" > ~/.pip/pip.conf; echo "cert = {0}/.pip/cacert.pem" >> ~/.pip/pip.conf;'.format(env.HOME))

    puts(green("\n******** VIRTUALENV SETUP COMPLETED!********\n"))



@task
def ngas_buildout(typ='archive'):
    """
    Perform just the buildout and virtualenv config

    if env.standalone is not 0 then the eggs from the additional_tars
    will be installed to avoid accessing the internet.
    """
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))
    set_env()
    run('if [ -a bin/python ] ; then rm bin/python ; fi') # avoid the 'busy' error message

    with cd(env.APP_DIR_ABS):

        # With ports we need to pass down the berkeley DB libs/include locations
        buildoutCommand = 'buildout'
        pkgmgr = check_brew_port()
        if pkgmgr == 'port':
            buildoutCommand += ' cjclient:ldflags=-L{0}/lib/db60 cjclient:cflags=-I{0}/include/db60'.format(MACPORT_DIR)

        # Main NGAMs compilation routine
        if (env.standalone):
            put('{0}/additional_tars/eggs.tar.gz'.format(env.src_dir), '{0}/eggs.tar.gz'.format(env.APP_DIR_ABS))
            run('tar -xzf eggs.tar.gz')
            if env.linux_flavor == 'Darwin':
                put('{0}/data/common.py.patch'.format(env.src_dir), '.')
                run('patch eggs/minitage.recipe.common-1.90-py2.7.egg/minitage/recipe/common/common.py common.py.patch')
            run('find . -name "._*" -exec rm -rf {} \;') # get rid of stupid stuff left over from MacOSX
            virtualenv('{0} -Nvo'.format(buildoutCommand))
        else:
            run('find . -name "._*" -exec rm -rf {} \;')
            virtualenv(buildoutCommand)

        # Installing and initializing an NGAS_ROOT directory
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


    puts(green("\n******** NGAS_BUILDOUT COMPLETED!********\n"))



@task
def install_user_profile():
    """
    Put the activation of the virtualenv into the login profile of the user
    
    NOTE: This will be executed for the user running NGAS.
    """
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))
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

    puts(green("\n******** .bash_profile updated!********\n"))



@task
def ngas_full_buildout(typ='archive'):
    """
    Perform the full install and buildout
    """
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))
    set_env()

    # First get the sources
    #
    if (env.standalone):
        git_clone_tar()
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
        #The following will only work if the Berkeley DB has been installed already
        if env.linux_flavor == 'Darwin':
            puts('>>>> Installing Berkeley DB')
            system_install()
            virtualenv('cd /tmp; tar -xzf {0}/additional_tars/bsddb3-6.1.0.tar.gz'.format(env.APP_DIR_ABS))

            # Different flags given to the setup.py script depending on whether
            # the berkeley DB was installed using brew or port
            dbLocFlags='--berkeley-db-incdir={0}/include/db60 --berkeley-db-libdir={0}/lib/db60/'.format(MACPORT_DIR)
            pkgmgr = check_brew_port()
            if pkgmgr == 'brew':
               cellardir=check_brew_cellar()
               db_version = run('ls -tr1 {0}/berkeley-db'.format(cellar_dir)).split()[-1]
               dbLocFlags = '--berkeley-db={0}/berkeley-db/{1}'.format(cellardir,db_version)

            virtualenv('cd /tmp/bsddb3-6.1.0; ' + \
                       'export YES_I_HAVE_THE_RIGHT_TO_USE_THIS_BERKELEY_DB_VERSION=1; ' +\
                       'python{1} setup.py {0} build'.format(dbLocFlags, APP_PYTHON_VERSION))
            virtualenv('cd /tmp/bsddb3-6.1.0; ' + \
                       'export YES_I_HAVE_THE_RIGHT_TO_USE_THIS_BERKELEY_DB_VERSION=1; ' +\
                       'python{1} setup.py {0} install'.format(dbLocFlags, APP_PYTHON_VERSION))
        elif env.linux_flavor == 'Ubuntu':
            virtualenv('BERKELEYDB_DIR=/usr pip install additional_tars/bsddb3-6.1.0.tar.gz')
        else:
            virtualenv('pip install --install-option="--berkeley-db=/usr" additional_tars/bsddb3-6.1.0.tar.gz')
        virtualenv('pip install additional_tars/bottle-0.11.6.tar.gz')

    ngas_buildout(typ=typ)
    install_user_profile()

    puts(green("\n******** NGAS_FULL_BUILDOUT COMPLETED!********\n"))




@task
@serial
def test_env():
    """Configure the test environment on EC2

    Ask a series of questions before deploying to the cloud.

    Allow the user to select if a Elastic IP address is to be used
    """
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))
    if not env.has_key('AWS_PROFILE') or not env.AWS_PROFILE:
        env.AWS_PROFILE = AWS_PROFILE
    if not env.has_key('BRANCH') or not env.BRANCH:
        env.BRANCH = BRANCH
    if not env.has_key('instance_name') or not env.instance_name:
        env.instance_name = INSTANCE_NAME.format(env.BRANCH)
    if not env.has_key('use_elastic_ip') or not env.use_elastic_ip:
        env.use_elastic_ip = ELASTIC_IP
    if not env.has_key('key_filename') or not env.key_filename:
        env.key_filename = AWS_KEY
    if not env.has_key('AMI_NAME') or not env.AMI_NAME:
        env.AMI_NAME = 'CentOS'
    env.instance_name = INSTANCE_NAME.format(env.BRANCH)
    if not env.has_key('user') or not env.user:
        env.user = USERNAME
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

    if env.AMI_NAME in ['CentOS', 'SLES']:
        env.user = 'root'
    # Check and create the key_pair if necessary
    create_key_pair()
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
    puts(green("\n******** EC2 INSTANCE SETUP COMPLETE!********\n"))



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
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))
    if not env.has_key('APP_USERS') or not env.APP_USERS:
        # if not defined on the command line use the current user
        if env.user:
            env.APP_USERS = [env.user]
        else:
            env.APP_USERS = os.environ['HOME'].split('/')[-1]

    install(sys_install=False, user_install=False,
            init_install=False, typ=typ)
    with settings(user=env.APP_USERS[0]):
        run('ngamsDaemon start')
    puts(green("\n******** SERVER STARTED!********\n"))
    if test_status():
        puts(green("\n>>>>> SERVER STATUS CHECKED <<<<<<<<<<<\n"))
    else:
        puts(red("\n>>>>>>> SERVER STATUS NOT OK <<<<<<<<<<<<\n"))
    
    puts(green("\n******** USER INSTALLATION COMPLETED!********\n"))


@task
def init_deploy(typ='archive'):
    """
    Install the NGAS init script for an operational deployment

    Typical usage:
    
    fab -f machine-setup/deploy.py init_deploy -H <host> -i <ssh-key-file> -u <sudo_user>
    """
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))
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
    puts(green("\n******** CONFIGURED INIT SCRIPTS!********\n"))


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

    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))
    install(sys_install=sys_install, user_install=user_install, 
            init_install=True, typ=typ)
    
    puts(green("\n******** OPERATIONS_DEPLOY COMPLETED!********\n"))
    puts(green("\nThe server could be started now using the sqlite backend."))
    puts(green("In most cases this is not reflecting the operational requirements though."))
    puts(green("Thus some local adjustments of the NGAS configuration is most probably"))
    puts(green("required. This includes the DB backend config as well as the configuration"))
    puts(green("of the data volumes.\n"))


@task
@serial
def test_deploy():
    """
    ** MAIN TASK **: Deploy the full NGAS EC2 test environment.

    Typical usage:
    
    fab -f machine-setup/deploy.py test_deploy
    """

    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))
    test_env()
    # set environment to default for EC2, if not specified otherwise.
    set_env()
    install(sys_install=True, user_install=True, init_install=True)
    with settings(user=env.APP_USERS[0]):
        sudo('chown -R {0}:{0} /home/{0}'.format(user))
        run('ngamsDaemon start')
    puts(green("\n******** SERVER STARTED!********\n"))
    if test_status():
        puts(green("\n>>>>> SERVER STATUS CHECKED <<<<<<<<<<<\n"))
    else:
        puts(red("\n>>>>>>> SERVER STATUS NOT OK <<<<<<<<<<<<\n"))
    
    puts(green("\n******** TEST_DEPLOY COMPLETED on AWS host: {0} ********\n".format(env.host_string)))

@task
def test_status():
    """
    Execute the STATUS command against a running NGAS server
    """
    puts(blue("\n\n***** Entering task {0} *****\n\n".format(inspect.stack()[0][3])))

    set_env()
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))
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
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))
    import ngamsPClient
    if not env.has_key('src_dir') or not env.src_dir:
        print 'Please specify the local source directory of the NGAS software'
        print 'on the command line using --set src_dir=your/local/directory'
        abort(red('\n******** ARCHIVE ABORTED!********\n'))
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
            init_install=True, typ='archive',
            python_install=False):
    """
    Install NGAS users and NGAS software on existing machine.
    Note: Requires root permissions!
    """
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))
    set_env()
    if sys_install and sys_install != 'False': system_install()
    if env.postfix:
        postfix_config()
    if user_install and user_install != 'False': user_setup()

    with settings(user=env.APP_USERS[0]):

        # Get the base dir of the current python installation
        # and check that it's what we need (i.e., our own
        # installation alongside the ngas installation)
        # Only check if we're not explicitly asked to install
        # python, in which case we do it anyway
#         if not python_install:
#             currentPython  = os.path.abspath(check_python())
#             currentDir     = os.path.sep.join(currentPython.split(os.path.sep)[:-2]) if currentPython else ''
#             intendedDir    = os.path.abspath(env.APP_DIR_ABS + os.path.sep + '..' + os.path.sep + 'python')
#             python_install = currentDir != intendedDir
#         if python_install:
#             python_setup()

        ppath = check_python()
        if not ppath or str(python_install) == 'True':
            python_setup()

    if env.PREFIX != env.HOME: # generate non-standard ngas_rt directory
        sudo('mkdir -p {0}'.format(env.PREFIX))
    with settings(user=env.APP_USERS[0]):
        virtualenv_setup()
    if env.PREFIX != env.HOME:
        sudo('chown -R {0}:{0} {1}'.format(env.APP_USERS[0], env.PREFIX))
    with settings(user=env.APP_USERS[0]):
        ngas_full_buildout(typ=typ)
        cleanup_tmp()
    if init_install and init_install != 'False': init_deploy()
    puts(green("\n******** INSTALLATION COMPLETED!********\n"))


@task
def uninstall(clean_system=False):
    """
    Uninstall the NGAS software 
    NGAS users and init script will only be removed if clean_system is True
    
    NOTE: This can only be used with a sudo user. Does not uninstall
          system packages.
    """
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))
    set_env()
    if env.PREFIX != env.HOME: # avoid removing the home directory
        sudo('rm -rf {0}'.format(env.PREFIX), warn_only=True)
    run('rm -rf {0}/../python {0}'.format(env.APP_DIR_ABS), warn_only=True)
    run('rm -rf /tmp/Py* /tmp/ngas* /tmp/virtual*')
    with settings(user = env.APP_USERS[0]):
        run('mv .bash_profile_orig .bash_profile', warn_only=True)
    
    if clean_system and clean_system != 'False': # don't delete the users and system settings by default.
        for u in env.APP_USERS:
            sudo('userdel -r {0}'.format(u), warn_only=True)
        sudo('groupdel ngas', warn_only=True)
        sudo('rm /etc/ngamsServer.conf', warn_only=True)
        sudo('rm /etc/init.d/ngamsServer', warn_only=True)

    puts(green("\n******** UNINSTALL COMPLETED!********\n"))

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
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))
    if not env.has_key('PREFIX') or not env.PREFIX:
        env.PREFIX = run('echo $NGAS_PREFIX/..')
        env.APP_DIR_ABS = run('echo $NGAS_PREFIX')
    if not env.PREFIX:
        print 'Unable to identify location of NGAS installation!'
        print 'Please set the environment variable NGAS_PREFIX in .bash_profile.'
        print 'of the user running NGAS on the remote host.'
        abort(red('\n******** UPGRADE ABORTED!********\n'))
    if not env.has_key('src_dir') or not env.src_dir:
        print 'Please specify the local source directory of the NGAS software'
        print 'on the command line using --set src_dir=your/local/directory'
        abort(red('\n******** UPGRADE ABORTED!********\n'))
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
    if test_status():
        puts(green("\n>>>>> SERVER STATUS CHECKED <<<<<<<<<<<\n"))
    else:
        puts(red("\n>>>>>>> SERVER STATUS NOT OK <<<<<<<<<<<<\n"))
    puts(green("\n******** UPGRADE COMPLETED!********\n"))

    
@task
def assign_ddns():
    """
    This task installs the noip ddns client to the specified host.
    After the installation the configuration step is executed and that
    requires some manual input. Then the noip2 client is started in background.
    
    NOTE: Obviously this should only be carried out for one NGAS deployment!!
    """
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))
    sudo('yum-config-manager --enable epel')
    sudo('yum install -y noip')
    sudo('sudo noip2 -C')
    sudo('chkconfig noip on')
    sudo('service noip start')
    puts(green("\n***** Dynamic IP address assigned ******\n"))

@task
def connect():
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))
    if not env.has_key('AWS_PROFILE') or not env.AWS_PROFILE:
        env.AWS_PROFILE = AWS_PROFILE
    if not env.has_key('key_filename') or not env.key_filename:
        env.key_filename = AWS_KEY

    conn = boto.ec2.connect_to_region(AWS_REGION, profile_name=env.AWS_PROFILE)
    return conn

@task
def list_instances():
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))
    conn = connect()
    res = conn.get_all_instances()
    for r in res:
        print_instance(r.instances[0])
        print
        print

def print_instance(inst):
    inst_id    = inst.id
    inst_state = inst.state
    pub_name   = inst.public_dns_name
    tagdict    = inst.tags
    puts('Instance {0} is {1}'.format(inst_id, color_ec2state(inst_state)))
    for k in tagdict:
        puts('{0}: {1}'.format(k,tagdict[k]))
    if inst_state == 'running':
        puts("Connect:   ssh -i {0} {1}".format(AWS_KEY, pub_name))
        puts("Terminate: fab terminate:instance_id={0}".format(inst_id))

def color_ec2state(state):
    if state == 'running':
        return green(state)
    elif state == 'terminated':
        return red(state)
    elif state == 'shutting-down':
        return yellow(state)
    return state

@task
def terminate(instance_id):
    """
    Task to terminate the boto instances
    """
    puts(blue("\n***** Entering task {0} *****\n".format(inspect.stack()[0][3])))
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
