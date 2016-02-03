"""
Fabric file for installing NGAS servers

Test deployment on EC2 is simple as it only runs on one server
fab test_deploy

The tasks can be used individually and thus allow installations in very
diverse situations.

For a full deployment on AWS use the command

fab test_deploy

For a local installation under a normal user without sudo access

fab -u `whoami` -H <IP address> user_deploy

For a remote installation under non-default user ngas-user using a
non-default source directory for the installation you can use. This
installation is using a different (sudo) user on the target machine
to run the installation.

fab -u sudo_user -H <IP address> user_deploy --set APP_USERS=ngas-user,src_dir=/tmp/ngas_test

Please also refer to the INSTALL document in the root directory of the NGAS source tree.
"""
import glob

import boto, boto.ec2
import os, stat
import time, urllib
import threading

from fabric.api import put, env, local, task
from fabric.api import run as frun
from fabric.api import sudo as fsudo
from fabric.state import output
from fabric.context_managers import cd, hide, settings
from fabric.contrib.console import confirm
from fabric.contrib.files import exists
from fabric.contrib.project import rsync_project
from fabric.decorators import task as fabtask, serial
from fabric.operations import prompt
from fabric.utils import puts, abort, fastprint
from fabric.exceptions import NetworkError
from fabric.colors import blue, green, red, yellow

from Crypto.PublicKey import RSA


# Verbose tasks tell the user when they are being executed
_task_depth = threading.local()
_task_depth.value = 0
class VerboseTask(WrappedCallableTask):
    def run(self, *args, **kwargs):
        global _task_depth
        _task_depth.value += 1
        depth = _task_depth.value
        fname = self.wrapped.__name__
        if 'silent' not in env:
            puts(blue("*"*depth + " Entering task {0}".format(fname)))
        ret = super(VerboseTask, self).run(*args, **kwargs)
        if 'silent' not in env:
            puts(green("*"*depth + " Exiting task {0}".format(fname)))
        _task_depth.value -= 1
        return ret

# Replacement functions for running commands
# They wrap up the command with useful things
def run(*args, **kwargs):
    with hide('running'):
        com = args[0]
        com = 'unset PYTHONPATH; {0}'.format(com)
        puts('Executing: {0}'.format(com))
        res = frun(com, quiet=False, **kwargs)
    return res

def sudo(*args, **kwargs):
    with hide('running'):
        com = args[0]
        com = 'unset PYTHONPATH; {0}'.format(com)
        puts('Executing: {0}'.format(com))
    res = fsudo(com, quiet=True, **kwargs)
    return res

#Defaults
thisDir = os.path.dirname(os.path.realpath(__file__))
if not env.has_key('mykeys') and not env.has_key('okeys'): 
    env.okeys = env.keys() # save original keys


#### This should be replaced by another key and security group
AWS_REGION = 'us-east-1'
AWS_PROFILE = 'NGAS'
KEY_NAME = 'icrar_ngas'
AWS_KEY = os.path.expanduser('~/.ssh/{0}.pem'.format(KEY_NAME))
AWS_SEC_GROUP = 'NGAS' # Security group allows SSH and other ports


BRANCH = 'master'    # this is controlling which branch is used in git clone
USERNAME = 'ec2-user'
POSTFIX = False
AMI_IDs = {
           'Amazon':'ami-7c807d14',
           'Amazon-hvm': 'ami-60b6c60a',
           'CentOS': 'ami-8997afe0',
           'Old_CentOS':'ami-aecd60c7', 
           'SLES-SP2':'ami-e8084981',
           'SLES-SP3':'ami-c08fcba8'
           }
AMI_NAME = 'Amazon'
INSTANCE_NAME = 'NGAS_{0}'
INSTANCE_TYPE = 't1.micro'
env.instance_id = 'UNKNOWN' # preset
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
   'cfitsio-devel'
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
        'libcfitsio-dev'
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


PUBLIC_KEYS = os.path.expanduser('~/.ssh')
# WEB_HOST = 0
# UPLOAD_HOST = 1
# DOWNLOAD_HOST = 2

@task
def set_env(hideing='nothing', display=False):

    output.update({'nothing':[]}) # enable hideing nothing
    with hide(hideing):
        # Avoid multiple calls of set_env for most things
        if env.has_key('environment_already_set') and env.environment_already_set: 
            puts('Environment already established.')
            if check_aws_meta() and env.instance_id == 'UNKNOWN': # is this an AWS instance? 
                get_instance_id()
            elif env.instance_id == 'UNKNOWN': # not an AWS instance
                env.instance_id = 'N/A'
            if env.HOME[0] == '~' and check_user(env.APP_USERS[0]):
                with settings(user = env.APP_USERS[0]):
                    env.HOME = run("echo $HOME") # always set to $HOME of APP_USERS[0]
                env.PREFIX = env.HOME
                env.APP_DIR_ABS = '{0}/{1}'.format(env.PREFIX, APP_DIR)
        else:  # first time in set_env
        # puts(env)
            env.keepalive = 15
            env.connection_attempts = 5
            if not env.has_key('key_filename') or not env.key_filename:
                if os.path.exists(os.path.expanduser(AWS_KEY)):
                    env.key_filename = AWS_KEY
            else:
                puts(red("SSH key_filename: {0}".format(env.key_filename)))        
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
            if not env.has_key('src_dir') or not env.src_dir:
                env.src_dir = thisDir + '/../'
            if not env.has_key('hosts') or env.hosts:
                env.hosts = [env.host_string]
            if not env.has_key('HOME') or not env.HOME:
                if not check_user(env.APP_USERS[0]): #preset if user does not exist
                    env.HOME = '~{0}'.format(env.APP_USERS[0])
                else:
                    with settings(user=env.APP_USERS[0]):
                        env.HOME = run('echo $HOME')
            if not env.has_key('PREFIX') or env.PREFIX[0] == '~' or not env.PREFIX:
                env.PREFIX = env.HOME
            if not env.has_key('APP_DIR_ABS') or env.APP_DIR_ABS[0] == '~' \
            or not env.APP_DIR_ABS:
                env.APP_DIR_ABS = '{0}/{1}'.format(env.PREFIX, APP_DIR)
                env.APP_DIR = APP_DIR
            else:
                env.APP_DIR = env.APP_DIR_ABS.split('/')[-1]
            if not env.has_key('APP_CONF') or not env.APP_CONF:
                env.APP_CONF = APP_CONF
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

            if check_aws_meta(): # is this an AWS instance? 
                get_instance_id()
            else:
                env.instance_id = 'N/A'


            if env.instance_id != 'N/A' and (not env.has_key('AMI_NAME') or not env.AMI_NAME):
                env.AMI_NAME = AMI_NAME
                env.user = USERNAME
            if env.instance_id != 'N/A' and env.AMI_NAME in ['CentOS', 'SLES']:
                env.user = 'root'
            get_linux_flavor()
        
            env.nprocs = 1
            if env.linux_flavor in SUPPORTED_OS_LINUX:
                env.nprocs = int(run('grep -c processor /proc/cpuinfo'))
        
            nkeys = env.keys()
            env.mykeys = set(nkeys).difference(env.okeys)
            env.environment_already_set = True
        
    if display == 'all':
        print_env(all=True)
    elif len(str(display)) > 0:
        print_env()
    return

@task
def print_env(force=False, all=False):
    """
    Task prints the current fabric environment variables.
    
    force, if set, calls set_env again
    all, if set, prints all variables instead of the private ones
    """
    if not env.has_key('mykeys') or force:
        print blue('Calling set_env. This will take a moment...')
        with settings(environment_already_set = False):
            set_env(hideing='everything')
    print blue('Private variables:')
    for k in env.mykeys:
        if k not in ['okeys','mykeys']:
            print '{0:50s}>>>{1:>50s}'.format(k,repr(env[k]))
    if all:
        print blue('\nStandard FABRIC variables:')
        for k in env.okeys():
            if k not in ['okeys','mykeys']:
                print '{0:50s}>>>{1:>50s}'.format(k,repr(env[k]))

@task(task_class=VerboseTask)
def whatsmyip():
    """
    Returns the external IP address of the host running fab.
    
    NOTE: This is only used for EC2 setups, thus it is assumed
    that the host is on-line.
    """
    whatismyip = 'http://bot.whatismyipaddress.com/'
    try:
        myip = urllib2.urlopen(whatismyip, timeout=5).readlines()[0]
    except:
        puts(red('Unable to derive IP through {0}'.format(whatismyip)))
        myip = '127.0.0.1'
    return myip

@task(task_class=VerboseTask)
def check_ssh():
    """
    Check availability of SSH on HOST
    """
    ssh_available = False
    ntries = 30
    tries = 0
    test_period = 10
    timeout = 3
    t_sleep = test_period - timeout
    while tries < ntries and not ssh_available:
        try:
            with settings(timeout=timeout, warn_only=True):
                run("echo 'Is SSH working?'", combine_stderr=True)
            ssh_available = True
            puts(green("SSH is working!"))
        except NetworkError:
            puts(red("SSH is NOT working after {0} seconds!".format(str(tries*test_period))))
            tries += 1
            time.sleep(t_sleep)

def check_aws_meta():
    """
    Tasks checks the availability of the AWS meta data service.
    This is used to figure out whether a specific node is an
    AWS instance or not.
    """
    res = run('curl --connect-timeout 2 http://169.254.169.254/latest/meta-data/ami-id > /dev/null 2>&1; if [ $? -eq 0 ]; then echo 1; else echo 0; fi')
    if res == '0':
        return False
    else:
        return True

@task
def check_create_aws_sec_group():
    """
    Check whether default security group exists
    """
    conn = connect()
    sec = conn.get_all_security_groups()
    conn.close()
    for sg in sec:
        if sg.name.upper() == AWS_SEC_GROUP:
            puts(green("AWS Security Group {0} exists ({1})".format(AWS_SEC_GROUP, sg.id)))
            return sg.id

    # Not found, create a new one
    ngassg = conn.create_security_group(AWS_SEC_GROUP, 'NGAS default permissions')
    ngassg.authorize('tcp', 22, 22, '0.0.0.0/0')
    ngassg.authorize('tcp', 80, 80, '0.0.0.0/0')
    ngassg.authorize('tcp', 5678, 5678, '0.0.0.0/0')
    ngassg.authorize('tcp', 7777, 7777, '0.0.0.0/0')
    ngassg.authorize('tcp', 8888, 8888, '0.0.0.0/0')
    return ngassg.id

@task
def aws_create_key_pair():
    """
    Create the AWS_KEY if it does not exist already and copies it into ~/.ssh
    """
    if not env.has_key('AWS_PROFILE') or not env.AWS_PROFILE:
        env.AWS_PROFILE = AWS_PROFILE
    conn = boto.ec2.connect_to_region(AWS_REGION, profile_name=env.AWS_PROFILE)
    kp = conn.get_key_pair(KEY_NAME)
    if not kp: # key does not exist on AWS
        kp = conn.create_key_pair(KEY_NAME)
        puts(green("\n******** KEY_PAIR created!********\n"))
        if os.path.exists(os.path.expanduser(AWS_KEY)):
            os.unlink(AWS_KEY)
        kp.save('~/.ssh/')
        Rkey = RSA.importKey(kp.material)
        env.SSH_PUBLIC_KEY = Rkey.exportKey('OpenSSH')
        puts(green("\n******** KEY_PAIR written!********\n"))
    else:
        puts(green('***** KEY_PAIR exists! *******'))

    if not os.path.exists(os.path.expanduser(AWS_KEY)): # don't have the private key
        if not kp:
            kp = conn.get_key_pair(KEY_NAME)
        puts(green("\n******** KEY_PAIR retrieved********\n"))
        Rkey = RSA.importKey(kp.material)
        env.SSH_PUBLIC_KEY = Rkey.exportKey('OpenSSH')
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

    conn.close()
    return

@task
def create_key_pair():
    """
    Create a key pair using pycrypto and returning key object
    
    NOTE: access to
    private key: key.exportKey('PEM')
    public key:  key.exportKey('OpenSSH')
    """
    key_fname = os.path.expanduser(AWS_KEY)
    if os.path.exists(key_fname): # have the private key
        with open(key_fname, 'r') as content_file:
            key = content_file.read()
            okey = RSA.importKey(key)
    else:        
        okey = RSA.generate(2048, os.urandom)
        with open(key_fname, 'w') as content_file:
            os.chmod(key_fname, stat.S_IRWXU)
            content_file.write(okey.exportKey('PEM'))
    
    env.SSH_PUBLIC_KEY = okey.exportKey('OpenSSH')

def create_instance(names, instance_type, use_elastic_ip, public_ips, sgid):
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

    reservations = conn.run_instances(AMI_IDs[env.AMI_NAME], instance_type=instance_type, \
                                    key_name=KEY_NAME, security_group_ids=[sgid],\
                                    min_count=number_instances, max_count=number_instances)
    instances = reservations.instances
    # Sleep so Amazon recognizes the new instance
    for i in range(4):
        fastprint('.')
        time.sleep(5)

    # Are we running yet?
    iid = [x.id for x in instances]
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
def check_user(user):
    """
    Task checking existence of user
    """
    res = run('if id -u "{0}" >/dev/null 2>&1; then echo 1; else echo 0; fi;'.format(user))
    if res == '0': 
        puts('User {0} does not exist'.format(user))
        return False
    else:
        return True


@task
def check_python():
    """
    Check for the existence of correct version of python

    INPUT:
    None

    OUTPUT:
    path to python binary    string, could be empty string
    """
    # Try whether there is already a local python installation for this user
    set_env(hideing='everything', display=True)

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

def virtualenv(command, **kwargs):
    """
    Just a helper function to execute commands in the virtualenv
    """
    env.activate = 'source {0}/bin/activate'.format(env.APP_DIR_ABS)
    with cd(env.APP_DIR_ABS):
        run(env.activate + '&&' + command, **kwargs)

@task
def git_clone_tar(unpack=True):
    """
    Clones the repository into /tmp and packs it into a tar file

    TODO: This does not work outside iVEC. The current implementation
    is thus using a tar-file, copied over from the calling machine.
    """
    set_env(hideing='everything')

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
    set_env(hideing='everything', display=True)
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

    puts(blue("Remote machine running %s" % linux_flavor))
    env.linux_flavor = linux_flavor
    return linux_flavor

@task
def get_instance_id():
    """
    Tasks retrieves the instance_id of the current instance internally.
    It alos sets the variable env.instance_id
    """
    if check_command('wget'):
        env.instance_id = run("wget -qO- http://169.254.169.254/latest/meta-data/instance-id")
    else:
        env.instance_id = 'UNKNOWN'
    puts(env.instance_id)
    return env.instance_id


@task
def system_install():
    """
    Perform the system installation part.

    NOTE: Most of this requires sudo access on the machine(s)
    """
    with settings(environment_already_set=False):
        set_env(hideing='everything')


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
    with hide('running','stderr','stdout'):
        set_env(hideing='everything', display=True)


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
        for package in APT_PACKAGES:
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
    with settings(environment_setup_already = False):
        set_env(hideing='everything', display=True)

    if not env.user:
        env.user = USERNAME # defaults to ec2-user
    group = env.user # defaults to the same as the user name
    sudo('groupadd ngas', warn_only=True)
    for user in env.APP_USERS:
        sudo('useradd -g {0} -m -s /bin/bash {1}'.format(group, user), warn_only=True)
        sudo('mkdir /home/{0}/.ssh'.format(user), warn_only=True)
        sudo('chmod 700 /home/{0}/.ssh'.format(user))
        sudo('chown -R {0}:{1} /home/{0}/.ssh'.format(user,group))
        _ = run('echo $HOME')
        create_key_pair()
        sudo("echo '{0}' >> /home/{1}/.ssh/authorized_keys".format(env.SSH_PUBLIC_KEY, user))
        sudo('chmod 600 /home/{0}/.ssh/authorized_keys'.format(user))
        sudo('chown {0}:{1} /home/{0}/.ssh/authorized_keys'.format(user, group))
        if not env.has_key('key_filename') or not env.key_filename:
            if os.path.exists(os.path.expanduser(AWS_KEY)):
                env.key_filename = AWS_KEY
        
    # create NGAS directories and chown to correct user and group
    for dirname in env.APP_DIR_ABS, getNgasRootDir():
        sudo('mkdir -p {0}'.format(dirname))
        sudo('chown {0}:{1} {2}'.format(env.APP_USERS[0], group, dirname))

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
    set_env(hideing='everything', display=True)

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
    set_env(hideing='everything', display=True)

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
    set_env(hideing='everything')

    with cd(env.APP_DIR_ABS):

        # Main NGAMs compilation routine
        virtualenv("./build.sh")

        # Installing and initializing an NGAS_ROOT directory
        _,_,cfg,lcfg,  = initName(typ=typ)
        ngasRootDir = getNgasRootDir()
        ngasTargetCfg = os.path.join(ngasRootDir, 'cfg', lcfg)
        with settings(warn_only=True):
            run('mkdir -p {0}'.format(ngasRootDir))
        run('cp -R {0}/NGAS/* {1}'.format(env.APP_DIR_ABS, ngasRootDir))
        with settings(warn_only=True):
            run('cp {0}/cfg/{1} {2}'.format(env.APP_DIR_ABS, cfg, ngasTargetCfg))
        if env.linux_flavor == 'Darwin': # capture stupid difference in sed on Mac OSX
            run("""sed -i '' 's/\*replaceRoot\*/{0}/g' {1}""".format(ngasRootDir.replace('/','\\/'), ngasTargetCfg))
        else:
            run("""sed -i 's/\*replaceRoot\*/{0}/g' {1}""".format(ngasRootDir.replace('/', '\\/'), ngasTargetCfg))

        with cd(ngasRootDir):
            with settings(warn_only=True):
                run('sqlite3 -init {0}/src/ngamsCore/ngamsSql/ngamsCreateTables-SQLite.sql ngas.sqlite <<< $(echo ".quit")'\
                    .format(env.APP_DIR_ABS))
                run('cp ngas.sqlite {0}/src/ngamsTest/ngamsTest/src/ngas_Sqlite_db_template'.format(env.APP_DIR_ABS))


    puts(green("\n******** NGAS_BUILDOUT COMPLETED!********\n"))

def getNgasRootDir():
    return os.path.abspath(os.path.join(env.APP_DIR_ABS, '..', 'NGAS'))

@task
def install_user_profile():
    """
    Put the activation of the virtualenv into the login profile of the user
    unless the NGAS_DONT_MODIFY_BASHPROFILE environment variable is defined

    NOTE: This will be executed for the user running NGAS.
    """
    if run('echo $NGAS_DONT_MODIFY_BASHPROFILE'):
        return

    set_env(hideing='everything')

    nuser = env.APP_USERS[0]
    appDir = env.APP_DIR_ABS
    ngasRootDir = getNgasRootDir()
    if env.user != nuser:
        with cd(env.HOME):
            if not exists("{0}/.bash_profile_orig".format(env.HOME)):
                sudo('sudo -u {0} cp .bash_profile .bash_profile_orig'.format(nuser), warn_only=True)
            else:
                sudo('sudo -u {0} cp .bash_profile_orig .bash_profile'.format(nuser))
            sudo('sudo -u {0} echo "export NGAS_PREFIX={1}" >> .bash_profile'.format(nuser, ngasRootDir))
            sudo('sudo -u {0} echo "source {1}/bin/activate" >> .bash_profile'.format(nuser, appDir))
    else:
        with cd(env.HOME):
            if not exists("{0}/.bash_profile_orig".format(env.HOME)):
                run('cp .bash_profile .bash_profile_orig'.format(nuser), warn_only=True)
            else:
                run('cp .bash_profile_orig .bash_profile'.format(nuser))
            run('echo "export NGAS_PREFIX={1}" >> .bash_profile'.format(nuser, ngasRootDir))
            run('echo "source {1}/bin/activate" >> .bash_profile'.format(nuser, appDir))


@task
def ngas_full_buildout(typ='archive'):
    """
    Perform the full install and buildout
    """
    set_env(hideing='everything')


    # First get the sources
    #
    if (env.standalone):
        git_clone_tar()
    elif check_path('{0}/README'.format(env.APP_DIR_ABS)) == '0':
        git_clone_tar()

    with cd(env.APP_DIR_ABS):
        #The following will only work if the Berkeley DB has been installed already
        if env.linux_flavor == 'Darwin':
            virtualenv('cd /tmp; tar -xzf {0}/additional_tars/bsddb3-6.1.0.tar.gz'.format(env.APP_DIR_ABS))

            # Different flags given to the setup.py script depending on whether
            # the berkeley DB was installed using brew or port
            dbLocFlags='--berkeley-db-incdir={0}/include/db60 --berkeley-db-libdir={0}/lib/db60/'.format(MACPORT_DIR)
            pkgmgr = check_brew_port()
            if pkgmgr == 'brew':
                cellardir = check_brew_cellar()
                db_version = run('ls -tr1 {0}/berkeley-db'.format(cellardir)).split()[-1]
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


def default_if_empty(env, key, default):
    if key not in env or not env[key]:
        env[key] = default

@task
@serial
def configure_test_env(n_instances=1):
    """Configure the test environment on EC2

    This method creates AWS instances and points the fabric environment to them with
    the current public IP and username.
    """
    env.BRANCH = local('git rev-parse --abbrev-ref HEAD', capture=True)
    default_if_empty(env, 'AMI_NAME',       AMI_NAME)
    default_if_empty(env, 'AWS_PROFILE',    AWS_PROFILE)
    default_if_empty(env, 'instance_name',  INSTANCE_NAME.format(env.BRANCH))
    default_if_empty(env, 'instance_type',  INSTANCE_TYPE)
    default_if_empty(env, 'use_elastic_ip', ELASTIC_IP)

    use_elastic_ip = to_boolean(env.use_elastic_ip)
    public_ip = None
    if use_elastic_ip:
        if 'public_ip' in env:
            public_ip = env.public_ip
        else:
            public_ip = prompt('What is the public IP address: ', 'public_ip')
    public_ips = [public_ip for _ in xrange(n_instances)]


    # Check and create the key_pair if necessary
    aws_create_key_pair()
    # Check and create security group if necessary
    sgid = check_create_aws_sec_group()
    # Create the instance in AWS
    if n_instances > 1:
        instance_names = ["%s_%d" % (env.instance_name, i) for i in xrange(n_instances)]
    else:
        instance_names = [env.instance_name]
    host_names = create_instance(instance_names, env.instance_type, use_elastic_ip, public_ips, sgid)

    # Update our fabric environment so from now on we connect to the
    # AWS machine (and using the correct usernames)
    env.hosts = host_names
    if 'key_filename' not in env or not env.key_filename:
        env.key_filename = AWS_KEY
    if env.AMI_NAME in ['CentOS', 'SLES']:
        env.user = 'root'
    else:
        env.user = 'ec2-user'

    # Instances have started, but are not useable yet, make sure SSH has started
    puts('Started the instance(s) now waiting for the SSH daemon to start.')
    execute(check_ssh)


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
    if not env.has_key('APP_USERS') or not env.APP_USERS:
        # if not defined on the command line use the current user
        if env.user:
            env.APP_USERS = [env.user]
        else:
            env.APP_USERS = os.environ['HOME'].split('/')[-1]

    install(sys_install=False, user_install=False, init_install=False, typ=typ)
    start_ngas_and_check_status()

@task
def start_ngas_and_check_status():
    """
    Starts the ngamsDaemon process and checks that the server is up and running
    """
    with settings(user=env.APP_USERS[0]):
        virtualenv('ngamsDaemon start')

    puts(green("\n******** SERVER STARTED!********\n"))
    if test_status():
        puts(green("\n>>>>> SERVER STATUS CHECKED <<<<<<<<<<<\n"))
    else:
        puts(red("\n>>>>>>> SERVER STATUS NOT OK <<<<<<<<<<<<\n"))

@task
def init_deploy(typ='archive'):
    """
    Install the NGAS init script for an operational deployment

    Typical usage:
    
    fab -f machine-setup/deploy.py init_deploy -H <host> -i <ssh-key-file> -u <sudo_user>
    """
    initFile, initLink, _, _ = initName(typ=typ)

    set_env(hideing='everything')


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
    configure_test_env()
    install(sys_install=True, user_install=True, init_install=True)
    start_ngas_and_check_status()
    puts(green("******** TEST_DEPLOY COMPLETED on AWS hosts: {0} ********\n".format(env.hosts)))

@task
def test_status():
    """
    Execute the STATUS command against a running NGAS server
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
def archiveSource():
    """
    Archive the NGAS source package on a NGAS server
    
    Typical usage:
    
    fab -f machine-setup/deploy.py archiveSource -H ngas.ddns.net --set src_dir=.
    
    NOTE: The ngamsPClient module must be on the python path for fab.
    """
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
    #set_env(hideing='everything')

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
    set_env(hideing='nothing', display=True)
    if sys_install and sys_install != 'False':
        system_install()
    if env.postfix:
        postfix_config()
    if user_install and user_install != 'False':
        user_setup()

    with settings(user=env.APP_USERS[0]):
        ppath = check_python()
        if not ppath or str(python_install) == 'True':
            python_setup()

        if env.PREFIX != env.HOME: # generate non-standard ngas_rt directory
            run('mkdir -p {0}'.format(env.PREFIX))
        virtualenv_setup()
        ngas_full_buildout(typ=typ)
        cleanup_tmp()

    if init_install and init_install != 'False':
        init_deploy()
    puts(green("\n******** INSTALLATION COMPLETED!********\n"))
    return env

@task
def uninstall(clean_system=False):
    """
    Uninstall the NGAS software 
    NGAS users and init script will only be removed if clean_system is True
    
    NOTE: This can only be used with a sudo user. Does not uninstall
          system packages.
    """
    set_env(hideing='everything')

    if env.PREFIX != env.HOME: # avoid removing the home directory
        sudo('rm -rf {0}'.format(env.PREFIX), warn_only=True)
    run('rm -rf {0}/../python {0}'.format(env.APP_DIR_ABS), warn_only=True)
    run('rm -rf /tmp/Py* /tmp/ngas* /tmp/virtual*')
    local('rm -rf /tmp/ngas*')
    with settings(user = env.APP_USERS[0]):
        if check_path('.bash_profile_orig'):
            run('mv .bash_profile_orig .bash_profile', warn_only=True)
        else: # if there was nothing before just remove the current one
            run('mv .bash_profile .bash_profile.bak', warn_only=True)
    
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
    set_env(hideing='everything')

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
    with cd('/usr/local/src'):
        sudo('wget http://www.no-ip.com/client/linux/noip-duc-linux.tar.gz')
        sudo('tar xf noip-duc-linux.tar.gz')
        sudo('cd noip-2.1.9-1')
        sudo('make install')
    sudo('noip2 -C')
    # TODO: put startup script in repo and install it
    # sudo('chkconfig noip on')
    # sudo('service noip start')
    puts(green("\n***** Dynamic IP address assigned ******\n"))

@task
def connect():
    if not env.has_key('AWS_PROFILE') or not env.AWS_PROFILE:
        env.AWS_PROFILE = AWS_PROFILE
    conn = boto.ec2.connect_to_region(AWS_REGION, profile_name=env.AWS_PROFILE)
    return conn

@task
def list_instances():
    """
    Lists the EC2 instances associated to the user's amazon key
    """
    conn = connect()
    res = conn.get_all_instances()
    for r in res:
        print
        print_instance(r.instances[0])
        print

def print_instance(inst):
    inst_id    = inst.id
    inst_state = inst.state
    inst_type  = inst.instance_type
    pub_name   = inst.public_dns_name
    tagdict    = inst.tags
    l_time     = inst.launch_time
    key_name   = inst.key_name
    puts('Instance {0} ({1}) is {2}'.format(inst_id, inst_type, color_ec2state(inst_state)))
    for k, val in tagdict.items():
        if k == 'Name':
            val = blue(val)
        puts('{0}: {1}'.format(k,val))
    if inst_state == 'running':
        puts("Connect:   ssh -i ~/.ssh/{0}.pem {1}".format(key_name, pub_name))
        puts("Terminate: fab terminate:instance_id={0}".format(inst_id))
    print 'Launch time: {0}'.format(l_time)

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
    if tagdict.has_key('Created By') and tagdict['Created By'] != userAThost:
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


# -- EOF --
