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

from fabric.api import run, sudo, put, env, require, local, task
from fabric.context_managers import cd
from fabric.contrib.console import confirm
from fabric.contrib.files import append, sed, comment
from fabric.decorators import task, serial
from fabric.operations import prompt
from fabric.utils import puts, abort, fastprint

#Defaults
USERNAME = 'ec2-user'
POSTFIX = False
AMI_ID = 'ami-aecd60c7'
INSTANCE_NAME = 'NGAS'
INSTANCE_TYPE = 't1.micro'
INSTANCES_FILE = os.path.expanduser('~/.aws/aws_instances')
AWS_KEY = os.path.expanduser('~/.ssh/icrarkey2.pem')
KEY_NAME = 'icrarkey2'
ELASTIC_IP = False
SECURITY_GROUPS = ['NGAS'] # Security group allows SSH
NGAS_PYTHON_VERSION = '2.7'
NGAS_PYTHON_URL = 'http://www.python.org/ftp/python/2.7.3/Python-2.7.3.tar.bz2'
NGAS_DIR = 'ngas_rt' #NGAS runtime directory
GITUSER = 'icrargit'
GITREPO = 'gitsrv.icrar.org:ngas'

PUBLIC_KEYS = os.path.expanduser('~/.ssh')
# WEB_HOST = 0
# UPLOAD_HOST = 1
# DOWNLOAD_HOST = 2

def set_env():
    # set environment to default for EC2, if not specified on command line.

    puts(env)
    if not env.has_key('GITUSER') or not env.GITUSER:
        env.GITUSER = GITUSER
    if not env.has_key('GITREPO') or not env.GITREPO:
        env.GITREPO = GITREPO
    if not env.has_key('instance_name') or not env.instance_name:
        env.instance_name = INSTANCE_NAME
    if not env.has_key('postfix') or not env.postfix:
        env.postfix = POSTFIX
    if not env.has_key('use_elastic_ip') or not env.use_elastic_ip:
        env.use_elastic_ip = ELASTIC_IP
    if not env.user or not env.user:
        env.user = USERNAME
    if not env.has_key('key_filename') or not env.key_filename:
        env.key_filename = AWS_KEY
    require('hosts', provided_by=[test_env])
    if not env.has_key('NGAS_DIR_ABS') or not env.NGAS_DIR_ABS:
        env.NGAS_DIR_ABS = '{0}/{1}'.format(run('printenv HOME'), NGAS_DIR)
    if not env.has_key('PYTHON'):
        env.PYTHON = check_python()
    puts('Environment: {0} {1} {2} {3} {4} {5}'.format(env.user, env.key_filename, env.hosts, 
                                                   env.host_string, env.postfix, env.NGAS_DIR_ABS))


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

    reservations = conn.run_instances(AMI_ID, instance_type=INSTANCE_TYPE, key_name=KEY_NAME, security_groups=SECURITY_GROUPS, min_count=number_instances, max_count=number_instances)
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
    res = run('if [ -d {0} ]; then echo 1; else echo ; fi'.format(directory))
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
    ppath = os.path.realpath(env.NGAS_DIR_ABS+'/../python')
    ppath = check_command('{0}/bin/python{1}'.format(ppath, NGAS_PYTHON_VERSION))
    if ppath:
        return ppath
    # Try python2.7 first
    ppath = check_command('python{0}'.format(NGAS_PYTHON_VERSION))
    if ppath:
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
        run('git clone {0}@{1}'.format(env.GITUSER, env.GITREPO))


@task
def git_clone_tar():
    """
    Clones the repository into /tmp and packs it into a tar file

    TODO: This does not work outside iVEC. The current implementation
    is thus using a tar-file, copied over from the calling machine.
    """
    local('cd /tmp && git clone {0}@{1}'.format(env.GITUSER, env.GITREPO))
    local('cd /tmp && mv ngas {0}'.format(NGAS_DIR))
    local('cd /tmp && tar -cjf {0}.tar.bz2 --exclude BIG_FILES {0}'.format(NGAS_DIR))

def processCentOSErrMsg(errmsg):
    if (errmsg == None or len(errmsg) == 0):
        return
    if (errmsg == 'Error: Nothing to do'):
        return
    firstKey = errmsg.split()[0]
    if (firstKey == 'Error:'):
        abort(errmsg)
    
    
@task
def system_install():
    """
    Perform the system installation part.
    
    NOTE: Most of this requires sudo access on the machine(s)
    """
    set_env()
   
    # Install required packages
    re = run('cat /etc/issue')
    linux_flavor = re.split()
    if (len(linux_flavor) > 0):
        if linux_flavor[0] == 'CentOS':
            linux_flavor = linux_flavor[0]
        elif linux_flavor[0] == 'Amazon':
            linux_flavor = ' '.join(linux_flavor[:2])
    if (linux_flavor in ['CentOS','Amazon Linux']):
         # Update the machine completely
        errmsg = sudo('yum --assumeyes --quiet update', combine_stderr=True, warn_only=True)
        processCentOSErrMsg(errmsg)
        
        errmsg = sudo('yum --assumeyes --quiet install python27-devel', combine_stderr=True, warn_only=True)
        processCentOSErrMsg(errmsg)
        errmsg = sudo('yum --assumeyes --quiet install git', combine_stderr=True, warn_only=True)
        processCentOSErrMsg(errmsg)
        errmsg = sudo('yum --assumeyes --quiet install autoconf', combine_stderr=True, warn_only=True)
        processCentOSErrMsg(errmsg)
        errmsg = sudo('yum --assumeyes --quiet install libtool', combine_stderr=True, warn_only=True)
        processCentOSErrMsg(errmsg)
        errmsg = sudo('yum --assumeyes --quiet install zlib-devel', combine_stderr=True, warn_only=True)
        processCentOSErrMsg(errmsg)
        errmsg = sudo('yum --assumeyes --quiet install db4-devel', combine_stderr=True, warn_only=True)
        processCentOSErrMsg(errmsg)
        errmsg = sudo('yum --assumeyes --quiet install gdbm-devel', combine_stderr=True, warn_only=True)
        processCentOSErrMsg(errmsg)
        errmsg = sudo('yum --assumeyes --quiet install readline-devel', combine_stderr=True, warn_only=True)
        processCentOSErrMsg(errmsg)
        errmsg = sudo('yum --assumeyes --quiet install sqlite-devel', combine_stderr=True, warn_only=True)
        processCentOSErrMsg(errmsg)
        errmsg = sudo('yum --assumeyes --quiet install make', combine_stderr=True, warn_only=True)
        processCentOSErrMsg(errmsg)
        errmsg = sudo ('yum --assumeyes --quiet install java-1.6.0-openjdk-devel.x86_64', combine_stderr=True, warn_only=True)
        processCentOSErrMsg(errmsg)
        errmsg = sudo ('yum --assumeyes --quiet install postfix', combine_stderr=True, warn_only=True)
        processCentOSErrMsg(errmsg)
        errmsg = sudo ('yum --assumeyes --quiet install openssl-devel.x86_64', combine_stderr=True, warn_only=True)
        processCentOSErrMsg(errmsg)
    elif (linux_flavor == 'Ubuntu'):
        sudo ('apt-get -qq -y install zlib1g-dbg')
        sudo ('apt-get -qq -y install libzlcore-dev')
        sudo ('apt-get -qq -y install libdb4.7-dev')
        sudo ('apt-get -qq -y install libgdbm-dev')  
        sudo ('apt-get -qq -y install openjdk-6-jdk')
        sudo ('apt-get -qq -y install libreadline-dev')
        sudo ('apt-get -qq -y install sqlite3')
        sudo ('apt-get -qq -y install libsqlite3-dev') 
        sudo ('apt-get -qq -y install libdb5.1-dev')
    else:
        abort("Unknown linux flavor detected: {0}".format(re))


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

    for user in ['ngas','ngasmgr']:
        sudo('useradd {0}'.format(user))
        sudo('mkdir /home/{0}/.ssh'.format(user))
        sudo('chmod 700 /home/{0}/.ssh'.format(user))
        sudo('chown {0}:{0} /home/{0}/.ssh'.format(user))
#        sudo('mv /home/ec2-user/{0}.pub /home/{0}/.ssh/authorized_keys'.format(user))
#        sudo('chmod 700 /home/{0}/.ssh/authorized_keys'.format(user))
#        sudo('chown {0}:{0} /home/{0}/.ssh/authorized_keys'.format(user))


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
        pdir = os.path.splitext(os.path.splitext(base)[0])[0]
        run('tar -xjf {0}'.format(base))
    with cd('/tmp/{0}'.format(pdir)):
        ppath = os.path.realpath(env.NGAS_DIR_ABS+'/../python')
        run('./configure --prefix {0};make;make install'.format(ppath))
        ppath = '{0}/bin/python{1}'.format(ppath,NGAS_PYTHON_VERSION)
    env.PYTHON = ppath

    
@task
def virtualenv_setup():
    """
    setup virtualenv with the detected or newly installed python
    """
    set_env()
    print "CHECK_DIR: {0}".format(check_dir(env.NGAS_DIR_ABS))
    if check_dir(env.NGAS_DIR_ABS):
        abort('ngas_rt directory exists already')
        
    with cd('/tmp'):
        run('wget --no-check-certificate -q https://raw.github.com/pypa/virtualenv/master/virtualenv.py')
        run('{0} virtualenv.py {1}'.format(env.PYTHON, env.NGAS_DIR_ABS))
    with cd(env.NGAS_DIR_ABS):
        virtualenv('pip install zc.buildout')        
        # make this installation self consistent
        virtualenv('pip install fabric')
        virtualenv('pip install boto')



@task
def ngas_full_buildout():
    """
    Perform the full install and buildout and virtualenv config
    """
    set_env()
    # First get the sources
    # 
    git_clone_tar()
    tarfile = '{0}.tar.bz2'.format(NGAS_DIR)
    put('/tmp/{0}'.format(tarfile), tarfile)
#    local('rm -rf {0}'.format(tarfile))  # cleanup local git clone
    run('tar -xjf {0} && rm {0}'.format(tarfile))
    
    # git_clone()
    
    with cd(env.NGAS_DIR_ABS):
        # run bootstrap with correct python version (explicit)
        run('if [ -a bin/python ] ; then rm bin/python ; fi') # avoid the 'busy' error message
        virtualenv('python{0} bootstrap.py'.format(NGAS_PYTHON_VERSION))
        virtualenv('buildout')
    run('ln -s {0}/NGAS NGAS'.format(NGAS_DIR))

@task
def ngas_buildout():
    """
    Perform just the buildout and virtualenv config
    """
    set_env()
    
    with cd(env.NGAS_DIR_ABS):
        virtualenv('buildout')
    run('ln -s {0}/NGAS NGAS'.format(NGAS_DIR))


@task
@serial
def test_env():
    """Configure the test environment on EC2

    Ask a series of questions before deploying to the cloud.

    Allow the user to select if a Elastic IP address is to be used
    """
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
    env.key_filename = AWS_KEY
    env.roledefs = {
        'ngasmgr' : host_names,
        'ngas' : host_names,
    }

@task
def user_deploy():
    """
    Deploy the system as a normal user without sudo access
    """
    ppath = check_python()
    if not ppath:
        python_setup()
    virtualenv_setup()
    ngas_full_buildout()


@task
@serial
def test_deploy():
    """
    ** MAIN TASK **: Deploy the full NGAS EC2 test environment. 
    (Does not include the NGAS users at this point)
    """

    test_env()
    # set environment to default for EC2, if not specified otherwise.
    set_env()
    system_install()
    if env.postfix:
        postfix_config()
    ppath = check_python()
    if not ppath:
        python_setup()
    virtualenv_setup()
    ngas_full_buildout()

@task
def start_server():
    """
    Start the installed NGAS server using the SQLite DB.
    """
    set_env()
    with cd(env.NGAS_DIR_ABS):
        run('{0}/bin/ngamsServer -cfg {0}/cfg/NgamsCfg.SQLite.mini.xml '.format(env.NGAS_DIR_ABS)+\
                   '-force -autoOnline -v 2')
