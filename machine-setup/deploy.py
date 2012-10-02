"""
Fabric file for installing NGAS servers

Test deployment on EC2 is simple as it only runs on one server
fab test_deploy

The tasks can be used individually and thus allow installations in very
diverse situations.

For a full deployment use the command

fab --set postfix=False -f machine-setup/deploy.py test_deploy
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

USERNAME = 'ec2-user'
AMI_ID = 'ami-aecd60c7'
INSTANCE_TYPE = 't1.micro'
INSTANCES_FILE = os.path.expanduser('~/.aws/aws_instances')
AWS_KEY = os.path.expanduser('~/.ssh/icrarkey2.pem')
KEY_NAME = 'icrarkey2'
SECURITY_GROUPS = ['default'] # Security group allows SSH
NGAS_DIR = 'ngas'
NGAS_DIR_ABS = '/home/%s/%s' % (USERNAME, NGAS_DIR)
env.GITUSER = 'andreas'
env.GITREPO = 'storage01.icrar.org:/mnt/raid6/gitrepos/ngas'
env['postfix'] = False

# PUBLIC_KEYS = os.path.expanduser('~/Documents/Keys')
# WEB_HOST = 0
# UPLOAD_HOST = 1
# DOWNLOAD_HOST = 2

def set_env():
    # set environment to default for EC2, if not specified otherwise.
    if not env.user:
        env.user = USERNAME
    if not env.key_filename:
        env.key_filename = AWS_KEY
    if env.postfix:
        env.postfix = to_boolean(env.postfix)
    require('hosts', provided_by=[test_env])


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

def copy_public_keys():
    """
    Copy the public keys to the remote servers
    """
    env.list_of_users = []
    for file in glob.glob(PUBLIC_KEYS + '/*.pub'):
        filename = os.path.basename(file)
        user, ext = os.path.splitext(filename)
        env.list_of_users.append(user)
        put(file, filename)

def virtualenv(command):
    """
    Just a helper function to execute commands in the virtualenv
    """
    env.activate = 'source {0}/bin/activate'.format(NGAS_DIR_ABS)
    with cd(NGAS_DIR_ABS):
        run(env.activate + '&&' + command)

def git_pull():
    """
    Updates the repository.
    TODO: This does not work outside iVEC. The current implementation
    is thus using a tar-file, copied over from the calling machine.
    """
    with cd(NGAS_DIR_ABS):    
        sudo('git pull', user=env.deploy_user)

@task
def git_clone_tar():
    """
    Clones the repository into /tmp and packs it into a tar file

    TODO: This does not work outside iVEC. The current implementation
    is thus using a tar-file, copied over from the calling machine.
    """
    local('cd /tmp && git clone {0}@{1}'.format(env.GITUSER, env.GITREPO))
    local('cd /tmp && tar -cjf ngas.tar.bz2 ngas')


@task
def system_install():
    """
    Perform the system installation part.
    
    NOTE: Most of this requires sudo access on the machine(s)
    """
    # Update the AMI completely
    sudo('yum --assumeyes --quiet update')

    # Install required packages
    sudo('yum --assumeyes --quiet install python27-devel')
    sudo('yum --assumeyes --quiet install git')
    sudo('yum --assumeyes --quiet install autoconf')
    sudo('yum --assumeyes --quiet install libtool')
    sudo('yum --assumeyes --quiet install zlib-devel')
    sudo('yum --assumeyes --quiet install db4-devel')
    sudo('yum --assumeyes --quiet install gdbm-devel')
    sudo('yum --assumeyes --quiet install readline-devel')
    sudo('yum --assumeyes --quiet install sqlite-devel')
    sudo('yum --assumeyes --quiet install make')
    sudo ('yum --assumeyes --quiet install java-1.6.0-openjdk-devel.x86_64')
    sudo ('yum --assumeyes --quiet install postfix')


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
def ngas_buildout():
    """
    Perform the full buildout and virtualenv config
    """

    set_env()
#    run('virtualenv-2.6 --no-site-packages {0}'.format(NGAS_DIR))
    puts('before wget')
    run('wget https://raw.github.com/pypa/virtualenv/master/virtualenv.py')
    puts('after wget')
    run('python2.7 virtualenv.py {0}'.format(NGAS_DIR))

    # First get the sources
    # 
    git_clone_tar()
    put('/tmp/ngas.tar.bz2','/tmp/ngas.tar.bz2')
    run('tar -xjf /tmp/ngas.tar.bz2')

    
    with cd(NGAS_DIR_ABS):
        virtualenv('pip install zc.buildout')
        
        # make this installation self consistent
        virtualenv('pip install fabric')
        virtualenv('pip install boto')
        virtualenv('python2.7 bootstrap.py')
        virtualenv('buildout')

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
    env.user = USERNAME
    env.key_filename = AWS_KEY
    env.roledefs = {
        'ngasmgr' : host_names,
        'ngas' : host_names,
    }

@task
@serial
def test_deploy():
    """
    ** MAIN TASK **: Deploy the full NGAS test environment. (Does not include the NGAS users at this point)
    """
    # set environment to default for EC2, if not specified otherwise.
    set_env()

    test_env()
    system_install()
    if env.postfix:
        postfix_config()
    ngas_buildout()

