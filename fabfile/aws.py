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
Module containing AWS-related methods and tasks
"""

import os
import time

from Crypto.PublicKey import RSA
import boto.ec2
from fabric.colors import green, red, blue, yellow
from fabric.contrib.console import confirm
from fabric.decorators import task
from fabric.state import env
from fabric.tasks import execute
from fabric.utils import puts, abort, fastprint

from ngas import ngas_branch
from utils import default_if_empty, to_boolean, whatsmyip, check_ssh,\
    key_filename


from fabric.operations import prompt


# Don't re-export the tasks imported from other modules
__all__ = ['create_aws_instances', 'list_instances', 'terminate']

# Available known AMI IDs
AMI_IDs = {
           'Amazon':'ami-7c807d14',
           'Amazon-hvm': 'ami-60b6c60a',
           'CentOS': 'ami-8997afe0',
           'Old_CentOS':'ami-aecd60c7', 
           'SLES-SP2':'ami-e8084981',
           'SLES-SP3':'ami-c08fcba8'
           }

# Instance creation defaults
AMI_NAME = 'Amazon'
INSTANCE_NAME = 'NGAS_{0}' # gets formatted with the git branch name
INSTANCE_TYPE = 't1.micro'
AWS_KEY_NAME = 'icrar_ngas'
AWS_SEC_GROUP = 'NGAS' # Security group allows SSH and other ports
USE_ELASTIC_IP = False

# Connection defaults
AWS_PROFILE = 'NGAS'
AWS_REGION = 'us-east-1'


def connect():
    default_if_empty(env, 'AWS_PROFILE', AWS_PROFILE)
    default_if_empty(env, 'AWS_REGION',  AWS_REGION)
    return boto.ec2.connect_to_region(env.AWS_REGION, profile_name=env.AWS_PROFILE)

def userAtHost():
    return os.environ['USER'] + '@' + whatsmyip()

def aws_create_key_pair(conn):

    key_name = env.AWS_KEY_NAME
    key_file = key_filename(key_name)

    # key does not exist on AWS, create it there and bring it back,
    # overwriting anything we have
    kp = conn.get_key_pair(key_name)
    if not kp:
        kp = conn.create_key_pair(key_name)
        if os.path.exists(key_file):
            os.unlink(key_file)

    # We don't have the private key locally, save it
    if not os.path.exists(key_file):
        kp.save('~/.ssh/')
        Rkey = RSA.importKey(kp.material)
        env.SSH_PUBLIC_KEY = Rkey.exportKey('OpenSSH')


def check_create_aws_sec_group(conn):
    """
    Check whether default security group exists
    """
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


def create_instance(conn, n_instances, sgid):
    """
    Create one or more EC2 instances
    """

    default_if_empty(env, 'AMI_NAME',       AMI_NAME)
    default_if_empty(env, 'instance_type',  INSTANCE_TYPE)
    default_if_empty(env, 'use_elastic_ip', USE_ELASTIC_IP)

    if n_instances > 1:
        names = ["%s_%d" % (env.instance_name, i) for i in xrange(n_instances)]
    else:
        names = [env.instance_name]
    puts('Creating instances {0}'.format(names))

    use_elastic_ip = to_boolean(env.use_elastic_ip)
    if use_elastic_ip:
        if 'public_ip' in env:
            public_ip = env.public_ip
        else:
            public_ip = prompt('What is the public IP address: ', 'public_ip')
    else:
        public_ips = [None] * n_instances

    if use_elastic_ip:
        # Disassociate the public IP
        for public_ip in public_ips:
            if not conn.disassociate_address(public_ip=public_ip):
                abort('Could not disassociate the IP {0}'.format(public_ip))

    reservations = conn.run_instances(AMI_IDs[env.AMI_NAME], instance_type=env.instance_type, \
                                    key_name=env.AWS_KEY_NAME, security_group_ids=[sgid],\
                                    min_count=n_instances, max_count=n_instances)
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
    while sum(running) != n_instances:
        fastprint('.')
        time.sleep(5)
        stat = conn.get_all_instance_status(iid)
        running = [x.state_name=='running' for x in stat]
    puts('.') #enforce the line-end

    # Local user and host
    userAThost = userAtHost()

    # Tag the instance
    for i in range(n_instances):
        conn.create_tags([instances[i].id], {'Name': names[i],
                                             'Created By':userAThost,
                                             })

    # Associate the IP if needed
    if use_elastic_ip:
        for i in range(n_instances):
            puts('Current DNS name is {0}. About to associate the Elastic IP'.format(instances[i].dns_name))
            if not conn.associate_address(instance_id=instances[i].id, public_ip=public_ips[i]):
                abort('Could not associate the IP {0} to the instance {1}'.format(public_ips[i], instances[i].id))

    # Load the new instance data as the dns_name may have changed
    host_names = []
    for i in range(n_instances):
        instances[i].update(True)
        print_instance(instances[i])
        host_names.append(str(instances[i].dns_name))
    return host_names


@task
def create_aws_instances(n_instances=1):
    """
    Create AWS instances and let Fabric point to them

    This method creates AWS instances and points the fabric environment to them with
    the current public IP and username.
    """

    branch = ngas_branch()
    default_if_empty(env, 'AWS_KEY_NAME',   AWS_KEY_NAME)
    default_if_empty(env, 'instance_name',  INSTANCE_NAME.format(branch))

    # Create the key pair and security group if necessary
    conn = connect()
    aws_create_key_pair(conn)
    sgid = check_create_aws_sec_group(conn)

    # Create the instance in AWS
    host_names = create_instance(conn, n_instances, sgid)

    # Update our fabric environment so from now on we connect to the
    # AWS machine using the correct user and SSH private key
    env.hosts = host_names
    env.key_filename = key_filename(env.AWS_KEY_NAME)
    if env.AMI_NAME in ['CentOS', 'SLES']:
        env.user = 'root'
    else:
        env.user = 'ec2-user'

    # Instances have started, but are not usable yet, make sure SSH has started
    puts('Started the instance(s) now waiting for the SSH daemon to start.')
    execute(check_ssh)

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
        puts("Terminate: fab aws.terminate:instance_id={0}".format(inst_id))
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
    if not instance_id:
        abort('No instance ID specified. Please provide one.')

    conn = connect()
    inst = conn.get_all_instances(instance_ids=[instance_id])
    inst = inst[0].instances[0]
    tagdict = inst.tags
    print_instance(inst)

    puts('')
    if tagdict.has_key('Created By') and tagdict['Created By'] != userAtHost():
        puts('******************************************************')
        puts('WARNING: This instances has not been created by you!!!')
        puts('******************************************************')
    if confirm("Do you really want to terminate this instance?"):
        puts('Teminating instance {0}'.format(instance_id))
        conn.terminate_instances(instance_ids=[instance_id])
    else:
        puts(red('Instance NOT terminated!'))
    return