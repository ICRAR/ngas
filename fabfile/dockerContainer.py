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
Module containing Docker related methods and tasks
"""

import os
from os import chmod
from Crypto.PublicKey import RSA

import shutil

from types import GeneratorType
import json

from fabric.colors import red
from fabric.contrib.console import confirm
from fabric.decorators import task
from fabric.state import env
from fabric.tasks import execute
from fabric.utils import puts, abort

from ngas import ngas_branch
from utils import default_if_empty, check_ssh, get_public_key


from fabric.operations import prompt


# Don't re-export the tasks imported from other modules
__all__ = ['create_docker_image', 'follow_stage1']

DOCKER_SSH_KEY_NAME = 'icrar_ngas_docker'

API_VERSION = '1.20'
CWD = os.getcwd()
BUILD_ROOT = os.path.join(CWD, 'fabfile')
BUILD_TAG = 'latest'
DOCKERFILE_STAGE1 = 'Dockerfile-stage1'
DOCKERFILE_FINAL = 'Dockerfile-final'

STAGE1_BUILD_NAME = 'ngas-stage1:latest'
STAGE2_REPO_NAME = 'ngas-stage2'
STAGE2_BUILD_NAME = 'ngas-stage2:latest'
FINAL_BUILD_NAME = 'ngas:latest'

JSON_STREAM = 'stream'
JSON_WARNINGS = 'Warnings'
JSON_ID = 'Id'
BUILD_SUCCESSFUL_STR = 'Successfully built'
CREATE_CONTAINER_SUCCESSFUL_STR = 'None'


def split_json(the_json):
    print the_json
    bracket_count = 0
    print_string = ''
    for ch in the_json:
        if ch == '{':
            bracket_count += 1
            print_string += ch
        elif ch == '}':
            bracket_count -= 1
            print_string += ch
            if bracket_count == 0:
                converted = json.loads(print_string)
                print json.dumps(converted,
                                 sort_keys=True,
                                 indent=4,
                                 separators=(',', ': '))
                print_string = ''
        elif bracket_count >= 1:
            print_string += ch


def handle_generator(the_generator):
    for value in the_generator:
        json_pretty_print(value)


def json_pretty_print(the_json):
    # Do pretty print of "the_json" taking into account different types.
    print(the_json.__class__)
    if type(the_json) == unicode or type(the_json) == str:
        split_json(the_json)
    elif type(the_json) == GeneratorType:
        handle_generator(the_json)
    else:
        print json.dumps(the_json,
                         sort_keys=True,
                         indent=4,
                         separators=(',', ': '))


def check_if_successful_build(the_json):
    return check_if_successful(the_json, JSON_STREAM, BUILD_SUCCESSFUL_STR)

def check_if_successful_create_container(the_json):
    success = check_if_successful(the_json, JSON_WARNINGS, CREATE_CONTAINER_SUCCESSFUL_STR)
    if success:
        # Get the Id of the container
        for value in the_json:
            print 'searching for field: ' + JSON_ID + ' in:' + value
            dct = json.loads(value)
            if JSON_ID in dct:
                container_id = dct[JSON_ID]
                return True, container_id

    return False, None


def check_if_successful(the_json, json_field, json_value):
    # Use the given "the_json" GeneratorType to find the required field and value.
    if type(the_json) != GeneratorType:
        return False

    # Default success to False in case we skip over generator.
    success = False
    for value in the_json:
        print 'searching for field: ' + json_field + ' and value: ' + json_value + ' in:' + value
        dct = json.loads(value)
        if json_field in dct:
            my_str = dct[json_field]
            position = my_str.find(json_value)
            if position >= 0:
                success = True
                break

    return success


def docker_add_ssh_key():
    # Copy the public key of our SSH key if we're using one
    for key_filename in [env.key_filename, os.path.expanduser("~/.ssh/id_rsa")]:
        if key_filename is not None:
            public_key = get_public_key(key_filename)
            if public_key:
                shutil.copyfile('{0}.pub'.format(key_filename), "fabfile/authorized_keys")
                break

    # Generate our own key pair???
        #key = RSA.generate(2048)
        #with open(private_key_file, 'w') as content_file:
        #    chmod(private_key_file, 0600)
        #    content_file.write(key.exportKey('PEM'))
        #    pubkey = key.publickey()
        #with open(public_key_file, 'w') as content_file:
        #    content_file.write(pubkey.exportKey('OpenSSH'))


@task
def create_docker_image():
    """
    Create an inital Docker containe and let Fabric point at it

    This method creates a Docker container and points the fabric environment to it with
    the current public IP.
    """

    from docker.client import AutoVersionClient

    branch = ngas_branch()
    default_if_empty(env, 'container_name',  STAGE1_BUILD_NAME.format(branch))

    # Create the key pair and security group if necessary
    #conn = connect()
    #aws_create_key_pair(conn)
    #sgid = check_create_aws_sec_group(conn)

    # Create the instance in AWS
    #host_names = create_instances(conn, n_instances, sgid)

    docker_add_ssh_key()


    # The main for testing the deployment and use of our own docker registry.
    cli = AutoVersionClient(base_url='unix://var/run/docker.sock', timeout=10)

    # Build the stage1 docker container to deploy NGAS into
    print 'do build'
    successful = check_if_successful_build(cli.build(
        path=BUILD_ROOT, tag=STAGE1_BUILD_NAME, rm=True, pull=True, dockerfile=DOCKERFILE_STAGE1))
    print successful

    container = cli.create_container(image=STAGE1_BUILD_NAME, detach=True, name="ngas")

    (successful, container_id) = check_if_successful_create_container(container)

    cli.start(container=container, publish_all_ports=True)

    info = cli.inspect_container(container)

    host_ip = info['NetworkSettings']['IPAddress']
    env.hosts = host_ip

    #print 'do remove of image from local docker cache'
    #try:
    #    json_pretty_print(cli.remove_image(image=STAGE1_BUILD_NAME))
    #except errors.APIError as e:
    #    print e.explanation
    #    print e.message
    #    return



    # Update our fabric environment so from now on we connect to the
    # AWS machine using the correct user and SSH private key
    #env.hosts = host_names
    #env.key_filename = key_filename(env.AWS_KEY_NAME)
    #if env.AMI_NAME in ['CentOS', 'SLES']:
    #    env.user = 'root'
    #else:
    #    env.user = 'ec2-user'

    # Instances have started, but are not usable yet, make sure SSH has started
    #puts('Started the instance(s) now waiting for the SSH daemon to start.')
    execute(check_ssh)
    return container

@task
def follow_stage1(container):
    from docker.client import AutoVersionClient
    cli = AutoVersionClient(base_url='unix://var/run/docker.sock', timeout=10)
    print 'do stop'
    json_pretty_print(cli.stop(container))
    print 'do commit'
    json_pretty_print(cli.commit(container, repository=STAGE2_REPO_NAME, tag=BUILD_TAG))
    print 'do build'
    json_pretty_print(cli.build(path=BUILD_ROOT, tag=FINAL_BUILD_NAME, rm=True, pull=False, dockerfile=DOCKERFILE_FINAL))
    print 'do remove'
    json_pretty_print(cli.remove_container(container))
