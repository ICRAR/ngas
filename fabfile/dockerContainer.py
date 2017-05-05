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

import json
import os
import shutil
from types import GeneratorType

from fabric.colors import red, green
from fabric.context_managers import settings
from fabric.decorators import task
from fabric.state import env
from fabric.tasks import execute
from fabric.utils import puts

from ngas import ngas_root_dir, ngas_user
from utils import default_if_empty, check_ssh, get_public_key, sudo


# Don't re-export the tasks imported from other modules
__all__ = ['create_stage1_docker_container', 'create_stage2_docker_image', 'create_final_docker_image']

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

def keep_ngas_root():
    key = 'DOCKER_KEEP_NGAS_ROOT'
    return key in env

class DockerContainerState(object):

    def __init__(self, client, container=None):
        self.client = client
        self.container = container

    @property
    def client(self):
        return self._client

    @client.setter
    def client(self, client):
        self._client = client

    @property
    def container(self):
        return self._container

    @container.setter
    def container(self, container):
        self._container = container


def split_json(the_json):
    puts(the_json)
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
                puts(json.dumps(converted,
                                 sort_keys=True,
                                 indent=4,
                                 separators=(',', ': ')))
                print_string = ''
        elif bracket_count >= 1:
            print_string += ch


def handle_generator(the_generator):
    for value in the_generator:
        json_pretty_print(value)


def json_pretty_print(the_json):
    # Do pretty print of "the_json" taking into account different types.
    if type(the_json) == unicode or type(the_json) == str:
        split_json(the_json)
    elif type(the_json) == GeneratorType:
        handle_generator(the_json)
    else:
        puts(json.dumps(the_json,
                         sort_keys=True,
                         indent=4,
                         separators=(',', ': ')))


def check_if_successful_build(the_json):
    return check_if_successful(the_json, JSON_STREAM, BUILD_SUCCESSFUL_STR)

def check_if_successful_commit(the_json):
    if the_json is None or the_json[JSON_ID] is None:
        return False

    return True


def check_if_successful_create_container(the_json):
    success = check_if_successful(the_json, JSON_WARNINGS, CREATE_CONTAINER_SUCCESSFUL_STR)
    if success:
        # Get the Id of the container
        for value in the_json:
            puts('searching for field: ' + JSON_ID + ' in:' + value)
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
        puts('searching for field: ' + json_field + ' and value: ' + json_value + ' in:' + value)
        dct = json.loads(value)
        if json_field in dct:
            my_str = dct[json_field]
            position = my_str.find(json_value)
            if position >= 0:
                success = True
                break

    return success


def docker_public_add_ssh_key():
    # Copy the public key of our SSH key if we're using one
    public_key_installed = False
    for key_filename in [env.key_filename, os.path.expanduser("~/.ssh/id_rsa")]:
        if key_filename is not None:
            public_key = get_public_key(key_filename)
            if public_key:
                shutil.copyfile('{0}.pub'.format(key_filename), "fabfile/authorized_keys")
                public_key_installed = True
                puts(green("\n******** INSTALLED PUBLIC KEY INTO CONTAINER ********\n"))
                break

    if not public_key_installed:
        puts(red("\n******** FAILED TO INSTALL PUBLIC KEY INTO CONTAINER ********\n"))

    # Generate our own key pair???
        #key = RSA.generate(2048)
        #with open(private_key_file, 'w') as content_file:
        #    chmod(private_key_file, 0600)
        #    content_file.write(key.exportKey('PEM'))
        #    pubkey = key.publickey()
        #with open(public_key_file, 'w') as content_file:
        #    content_file.write(pubkey.exportKey('OpenSSH'))


@task
def create_stage1_docker_container():
    """
    Create an inital Docker container and let Fabric point at it

    This method creates a Docker container and points the fabric environment to it with
    the container IP.

    If using Docker in a virtual machine such as on a Mac (setup with say docker-machine),
    the code will look for three environemnt variables
        DOCKER_TLS_VERIFY
        DOCKER_HOST
        DOCKER_CERT_PATH
    using these to connect to the remote daemon. Extra steps are then taken during setup to
    then connect to the correct exposed port on the docker daemon host rather than trying to connect
    to the docker container we create as we likely don't have a route to the IP address used.
    """

    from docker.client import DockerClient
    from docker import errors
    from docker import tls

    default_if_empty(env, 'container_name', STAGE1_BUILD_NAME)

    # Copy our current public SSH key into the to-be-built container so we
    # can connect to the root user afterwards
    docker_public_add_ssh_key()

    # Get the following environment in case Docker daemon is not local!
    base_url = os.getenv('DOCKER_HOST')
    tls_verify = os.getenv('DOCKER_TLS_VERIFY')
    cert_path = os.getenv('DOCKER_CERT_PATH')

    if base_url is None:
        base_url = 'unix://var/run/docker.sock'
    else:
        base_url = base_url.replace('tcp','https')

    if tls_verify is not None and cert_path is not None:
        # client_cert is a tuple of client cert and key. We don't use TLS as causes issues when accessing
        # server using IP address
        tls_config = tls.TLSConfig(client_cert=(os.path.join(cert_path, 'cert.pem'),os.path.join(cert_path, 'key.pem')),
                                   verify=False)
    else:
        tls_config = False

    puts(green('URL is ' + base_url + ' for connection to docker daemon'))

    # The main connection for testing the deployment and use of our own docker registry.
    cli = DockerClient(base_url=base_url, timeout=10, tls=tls_config, version='auto')
    if cli is None:
        puts(red("\n******** FAILED TO INSTALL CREATE CONNECTION TO DOCKER DAEMON ********\n"))
        return None
    cli = cli.api

    # Build the stage1 docker container to deploy NGAS into
    print 'do build'
    successful = check_if_successful_build(cli.build(
        path=BUILD_ROOT, tag=STAGE1_BUILD_NAME, rm=True, pull=True, dockerfile=DOCKERFILE_STAGE1))

    if not successful:
        puts(red("\n******** FAILED TO BUILD STAGE1 DOCKER IMAGE ********\n"))
        return None

    hconfig_args = {'publish_all_ports': True}
    if base_url is not None:
        hconfig_args['port_bindings'] = {7777: ('0.0.0.0', 7777)}
    host_config = cli.create_host_config(**hconfig_args)
    container = cli.create_container(image=STAGE1_BUILD_NAME, detach=True, name="ngas", host_config=host_config)

    if not check_if_successful_create_container(container):
        puts(red("\n******** FAILED TO CREATE STAGE1 DOCKER CONTAINER FROM IMAGE ********\n"))
        # Cleanup
        cli.remove_image(STAGE1_BUILD_NAME, force=True)
        return None

    try:
        cli.start(container)
    except errors.APIError as e:
        print e.explanation
        print e.message
        puts(red("\n******** FAILED TO START STAGE1 DOCKER CONTAINER FROM IMAGE ********\n"))
        # Cleanup
        cli.remove_container(container)
        cli.remove_image(STAGE1_BUILD_NAME, force=True)
        return None

    info = cli.inspect_container(container)

    if tls_config:
        port = info['NetworkSettings']['Ports']['22/tcp'][0]['HostPort']
        ip_address = base_url.replace('https://','').split(':')[0]
        host_ip = ip_address + ':' + port
    else:
        host_ip = info['NetworkSettings']['IPAddress']

    puts('ssh connection IP address will be {0}'.format(host_ip))
    if host_ip is None:
        puts(red("\n******** FAILED TO GET IP ADDRESS OF CONTAINER ********\n"))
        # Cleanup
        cli.remove_container(container)
        cli.remove_image(STAGE1_BUILD_NAME, force=True)
        return None

    # From now on we connect to root@host_ip using our SSH key
    env.hosts = host_ip
    env.user = 'root'
    if 'key_filename' not in env:
        env.key_filename = os.path.expanduser("~/.ssh/id_rsa")

    # Instances have started, but are not usable yet, make sure SSH has started
    puts(green('\nStarted the docker container now waiting for the SSH daemon to start.\n'))

    execute(check_ssh)

    # This is needed in the follow_stage1 function following installation
    state = DockerContainerState(client=cli, container=container)
    return state

@task
def cleanup_stage1():
    """
    Perform any post installation cleanup so that they don't become part of the Docker image.

    This method simply runs any command on the container to cleanup anything that can be cleaned up
    before creating an image from the container. It is a task so that the environment is picked up.
    """

    # Remove all packages we installed
    sudo('yum clean all')

    # Do not ship NGAS with a working NGAS directory
    if not keep_ngas_root():
        with settings(user=ngas_user()):
            run("rm -rf %s" % (ngas_root_dir()))

@task
def create_stage2_docker_image(state):
    """
    Create stage2 image from stage1 container

    This method creates the stage2 docker images from the running stage1 docker container.
    The methd then stops and removes the stage1 containe plus the stage1 image.
    """

    from docker import errors

    if type(state) is not DockerContainerState:
        return False

    create_successful = True
    puts(green("\n******** NOW BUILD THE STAGE2 DOCKER IMAGE ********\n"))

    # First need to cleanup container before we stop and commit it.
    execute(cleanup_stage1)

    try:
        puts('\ndo stop of stage1 container')
        try:
            json_pretty_print(state.client.stop(state.container))
        except errors.APIError as e:
            print e.explanation
            print e.message
            puts(red("\n******** FAILED TO STOP STAGE1 CONTAINER ********\n"))
            return False

        puts(green('\nStopped stage1 docker container.\n'))

        puts('\ndo commit of container to stage2 image')
        result = state.client.commit(state.container, repository=STAGE2_REPO_NAME, tag=BUILD_TAG)
        json_pretty_print(result)
        if not check_if_successful_commit(result):
            puts(red("\n******** FAILED TO COMMIT STAGE1 CONTAINER TO STAGE2 IMAGE ********\n"))
            create_successful = False
            # We keep going from here as next steps are cleanup, return failure at end!
        else:
            puts(green('\nCompleted commit of stage2 image from stage1 container.\n'))

        puts('\ndo remove of stage1 container')
        try:
            json_pretty_print(state.client.remove_container(state.container))
            puts(green('\nRemoved stage1 docker container.\n'))
        except errors.APIError as e:
            print e.explanation
            print e.message
            puts(red("\n******** FAILED TO REMOVE STAGE1 CONTAINER ********\n"))

        state.container = None

        puts("\ndo remove of stage1 docker image")
        try:
            json_pretty_print(state.client.remove_image(STAGE1_BUILD_NAME, force=True))
            puts(green('\nRemoved stage1 docker image.\n'))
        except errors.APIError as e:
            print e.explanation
            print e.message
            puts(red("\n******** FAILED TO REMOVE STAGE1 IMAGE ********\n"))
    except Exception as e:
        puts(red('\n' + e.message + '\n'))
        create_successful = False
    finally:
        return create_successful


@task
def create_final_docker_image(state):
    """
    Create final image from stage2 image

    This method creates the final docker images from the stage2 docker image.
    The methd then removes the stage2 image.
    """

    from docker import errors

    if type(state) is not DockerContainerState:
        return False

    try:
        puts('\ndo build of final image')
        successful = check_if_successful_build(state.client.build(
            path=BUILD_ROOT, tag=FINAL_BUILD_NAME, rm=True, pull=False, dockerfile=DOCKERFILE_FINAL))

        if not successful:
            puts(red("\n******** FAILED TO BUILD FINAL DOCKER IMAGE ********\n"))
            return False

        puts("\ndo remove of stage2 docker image")
        try:
            json_pretty_print(state.client.remove_image(STAGE2_BUILD_NAME, force=True))
            puts(green('\nRemoved stage2 docker image.\n'))
        except errors.APIError as e:
            print e.explanation
            print e.message
            puts(red("\n******** FAILED TO REMOVE STAGE2 IMAGE ********\n"))

        return True

    except Exception as e:
        puts(red('\n' + e.message + '\n'))
        return False
    finally:
        state.client.close()
        state.client = None