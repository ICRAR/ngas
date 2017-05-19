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

import collections
import os
import shutil
import tempfile

from fabric.colors import blue
from fabric.context_managers import settings
from fabric.decorators import task
from fabric.state import env
from fabric.tasks import execute
from fabric.utils import puts

from ngas import ngas_root_dir, ngas_user
from system import get_fab_public_key
from utils import check_ssh, sudo, generate_key_pair, run, success, failure,\
    default_if_empty


# Don't re-export the tasks imported from other modules
__all__ = []


DockerContainerState = collections.namedtuple('DockerContainerState', 'client container stage1_image')

def docker_keep_ngas_root():
    key = 'DOCKER_KEEP_NGAS_ROOT'
    return key in env

def docker_image_repository():
    default_if_empty(env, 'DOCKER_IMAGE_REPOSITORY', 'icrar/ngas')
    return env.DOCKER_IMAGE_REPOSITORY

def docker_public_add_ssh_key(build_dir):

    # Generate a private/public key pair if there's not one already in use
    public_key = get_fab_public_key()
    if not public_key:
        private, public_key = generate_key_pair()
        env.key = private

    with open('%s/authorized_keys' % (build_dir,), 'wb') as f:
        f.write(public_key)

def create_stage1_container():
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

    stage1_tag = 'ngas-stage1:latest'
    container_name = 'ngas_installation_target'
    cli = DockerClient.from_env(version='auto', timeout=10)

    # Build the stage1 image which contains only an SSH server and sudo
    # We use a temporary build dir containing only the things we need there
    puts(blue("Building stage1 image"))
    build_dir = tempfile.mkdtemp()
    try:

        dockerfile = os.path.join(os.path.dirname(__file__), 'Dockerfile-stage1')
        shutil.copy(dockerfile, os.path.join(build_dir, 'Dockerfile'))
        docker_public_add_ssh_key(build_dir)

        # Build the stage1 docker container to deploy NGAS into
        stage1_image = cli.images.build(path=build_dir, tag=stage1_tag, rm=True, pull=True)

        success("Built image %s" % (stage1_tag,))
    finally:
        shutil.rmtree(build_dir)

    # Create and start a container using the newly created stage1 image
    puts(blue("Starting new container from stage1 image"))
    container = None
    try:
        container = cli.containers.run(image=stage1_image, remove=False,
                                       detach=True, name=container_name)
        success("Started container %s" % (container_name,))
    except:
        if container is not None:
            container.remove()
        cli.images.remove(stage1_image.id)
        raise

    # From now on we connect to root@host_ip using our SSH key
    try:
        host_ip = cli.api.inspect_container(container.id)['NetworkSettings']['IPAddress']
    except:
        failure("Cannot get container's IP address")
        container.stop()
        container.remove()
        cli.images.remove(stage1_image.id)
        raise

    env.hosts = host_ip
    env.user = 'root'
    if 'key_filename' not in env and 'key' not in env:
        env.key_filename = os.path.expanduser("~/.ssh/id_rsa")

    # Make sure we can connect via SSH to the newly started container
    execute(check_ssh)

    return DockerContainerState(cli, container, stage1_image)

@task
def cleanup_stage1():
    # Remove all packages we installed
    sudo('yum clean all')

    # Do not ship NGAS with a working NGAS directory
    if not docker_keep_ngas_root():
        with settings(user=ngas_user()):
            run("rm -rf %s" % (ngas_root_dir()))

def create_final_image(state):
    """
    Create stage2 image from stage1 container

    This method creates the stage2 docker images from the running stage1 docker container.
    The methd then stops and removes the stage1 containe plus the stage1 image.
    """

    puts(blue("Building final image"))

    # First need to cleanup container before we stop and commit it.
    execute(cleanup_stage1)

    conf = {'Cmd': ["/usr/bin/su", "-", "ngas", "-c", "/home/ngas/ngas_rt/bin/ngamsServer -cfg /home/ngas/NGAS/cfg/ngamsServer.conf -autoOnline -force -multiplesrvs -v 4"]}
    cont = state.container
    image_repo = docker_image_repository()

    try:
        cont.stop()
        cont.commit(repository=image_repo, tag='latest', conf=conf)
        success("Created Docker image %s:latest" % (image_repo,))
    except Exception as e:
        failure("Failed to build final image: %s" % (str(e)))
        raise
    finally:
        # Cleanup the docker environment from all our temporary stuff
        cont.remove()