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
Module with a few high-level fabric tasks users are likely to use
"""

from fabric.decorators import task
from fabric.tasks import execute

from aws import create_aws_instances
from ngas import install_and_check
from dockerContainer import create_stage1_docker_container, create_stage2_docker_image, create_final_docker_image


# Don't re-export the tasks imported from other modules, only ours
__all__ = ['user_deploy', 'operations_deploy', 'aws_deploy', 'docker_image']

@task
def user_deploy(typ = 'archive'):
    """
    Deploy the system as a normal user without sudo access
    """
    install_and_check(sys_install=False, user_install=False, init_install=False, typ=typ)

@task
def operations_deploy(typ = 'archive'):
    """
    Deploy the full NGAS operational environment.
    """
    install_and_check(sys_install=True, user_install=True, init_install=True, typ=typ)

@task
def aws_deploy(n_instances=1, typ='archive'):
    """
    Deploy NGAS into a fresh EC2 instance.
    """
    create_aws_instances(n_instances)
    execute(install_and_check, sys_install=True, user_install=True, init_install=True, typ=typ)

@task
def docker_image(typ='archive'):
    """
    Create a Docker image running NGAS.
    """
    # Build and start the stage1 container holding onto the container info to use later.
    dockerState = create_stage1_docker_container()
    if not dockerState:
        return

    # Now install into the docker container.
    # We assume above has set the environment host IP address to install into
    execute(install_and_check, sys_install=True, user_install=True, init_install=True, typ=typ)

    # Now that NGAS is istalled in container do cleanup on it and build final image.
    if not create_stage2_docker_image(dockerState):
        return

    # Now build the final NGAS docker image
    if not create_final_docker_image(dockerState):
        # This is not really needed by included in case code is added below this point
        return