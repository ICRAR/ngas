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

from fabric.colors import green
from fabric.decorators import task
from fabric.state import env
from fabric.tasks import execute
from fabric.utils import puts

from aws import create_aws_instances
from ngas import install, install_and_check


# Don't re-export the tasks imported from other modules, only ours
__all__ = ['user_deploy', 'operations_deploy', 'aws_deploy']

# The rest of the imports are all done within the method bodies so we don't
# pollute this module's global namespace with other modules' tasks (which makes
# "fab -l" list tasks more than once.
@task
def user_deploy(typ = 'archive'):
    """
    Deploy the system as a normal user without sudo access
    """
    install_and_check(sys_install=False, user_install=False, init_install=False, typ=typ)

@task
def operations_deploy(sys_install = True, user_install = True, typ = 'archive'):
    """
    Deploy the full NGAS operational environment.
    """
    install(sys_install = sys_install, user_install = user_install,
            init_install = True, typ = typ)

@task
def aws_deploy():
    """
    Deploy NGAS into a fresh EC2 instance.
    """
    create_aws_instances()
    execute(install_and_check, sys_install=True, user_install=True, init_install=True, typ='archive')
    puts(green("******** AWS deployment COMPLETED on AWS hosts: {0} ********".format(env.hosts)))