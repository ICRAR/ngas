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
Various utilities used throughout the rest of the modules
"""
import os
import socket
import time
import types
import urllib2

from Crypto.PublicKey import RSA

from fabric.colors import green, red
from fabric.context_managers import settings, hide
from fabric.decorators import task, parallel
from fabric.exceptions import NetworkError
from fabric.operations import run as frun, sudo as fsudo
from fabric.state import env
from fabric.utils import puts


def to_boolean(choice, default=False):
    """Convert the yes/no to true/false

    :param choice: the text string input
    :type choice: string
    """
    if type(choice) == types.BooleanType:
        return choice
    valid = {"True":  True,  "true": True, "yes":True, "ye":True, "y":True,
             "False":False, "false":False,  "no":False, "n":False}
    choice_lower = choice.lower()
    if choice_lower in valid:
        return valid[choice_lower]
    return default

def default_if_empty(env, key, default):
    if key not in env or not env[key]:
        if hasattr(default, '__call__'):
            env[key] = default()
        else:
            env[key] = default

@task
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

def home():
    return run('echo $HOME')

@task
@parallel
def check_ssh():
    """
    Check availability of SSH
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

def is_localhost():
    return env.host == 'localhost' or \
           env.host.startswith("127.0.") or \
           env.host == socket.gethostname()

def key_filename(key_name):
    return os.path.join(os.path.expanduser('~/.ssh/'), '{0}.pem'.format(key_name))

def get_public_key(key_filename):
    with open(key_filename) as f:
        okey = RSA.importKey(f.read())
        return okey.exportKey('OpenSSH')

def repo_root():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))