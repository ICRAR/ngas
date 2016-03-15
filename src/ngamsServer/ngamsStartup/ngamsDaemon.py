#!/usr/bin/env python
#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2012
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

import errno
import os
import signal
import subprocess
import sys
import time

import daemon
from lockfile.pidlockfile import PIDLockFile

from ngamsLib import ngamsConfig
from ngamsLib.ngamsCore import get_contact_ip
from ngamsServer import ngamsServer


def err(s):
    sys.stderr.write(s + '\n')

def start(cmdline_name, cfgFile, pidfile):

    # Build up a fake command-line before launching the NGAS server
    # This is because deep inside the NGAS code directly reads sometimes the
    # sys.argv variable (big TODO: remove that hidden dependency)
    srv = 'ngamsServer'
    if cmdline_name == 'ngamsCacheDaemon': #check how we are called
        srv = 'ngamsCacheServer'
    sys.argv = [srv, '-cfg', cfgFile, '-force', '-autoOnline', '-multiplesrvs', '-v', '3']

    # Go!
    if os.path.isfile(pidfile):
        err('PID file already exists, not overwriting possibly existing instance')
        return 1

    print 'Starting: %s' % (' '.join(sys.argv),)
    with daemon.DaemonContext(pidfile=PIDLockFile(pidfile, timeout=1)):
        ngamsServer.ngamsServer().init(sys.argv)

    return 0

def stop(pidfile):
    pid = PIDLockFile(pidfile).read_pid()
    if pid is None:
        err('Cannot read PID file, is there an instance running?')
        return 1
    else:
        try:
            return kill_and_wait(pid, pidfile)
        except OSError, e:
            if e.errno == errno.ESRCH:
                err('PID file points to non-existing process, removing it')
                os.unlink(pidfile)
                return 1

def kill_and_wait(pid, pidfile):

    os.kill(pid, signal.SIGTERM)

    tries = 0
    max_tries = 20
    sleep_period = 1

    start = time.time()
    while tries < max_tries:
        if not os.path.exists(pidfile):
            break
        tries += 1
        time.sleep(sleep_period)
        sys.stdout.write('.')
        sys.stdout.flush()
    sys.stdout.write('\n')
    sys.stdout.flush()
    end = time.time()

    if tries == max_tries:
        err("Process didn't die after %.2f [s], killing it with -9" % (end-start))
        os.kill(pid, signal.SIGKILL)
        return 1

    return 0

def status(configFile):
    """
    Send a STATUS command to server
    """

    cfgObj = ngamsConfig.ngamsConfig()
    cfgObj.load(configFile)
    ipAddress = get_contact_ip(cfgObj)
    port = cfgObj.getPortNo()

    # TODO: This creates a dependency on ngamsPClient
    SCMD = "ngamsPClient -host {0} -port {1} -cmd STATUS -v 1 -timeout 1".format(ipAddress, port)
    return subprocess.call(SCMD,shell=True)

def main(argv=sys.argv):
    """
    Entry point function. It's mapped to two different scripts, which is why
    we can distinguish here between them and start different processes
    """

    # Check the NGAS_PREFIX environment variable, which, if present, points to
    # our NGAS installation
    if os.environ.has_key('NGAS_PREFIX'):
        NGAS_PREFIX = os.path.abspath(os.environ['NGAS_PREFIX'])
    else:
        HOME = os.environ['HOME']
        NGAS_PREFIX = '{0}/NGAS'.format(HOME)
    if not NGAS_PREFIX or not os.path.exists(NGAS_PREFIX):
        raise Exception("NGAS_PREFIX not found or not defined")

    # The default configuration file
    configFile = os.path.join(NGAS_PREFIX, 'cfg', 'ngamsServer.conf')
    if not os.path.exists(configFile):
        msg = "Configuration file not found: {0}".format(configFile)
        raise ValueError(msg)

    # The daemon PID file, prepare its playground
    pidfile = os.path.join(NGAS_PREFIX, 'var', 'run', 'ngamsDaemon.pid')
    try:
        os.makedirs(os.path.join(NGAS_PREFIX, 'var', 'run'))
        os.makedirs(os.path.join(NGAS_PREFIX, 'var', 'log'))
    except OSError:
        pass

    name = argv[0]
    if len(argv) < 2:
        print "usage: %s start|stop|restart|status" % (name,)
        sys.exit(2)
    cmd = argv[1]

    # Main command switch
    if 'start' == cmd:
        exitCode = start(name, configFile, pidfile)
    elif 'stop' == cmd:
        exitCode = stop(pidfile)
    elif 'restart' == cmd:
        stop(pidfile)
        exitCode = start(name, configFile, pidfile)
    elif 'status' == cmd:
        exitCode = status(configFile)
    else:
        print "Unknown command: %s" % (cmd,)
        print "usage: %s start|stop|restart|status" % (name,)
        exitCode = 1

    sys.exit(exitCode)