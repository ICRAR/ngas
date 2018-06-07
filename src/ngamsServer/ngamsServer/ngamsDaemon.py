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
import sys
import time

import daemon
import lockfile.pidlockfile

from ngamsLib import ngamsConfig, ngamsHighLevelLib
from ngamsLib.ngamsCore import get_contact_ip
from . import ngamsServer


def err(s):
    sys.stderr.write(s + '\n')

def start(args, cfg, pidfile):

    # Ensure the directory is there to keep the PID file
    try:
        os.makedirs(os.path.join(cfg.getRootDirectory(), 'var', 'run'))
    except OSError:
        pass

    if os.path.isfile(pidfile):
        pid = lockfile.pidlockfile.read_pid_from_pidfile(pidfile)
        err('PID file %s already exists (pid=%d), not overwriting possibly existing instance' % (pidfile, pid,))
        return 1

    # Go, go, go!
    with daemon.DaemonContext(pidfile=lockfile.pidlockfile.PIDLockFile(pidfile, timeout=1)):
        ngamsServer.main(args=args[1:], prog='ngamsDaemon')

    return 0

def stop(pidfile):
    pid = lockfile.pidlockfile.read_pid_from_pidfile(pidfile)
    if pid is None:
        err('Cannot read PID file, is there an instance running?')
        return 1
    else:
        try:
            return kill_and_wait(pid, pidfile)
        except OSError as e:
            if e.errno == errno.ESRCH:
                err('PID file points to non-existing process, removing it')
                os.unlink(pidfile)
                return 1

def kill_and_wait(pid, pidfile):

    # The NGAS server should nicely shut down itself after receiving this signal
    os.kill(pid, signal.SIGTERM)

    # We previously checked that the pidfile was gone, but this is not enough
    # In some cases the main thread finished correctly, but there are other
    # threads (e.g., HTTP servicing threads) that are still running,
    # and thus the PID file disappears but the process is still ongoing
    tries = 0
    max_tries = 20
    sleep_period = 1
    start = time.time()
    while tries < max_tries:

        # SIGCONT can be sent many times without fear of side effects
        # If we get ESRCH then the process has finished
        try:
            os.kill(pid, signal.SIGCONT)
        except OSError as e:
            if e.errno == errno.ESRCH:
                # Bingo! the process is gone
                break

        tries += 1
        time.sleep(sleep_period)
        sys.stdout.write('.')
        sys.stdout.flush()

    sys.stdout.write('\n')
    sys.stdout.flush()
    end = time.time()

    ret = 0x00

    # We waited a lot but the process is still there, kill it with 9
    if tries == max_tries:
        err("Process didn't die after %.2f [s], killing it with -9" % (end-start))
        os.kill(pid, signal.SIGKILL)
        ret += 0x01

    # The process should have removed its own pidfile...
    if os.path.exists(pidfile):
        err("Removing PID file manually, the daemon process didn't remove it by itself")
        os.unlink(pidfile)
        ret += 0x02

    return ret

def status(cfg):
    """
    Check if the server is up and running
    """

    ipAddress = get_contact_ip(cfg)
    port = cfg.getPortNo()

    try:
        ngamsHighLevelLib.pingServer(ipAddress, port, 10)
        return 0
    except:
        return 1

def print_usage(name):
    err("usage: %s [start|stop|restart|status] <ngamsServer options>" % (name,))

def main(args=sys.argv):
    """
    Entry point function. It's mapped to two different scripts, which is why
    we can distinguish here between them and start different processes
    """

    # A minimum of 4 because we require at least -cfg <file>
    name = args[0]
    if len(args) < 4:
        print_usage(name)
        sys.exit(1)

    cmd = args[1]
    args = args[0:1] + args[2:]

    # We need to load the configuration file to know the root directory
    # and the IP address the server is listening on
    largs = [x.lower() for x in args]
    if '-cfg' not in largs:
        err('At least -cfg <file> should be specified')
        sys.exit(2)
    cfg_idx = largs.index('-cfg')
    if cfg_idx == len(largs) - 1:
        err('At least -cfg <file> should be specified')
        sys.exit(2)

    cfgfile = args[cfg_idx + 1]
    cfg = ngamsConfig.ngamsConfig()
    cfg.load(cfgfile)

    # The daemon PID file
    pidfile = os.path.join(cfg.getRootDirectory(), 'var', 'run', 'ngamsDaemon.pid')

    # Main command switch
    if 'start' == cmd:
        exitCode = start(args, cfg, pidfile)
    elif 'stop' == cmd:
        exitCode = stop(pidfile)
    elif 'restart' == cmd:
        stop(pidfile)
        exitCode = start(args, cfg, pidfile)
    elif 'status' == cmd:
        exitCode = status(cfg)
    else:
        err("Unknown command: %s" % (cmd,))
        print_usage(name)
        exitCode = 1

    sys.exit(exitCode)

if __name__ == '__main__':
    main(sys.argv)