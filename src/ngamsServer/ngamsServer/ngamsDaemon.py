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
import inspect
import os
import signal
import sys
import time

import daemon
import lockfile.pidlockfile

from ngamsLib import ngamsConfig, ngamsHighLevelLib
from ngamsLib.ngamsCore import get_contact_ip
from . import ngamsServer


def errmsg(s):
    sys.stderr.write(s + '\n')


def start(args, cfg, pid_path):
    # Ensure the directory is there to keep the PID file
    try:
        os.makedirs(os.path.join(cfg.getRootDirectory(), 'var', 'run'))
    except OSError:
        pass

    if os.path.isfile(pid_path):
        pid = lockfile.pidlockfile.read_pid_from_pidfile(pid_path)
        errmsg("PID file %s already exists (PID=%s), not overwriting possibly existing instance" % (pid_path, str(pid)))
        return 1

    # Old versions of lockfile don't offer a timeout parameter for PIDLockFile.
    # We thus use it only when available (because we still want to have the
    # behavior available when possible)
    pid_file = lockfile.pidlockfile.PIDLockFile
    pid_file_kwargs = {}
    if 'timeout' in inspect.getargspec(pid_file.__init__).args:
        pid_file_kwargs['timeout'] = 1

    # Go, go, go!
    with daemon.DaemonContext(pidfile=pid_file(pid_path, **pid_file_kwargs)):
        ngamsServer.main(args=args[1:], prog='ngamsDaemon')

    return 0


def stop(pid_path):
    pid = lockfile.pidlockfile.read_pid_from_pidfile(pid_path)
    if pid is None:
        errmsg("Cannot read PID file, is there an instance running?")
        return 1
    else:
        try:
            return kill_and_wait(pid, pid_path)
        except OSError as e:
            if e.errno == errno.ESRCH:
                errmsg("PID lock file points to non-existing PID %d, removing PID lock file" % (pid,))
                os.unlink(pid_path)
                return 1


def kill_and_wait(pid, pid_path):
    # The NGAS server should nicely shut down itself after receiving this signal
    os.kill(pid, signal.SIGTERM)

    # We previously checked that the pid_file was gone, but this is not enough
    # In some cases the main thread finished correctly, but there are other
    # threads (e.g., HTTP servicing threads) that are still running,
    # and thus the PID file disappears but the process is still ongoing
    tries = 0
    max_tries = 20
    sleep_period = 1
    start_time = time.time()
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
    end_time = time.time()

    exit_status = 0x00

    # We waited a lot but the process is still there, now forcibly kill it with -9
    if tries == max_tries:
        errmsg("Process didn't die after %.2f [s], killing it with -9" % (end_time - start_time))
        os.kill(pid, signal.SIGKILL)
        exit_status += 0x01

    # The process should have removed its own PID file
    if os.path.exists(pid_path):
        errmsg("Removing PID file manually, the daemon process didn't remove it by itself")
        os.unlink(pid_path)
        exit_status += 0x02

    return exit_status


def status(cfg):
    """
    Check if the server is up and running
    """
    ip_address = get_contact_ip(cfg)
    port = cfg.getPortNo()
    try:
        ngamsHighLevelLib.pingServer(ip_address, port, 10)
        return 0
    except:
        return 1


def print_usage(name):
    errmsg("Usage: %s [start|stop|restart|status] <ngamsServer options>" % (name,))


def main(args=sys.argv):
    """
    Entry point function. It's mapped to two different scripts, which is why
    we can distinguish here between them and start different processes
    """
    # A minimum of 4 because we require at least -cfg <file>
    # e.g. ngamsDaemon.py start -cfg /path/to/ngas-server-7777.xml
    name = args[0]
    if len(args) < 4:
        print_usage(name)
        sys.exit(1)

    cmd = args[1]
    args = args[0:1] + args[2:]

    # We need to load the configuration file to know the root directory
    # and the IP address the server is listening on
    lower_args = [x.lower() for x in args]
    if '-cfg' not in lower_args:
        errmsg('At least -cfg <file> should be specified')
        sys.exit(2)
    cfg_idx = lower_args.index('-cfg')
    if cfg_idx == len(lower_args) - 1:
        errmsg('At least -cfg <file> should be specified')
        sys.exit(2)

    cfg_file = args[cfg_idx + 1]
    cfg = ngamsConfig.ngamsConfig()
    cfg.load(cfg_file)

    # The daemon PID file
    pid_path = os.path.join(cfg.getRootDirectory(), 'var', 'run', 'ngamsDaemon.pid')

    # In the event we receive the '-force' command line option we will forcibly restart the NGAS daemon
    # This will require stopping any NGAS daemon that might be currently running and removing the PID lock file
    if '-force' in lower_args:
        sys.stdout.write("Force command line option is set. Will clean up PID lock file if one exists.\n")
        if os.path.isfile(pid_path):
            stop(pid_path)

    # Main command switch
    if 'start' == cmd:
        exit_code = start(args, cfg, pid_path)
    elif 'stop' == cmd:
        exit_code = stop(pid_path)
    elif 'restart' == cmd:
        stop(pid_path)
        exit_code = start(args, cfg, pid_path)
    elif 'status' == cmd:
        exit_code = status(cfg)
    else:
        errmsg("Unknown command: %s" % (cmd,))
        print_usage(name)
        exit_code = 1

    sys.exit(exit_code)


if __name__ == '__main__':
    main(sys.argv)
