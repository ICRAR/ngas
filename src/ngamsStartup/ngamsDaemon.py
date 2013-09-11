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

import os, subprocess
from ngamsServer import *
from logger import ngaslog
from daemon import Daemon

HOME = os.environ['HOME']
if os.environ.has_key('NGAMS_ROOT'):
    NGAMS_ROOT = os.environ['NGAMS_ROOT']
else:
    NGAMS_ROOT = '{0}/NGAS'.format(HOME)
    os.environ['NGAMS_ROOT'] = NGAMS_ROOT

if os.environ.has_key('NGAMS_ARGS'):
    NGAMS_ARGS = os.environ['NGAMS_ARGS']
else:
    NGAMS_ARGS = [
                  '%s/ngas_rt/bin/ngamsServer' % HOME,
                  '-cfg', '%s/cfg/ngamsServer.conf' % NGAMS_ROOT,
                  '-force',
                  '-autoOnline',
                  '-multiplesrvs',
                  '-v', '3',
         ]



PIDFILE = '%s/var/run/ngamsDaemon.pid' % NGAMS_ROOT
try:
    os.makedirs('{0}/var/run'.format(NGAMS_ROOT))
    os.makedirs('{0}/var/log'.format(NGAMS_ROOT))
except OSError:
    pass


class MyDaemon(Daemon):
    """
    This class inherits from the main Daemon class
    and overrides the run method for NGAMS.
    """
    def run(self):
        ngaslog('INFO', "Inside run...")
        ARGS_BCK = sys.argv
        try:
            ARGS_BCK = sys.argv       # store original arguments
            sys.argv = NGAMS_ARGS     # put the NGAMS_ARGS instead
            nserver = ngamsServer()   # instantiate server
            ngaslog('INFO', 'Initializing server: {}'.format(' '.join(NGAMS_ARGS)))
            nserver.init(NGAMS_ARGS, extlogger=ngaslog)  # initialize server
            sys.argv = ARGS_BCK
        except Exception as e:
            ngaslog('INFO', str(e))
            raise e

    def status(self):
        """
        Send a STATUS command to server
        """
        SCMD = "{0}/ngas_rt/bin/ngamsPClient -port 7777 -host $HOSTNAME -cmd STATUS -v 1".\
             format(HOME)
        subprocess.call(SCMD,shell=True)


def checkNgasPidFile(dum):
    """
    Check for existence of NGAS internal PID file.
    This function is used during shutdown to make
    sure that the server terminated cleanly, in which
    case the PID file is removed.
    """
    with open(PIDFILE, 'r') as f:
        ipid = f.readline().strip()
    pidfils = glob.glob('%s/.NGAS-*' % NGAMS_ROOT)
    for fil in pidfils:
        with open(fil, 'r') as f:
            pid = f.readline().strip()
        if ipid == pid:
            return True
    return False

def main(args=sys.argv):
    """
    Entry point function
    """
    daemon = MyDaemon(PIDFILE,)

    if sys.argv[0] == 'ngamsCacheDaemon': #check how we are called
        infoStr = 'NGAMS Cache Server'
        progrStr = 'ngamsCacheServer'
        NGAMS_ARGS[0] = '%s/ngas_rt/bin/ngamsCacheServer' % HOME
    else:
        progrStr = 'ngamsServer'
        infoStr = 'NGAMS Server'

    if len(sys.argv) == 2:
            if 'start' == sys.argv[1]:
                    ngaslog('INFO', '{0} Starting'.format(infoStr))
                    daemon.start()
            elif 'stop' == sys.argv[1]:
                    ngaslog('INFO', '{0} Stopping'.format(infoStr))
                    daemon.stop(cfunc=checkNgasPidFile)
            elif 'restart' == sys.argv[1]:
                    ngaslog('INFO', '{0} Restarting'.format(infoStr))
                    daemon.restart(cfunc=checkNgasPidFile)
            elif 'status' == sys.argv[1]:
                    ngaslog('INFO', 'Sending STATUS command')
                    daemon.status()
            else:
                    print "Unknown command"
                    print "usage: %s start|stop|restart|status" % sys.argv[0]
                    sys.exit(2)
            sys.exit(0)
    else:
            print "usage: %s start|stop|restart|status" % sys.argv[0]
            sys.exit(2)

if __name__ == "__main__":
    main()

