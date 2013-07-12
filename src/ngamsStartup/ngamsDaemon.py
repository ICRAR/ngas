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

import os
from ngamsServer import *
from logger import ngaslog
from daemon import Daemon

if os.environ.has_key('NGAMS_ARGS'):
    NGAMS_ARGS = os.environ['NGAMS_ARGS']
else:
    HOME = os.environ['HOME']
    NGAMS_ARGS = [
                  '%s/ngas_rt/bin/ngamsServer' % HOME,
                  '-cfg', '%s/ngas_rt/cfg/NgamsCfg.SQLite.mini.xml' % HOME,
                  '-force',
                  '-autoOnline',
#                  '-multiplsrv',
                  '-v', '0',
         ]
PIDFILE = '%s/var/run/ngamsDaemon.pid' % HOME
try:
    os.makedirs('{0}/var/run'.format(HOME))
    os.makedirs('{0}/var/log'.formate(HOME))
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
            nserver.init(NGAMS_ARGS)  # initialize server
            main()                    # start NGAMS
            sys.argv = ARGS_BCK
        except Exception as e:
            ngaslog(str(e))
            raise e

def checkNgasPidFile(dum):
    """
    Check for existence of NGAS internal PID file.
    This function is used during shutdown to make
    sure that the server terminated cleanly, in which
    case the PID file is removed.
    """
    with open(PIDFILE, 'r') as f:
        ipid = f.readline().strip()
    pidfils = glob.glob('%s/NGAS/.NGAS-*' % HOME)
    for fil in pidfils:
        with open(fil, 'r') as f:
            pid = f.readline().strip()
        if ipid == pid:
            return True
    return False

if __name__ == "__main__":

        daemon = MyDaemon(PIDFILE,)

        if len(sys.argv) == 2:
                if 'start' == sys.argv[1]:
                        ngaslog('INFO', 'NGAMS Server Started')
                        daemon.start()
                elif 'stop' == sys.argv[1]:
                        ngaslog('INFO', 'NGAMS Server Stopped')
                        daemon.stop(cfunc=checkNgasPidFile)
                elif 'restart' == sys.argv[1]:
                        ngaslog('INFO', 'NGAMS Server Restarting')
                        daemon.restart(cfunc=checkNgasPidFile)
                elif 'status' == sys.argv[1]:
                        ngaslog('INFO', 'Sending STATUS command')
                        print "Not implemented yet!"
                        pass
                else:
                        print "Unknown command"
                        print "usage: %s start|stop|restart|status" % sys.argv[0]
                        sys.exit(2)
                sys.exit(0)
        else:
                print "usage: %s start|stop|restart|status" % sys.argv[0]
                sys.exit(2)
