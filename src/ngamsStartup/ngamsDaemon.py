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

import sys, os, subprocess, socket, glob
from ngamsServer import ngamsServer
from ngams import getHostId
from logger import ngaslog
from daemon import Daemon
import ngamsHighLevelLib

HOME = os.environ['HOME']
if os.environ.has_key('NGAS_PREFIX'):
    NGAS_PREFIX = os.environ['NGAS_PREFIX']
else:
    NGAS_PREFIX = '{0}/ngas_rt'.format(HOME)
    os.environ['NGAS_PREFIX'] = NGAS_PREFIX

CFG = '%s/../NGAS/cfg/ngamsServer.conf' % NGAS_PREFIX
if not os.path.exists(CFG):
    ngaslog("ERROR", "Configuration file not found: {0}".format(CFG))
    raise(ValueError)
HOST_ID = getHostId(CFG)

# importing it here makes sure that getHostID is called with
# the config file.
from ngamsConfig import ngamsConfig
cfgObj = ngamsConfig()
cfgObj.load(CFG)
PORT = cfgObj.getPortNo()
if os.environ.has_key('NGAMS_ARGS'):
    NGAMS_ARGS = os.environ['NGAMS_ARGS'].split() # convert from command line (string) to a list
else:
    NGAMS_ARGS = [
                  '%s/bin/ngamsServer' % NGAS_PREFIX,
                  '-cfg', CFG,
                  '-force',
                  '-autoOnline',
                  '-multiplesrvs',
                  '-v', '3',
         ]



PIDFILE = '%s/../NGAS/var/run/ngamsDaemon.pid' % NGAS_PREFIX
try:
    os.makedirs('{0}/../NGAS/var/run'.format(NGAS_PREFIX))
    os.makedirs('{0}/../NGAS/var/log'.format(NGAS_PREFIX))
except OSError:
    pass


def internalPidFile():
        """
        Return the name of the PID file in which NG/AMS stores its PID.

        Returns:   Name of PID file (string).
        """
        hostId = getHostId(cfgFile=CFG)
        # Generate a PID file with the  name: <mt root dir>/.<NGAS ID>
        if ((not cfgObj.getRootDirectory()) or \
            (cfgObj.getPortNo() < 1)): return ""
        try:
            pidFile = os.path.join(cfgObj.getRootDirectory(), "." +
                                   hostId + ":" + str(cfgObj.getPortNo()) +
                                   ".pid")
        except Exception, e:
            errMsg = "Error occurred generating PID file name. Check " +\
                     "Mount Root Directory + Port Number in configuration. "+\
                     "Error: " + str(e)
            raise Exception, errMsg
        if glob.glob(pidFile):
            return pidFile
        else:
            return ""


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
        SCMD = "{0}/bin/ngamsPClient -port {1} -host $HOSTNAME -cmd STATUS -v 1".\
             format(NGAS_PREFIX, PORT)
        subprocess.call(SCMD,shell=True)


def checkNgasPidFile(dum):
    """
    Check for existence of NGAS internal PID file.
    This function is used during shutdown to make
    sure that the server terminated cleanly, in which
    case the PID file is removed.
    """
    f = open(PIDFILE, 'r')
    ipid = f.readline().strip()
    f.close()
    fil = internalPidFile()
    if fil:
        try:
            f = open(fil, 'r')
            pid = f.readline().strip()
            f.close()
        except:
            return False
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
        NGAMS_ARGS[0] = '%s/bin/ngamsCacheServer' % NGAS_PREFIX
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

