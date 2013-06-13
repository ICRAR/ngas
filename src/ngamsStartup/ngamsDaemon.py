#!/usr/bin/env python

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

class MyDaemon(Daemon):
        def run(self):
            ngaslog('INFO', "Inside run...")
            ARGS_BCK = sys.argv
            try:
                ARGS_BCK = sys.argv
                sys.argv = NGAMS_ARGS
                nserver = ngamsServer()
                nserver.init(NGAMS_ARGS)
                main()
                sys.argv = ARGS_BCK
            except Exception as e:
                raise e

def checkNgasPidFile(dum):
    """
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
