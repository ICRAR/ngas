#
#    ALMA - Atacama Large Millimiter Array
#    (c) European Southern Observatory, 2002
#    Copyright by ESO (in the framework of the ALMA collaboration),
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

#******************************************************************************
#
# "@(#) $Id: ngamsUserServiceThread.py,v 1.5 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  29/03/2007  Created
#

"""
This module contains the code for the User Service Thread, which execute
a plug-in provided by the user periodically.
"""

from ngams import *
import ngamsLib


USR_THR_STOP_TAG = "_STOP_USER_SERVICE_THREAD_"


def startUserServiceThread(srvObj):
    """
    Start the User Service Thread.

    srvObj:     Reference to server object (ngamsServer).
    
    Returns:    Void.
    """
    T = TRACE()
    
    # Start only if service is defined.
    userPlugPar = "NgamsCfg.SystemPlugIns[1].UserServicePlugIn"
    userServicePlugIn = srvObj.getCfg().getVal(userPlugPar)
    info(4,"User Service Plug-In Defined: %s" % str(userServicePlugIn))
    if (not userServicePlugIn): return

    info(1,"Loading User Service Plug-In module: %s" % userServicePlugIn)
    try:
        exec "import %s" % userServicePlugIn
    except Exception, e:
        msg = "Error loading User Service Plug-In: %s. Error: %s"
        errMsg = msg % (userServicePlugIn, str(e))
        raise Exception, errMsg
    srvObj._userServicePlugIn = userServicePlugIn

    info(1,"Starting User Service Thread ...")
    srvObj._userServiceRunSync.set()
    args = (srvObj, None)
    srvObj._userServiceThread = threading.Thread(None, userServiceThread,
                                                 NGAMS_USER_SERVICE_THR, args)
    srvObj._userServiceThread.setDaemon(0)
    srvObj._userServiceThread.start()
    info(3,"User Service Thread started")


def stopUserServiceThread(srvObj):
    """
    Stop the User Service Thread.

    srvObj:     Reference to server object (ngamsServer).
    
    Returns:    Void.
    """
    T = TRACE()

    info(1,"Stopping User Service Thread ...")
    srvObj._userServiceRunSync.clear()
    srvObj._userServiceRunSync.wait(10)
    srvObj._userServiceRunSync.clear()
    srvObj._userServiceThread = None
    info(3,"User Service Thread stopped")


def checkStopUserServiceThread(srvObj):
    """
    Convenience function used to check if the User Service Thread should
    stop execution.

    srvObj:      Reference to instance of ngamsServer object (ngamsServer).

    Returns:     Void.
    """
    if (not srvObj._userServiceRunSync.isSet()):
        info(2,"Stopping User Service Thread")
        srvObj._userServiceRunSync.set()
        raise Exception, USR_THR_STOP_TAG


def userServiceThread(srvObj,
                      dummy):
    """
    The User Service Thread runs periodically a user provided plug-in
    (User Service Plug-In) which carries out tasks needed in a specific
    context.

    srvObj:      Reference to server object (ngamsServer).

    dummy:       Needed by the thread handling ... 
    
    Returns:     Void.
    """
    T = TRACE(1)

    usrPlugPar = "NgamsCfg.SystemPlugIns[1]"
    userServicePlugInPars = srvObj.getCfg().getVal(usrPlugPar +\
                                                   ".UserServicePlugInPars")
    userServicePlugPeriod = srvObj.getCfg().getVal(usrPlugPar +\
                                                   ".UserServicePlugInPeriod")
    if (not userServicePlugPeriod):
        # Set the period to default value 5 minutes.
        period = 300
    else:
        period = isoTime2Secs(userServicePlugPeriod)

    # Main loop.
    while (True):
        try:
            startTime = time.time()
            info(4,"Executing User Service Plug-In: %s" %\
                 srvObj._userServicePlugIn)
            try:
                exec "import %s" % srvObj._userServicePlugIn
            except Exception, e:
                msg = "Error loading User Service Plug-In: %s. Error: %s"
                errMsg = msg % (srvObj._userServicePlugIn, str(e))
                raise Exception, errMsg
            plugInCmd = "%s.%s(srvObj, userServicePlugInPars)" %\
                        (srvObj._userServicePlugIn, srvObj._userServicePlugIn)
            info(5,"Executing User Service Plug-In with command: %s" %\
                 plugInCmd)
            eval(plugInCmd)
            stopTime = time.time()
            sleepTime = (period - (stopTime - startTime))
            if (sleepTime > 0):
                msg = "Executed User Service Plug-In: %s. Sleeping: %.3fs"
                info(4,msg % (srvObj._userServicePlugIn, sleepTime))
                while ((period - (time.time() - startTime)) > 0):
                    time.sleep(1.0)
                    checkStopUserServiceThread(srvObj)
            else:
                msg = "Executed User Service Plug-In: %s. Scheduling " +\
                      "immediately"
                info(4,msg % (srvObj._userServicePlugIn))
        except Exception, e:
            if (str(e).find(USR_THR_STOP_TAG) != -1):
                info(1,"User Service Thread manager terminating")
                thread.exit()
            errMsg = "Error occurred during execution of the User " +\
                     "Service Thread. Exception: " + str(e)
            alert(errMsg)
            # We make a small wait here to avoid that the process tries
            # too often to carry out the tasks that failed.
            time.sleep(2.0)


# EOF
