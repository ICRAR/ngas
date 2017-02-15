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
#******************************************************************************
#
# "@(#) $Id: ngamsJanitorThread.py,v 1.14 2010/03/25 14:47:44 jagonzal Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  29/01/2002  Created
#
"""
This module contains the code for the Janitor Thread, which is used to perform
various background activities as cleaning up after processing, waking up
suspended NGAS hosts, suspending itself.
"""

import logging
import time

from ngamsDbSnapshotUtils import checkUpdateDbSnapShots, updateDbSnapShots
from ngamsJanitorCommon import StopJanitorThreadException, checkStopJanitorThread, suspend
from ngamsLib.ngamsCore import isoTime2Secs, loadPlugInEntryPoint


logger = logging.getLogger(__name__)


def JanitorCycle(plugins, srvObj, stopEvt, suspendTime, JanQue):
    """
    A single run of all the janitor plug-ins
    """

    checkStopJanitorThread(stopEvt)
    logger.debug("Janitor Thread running-Janitor Cycle.. ")

    for p in plugins:
        try:
            p(srvObj, stopEvt)
        except StopJanitorThreadException:
            raise
        except:
            logger.exception("Unexpected error in janitor plug-in %s", p.__name__)

    # Suspend the thread for the time indicated.
    # Update the Janitor Thread run count.
    srvObj.incJanitorThreadRunCount()
    JanQue.put(srvObj.getJanitorThreadRunCount())

    # Suspend the thread for the time indicated.
    logger.debug("Janitor Thread executed - suspending for %d [s] ...", suspendTime)
    startTime = time.time()
    event_info_list = JanQue.get()  #==================================================
    while ((time.time() - startTime) < suspendTime):
        # Check if we should update the DB Snapshot.
        if (event_info_list):
            time.sleep(0.5)
            try:
                diskInfo = None
                if (event_info_list):
                    for diskInfo in event_info_list:
                        updateDbSnapShots(srvObj, stopEvt, diskInfo)
            except StopJanitorThreadException:
                raise
            except Exception:
                if (diskInfo):
                    msg = "Error encountered handling DB Snapshot " +\
                          "for disk: %s/%s"
                    args = (diskInfo[0], diskInfo[1])
                else:
                    msg, args = "Error encountered handling DB Snapshot", ()
                logger.exception(msg, *args)
                time.sleep(5)
        suspend(stopEvt, 1.0)


def get_plugins(srvObj):
    """
    Returns the list of plug-ins that need to be run in this janitor process
    """

    hardcoded = [
        'ngamsJanitorHandleTempDBSnapshotFiles',
        'ngamsJanitorCheckProcessingDirectory',
        'ngamsJanitorCheckOldRequestsinDBM',
        'ngamsJanitorCheckSubscrBacklognTempDir',
        'ngamsJanitorCheckRetainedNotificationMsgs',
        'ngamsJanitorCheckUnsavedLogFile',
        'ngamsJanitorRotatedLogFilestoRemove',
        'ngamsJanitorCheckDiskSpace',
        'ngamsJanitorChecktoWakeupOtherNGASHost',
        'ngamsJanitorChecktoSuspendNGASHost',
    ]

    plugins = []
    for h in hardcoded:
        plugins.append(loadPlugInEntryPoint(h))

    # TODO: add configuration item on server for user-provided plugins
    return plugins

def janitorThread(srvObj, stopEvt, JanQue):
    """
    The Janitor Thread runs periodically when the NG/AMS Server is
    Online to 'clean up' the NG/AMS environment. Task performed are
    checking if any data is available in the Back-Log Buffer, and
    archiving of these in case yes, checking if there are any Processing
    Directories to be deleted.

    srvObj:      Reference to server object (ngamsServer).

    dummy:       Needed by the thread handling ...

    Returns:     Void.
    """

    suspendTime = isoTime2Secs(srvObj.getCfg().getJanitorSuspensionTime())
    plugins = get_plugins(srvObj)

    # => Update NGAS DB + DB Snapshot Document for the DB connected.
    try:
        checkUpdateDbSnapShots(srvObj, stopEvt)
    except StopJanitorThreadException:
        return
    except Exception:
        logger.exception("Problem updating DB Snapshot files")

    #==========================================================
    #=== Move contents of while loop to JanitorCycle method
    #===========================================================

    try:
        while True:
            JanitorCycle(plugins, srvObj, stopEvt, suspendTime, JanQue)
    except StopJanitorThreadException:
        return


# EOF