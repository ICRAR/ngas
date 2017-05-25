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

import Queue
import logging
import signal
import time

from ngamsDbSnapshotUtils import checkUpdateDbSnapShots, updateDbSnapShots
from ngamsJanitorCommon import StopJanitorThreadException, checkStopJanitorThread, suspend
from ngamsLib.ngamsCore import isoTime2Secs, loadPlugInEntryPoint


logger = logging.getLogger(__name__)


def JanitorCycle(plugins, srvObj, stopEvt, jan_to_srv_queue):
    """
    A single run of all the janitor plug-ins
    """

    logger.debug("Janitor Thread running-Janitor Cycle.. ")

    for p in plugins:
        checkStopJanitorThread(stopEvt)
        try:
            logger.debug("Executing plugin %s", p.__name__)
            p(srvObj, stopEvt, jan_to_srv_queue)
        except StopJanitorThreadException:
            raise
        except:
            logger.exception("Unexpected error in janitor plug-in %s", p.__name__)


def get_plugins(srvObj):
    """
    Returns the list of plug-ins that need to be run in this janitor process
    """

    hardcoded = [
        'ngamsJanitorHandleTempDBSnapshotFiles',
        'ngamsJanitorCheckOldRequestsinDBM',
        'janitor.expired_data_cleaner',
        'janitor.notifications_sender',
        'ngamsJanitorCheckUnsavedLogFile',
        'janitor.rotated_logfiles_remover',
        'janitor.disk_space_checker',
        'janitor.wake_up_request_processor',
        'janitor.host_suspender'
    ]

    user_plugins = srvObj.getCfg().getJanitorPlugins()
    return [loadPlugInEntryPoint(n, entryPointMethodName='run') for n in hardcoded + user_plugins]


class ForwarderHandler(logging.Handler):

    def __init__(self, queue):
        super(ForwarderHandler, self).__init__()
        self.queue = queue

    def _format_record(self, record):
        # ensure that exc_info and args
        # have been stringified. Removes any chance of
        # unpickleable things inside and possibly reduces
        # message size sent over the pipe.
        if record.args:
            record.msg = record.msg % record.args
            record.args = None
        if record.exc_info:
            self.format(record)
            record.exc_info = None
        record.threadName = 'JANITOR-PROC'
        return record

    def emit(self, record):
        try:
            self.queue.put_nowait(('log-record', self._format_record(record)))
        except:
            self.handleError(record)

def janitorThread(srvObj, stopEvt, srv_to_jan_queue, jan_to_srv_queue):
    """
    Entry point for the janitor process. It checks which plug-ins should be run,
    how frequently, and runs them in an infinite loop.
    """

    # No need to shut down anything on this process
    def noop(*args):
        pass
    signal.signal(signal.SIGTERM, noop)
    signal.signal(signal.SIGINT, noop)

    # Set up the logging so it outputs the records into the jan->srv queue
    for h in list(logging.root.handlers):
        logging.root.removeHandler(h)
    logging.root.addHandler(ForwarderHandler(jan_to_srv_queue))

    # Reset the db pointer in our server object to get fresh connections
    srvObj.reconnect_to_db()

    # => Update NGAS DB + DB Snapshot Document for the DB connected.
    try:
        checkUpdateDbSnapShots(srvObj, stopEvt)
    except StopJanitorThreadException:
        return
    except:
        logger.exception("Problem updating DB Snapshot files")

    # Main loop
    suspendTime = isoTime2Secs(srvObj.getCfg().getJanitorSuspensionTime())
    plugins = get_plugins(srvObj)
    run_count = 0
    try:
        while True:

            JanitorCycle(plugins, srvObj, stopEvt, jan_to_srv_queue)

            # Suspend the thread for the time indicated.
            # Update the Janitor Thread run count.
            run_count += 1
            jan_to_srv_queue.put(('janitor-run-count', run_count), timeout=0.1)

            # Suspend the thread for the time indicated.
            logger.info("Janitor Thread executed - suspending for %d [s] ...", suspendTime)
            startTime = time.time()
            while (time.time() - startTime) < suspendTime:

                # Check if we should update the DB Snapshot.
                try:
                    event_info_list = srv_to_jan_queue.get(timeout=0.1)[1]
                except Queue.Empty:
                    event_info_list = None

                if event_info_list is not None:
                    try:
                        diskInfo = None
                        for diskInfo in event_info_list:
                            updateDbSnapShots(srvObj, stopEvt, diskInfo)
                    except:
                        if (diskInfo):
                            msg = "Error encountered handling DB Snapshot " +\
                                  "for disk: %s/%s"
                            args = (diskInfo[0], diskInfo[1])
                        else:
                            msg, args = "Error encountered handling DB Snapshot", ()
                        logger.exception(msg, *args)
                        suspend(stopEvt, 5)

                suspend(stopEvt, 1.0)

    except StopJanitorThreadException:
        return


# EOF
