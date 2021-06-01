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
# "@(#) $Id: ngamsSrvUtils.py,v 1.9 2009/11/26 14:55:40 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  27/09/2002  Created
#
"""
This module contains various utilities used by the NG/AMS Server.
"""

import contextlib
import glob
import logging

from ngamsLib.ngamsCore import NGAMS_NOT_RUN_STATE,\
    NGAMS_ONLINE_STATE, NGAMS_SUBSCRIBE_CMD, NGAMS_SUCCESS, genLog, \
    NGAMS_SUBSCRIBER_THR, NGAMS_UNSUBSCRIBE_CMD, NGAMS_HTTP_INT_AUTH_USER,\
    loadPlugInEntryPoint, toiso8601, fromiso8601, NGAMS_NOTIF_ERROR
from ngamsLib import ngamsStatus, ngamsLib, ngamsHttpUtils, utils
from ngamsLib import ngamsSubscriber
from ngamsLib import ngamsHighLevelLib, ngamsDiskUtils
from ngamsLib import ngamsNotification
from . import ngamsSubscriptionThread


logger = logging.getLogger(__name__)

def ngamsBaseExitHandler(srvObj):
    """
    NG/AMS Exit Handler Function. Is invoked when the NG/AMS Server
    is killed terminated.

    srvObj:      Reference to NG/AMS server class object (ngamsServer).

    signalNo:    Number of signal received (integer).

    killServer:  If set to 1 the server will be killed by the
                 function (integer/0|1).

    exitCode:    The exit code with which NG/AMS should exit (integer).

    delPidFile:  Flag indicating if NG/AMS PID file should be deleted or
                 not (integer/0|1).

    Returns:     Void.
    """
    logger.info("NG/AMS Exit Handler - cleaning up ...")

    if (srvObj.getState() == NGAMS_ONLINE_STATE):
        handleOffline(srvObj)

    # Update the ngas_hosts table.
    srvObj.updateHostInfo("", None, 0, 0, 0, 0, 0, NGAMS_NOT_RUN_STATE)


def _create_remote_subscriptions(srvObj, stop_evt):
    """
    Creates subscriptions in remote servers so they push their data into
    ours.
    """

    subscriptions_in_cfg = srvObj.cfg.getSubscriptionsDic()
    subscriptions = [v for _, v in subscriptions_in_cfg.items()]
    subscriptions_created = 0

    def mark_as_active(s):
        # Removes s from list of subscriptions pending creation, and
        # ensures the corresponding UNSUBSCRIBE command will be called
        # at server shutdown
        srvObj.getSubscrStatusList().append(s)
        subscriptions.remove(s)

    while True:

        # Done with all of them
        if not subscriptions:
            break

        # Iterate over copy, since we modify the original inside the loop
        for subscrObj in list(subscriptions):

            if stop_evt.is_set():
                logger.info("Terminating Subscriber thread ...")
                return

            our_host, our_port = srvObj.get_self_endpoint()
            subs_host, subs_port = subscrObj.getHostId(), subscrObj.getPortNo()

            # Not subscribing to ourselves
            if srvObj.is_it_us(subs_host, subs_port):
                logger.warning("Skipping subscription to %s:%d because that's us", subs_host, subs_port)
                continue

            # Because properly supporting the "Command" configuration mechanism
            # still requires some more work, we prefer the "SubscriberUrl"
            # attribute as the main source of URL information.
            # We still support "Command", but with the following caveats:
            #  * TODO: include reverse proxy information when we add support
            #  * TODO: hardcoded http will need to be changed when we add support
            #          for https
            #  * TODO: fails with IpAddress == '0.0.0.0'
            url = subscrObj.getUrl()
            if not url:
                url = 'http://%s:%d/%s' % (our_host, our_port, subscrObj.command or 'QARCHIVE')
            logger.info("Creating subscription to %s:%d with url=%s", subs_host, subs_port, url)

            pars = [["subscr_id", subscrObj.getId()],
                    ["priority", subscrObj.getPriority()],
                    ["url",      url],
                    ["start_date", toiso8601(local=True)]]
            if subscrObj.getFilterPi():
                pars.append(["filter_plug_in", subscrObj.getFilterPi()])
            if subscrObj.getFilterPiPars():
                pars.append(["plug_in_pars", subscrObj.getFilterPiPars()])

            try:
                # Issue SUBSCRIBE command
                resp = ngamsHttpUtils.httpGet(subs_host, subs_port, cmd=NGAMS_SUBSCRIBE_CMD, pars=pars)
                with contextlib.closing(resp):
                    stat = ngamsStatus.to_status(resp, subscrObj.getHostId(), NGAMS_SUBSCRIBE_CMD)

                if stat.getStatus() != NGAMS_SUCCESS:
                    if stat.http_status == 409:
                        short_msg = "Different subscription with ID '%s' already exists, giving up"
                        long_msg = (
                            "NGAS attempted to create an automatic subscription "
                            "with ID=%s to obtain data from %s:%d, but the remote server "
                            "already has a subscription registered with the same ID, "
                            "but different details.\n\n"
                            "Instead of retrying to create this subscription over and over, "
                            "this server will give up now. To fix this either remove "
                            "the remote subscription, or change the ID of the subscription to be created "
                            "in the local server configuration."
                        )
                        logger.error(short_msg, subscrObj.getId())
                        ngamsNotification.notify(
                            srvObj.host_id, srvObj.cfg, NGAMS_NOTIF_ERROR,
                            "Automatic subscription cannot be created",
                            long_msg % (subscrObj.getId(), subs_host, subs_port),
                            force=True)
                        mark_as_active(subscrObj)
                        continue
                    else:
                        msg = "Unsuccessful NGAS XML response. Status: %s, message: %s. Will try again later"
                        logger.warning(msg, stat.getStatus(), stat.getMessage())
                        continue

                subscriptions_created += 1
                logger.info("Successfully subscribed to %s:%d with url=%s", subs_host, subs_port, subscrObj.getUrl())
                mark_as_active(subscrObj)

            except:
                logger.exception("Error while adding subscription, will try later")

        if stop_evt.wait(10):
            return

    if subscriptions_created:
        logger.info("Successfully established %d Subscription(s)", subscriptions_created)
    else:
        logger.info("No Subscriptions established")


def getDiskInfo(srvObj,
                reqPropsObj = None):
    """
    Invokes the plug-in listed in the configuration to extract the information
    about the disks mounted. The information is returned in a dictionary which
    contains the Slot IDs as keys, and elements which each is an object of type
    ngamsPhysDiskInfo.

    srvObj:        Reference to NG/AMS Server class instance (ngamsServer).

    reqPropsObj:   NG/AMS request properties object (ngamsReqProps).

    Returns:       Dictionary containing information about the
                   disks (dictionary).
    """
    plugIn = srvObj.getCfg().getOnlinePlugIn()
    logger.info("Invoking System Online Plug-In: %s(srvObj)", plugIn)
    plugInMethod = loadPlugInEntryPoint(plugIn)
    diskInfoDic = plugInMethod(srvObj, reqPropsObj)
    if not diskInfoDic:
        if (not ngamsLib.trueArchiveProxySrv(srvObj.getCfg())):
            errMsg = genLog("NGAMS_NOTICE_NO_DISKS_AVAIL",
                            [srvObj.getHostId(),
                             srvObj.getHostId()])
            logger.warning(errMsg)
    logger.debug("Disk Dictionary: %s", str(diskInfoDic))
    return diskInfoDic


def handleOnline(srvObj,
                 reqPropsObj = None):
    """
    Initialize the system. This implies loading the NG/AMS
    Configuration making the internal initialization from this,
    obtaining information about the disk configuration.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    NG/AMS request properties object (ngamsReqProps).

    Returns:        Void.
    """
    logger.debug("Prepare NGAS and NG/AMS for Online State ...")

    # Re-load Configuration + check disk configuration.
    srvObj.loadCfg()

    hostId = srvObj.getHostId()
    for stream in srvObj.getCfg().getStreamList():
        srvObj.getMimeTypeDic()[stream.getMimeType()] = stream.getPlugIn()

    # Flush/send out possible retained Email Notification Messages.
    flushMsg = "NOTE: Distribution of retained Email Notification Messages " +\
               "forced at Online"
    ngamsNotification.checkNotifRetBuf(srvObj.getHostId(), srvObj.getCfg(), 1, flushMsg)

    # Get possible Subscribers from the DB.
    subscrList = srvObj.getDb().getSubscriberInfo("", hostId,
                                                  srvObj.getCfg().getPortNo())
    num_bl =  srvObj.getDb().getSubscrBackLogCount(hostId, srvObj.getCfg().getPortNo())
    #debug_chen
    if (num_bl > 0):
        logger.debug('Preset the backlog count to %d', num_bl)
        srvObj.presetSubcrBackLogCount(num_bl)

    # TODO: unify this "db-to-object" reading (there is something similar elsewhere,
    #       I'm pretty sure, and hide these low-level details from here
    logger.info("Creating %d subscription objects from subscription DB info", len(subscrList))
    for subscrInfo in subscrList:
        start_date = fromiso8601(subscrInfo[5], local=True) if subscrInfo[5] else None
        last_ingested_date = fromiso8601(subscrInfo[8], local=True) if subscrInfo[8] else None
        tmpSubscrObj = ngamsSubscriber.ngamsSubscriber(subscrInfo[0],
                                                       subscrInfo[1],
                                                       subscrInfo[2],
                                                       subscrInfo[4],
                                                       start_date,
                                                       subscrInfo[6],
                                                       subscrInfo[7],
                                                       last_ingested_date,
                                                       subscrInfo[3])
        tmpSubscrObj.setConcurrentThreads(subscrInfo[9])
        # Take only subscribers for this NG/AMS Server.
        if ((tmpSubscrObj.getHostId() == hostId) and
            (tmpSubscrObj.getPortNo() == srvObj.getCfg().getPortNo())):
            #srvObj.getSubscriberDic()[tmpSubscrObj.getId()] = tmpSubscrObj
            #if (srvObj.getDataMoverOnlyActive() and len(srvObj.getSubscriberDic()) > 0):
            #   break #only load one subscriber under the data mover mode
            srvObj.registerSubscriber(tmpSubscrObj)

    try:

        # rtobar, Feb 2018
        #
        # We need to do this import here if we want support both python 2 and 3.
        # This is due to the following circular dependency:
        #
        # ngamsArchiveUtils -> ngamsFileUtils -> ngamsSrvUtils -> ngamsArchiveUtils
        #
        # A top-level absolute import is impossible, because
        # "import ngamsServer.ngamsArchiveUtils" interprets ngamsServer as the
        # sibling module in python 2 (and thus fails).
        # A top-level relative import fails in python 2 too.
        #
        # TODO: in the long run we should avoid the circular dependency, of course
        from . import ngamsArchiveUtils

        # Get information about the disks of this system.
        srvObj.setDiskDic(getDiskInfo(srvObj, reqPropsObj))
        ngamsArchiveUtils.resetDiskSpaceWarnings()

        # Update disk info in DB.
        ngamsDiskUtils.checkDisks(hostId, srvObj.getDb(), srvObj.getCfg(),
                                  srvObj.getDiskDic())

        # Write status file on disk.
        ngamsDiskUtils.dumpDiskInfoAllDisks(srvObj.getHostId(),
                                            srvObj.getDb(), srvObj.getCfg())
    except Exception:
        errMsg = "Error occurred while bringing the system Online"
        logger.exception(errMsg)
        handleOffline(srvObj)
        raise

    # Check that there is enough disk space in the various system directories.
    srvObj.checkDiskSpaceSat()

    # Check if files are located in the Staging Areas of the Disks. In case
    # yes, move them to the Back-Log Area to have them treated later.
    checkStagingAreas(srvObj)

    # Start threads + inform threads that they are allowed to execute.
    srvObj.startJanitorThread()
    srvObj.startDataCheckThread()
    ngamsSubscriptionThread.startSubscriptionThread(srvObj)
    srvObj.startUserServiceThread()
    srvObj.startMirControlThread()
    srvObj.startCacheControlThread()
    srvObj.run_async_commands = True

    # Change state to Online.
    srvObj.setState(NGAMS_ONLINE_STATE)

    # If specified in the configuration file that this server should act as a
    # Subscriber, then subscribe to the Data Providers specified. We do this
    # in a thread so that the server can continuously try to subscribe,
    # even after errors occur.
    t = utils.Task(NGAMS_SUBSCRIBER_THR, _create_remote_subscriptions)
    srvObj.remote_subscription_creation_task = t
    if (srvObj.getCfg().getSubscrEnable()):
        t.start(srvObj)

    logger.info("NG/AMS prepared for Online State")


def handleOffline(srvObj,
                  reqPropsObj = None):
    """
    Carry out the actions to put the system in Offline state (Standby).

    srvObj:          Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:     NG/AMS request properties object (ngamsReqProps).

    stopJanitorThr:  Setting this to 0, the function will not try to
                     stop the Janitor Thread. The reason for this is that
                     the Janitor Thread may bring the server to Offline
                     (integer/0|1).

    Returns:         Void.
    """
    # Stop/delete Janitor Thread + Data Check Thread + inform other
    # possible threads to stop execution (if running).
    srvObj.run_async_commands = False
    srvObj.stopJanitorThread()
    srvObj.stopDataCheckThread()
    ngamsSubscriptionThread.stopSubscriptionThread(srvObj)
    srvObj.stopUserServiceThread()
    srvObj.stopMirControlThread()
    srvObj.stopCacheControlThread()
    if srvObj.remote_subscription_creation_task:
        srvObj.remote_subscription_creation_task.stop()

    logger.debug("Prepare NG/AMS for Offline State ...")

    # Unsubscribe possible subscriptions. This is tried only once.
    if (srvObj.getCfg().getAutoUnsubscribe()):
        for subscrObj in srvObj.getSubscrStatusList():
            subs_host, subs_port = subscrObj.getHostId(), subscrObj.getPortNo()
            try:
                resp = ngamsHttpUtils.httpGet(subs_host, subs_port, NGAMS_UNSUBSCRIBE_CMD, [["url", subscrObj.getId()]])
                with contextlib.closing(resp):
                    stat = ngamsStatus.to_status(resp, "%s:%d" % (subs_host, subs_port), NGAMS_UNSUBSCRIBE_CMD)
                if stat.getStatus() != NGAMS_SUCCESS:
                    logger.error("Error when unsubscribing from %s:%d: %s", subs_host, subs_port, stat.getMessage())
            except Exception:
                msg = "Problem occurred while cancelling subscription " +\
                      "(host/port): %s/%d. Subscriber ID: %s"
                logger.exception(msg, subscrObj.getHostId(),
                                 subscrObj.getPortNo(), subscrObj.getId())
        srvObj.resetSubscrStatusList()

    # Check if there are files located in the Staging Areas of the
    # Disks. In case yes, move these to the Back-Log Area to have them
    # treated at a later stage.
    checkStagingAreas(srvObj)

    # Dump disk info on all disks, invoke the Offline Plug-In to prepare the
    # disks for offline, and mark the disks as unmounted in the DB.
    ngamsDiskUtils.dumpDiskInfoAllDisks(srvObj.getHostId(),
                                        srvObj.getDb(), srvObj.getCfg())
    plugIn = srvObj.getCfg().getOfflinePlugIn()
    logger.info("Invoking System Offline Plug-In: %s(srvObj, reqPropsObj)", plugIn)
    plugInMethod = loadPlugInEntryPoint(plugIn)
    plugInMethod(srvObj, reqPropsObj)
    ngamsDiskUtils.markDisksAsUnmountedInDb(srvObj.getHostId(), srvObj.getDb(), srvObj.getCfg())

    # Send out possible Retained Email Notification Messages.
    flushMsg = "NOTE: Distribution of retained Email Notification Messages " +\
               "forced at Offline"
    ngamsNotification.checkNotifRetBuf(srvObj.getHostId(), srvObj.getCfg(), 1, flushMsg)

    logger.info("NG/AMS prepared for Offline State")


def wakeUpHost(srvObj,
               suspHost):
    """
    Wake up a host which has been suspended. After invoking the appropriate
    Wake-Up Plug-In it is checked within the time-out defined in the
    NG/AMS Configuration File if the server, which was woken up is up
    and running. If this is not the case within the specified time-out,
    an exception is thrown.

    srvObj:         Reference to instance of the NG/AMS server (ngamsServer).

    suspHost:       Host name of the suspended host (string).

    Returns:        Void.
    """
    wakeUpPi = srvObj.getCfg().getWakeUpPlugIn()
    portNo = srvObj.getDb().getPortNoFromHostId(suspHost)
    try:
        plugInMethod = loadPlugInEntryPoint(wakeUpPi)
        plugInMethod(srvObj, suspHost)

        ipAddress = srvObj.getDb().getIpFromHostId(suspHost)
        ngamsHighLevelLib.pingServer(ipAddress, portNo,
                                     srvObj.getCfg().getWakeUpCallTimeOut())
    except Exception:
        logger.exception("Error waking up host %s", suspHost)
        raise


def checkStagingAreas(srvObj):
    """
    Check the Staging Areas of the disks registered, and in case files
    are found on these move htese to the Back-Log Area.

    srvObj:    Reference to NG/AMS server class object (ngamsServer).

    Returns:   Void.
    """
    diskList = ngamsDiskUtils.\
               getDiskInfoForMountedDisks(srvObj.getDb(), srvObj.getHostId(),
                                          srvObj.getCfg().\
                                          getRootDirectory())
    # Generate first a dictionary with all files in the staging areas.
    stagingFileDic = {}
    for disk in diskList:
        stagingArea = disk.getStagingArea()
        if (stagingArea != ""):
            fileList = glob.glob(stagingArea + "/*")
            fileList += glob.glob(stagingArea + "/.NGAMS*")
            for filename in fileList: stagingFileDic[filename] = 1
    # Go through all files in the staging file dictionary and move them to
    # the Bad Files Area.
    for filename in stagingFileDic.keys():
        ngamsHighLevelLib.moveFile2BadDir(srvObj.getCfg(), filename)


def genIntAuthHdr(srvObj):
    """
    Generate an internal HTTP Authentication Header if possible.

    srvObj:    Reference to NG/AMS server class object (ngamsServer).

    Returns:   Authentication code or '' if no internal NGAS user defined
               (string).
    """
    # If the NGAS Internal Authorization User is defined generate
    # an internal Authorization Code.
    if (srvObj.getCfg().hasAuthUser(NGAMS_HTTP_INT_AUTH_USER)):
        authHttpHdrVal = srvObj.getCfg().\
                         getAuthHttpHdrVal(NGAMS_HTTP_INT_AUTH_USER)
    else:
        authHttpHdrVal = ""
    return authHttpHdrVal


# EOF
