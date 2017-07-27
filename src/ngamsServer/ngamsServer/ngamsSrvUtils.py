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

import glob
import logging
import os
import re
import string
import thread
import threading
import time

from ngamsLib.ngamsCore import NGAMS_NOT_RUN_STATE,\
    NGAMS_ONLINE_STATE, NGAMS_DEFINE, NGAMS_SUBSCRIBE_CMD,\
    NGAMS_SUCCESS, TRACE, genLog, NGAMS_DISK_INFO, checkCreatePath,\
    NGAMS_SUBSCRIBER_THR, NGAMS_UNSUBSCRIBE_CMD, NGAMS_HTTP_INT_AUTH_USER,\
    loadPlugInEntryPoint, toiso8601, fromiso8601
from ngamsLib import ngamsStatus, ngamsLib, ngamsHttpUtils
from ngamsLib import ngamsPhysDiskInfo
from ngamsLib import ngamsSubscriber
from ngamsLib import ngamsHighLevelLib, ngamsDiskUtils
from ngamsLib import ngamsNotification
import ngamsArchiveUtils
import ngamsSubscriptionThread


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


def _subscriberThread(srvObj,
                      dummy):
    """
    To be executed as thread, which tries periodically to subscribe this
    NG/AMS Server to one or more Data Providers.

    srvObj:      Reference to NG/AMS Server class instance (ngamsServer).

    dummy:       Needed by the thread handling ...

    Returns:     Void.
    """
    if (not srvObj.getCfg().getSubscriptionsDic().keys()): thread.exit()

    # Generate a list with Subscriptions from the configuration file.
    subscrList = []
    for subscrId in srvObj.getCfg().getSubscriptionsDic().keys():
        subscrList.append(srvObj.getCfg().getSubscriptionsDic()[subscrId])

    myPort = srvObj.getCfg().getPortNo()
    myHost = srvObj.getHostId()

    # Run this loop until all requested Subscriptions have been
    # successfully executed, or until the server goes Offline.
    subscrCount = 0
    statObj = ngamsStatus.ngamsStatus()
    while (1):
        if (not srvObj.getThreadRunPermission()):
            logger.info("Terminating Subscriber thread ...")
            thread.exit()
        for idx in range(len(subscrList)):
            if (not srvObj.getThreadRunPermission()):
                logger.info("Terminating Subscriber thread ...")
                thread.exit()

            subscrObj = subscrList[idx]
            if (subscrObj.getHostId() == NGAMS_DEFINE):
                idx += 1
                continue
            hostPort = subscrObj.getHostId() + "/" + str(subscrObj.getPortNo())

            # Check that the server is not subscribing to itself.
            if ((myPort == subscrObj.getPortNo()) and \
                (myHost == subscrObj.getHostId())):
                logger.warning("NG/AMS cannot subscribe to itself - ignoring " +\
                        "subscription (host/port): %s", hostPort)
                del subscrList[idx]
                break

            logger.debug("Attempting to subscribe to Data Provider (host/port): %s", hostPort)
            pars = [["priority", subscrObj.getPriority()],
                    ["url",      subscrObj.getUrl()]]
            if (subscrObj.getId()):
                pars.append(["subscr_id", subscrObj.getId()])
            if (subscrObj.getStartDate() is not None):
                pars.append(["start_date", toiso8601(subscrObj.getStartDate())])
            if (subscrObj.getFilterPi()):
                pars.append(["filter_plug_in", subscrObj.getFilterPi()])
            if (subscrObj.getFilterPiPars()):
                pars.append(["plug_in_pars", subscrObj.getFilterPiPars()])
            ex = ""
            statObj.clear()
            try:
                resp, stat, msgObj, data = \
                      ngamsHttpUtils.httpGet(subscrObj.getHostId(),
                                       subscrObj.getPortNo(),
                                       NGAMS_SUBSCRIBE_CMD, pars)
                statObj.unpackXmlDoc(data, 1)
            except Exception as e:
                ex = "Exception: " + str(e)
            if (statObj.getStatus() == NGAMS_SUCCESS):
                subscrCount += 1
                logger.info("Successfully subscribed to Data Provider (host/port): %s/%s",
                            subscrObj.getHostId(), str(subscrObj.getPortNo()))
                # Add this reference in the Subscription Status List to make it
                # possible to unsubscribe the subscriptions when going Offline.
                # Should be semaphore protected to avoid conflicts.
                srvObj.getSubscrStatusList().append(subscrObj)
                del subscrList[idx]
                break
            else:
                errMsg = "Failed in subscribing to Data Provider " +\
                         "(host/port): " + subscrObj.getHostId() + "/" +\
                         str(subscrObj.getPortNo())
                if (ex != ""): errMsg += ". " + ex
                errMsg += ". Retrying later."
                logger.debug(errMsg)
        if (not len(subscrList)): break
        for n in range(20):
            time.sleep(0.5)
            if (not srvObj.getThreadRunPermission()):
                logger.info("Terminating Subscriber Thread ...")
                thread.exit()

    if (subscrCount):
        logger.info("Successfully established %d Subscription(s)", subscrCount)
    else:
        logger.info("No Subscriptions established")
    thread.exit()


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
    T = TRACE()

    diskInfoDic = {}

    plugIn = srvObj.getCfg().getOnlinePlugIn()
    if (not srvObj.getCfg().getSimulation()):
        logger.info("Invoking System Online Plug-In: %s(srvObj)", plugIn)
        plugInMethod = loadPlugInEntryPoint(plugIn)
        diskInfoDic = plugInMethod(srvObj, reqPropsObj)
        if (len(diskInfoDic) == 0):
            if (not ngamsLib.trueArchiveProxySrv(srvObj.getCfg())):
                errMsg = genLog("NGAMS_NOTICE_NO_DISKS_AVAIL",
                                [srvObj.getHostId(),
                                 srvObj.getHostId()])
                logger.warning(errMsg)
    else:
        if (srvObj.getCfg().getAllowArchiveReq()):
            logger.debug("Running as a simulated archiving system - generating " +\
                 "simulated disk info ...")
            portNo = 0
            slotIdLst = srvObj.getCfg().getSlotIds()
            for slotId in slotIdLst:
                if (slotId != ""):
                    storageSet = srvObj.getCfg().\
                                 getStorageSetFromSlotId(slotId)
                    if (storageSet.getMainDiskSlotId() == slotId):
                        diskType = "Main"
                    else:
                        diskType = "Rep"
                    mtRootDir = srvObj.getCfg().getRootDirectory()
                    mtPt = os.path.normpath(mtRootDir +\
                                            "/"+storageSet.getStorageSetId() +\
                                            "-" + diskType + "-" + str(slotId))
                    diskId = re.sub("/", "-", mtPt)[1:]
                    serialNo = "SERIAL-NUMBER-" + str(portNo)
                    manufact = "TEST-MANUFACTURER"
                    diskInfoDic[slotId] = ngamsPhysDiskInfo.ngamsPhysDiskInfo()
                    diskInfoDic[slotId].\
                                          setPortNo(portNo).\
                                          setSlotId(slotId).\
                                          setMountPoint(mtPt).\
                                          setStatus("OK").\
                                          setCapacityGb(100).\
                                          setModel("TEST-MODEL").\
                                          setSerialNo(serialNo).\
                                          setType("TEST-TYPE").\
                                          setManufacturer(manufact).\
                                          setDiskId(diskId).\
                                          setDeviceName("/dev/dummy" + slotId)
                    # We create the mount directory if it does not exist
                    checkCreatePath(mtPt)

                    portNo = portNo + 1
        else:
            # It is not an Archiving System. We check which of the directories
            # in the Ngas.RootDirectory contain an NgasDiskInfo file.
            # These are considered as available disks in simulation mode.
            baseDir = srvObj.getCfg().getRootDirectory()
            dirList = glob.glob(os.path.normpath(baseDir + "/*"))
            for mtPt in dirList:
                checkPath = os.path.normpath(mtPt + "/" + NGAMS_DISK_INFO)
                logger.debug("Checking if path exists: %s", checkPath)
                if (os.path.exists(checkPath)):
                    slotId = string.split(mtPt, "-")[-1]
                    portNo = (int(slotId) - 1)
                    diskId = re.sub("/", "-", mtPt)
                    serialNo = "SERIAL-NUMBER-" + str(portNo)
                    diskInfoDic[slotId]=ngamsPhysDiskInfo.ngamsPhysDiskInfo().\
                                         setPortNo(portNo).\
                                         setSlotId(slotId).\
                                         setMountPoint(mtPt).\
                                         setStatus("OK").\
                                         setCapacityGb(100).\
                                         setModel("TEST-MODEL").\
                                         setSerialNo(serialNo).\
                                         setType("TEST-TYPE").\
                                         setManufacturer("TEST-MANUFACTURER").\
                                         setDiskId(diskId).\
                                         setDeviceName("/dev/dummy" + slotId)

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
    logger.info("Creating %d subscrption objects from subscription DB info", len(subscrList))
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
    srvObj.setThreadRunPermission(1)
    srvObj.startJanitorThread()
    srvObj.startDataCheckThread()
    ngamsSubscriptionThread.startSubscriptionThread(srvObj)
    srvObj.startUserServiceThread()
    srvObj.startMirControlThread()
    srvObj.startCacheControlThread()

    # Change state to Online.
    srvObj.setState(NGAMS_ONLINE_STATE)

    # If specified in the configuration file that this server should act as a
    # Subscriber, then subscribe to the Data Providers specified. We do this
    # in a thread so that the server will continuesly try to  subscribe in
    # case not possible for one or more Subscribers when first tried.
    if (srvObj.getCfg().getSubscrEnable()):
        if (srvObj.getCfg().getSubscriptionsDic().keys()):
            thrObj = threading.Thread(None, _subscriberThread,
                                      NGAMS_SUBSCRIBER_THR, (srvObj, None))
            thrObj.setDaemon(0)
            thrObj.start()

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
    srvObj.stopJanitorThread()
    srvObj.stopDataCheckThread()
    ngamsSubscriptionThread.stopSubscriptionThread(srvObj)
    srvObj.stopUserServiceThread()
    srvObj.stopMirControlThread()
    srvObj.stopCacheControlThread()
    srvObj.setThreadRunPermission(0)

    logger.debug("Prepare NG/AMS for Offline State ...")

    # Unsubscribe possible subscriptions. This is only tried once.
    if (srvObj.getCfg().getAutoUnsubscribe()):
        for subscrObj in srvObj.getSubscrStatusList():
            try:
                resp, stat, msgObj, data = \
                      ngamsHttpUtils.httpGet(subscrObj.getHostId(),
                                       subscrObj.getPortNo(),
                                       NGAMS_UNSUBSCRIBE_CMD,
                                       [["url", subscrObj.getId()]])
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
    if (srvObj.getCfg().getSimulation() == 0):
        logger.info("Invoking System Offline Plug-In: %s(srvObj, reqPropsObj)", plugIn)
        plugInMethod = loadPlugInEntryPoint(plugIn)
        plugInRes = plugInMethod(srvObj, reqPropsObj)
    else:
        pass
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
    T = TRACE()

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
    T = TRACE()

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
