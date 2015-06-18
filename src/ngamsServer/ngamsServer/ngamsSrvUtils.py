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

import os, re, string, thread, threading, time, glob, commands

from pccLog import PccLog
from pccUt import PccUtTime
from ngamsLib.ngamsCore import logFlush, getLocation, info, NGAMS_NOT_RUN_STATE,\
    NGAMS_ONLINE_STATE, getHostId, NGAMS_DEFINE, warning, NGAMS_SUBSCRIBE_CMD,\
    NGAMS_SUCCESS, TRACE, genLog, notice, NGAMS_DISK_INFO, checkCreatePath,\
    error, NGAMS_SUBSCRIBER_THR, NGAMS_UNSUBSCRIBE_CMD, NGAMS_HTTP_INT_AUTH_USER,\
    loadPlugInEntryPoint
from ngamsLib import ngamsStatus, ngamsLib
from ngamsLib import ngamsPhysDiskInfo
from ngamsLib import ngamsSubscriber
from ngamsLib import ngamsHighLevelLib, ngamsDiskUtils
from ngamsLib import ngamsNotification
import ngamsArchiveUtils
import ngamsJanitorThread, ngamsDataCheckThread, ngamsSubscriptionThread
import ngamsUserServiceThread, ngamsMirroringControlThread


def ngamsBaseExitHandler(srvObj,
                         signalNo,
                         killServer = 1,
                         exitCode = 0,
                         delPidFile = 1):
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
    info(1,"In NG/AMS Exit Handler - received signal: " + str(signalNo))
    info(1,"NG/AMS Exit Handler - cleaning up ...")

    if (srvObj.getState() == NGAMS_ONLINE_STATE): handleOffline(srvObj)

    # Update the ngas_hosts table.
    srvObj.updateHostInfo("", None, 0, 0, 0, 0, 0, NGAMS_NOT_RUN_STATE)

    info(1,"NG/AMS Exit Handler finished cleaning up - terminating server ...")

    if (killServer):
        srvObj.killServer(delPidFile)
    else:
        try:
            logFile = srvObj.getCfg().getLocalLogFile()
            logPath = os.path.dirname(logFile)
            rotLogFile = "LOG-ROTATE-" +\
                        PccUtTime.TimeStamp().getTimeStamp()+\
                        ".nglog"
            rotLogFile = os.path.normpath(logPath + "/" + rotLogFile)
            PccLog.info(1, "Rotating log file: %s -> %s" %\
                        (logFile, rotLogFile), getLocation())
            logFlush()
            commands.getstatusoutput("mv " + logFile + " " +\
                                                         rotLogFile)
        except:
            pass
    os._exit(exitCode)


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
    myHost = getHostId()

    # Run this loop until all requested Subscriptions have been
    # successfully executed, or until the server goes Offline.
    subscrCount = 0
    statObj = ngamsStatus.ngamsStatus()
    while (1):
        if (not srvObj.getThreadRunPermission()):
            info(2,"Terminating Subscriber thread ...")
            thread.exit()
        for idx in range(len(subscrList)):
            if (not srvObj.getThreadRunPermission()):
                info(2,"Terminating Subscriber thread ...")
                thread.exit()

            subscrObj = subscrList[idx]
            if (subscrObj.getHostId() == NGAMS_DEFINE):
                idx += 1
                continue
            hostPort = subscrObj.getHostId() + "/" + str(subscrObj.getPortNo())

            # Check that the server is not subscribing to itself.
            if ((myPort == subscrObj.getPortNo()) and \
                (myHost == subscrObj.getHostId())):
                warning("NG/AMS cannot subscribe to itself - ignoring " +\
                        "subscription (host/port): " + hostPort)
                del subscrList[idx]
                break

            info(4,"Attempting to subscribe to Data Provider (host/port): " +\
                 hostPort + " ...")
            pars = [["priority", subscrObj.getPriority()],
                    ["url",      subscrObj.getUrl()]]
            if (subscrObj.getId()):
                pars.append(["subscr_id", subscrObj.getId()])
            if (subscrObj.getStartDate()):
                pars.append(["start_date", subscrObj.getStartDate()])
            if (subscrObj.getFilterPi()):
                pars.append(["filter_plug_in", subscrObj.getFilterPi()])
            if (subscrObj.getFilterPiPars()):
                pars.append(["plug_in_pars", subscrObj.getFilterPiPars()])
            ex = ""
            statObj.clear()
            try:
                resp, stat, msgObj, data = \
                      ngamsLib.httpGet(subscrObj.getHostId(),
                                       subscrObj.getPortNo(),
                                       NGAMS_SUBSCRIBE_CMD, 1, pars)
                statObj.unpackXmlDoc(data, 1)
            except Exception, e:
                ex = "Exception: " + str(e)
            if (statObj.getStatus() == NGAMS_SUCCESS):
                subscrCount += 1
                info(1,"Successfully subscribed to Data Provider " +\
                     "(host/port): " + subscrObj.getHostId() + "/" +\
                     str(subscrObj.getPortNo()))
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
                info(3,errMsg)
        if (not len(subscrList)): break
        for n in range(20):
            time.sleep(0.5)
            if (not srvObj.getThreadRunPermission()):
                info(2,"Terminating Subscriber Thread ...")
                thread.exit()

    if (subscrCount):
        info(2,"Successfully established " + str(subscrCount) +\
             " Subscription(s)")
    else:
        info(2,"No Subscriptions established")
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
        info(3,"Invoking System Online Plug-In: " + plugIn + "(srvObj)")
        plugInMethod = loadPlugInEntryPoint(plugIn)
        diskInfoDic = plugInMethod(srvObj, reqPropsObj)
        if (len(diskInfoDic) == 0):
            if (not ngamsLib.trueArchiveProxySrv(srvObj.getCfg())):
                errMsg = genLog("NGAMS_NOTICE_NO_DISKS_AVAIL",
                                [ngamsHighLevelLib.genNgasId(srvObj.getCfg()),
                                 getHostId()])
                notice(errMsg)
    else:
        if (srvObj.getCfg().getAllowArchiveReq()):
            info(3,"Running as a simulated archiving system - generating " +\
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
                info(5,"Checking if path exists: " + checkPath + " ...")
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

    info(5,"Disk Dictionary: " + str(diskInfoDic))
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
    info(3,"Prepare NGAS and NG/AMS for Online State ...")

    # Re-load Configuration + check disk configuration.
    srvObj.loadCfg()
    for stream in srvObj.getCfg().getStreamList():
        srvObj.getMimeTypeDic()[stream.getMimeType()] = stream.getPlugIn()

    # Flush/send out possible retained Email Notification Messages.
    flushMsg = "NOTE: Distribution of retained Email Notification Messages " +\
               "forced at Online"
    ngamsNotification.checkNotifRetBuf(srvObj.getCfg(), 1, flushMsg)

    # Get possible Subscribers from the DB.
    subscrList = srvObj.getDb().getSubscriberInfo("", getHostId(),
                                                  srvObj.getCfg().getPortNo())
    num_bl =  srvObj.getDb().getSubscrBackLogCount(getHostId(), srvObj.getCfg().getPortNo())
    #debug_chen
    if (num_bl > 0):
        info(3, 'Preset the backlog count to %d' % num_bl)
        srvObj.presetSubcrBackLogCount(num_bl)

    for subscrInfo in subscrList:
        tmpSubscrObj = ngamsSubscriber.ngamsSubscriber().\
                       unpackSqlResult(subscrInfo)
        # Take only subscribers for this NG/AMS Server.
        if ((tmpSubscrObj.getHostId() == getHostId()) and
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
        ngamsDiskUtils.checkDisks(srvObj.getDb(), srvObj.getCfg(),
                                  srvObj.getDiskDic())

        # Write status file on disk.
        ngamsDiskUtils.dumpDiskInfoAllDisks(srvObj.getDb(), srvObj.getCfg())
    except Exception, e:
        errMsg = "Error occurred while bringing the system Online: " + str(e)
        error(errMsg)
        handleOffline(srvObj)
        raise e

    # Check that there is enough disk space in the various system directories.
    srvObj.checkDiskSpaceSat()

    # Check if files are located in the Staging Areas of the Disks. In case
    # yes, move them to the Back-Log Area to have them treated later.
    checkStagingAreas(srvObj)

    # Start threads + inform threads that they are allowed to execute.
    srvObj.setThreadRunPermission(1)
    ngamsJanitorThread.startJanitorThread(srvObj)
    ngamsDataCheckThread.startDataCheckThread(srvObj)
    ngamsSubscriptionThread.startSubscriptionThread(srvObj)
    ngamsUserServiceThread.startUserServiceThread(srvObj)
    ngamsMirroringControlThread.startMirControlThread(srvObj)
    if (srvObj.getCachingActive()):
        import ngamsCacheControlThread
        ngamsCacheControlThread.startCacheControlThread(srvObj)

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

    info(3,"NG/AMS prepared for Online State")


def handleOffline(srvObj,
                  reqPropsObj = None,
                  stopJanitorThr = 1):
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
    if (stopJanitorThr): ngamsJanitorThread.stopJanitorThread(srvObj)
    ngamsDataCheckThread.stopDataCheckThread(srvObj)
    ngamsSubscriptionThread.stopSubscriptionThread(srvObj)
    ngamsUserServiceThread.stopUserServiceThread(srvObj)
    ngamsMirroringControlThread.stopMirControlThread(srvObj)
    srvObj.setThreadRunPermission(0)

    info(3, "Prepare NG/AMS for Offline State ...")

    # Unsubscribe possible subscriptions. This is only tried once.
    if (srvObj.getCfg().getAutoUnsubscribe()):
        for subscrObj in srvObj.getSubscrStatusList():
            try:
                resp, stat, msgObj, data = \
                      ngamsLib.httpGet(subscrObj.getHostId(),
                                       subscrObj.getPortNo(),
                                       NGAMS_UNSUBSCRIBE_CMD, 1,
                                       [["url", subscrObj.getId()]])
            except Exception, e:
                warning("Problem occurred while cancelling subscription " +\
                        "(host/port):" + subscrObj.getHostId() + "/" +\
                        str(subscrObj.getPortNo()) + ". Subscriber ID: " +\
                        subscrObj.getId() + ". Exception: " + str(e))
        srvObj.resetSubscrStatusList()

    # Check if there are files located in the Staging Areas of the
    # Disks. In case yes, move these to the Back-Log Area to have them
    # treated at a later stage.
    checkStagingAreas(srvObj)

    # Dump disk info on all disks, invoke the Offline Plug-In to prepare the
    # disks for offline, and mark the disks as unmounted in the DB.
    ngamsDiskUtils.dumpDiskInfoAllDisks(srvObj.getDb(), srvObj.getCfg())
    plugIn = srvObj.getCfg().getOfflinePlugIn()
    if (srvObj.getCfg().getSimulation() == 0):
        info(3, "Invoking System Offline Plug-In: " + plugIn +\
             "(srvObj, reqPropsObj)")
        plugInMethod = loadPlugInEntryPoint(plugIn)
        plugInRes = plugInMethod(srvObj, reqPropsObj)
    else:
        pass
    ngamsDiskUtils.markDisksAsUnmountedInDb(srvObj.getDb(), srvObj.getCfg())

    # Send out possible Retained Email Notification Messages.
    flushMsg = "NOTE: Distribution of retained Email Notification Messages " +\
               "forced at Offline"
    ngamsNotification.checkNotifRetBuf(srvObj.getCfg(), 1, flushMsg)

    info(3,"NG/AMS prepared for Offline State")


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
        ngamsHighLevelLib.pingServer(suspHost, ipAddress, portNo,
                                     srvObj.getCfg().getWakeUpCallTimeOut())
    except Exception, e:
        errMsg = "Error waking up host: " + suspHost + ". Error: " + str(e)
        notice(errMsg)
        raise Exception, errMsg


def checkStagingAreas(srvObj):
    """
    Check the Staging Areas of the disks registered, and in case files
    are found on these move htese to the Back-Log Area.

    srvObj:    Reference to NG/AMS server class object (ngamsServer).

    Returns:   Void.
    """
    T = TRACE()

    diskList = ngamsDiskUtils.\
               getDiskInfoForMountedDisks(srvObj.getDb(), getHostId(),
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
        ngamsHighLevelLib.moveFile2BadDir(srvObj.getCfg(), filename, filename)


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
