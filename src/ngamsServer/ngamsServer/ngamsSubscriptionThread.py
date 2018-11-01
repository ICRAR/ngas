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
# "@(#) $Id: ngamsSubscriptionThread.py,v 1.10 2009/11/25 21:47:11 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  06/11/2002  Created
#

"""
This module contains the code for the (Data) Subscription Thread, which is
used to handle the delivery of data to Subscribers.
"""

import logging
import threading
import time
import os
import base64

from six.moves.urllib import parse as urlparse  # @UnresolvedImport
from six.moves.queue import Queue, Empty, PriorityQueue  # @UnresolvedImport

from . import ngamsCacheControlThread
from ngamsLib.ngamsCore import NGAMS_SUBSCRIPTION_THR, isoTime2Secs,\
    NGAMS_SUBSCR_BACK_LOG, NGAMS_DELIVERY_THR,\
    NGAMS_HTTP_INT_AUTH_USER, NGAMS_REARCHIVE_CMD, NGAMS_FAILURE,\
    NGAMS_HTTP_SUCCESS, NGAMS_SUCCESS, getFileSize, rmFile, loadPlugInEntryPoint,\
    toiso8601, NGAMS_HTTP_HDR_CHECKSUM, NGAMS_HTTP_HDR_FILE_INFO, fromiso8601
from ngamsLib import ngamsDbm, ngamsStatus, ngamsHighLevelLib, ngamsFileInfo, ngamsDbCore,\
    ngamsHttpUtils


logger = logging.getLogger(__name__)

# TODO:
# - Should not hardcode no_versioning=1.
# - Should not back-log buffer data 'physically'.


# Some 'constant abbreviatins' used in this module.
FILE_ID   = 0
FILE_NM   = 1
FILE_VER  = 2
FILE_DATE = 3
FILE_MIME = 4
FILE_DISK_ID   = 5
FILE_BL   = 6

FPI_MODE_METADATA_ONLY = 1 # only check meta-data related conditions (e.g. project id), a preliminary filtering
FPI_MODE_DATA_ONLY = 2 # only check data related conditions (e.g. if data has been sent, if it is offline, etc.)
FPI_MODE_BOTH = 3 # check both

NGAS_JOB_DELIMIT = "__nj__"
NGAS_JOB_URI_SCHEME = "ngasjob"

def startSubscriptionThread(srvObj):
    """
    Start the Data Subscription Thread.

    srvObj:     Reference to server object (ngamsServer).

    Returns:    Void.
    """
    logger.debug("Starting Subscription Thread ...")
    srvObj._subscriptionRunSync.set()
    args = (srvObj, None)
    srvObj._subscriptionThread = threading.Thread(None, subscriptionThread,
                                                  NGAMS_SUBSCRIPTION_THR, args)
    srvObj._subscriptionThread.setDaemon(0)
    srvObj._subscriptionThread.start()

    if (srvObj._deliveryStopSync.isSet()):
        srvObj._deliveryStopSync.clear() #revoke the shutdown (offline) setting

    logger.info("Subscription Thread started")


def stopSubscriptionThread(srvObj):
    """
    Stop the Data Subscription Thread.

    srvObj:     Reference to server object (ngamsServer).

    Returns:    Void.
    """
    logger.debug("Stopping Subscription Thread ...")
    srvObj._subscriptionStopSyncConf.clear()
    srvObj._subscriptionStopSync.set()
    srvObj._deliveryStopSync.set()
    srvObj._subscriptionRunSync.set()
    srvObj._subscriptionStopSyncConf.wait(10)
    srvObj._subscriptionStopSync.clear()
    srvObj._subscriptionThread = None
    #_backupQueueToBacklog(srvObj) # this is too time-consuming. No need any more, since the thread will trigger all subscribers when it is just started
    logger.info("Subscription Thread stopped")


def _checkStopSubscriptionThread(srvObj):
    """
    The function is used by the Subscription Thread when checking if it
    should stop execution. If this is the case, the function will terminate
    the thread.

    srvObj:     Reference to server object (ngamsServer).

    Returns:    Void.
    """
    if (srvObj._subscriptionStopSync.isSet()):
        logger.debug("Stopping Subscription Thread ...")
        srvObj._subscriptionStopSyncConf.set()
        raise Exception("_STOP_SUBSCRIPTION_THREAD_")


def _checkStopDataDeliveryThread(srvObj, subscrbId):
    """
    Function used by the Data Delivery Threads to check if they should
    stop execution.

    srvObj:     Reference to server object (ngamsServer).

    Returns:    Void.
    """
    deliveryThreadRefDic = srvObj._subscrDeliveryThreadDicRef
    tname = threading.current_thread().name
    if (srvObj._deliveryStopSync.isSet() or # server is about to shutdown
        (tname not in deliveryThreadRefDic) or # this thread's reference has been removed by the USUBSCRIBE command, see ngamsPlugIns/ngamsCmd_USUBSCRIBE.changeNumThreads()
        (subscrbId not in srvObj.getSubscriberDic())): # the UNSUBSCRIBE command is issued
        logger.debug("Stopping Data Delivery Thread ... %s", tname)
        raise Exception("_STOP_DELIVERY_THREAD_%s" % tname)


def _waitForScheduling(srvObj):
    """
    Small function to let the Data Subscription Thread wait to be scheduled
    to check if data should be deliveried.

    srvObj:     Reference to server object (ngamsServer).

    Returns:    Tuple with list of files to check for delivery and Subscribers
                that should be checked to see if there is data to deliver
                (tuple/string, ngamsSubscriber).
    """
    logger.debug("Data Subscription Thread suspending itself (waiting to " +\
                 "be scheduled) ...")
    # If there are no pending deliveries in the Subscription Back-Log,
    # we suspend until the thread is woken up by another thread, e.g. when
    # new data is available.
    if (srvObj.getSubcrBackLogCount() > 0):
        suspTime = isoTime2Secs(srvObj.getCfg().getSubscrSuspTime())
        #debug_chen
        logger.debug('Subscription thread will suspend %s seconds before re-trying delivering back-logged files', str(suspTime))
        srvObj._subscriptionRunSync.wait(suspTime)
    elif (srvObj.getDataMoverOnlyActive()):
        tmout = isoTime2Secs(srvObj.getCfg().getDataMoverSuspenstionTime()) # in general, tmout > suspTime
        #debug_chen
        logger.debug('Data mover thread will suspend %s seconds before re-trying querying the db to get new files', str(tmout))
        srvObj._subscriptionRunSync.wait(tmout)
    else:
        logger.debug("Data Subscription Thread is going to sleep ...")
        srvObj._subscriptionRunSync.wait()

    _checkStopSubscriptionThread(srvObj)
    logger.debug("Data Subscription Thread received wake-up signal ...")
    try:
        srvObj._subscriptionSem.acquire()
        srvObj._subscriptionRunSync.clear()
        filenames = srvObj._subscriptionFileList
        srvObj._subscriptionFileList = []
        if (srvObj.getDataMoverOnlyActive()):
            return (filenames, [])
        subscrObjs = srvObj._subscriptionSubscrList
        srvObj._subscriptionSubscrList = []
        return (filenames, subscrObjs)
    except Exception as e:
        errMsg = "Error occurred in ngamsSubscriptionThread." +\
                  "_waitForScheduling(). Exception: " + str(e)
        logger.warning(errMsg)
        return ([], [])
    finally:
        srvObj._subscriptionSem.release()


def _addFileDeliveryDic(subscrId,
                        fileInfo,
                        deliverReqDic,
                        fileDeliveryCountDic,
                        fileDeliveryCountDic_Sem,
                        srvObj):
    """
    Add a file in the delivery dictionary. If file already registered,
    replace the existing entry only if (1) the old entry is not a back-log buffered file, and (2) the
    new entry is back-log buffered

    subscrId:         Subscriber ID (string).

    fileInfo:         List with file infomation as returned by
                      ngams.getFileSummary2() or on the internal format (list).

    deliverReqDic:    Dictionary with Subscriber IDs as keys referring
                      to lists with the information about the files to
                      deliver to each of the Subscribers (dictionary/list).

    Returns:          Void.
    """
    fileInfo            = _convertFileInfo(fileInfo)
    fileId              = fileInfo[FILE_ID]
    filename            = fileInfo[FILE_NM]
    fileVersion         = fileInfo[FILE_VER]
    fileBackLogBuffered = fileInfo[FILE_BL]
    replaceWithBL = 0
    add = 1
    #First, Check if the file is already registered for that Subscriber.
    if (fileBackLogBuffered == NGAMS_SUBSCR_BACK_LOG and subscrId in deliverReqDic):
        for idx in range(len(deliverReqDic[subscrId])): #TODO - this for loop should be a hashtable lookup!!
            tstFileInfo            = deliverReqDic[subscrId][idx]
            tstFilename            = tstFileInfo[FILE_NM]
            tstFileVersion         = tstFileInfo[FILE_VER]
            tstFileBackLogBuffered = tstFileInfo[FILE_BL]
            if ((tstFilename == filename) and
                (tstFileVersion == fileVersion) and
                tstFileBackLogBuffered != NGAMS_SUBSCR_BACK_LOG):

                # The new entry is back-log buffered, the old not,
                # replace the old entry.
                deliverReqDic[subscrId][idx] = fileInfo
                add = 0
                replaceWithBL = 1
                break

    #Second, Check if the file is a back-logged file that has been previously registered
    if (fileBackLogBuffered == NGAMS_SUBSCR_BACK_LOG):
        if (subscrId not in srvObj._subscrBlScheduledDic):
            srvObj._subscrBlScheduledDic[subscrId] = {}
        k = _fileKey(fileId, fileVersion)
        if (k in srvObj._subscrBlScheduledDic[subscrId]):
            add = 0 # if this is an old entry, do not add again
        else:
            # if this is a new entry, maybe it will be added unless the first check set 'add' to 0.
            # Must occupy the dic key space
            srvObj._subscrBlScheduledDic[subscrId][k] = None
    if (add):
        if (subscrId in deliverReqDic):
            deliverReqDic[subscrId].append(fileInfo)
        else:
            # It was a new entry, create new list for this Subscriber.
            deliverReqDic[subscrId] = [fileInfo]

    if (srvObj.getCachingActive() and
        (fileBackLogBuffered != NGAMS_SUBSCR_BACK_LOG or replaceWithBL)):
        # if the server is running in a cache mode,
        #   then  prepare for marking deletion - increase by 1 the reference count to this file
        #   but do not bother if it is a backlogged file, since it has its own deletion mechanism
        fkey = fileInfo[FILE_ID] + "/" + str(fileVersion)
        fileDeliveryCountDic_Sem.acquire()
        try:
            if (fkey in fileDeliveryCountDic):
                if (fileBackLogBuffered != NGAMS_SUBSCR_BACK_LOG):
                    fileDeliveryCountDic[fkey]  += 1
                elif (replaceWithBL):
                    fileDeliveryCountDic[fkey]  -= 1
                    if (fileDeliveryCountDic[fkey] == 0):
                        del fileDeliveryCountDic[fkey]
            else:
                fileDeliveryCountDic[fkey] = 1
        finally:
            fileDeliveryCountDic_Sem.release()

def _checkIfDeliverFile(srvObj,
                        subscrObj,
                        fileInfo,
                        deliverReqDic,
                        deliveredStatus,
                        scheduledStatus,
                        fileDeliveryCountDic,
                        fileDeliveryCountDic_Sem,
                        explicitFileDelivery = False):
    """
    Analyze if a file should be delivered to a Subscriber.

    srvObj:           Reference to server object (ngamsServer).

    subscrObj:        Subscriber object (ngamsSubscriber).

    fileInfo:         List with file infomation as returned by
                      ngams.getFileSummary2() (sub-list) (list).

    deliverReqDic:    Dictionary with Subscriber IDs as keys referring
                      to lists with the information about the files to
                      deliver to each of the Subscribers (dictionary/list).

    deliveredStatus:  Dictionary that contains the Subscriber IDs as keys
                      and where the corresponding value is the time
                      for the last file delivery
                      (dictionary/string (ISO 8601)).

    Returns:          Void.
    """
    lastDelivery        = deliveredStatus[subscrObj.getId()]
    if (subscrObj.getId() in scheduledStatus):
        lastSchedule = scheduledStatus[subscrObj.getId()]
    else:
        lastSchedule = None

    fileInfo            = _convertFileInfo(fileInfo)
    fileId              = fileInfo[FILE_ID]
    filename            = fileInfo[FILE_NM]
    fileVersion         = fileInfo[FILE_VER]
    fileIngDate         = fromiso8601(fileInfo[FILE_DATE], local=True)
    fileBackLogBuffered = fileInfo[FILE_BL]
    subs_start = subscrObj.getStartDate()

    if lastSchedule is not None and lastSchedule > lastDelivery:
        # assume what have been scheduled are already delivered, this avoids multiple schedules for the same file across multiple main thread iterations
        # (so that we do not have to block the main iteration anymore)
        # if a file is scheduled but fail to deliver, it will be picked up by backlog in the future
        lastDelivery = lastSchedule

    deliverFile = False
    if subs_start is None:
        deliverFile = True
    elif fileIngDate >= subs_start:
        deliverFile = explicitFileDelivery or lastDelivery is None or fileIngDate >= lastDelivery

    # Register the file if we should deliver this file to the Subscriber.
    if deliverFile:
        filterMatched = _checkIfFilterPluginSayYes(srvObj, subscrObj, filename, fileId, fileVersion, fpiMode = FPI_MODE_METADATA_ONLY)
        if (filterMatched):
            _addFileDeliveryDic(subscrObj.getId(), fileInfo,
                                                deliverReqDic, fileDeliveryCountDic, fileDeliveryCountDic_Sem, srvObj)
        #debug_chen
        logger.debug('File %s is accepted to delivery list', fileId)
    else:
        logger.debug('File %s is out, ingDate = %s, lastDelivery = %s', fileId, toiso8601(fileIngDate), toiso8601(lastDelivery))


def _convertFileInfo(fileInfo):
    """
    Convert the file info (in DB Summary 2 format) to the internal format
    reflecting the ngas_subscr_back_log table.

    If already in the right format, nothing is done.

    fileInfo:       File info in DB Summary 2 format or
    """

    # HACK HACK HACK HACK HACK
    #
    # If fileInfo has the same number of elements than what the DB summary 2
    # then we convert it into our own list... which simply has one more None
    # element. We thus use the length of the list to determine the kind of list
    # we are dealing with
    #
    # TODO: In the long run we probably want to replace this nonesense with
    # something more understandable, like a namedtuple, or even bypass this
    # whole sequence translation process entirely.
    #
    # HACK HACK HACK HACK HACK
    if len(fileInfo) != len(ngamsDbCore.getNgasSummary2Def()):
        return fileInfo

    logger.debug("Converting %d-element sequence to fileInfo list: %r", len(fileInfo), fileInfo)
    locFileInfo = (len(ngamsDbCore.getNgasSummary2Def()) + 1) * [None]
    locFileInfo[FILE_ID] = fileInfo[ngamsDbCore.SUM2_FILE_ID]
    locFileInfo[FILE_NM] = os.path.normpath(fileInfo[ngamsDbCore.SUM2_MT_PT] +\
                                              os.sep +\
                                              fileInfo[ngamsDbCore.SUM2_FILENAME])

    locFileInfo[FILE_VER] = fileInfo[ngamsDbCore.SUM2_VERSION]
    locFileInfo[FILE_DATE] = fileInfo[ngamsDbCore.SUM2_ING_DATE]
    locFileInfo[FILE_MIME] = fileInfo[ngamsDbCore.SUM2_MIME_TYPE]
    locFileInfo[FILE_DISK_ID] = fileInfo[ngamsDbCore.SUM2_DISK_ID]
    return locFileInfo



_backlog_area_lock = threading.Lock()
def _genSubscrBackLogFile(srvObj,
                          subscrObj,
                          fileInfo):
    """
    Make a copy of a file that could not be delivered to the Subscription
    Back-Log Area, and create an entry in the Subscription Back-Log Table
    in the DB. This is only done if the file is not already back-logged.

    srvObj:        Reference to server object (ngamsServer).

    subscrObj:     Subscriber Object (ngamsSubscriber).

    fileInfo:      List with sub-lists with information about file
                   (list/list).

    Returns:       Void.
    """
    # If in ngamsDbBase.SUM2 format, convert to the internal format.
    locFileInfo = _convertFileInfo(fileInfo)

    # If file was already back-logged, nothing is done.
    if (locFileInfo[FILE_BL] == NGAMS_SUBSCR_BACK_LOG): return

    #info(3, 'Generating backlog db entry for file %s' % locFileInfo[FILE_ID])

    # NOTE: The actions carried out by this function are critical and need
    #       to be semaphore protected (Back-Log Operations Semaphore).
    with _backlog_area_lock:
        # chen.wu@icrar.org:
        # we no longer copy files, the limitation now is that the storage media is not movable
        ## Create copy of file in Subscription Back-Log Area + make entry in
        ## the DB for the file.
        fileId        = locFileInfo[FILE_ID]
        filename      = locFileInfo[FILE_NM]
        fileVersion   = locFileInfo[FILE_VER]
        fileIngDate   = locFileInfo[FILE_DATE]
        fileMimeType  = locFileInfo[FILE_MIME]
#        backLogName   = os.path.\
#                        normpath(srvObj.getCfg().getBackLogBufferDirectory() +\
#                                 "/" + NGAMS_SUBSCR_BACK_LOG_DIR + "/" +\
#                                 fileId + "/" + str(fileVersion) +\
#                                 "/" + os.path.basename(filename))
#        if (not os.path.exists(backLogName)):
#            checkCreatePath(os.path.dirname(backLogName))
#            commands.getstatusoutput("cp " + filename +\
#                                     " " + backLogName)
        srvObj.getDb().addSubscrBackLogEntry(srvObj.getHostId(),
                                             srvObj.getCfg().getPortNo(),
                                             subscrObj.getId(),
                                             subscrObj.getUrl(),
                                             fileId,
                                             filename,
                                             fileVersion,
                                             fileIngDate,
                                             fileMimeType)

        # Increase the Subscription Back-Log Counter to indicate to the Data
        # Subscription Thread that it should only suspend itself temporarily.
        srvObj.incSubcrBackLogCount()


def _delFromSubscrBackLog(srvObj,
                          subscrId,
                          fileId,
                          fileVersion,
                          fileName):
    """
    Delete a back-logged file from the Subscription Back-Log Area Table (DB).
    Delete also the file if there are no other deliveries pending for this.

    srvObj:        Reference to server object (ngamsServer).

    subscrId:      Subscriber ID (string).

    fileId:        File ID (string).

    fileVersion:   File Version (string).

    fileName:      Complete filename (string).

    Returns:       Void
    """
    # NOTE: The actions carried out by this function are critical and need
    #       to be semaphore protected (Back-Log Operations Semaphore).

    with _backlog_area_lock:
        srvObj.decSubcrBackLogCount()
        # Delete the entry from the DB for that file/Subscriber.
        srvObj.getDb().delSubscrBackLogEntry(srvObj.getHostId(),
                                             srvObj.getCfg().getPortNo(),
                                             subscrId, fileId, fileVersion)
        # If there are no other Subscribers interested in this file, we
        # delete the file.
        # subscrIds = srvObj.getDb().getSubscrsOfBackLogFile(fileId, fileVersion)
        # if (subscrIds == []): commands.getstatusoutput("rm -f " + fileName)

def _markDeletion(srvObj,
                  diskId,
                  fileId,
                  fileVersion):
    """
    srvObj:       Reference to server object (ngamsServer)

    diskId:       Disk ID of volume hosting the file (string).

    fileId:       File ID for file to consider (string).

    fileVersion:  Version of file (integer).

    """
    sqlFileInfo = (diskId, fileId, fileVersion)
    ngamsCacheControlThread.requestFileForDeletion(srvObj, sqlFileInfo)

def _backupQueueToBacklog(srvObj):
    """
    When NGAS server is shutdown, pending files in the queue for each subscriber need to be kept in the backlog
    so that their delivery can be resumed when server is restarted

    This is necessary because the current three triggering mechanism - (explicit file ref, explicit subscribers, backlog) -
    cannot trigger "resuming" delivering these "in-queue and delivery-pending" files after the server is restarted, when all queues are cleared to empty.
    """
    logger.debug('Started - backing up pending files from delivery queue to back logs ......')
    queueDict = srvObj._subscrQueueDic
    subscrbDict =srvObj.getSubscriberDic()
    for subscrbId, qu in queueDict.items():
        if (subscrbId in subscrbDict):
            subscrObj = subscrbDict[subscrbId]
        else:
            logger.warning('Cannot find the file queue for subscriber %s during backing up', subscrbId)
            break
        while (1):
            fileInfo = None
            try:
                fileInfo = qu.get_nowait()
            except Empty:
                break
            if (fileInfo is None):
                break
            fileInfo = _convertFileInfo(fileInfo)
            _genSubscrBackLogFile(srvObj, subscrObj, fileInfo)
            logger.debug('File %s for subscriber %s is backed up to backlog', fileInfo[FILE_ID], subscrbId)
    logger.debug('Completed - backing up pending files from delivery queue to back logs')


def _checkIfFilterPluginSayYes(srvObj, subscrObj, filename, fileId, fileVersion, fpiMode = FPI_MODE_BOTH):
    deliverFile = 0
    # If a Filter Plug-In is specified, apply it.
    plugIn = subscrObj.getFilterPi()
    if (plugIn != ""):
        # Apply Filter Plug-In
        plugInPars = subscrObj.getFilterPiPars()
        logger.debug("Invoking FPI: %s on file (version/ID): %s/%s. " +\
                     "Subscriber: %s",
                     fileId, str(fileVersion), subscrObj.getId(), plugIn)
        plugInMethod = loadPlugInEntryPoint(plugIn)
        fpiRes = plugInMethod(srvObj, plugInPars, filename, fileId, fileVersion)
        if (fpiRes):
            logger.debug("File (version/ID): %s/%s accepted by the FPI: " + \
                         "%s for Subscriber: %s",
                         fileId, str(fileVersion), plugIn, subscrObj.getId())
            deliverFile = 1
        else:
            logger.debug("File (version/ID): " + fileId + "/" +\
                 str(fileVersion) + " not accepted by the FPI: " +\
                 plugIn + " for Subscriber: " +  subscrObj.getId())
    else:
        # If no filter is specified, we always take the file.
        logger.debug("No FPI specified, file (version/ID): %s/%s " + \
                     "selected for Subscriber: %s",
                     fileId, str(fileVersion), subscrObj.getId())
        deliverFile = 1

    return deliverFile


def _deliveryThread(srvObj,
                    subscrObj,
                    quChunks,
                    fileDeliveryCountDic,
                    fileDeliveryCountDic_Sem,
                    dummy):
    """
    Function to be executed as a thread to delivery data to a Data Subscriber.

    srvObj:        Reference to server object (ngamsServer).

    subscrObj:     Subscriber Object (ngamsSubscriber).

    quChunks:      The queue associated with this subscriber, each element
                   in the queue is a fileInfoList (defined below)

                   A fileInfoList is a List with sub-lists with information about file
                   (sub-list generated by ngamsDb.getFileSummary2().
                   Note: In case the file was contained in the Subscription
                   Back-Log, it will have an extra element appended to the
                   file info list, with the value of NGAMS_SUBSCR_BACK_LOG
                   (list/list).

    fileDeliveryCountDic:
                   The counter dict for tracking the references to a file to be delivered

    dummy:         Needed by the thread handling ...

    Returns:       Void.
    """

    subscrbId = subscrObj.getId();
    tname = threading.current_thread().name
    tident = threading.current_thread().ident
    remindMainThread = True # whether to notify the subscriptionThread when the queue is empty in order to bypass static suspension time
    firstThread = (threading.current_thread().name == NGAMS_DELIVERY_THR + subscrbId + '0')

    while (1): # the delivery is always running unless either unsubscribeCmd is called, or server is shutting down, or it is kicked out by the USUBSCRIBE command
        try:
            _checkStopDataDeliveryThread(srvObj, subscrbId)
            srvObj._subscrSuspendDic[subscrbId].wait() # to check if it should suspend file delivery
            _checkStopDataDeliveryThread(srvObj, subscrbId)
            fileInfo = None

            # block for up to 1 minute if the queue is empty.
            try:
                fileInfo = quChunks.get(timeout = 1)
                srvObj._subscrDeliveryFileDic[tname] = fileInfo # once it is dequeued, it is no longer safe, so need to record it in case server shut down.
            except Empty:
                logger.debug("Data delivery thread [%s] block timeout", str(tident))
                _checkStopDataDeliveryThread(srvObj, subscrbId) # Timeout allows it to check if the delivery thread should stop
                # if delivery thread is to continue, trigger the subscriptionThread to get more files in
                if (srvObj.getDataMoverOnlyActive() and remindMainThread and firstThread):
                    # But only the first thread does this to avoid repeated notifications, but what if the first thread is busy (i.e. sending a file)
                    srvObj.triggerSubscriptionThread()
                    remindMainThread = False # only notify once within each "empty session"
            if (fileInfo == None):
                continue
            if (srvObj.getDataMoverOnlyActive() and firstThread):
                remindMainThread = True
            fileInfo = _convertFileInfo(fileInfo)
            # Prepare info and POST the file.
            fileId         = fileInfo[FILE_ID]
            filename       = fileInfo[FILE_NM]
            fileVersion    = fileInfo[FILE_VER]
            fileIngDate    = fromiso8601(fileInfo[FILE_DATE], local=True)
            fileMimeType   = fileInfo[FILE_MIME]
            fileBackLogged = fileInfo[FILE_BL]
            diskId         = fileInfo[FILE_DISK_ID]

            if (fileIngDate < subscrObj.getStartDate() and fileBackLogged != NGAMS_SUBSCR_BACK_LOG): #but backlog files will be sent regardless
                # subscr_start_date is changed (through USUBSCRIBE command) in order to skip unchechked files
                logger.warning('File %s skipped, ingestion date %s < %s', fileId, toiso8601(fileIngDate), toiso8601(subscrObj.getStartDate()))
                continue

            if (fileBackLogged == NGAMS_SUBSCR_BACK_LOG and (not diskId)):
                logger.warning('File %s has invalid diskid, removing it from the backlog', filename)
                _delFromSubscrBackLog(srvObj, subscrObj.getId(), fileId, fileVersion, filename)
                continue

            if (fileBackLogged == NGAMS_SUBSCR_BACK_LOG and (not os.path.isfile(filename))):
                # check if this file is removed by an agent outside of NGAS (e.g. Cortex volunteer cleanup)
                mtPt = srvObj.getDb().getMtPtFromDiskId(diskId)
                if (os.path.exists(mtPt)):
                    # the mount point is still there, but not the file, which means the file was removed by external agents
                    logger.warning('File %s is no longer available, removing it from the backlog', filename)
                    _delFromSubscrBackLog(srvObj, subscrObj.getId(), fileId, fileVersion, filename)
                continue

            status = getSubscrQueueStatus(srvObj, subscrbId, fileId, fileVersion, diskId)
            if (status in [0, -1]): # delivered or being delivered by other threads
                if (fileBackLogged == NGAMS_SUBSCR_BACK_LOG and status == 0):
                    logger.debug('Removing backlog file %s that is no longer needed to be de_livered', fileId)
                    _delFromSubscrBackLog(srvObj, subscrObj.getId(), fileId, fileVersion, filename)
                continue

            baseName = os.path.basename(filename)
            contDisp = 'attachment; filename="{0}"; file_id={1}'.format(baseName, fileId)

            msg = "Thread [%s] Delivering file: %s/%s - to Subscriber with ID: %s"
            logger.info(msg, str(tident), baseName, str(fileVersion), subscrObj.getId())

            ex = ""
            stat = ngamsStatus.ngamsStatus()

            # If the target does not turn on the authentication (or even not an NGAS), this still works
            # as long as there is a user named "ngas-int" in the configuration file for the current server
            # But if the target is an NGAS server and the authentication is on, the target must have set a user named "ngas-int"
            authHdr = None
            if srvObj.getCfg().getAuthUserInfo(NGAMS_HTTP_INT_AUTH_USER) is not None:
                authHdr = srvObj.getCfg().getAuthHttpHdrVal(NGAMS_HTTP_INT_AUTH_USER)
            fileInfoObjHdr = None
            urlList = subscrObj.getUrlList()
            urlListLen = len(urlList)
            for udx in range(urlListLen):
                """
                checking ngas job parameters in the url
                """
                sendUrl = urlList[udx]
                logger.debug('sendURL is %s', sendUrl)
                urlres = urlparse.urlparse(sendUrl)
                runJob = False
                redo_on_fail = False
                if (urlres.scheme.lower() == NGAS_JOB_URI_SCHEME):
                    # e.g. ngasjob://ngamsMWA_Compress_JobPlugin?redo_on_fail=0&plugin_params=scale_factor=4,threshold=1E-5
                    runJob = True
                    plugIn = urlres.netloc # hostname will return all lower cases
                    plugInPars = None
                    if (plugIn):
                        tmpUrl = sendUrl.lower().replace(NGAS_JOB_URI_SCHEME, 'http')
                        urlres_query = urlparse.urlparse(tmpUrl).query
                        if (urlres_query):
                            jqdict = urlparse.parse_qs(urlres_query)
                            if (jqdict):
                                if ('redo_on_fail' in jqdict and
                                '1' == jqdict['redo_on_fail'][0]):
                                    redo_on_fail = True
                                if ('plugin_params' in jqdict):
                                    plugInPars = jqdict['plugin_params'][0]
                    else:
                        raise Exception('invalid ngas job plugin')

                if ((not runJob) and sendUrl.upper().endswith('/' + NGAMS_REARCHIVE_CMD) and diskId):
                    try:
                        fileInfoObj = ngamsFileInfo.ngamsFileInfo().read(srvObj.getHostId(),
                                                                         srvObj.getDb(), fileId, fileVersion, diskId)
                        fileInfoObjHdr = base64.b64encode(fileInfoObj.genXml().toxml())
                    except Exception:
                        logger.exception('Fail to obtain fileInfo from DB: fileId/version/diskId - %s / %d / %s',
                                       fileId, fileVersion, diskId)
                updateSubscrQueueStatus(srvObj, subscrbId, fileId, fileVersion, diskId, -1)
                st = time.time()
                try:
                    stageFile(srvObj, filename)
                    if (runJob):
                        logger.debug("Invoking Job Plugin: %s on file (version/ID): %s/%s. " + \
                                     "Subscriber: %s",
                                     fileId, str(fileVersion), subscrObj.getId(), plugIn)
                        plugInMethod = loadPlugInEntryPoint(plugIn)
                        jpiCode, jpiResult = plugInMethod(srvObj, plugInPars, filename, fileId, fileVersion, diskId)
                        if (0 == jpiCode):
                            reply = NGAMS_HTTP_SUCCESS
                            stat.setStatus(NGAMS_SUCCESS)
                        else:
                            raise Exception(str(jpiCode) + NGAS_JOB_DELIMIT + jpiResult)
                    else:
                        fileChecksum = srvObj.getDb().getFileChecksum(diskId, fileId, fileVersion)
                        if fileChecksum is None:
                            logger.warning('Fail to get file checksum for file %s', fileId)

                        # TODO: validate the URL before blindly using it
                        hdrs = {NGAMS_HTTP_HDR_CHECKSUM: fileChecksum}
                        if fileInfoObjHdr:
                            hdrs[NGAMS_HTTP_HDR_FILE_INFO] = fileInfoObjHdr
                        with open(filename, "rb") as f:
                            reply, msg, hdrs, data = \
                                   ngamsHttpUtils.httpPostUrl(sendUrl, f, fileMimeType,
                                                        contDisp=contDisp,
                                                        auth=authHdr,
                                                        hdrs=hdrs,
                                                        timeout=120)
                        stat.clear()
                        if data:
                            stat.unpackXmlDoc(data)
                        if reply != NGAMS_HTTP_SUCCESS:
                            stat.setStatus(NGAMS_FAILURE)
                            raise Exception('Error handling %s' % sendUrl)
                        stat.setStatus(NGAMS_SUCCESS)

                except Exception as e:
                    ex = str(e)
                    logger.error('%s Message: %s' % (ex, stat.getMessage()))

                if ex or reply != NGAMS_HTTP_SUCCESS or stat.getStatus() == NGAMS_FAILURE:

                    if udx < urlListLen - 1: #try the next url
                        continue
                    # If an error occurred during data delivery, we should not update
                    # the Subscription Status table for this Subscriber, but should
                    # instead make an entry in the Subscription Back-Log Table
                    # (if the file is not an already back log buffered file, which
                    # was attempted re-posted).

                    #
                    # SLOW DOWN THE WORLD
                    #
                    # When our subscribers cannot be reached, we might end up
                    # trying to contact them continuously, either to send
                    # different files within the same subscription loop
                    # iteration, or the same files after the outer loop has
                    # started a new iteration. This can result in errors being
                    # produced continuously at a high rate, for example if there
                    # are many files that still need to be sent. This rate will
                    # only grow if, on top of that, files keep being archived
                    # into us. Apart from potentially imposing a high load on
                    # the server itself, large error rates produce high load on
                    # the central database (because two rows are updated in two
                    # different tables). This is not only sub-optimal in itself,
                    # but it also can affect other applications that are
                    # connected to the same central database. Therefore, for our
                    # own honour and to be be better citizens we need to address
                    # this.
                    #
                    # A proper solution would obviously be to properly collect
                    # all the errors individually and react intelligently to
                    # that (probably with a "retry-period" setting or similar),
                    # but for the time being we are simply resorting to slow
                    # down the error rate. This will bring down the load on the
                    # server and on the central database, which is the immediate
                    # problem we need to solve.
                    time.sleep(3)

                    if (runJob):
                        if (redo_on_fail):
                            _genSubscrBackLogFile(srvObj, subscrObj, fileInfo)
                        jobErrorInfo = ex.split(NGAS_JOB_DELIMIT)
                        if (len(jobErrorInfo) == 2): # job plug-in application exception
                            updateSubscrQueueStatus(srvObj, subscrbId, fileId, fileVersion, diskId, int(jobErrorInfo[0]), jobErrorInfo[1])
                        else:
                            # run-time error / or unexpected exception
                            updateSubscrQueueStatus(srvObj, subscrbId, fileId, fileVersion, diskId, 1, ex)
                        errMsg = "Error occurred while executing job plugin on file: " + baseName +\
                                 "/" + str(fileVersion) +\
                                 " - for Subscriber/url: " + subscrObj.getId() + "/" + subscrObj.getUrl() +\
                                 " by Job Thread [" + str(tident) + "]"
                    else:
                        _genSubscrBackLogFile(srvObj, subscrObj, fileInfo)
                        updateSubscrQueueStatus(srvObj, subscrbId, fileId, fileVersion, diskId, 1, ex + stat.getMessage())
                        errMsg = "Error occurred while delivering file: " + baseName +\
                                 "/" + str(fileVersion) +\
                                 " - to Subscriber/url: " + subscrObj.getId() + "/" + subscrObj.getUrl() +\
                                 " by Delivery Thread [" + str(tident) + "]"

                    if (fileBackLogged == NGAMS_SUBSCR_BACK_LOG):
                        # remove bl record from the dict
                        if (subscrbId in srvObj._subscrBlScheduledDic):
                            k = _fileKey(fileId, fileVersion)
                            srvObj._subscrBlScheduledDic_Sem.acquire()
                            try:
                                if (k in srvObj._subscrBlScheduledDic[subscrbId]):
                                    del srvObj._subscrBlScheduledDic[subscrbId][k]
                            finally:
                                srvObj._subscrBlScheduledDic_Sem.release()
                else:
                    if (runJob):
                        updateSubscrQueueStatus(srvObj, subscrbId, fileId, fileVersion, diskId, 0, jpiResult)
                        logger.info("File: %s/%s executed by %s for Subscriber: %s by Job Thread [%s]",
                                     baseName, str(fileVersion), plugIn, subscrObj.getId(), str(tident))
                    else:
                        howlong = time.time() - st
                        fileSize = getFileSize(filename)
                        transfer_rate = '%.0f Bytes/s' % (fileSize / howlong)
                        updateSubscrQueueStatus(srvObj, subscrbId, fileId, fileVersion, diskId, 0, transfer_rate)
                        logger.info("File: %s/%s delivered to Subscriber: %s by Delivery Thread [%s]",
                                     baseName, str(fileVersion), subscrObj.getId(), str(tident))

                    if (srvObj.getCachingActive()):
                        fkey = fileId + "/" + str(fileVersion)
                        fileDeliveryCountDic_Sem.acquire()
                        try:
                            if (fkey in fileDeliveryCountDic):
                                fileDeliveryCountDic[fkey] -= 1
                                if (fileDeliveryCountDic[fkey] == 0):
                                    _markDeletion(srvObj, fileInfo[FILE_DISK_ID], fileId, fileVersion)
                                    ff = fileDeliveryCountDic.pop(fkey)
                                    del ff
                            else:
                                if (fileBackLogged == NGAMS_SUBSCR_BACK_LOG):
                                    # it is possible that backlogged files cannot find an entry in the reference count dic -
                                    # e.g. when the server is restarted, refcount dic is empty. Later on, back-logged files are queued for delivery.
                                    # but they did not create entries in refcount dic when they are queued
                                    _markDeletion(srvObj, fileInfo[FILE_DISK_ID], fileId, fileVersion)
                                elif 'NGAS_FORCE_MARK_FOR_DELETION_AFTER_DELIVERY' in os.environ:
                                    # Last chance to get marked for deletion
                                    logger.warning('File %s/%d not found in the fileDeliveryCountDic, but marking for deletion anyway', fileId, fileVersion)
                                    _markDeletion(srvObj, fileInfo[FILE_DISK_ID], fileId, fileVersion)
                                else:
                                    logger.warning("Fail to find %s/%d in the fileDeliveryCountDic", fileId, fileVersion)
                        finally:
                            fileDeliveryCountDic_Sem.release()


                    # Update the Subscriber Status to avoid that this file
                    # gets delivered again.
                    try:
                        subscrObj.setLastFileIngDate(fileIngDate)
                        srvObj.getDb().updateSubscrStatus(subscrObj.getId(), fileIngDate)
                    except Exception as e:
                        # continue with warning message. this means the database (i.e. last_ingestion_date) is not synchronised for this file,
                        # but at least remaining files can be delivered continuously, the database may be back in sync upon delivering remaining files
                        errMsg = "Error occurred during update the ngas_subscriber table " +\
                             "_devliveryThread [" + str(tident) + "] Exception: " + str(e)
                        logger.warning(errMsg)

                    # If the file is back-log buffered, we check if we can delete it.
                    if (fileBackLogged == NGAMS_SUBSCR_BACK_LOG):
                        srvObj._subscrBlScheduledDic_Sem.acquire()
                        try: # the following block must be atomic
                            _delFromSubscrBackLog(srvObj, subscrObj.getId(), fileId,
                                              fileVersion, filename)
                            if (subscrbId in srvObj._subscrBlScheduledDic):
                                k = _fileKey(fileId, fileVersion)
                                if (k in srvObj._subscrBlScheduledDic[subscrbId]):
                                    del srvObj._subscrBlScheduledDic[subscrbId][k]
                        finally:
                            srvObj._subscrBlScheduledDic_Sem.release()
                    break # do not try the next url after success

            srvObj._subscrDeliveryFileDic[tname] = None
        except Exception as be:
            if (str(be).find("_STOP_DELIVERY_THREAD_") != -1):
                # Stop delivery thread.
                logger.debug('Delivery thread [%s] is exiting.', str(tident))
                break
            logger.exception("Error occurred during file delivery: %s", str(be))


def _fileKey(fileId,
             fileVersion):
    """
    Generate file identifier.

    fileId:        File ID (string).

    fileVersion:   File Version (integer).

    Returns:       File identifier (string).
    """
    return "%s___%s" % (str(fileId), str(fileVersion))

def buildSubscrQueue(srvObj, subscrId, dataMoverOnly = False):
    """
    initialise the subscription queue and
    load records from the persistent ngas_subscr_queue
    into the cache queue

    This is to sync cache queue with the persistent queue

    Returns:    the subscriber (cache) queue
    """
    if (dataMoverOnly): # for data movers, file ids (which is the first field of the fileInfo) close to one another are sent in sequence
        quChunks = PriorityQueue()
    else:
        quChunks = Queue()

    try:
        # change status to "scheduled" for files "being transferred" before system restart
        srvObj.getDb().updateSubscrQueueEntryStatus(subscrId, -1, -2)
        #grab those files that have been scheduled from the persistent queue
        files = srvObj.getDb().getSubscrQueue(subscrId, status = -2)
    except Exception as ee:
        logger.error('Failed db operation when building subscriber cache queue: %s', str(ee))
        return quChunks

    for locFileInfo in files:
        locFileInfo = list(locFileInfo)
        locFileInfo.append(None) # see function _convertFileInfo(fileInfo)
        quChunks.put(locFileInfo) # load them into the cache queue

    return quChunks


def updateSubscrQueueStatus(srvObj, subscrId, fileId, fileVersion, diskId, status, comment = None):
    ts = time.time()
    if (comment and len(comment) > 255):
        comment = comment[0:255]
    try:
        if (status > 0 and comment):
            ffif = getSubscrQueueStatus(srvObj, subscrId, fileId, fileVersion, diskId)
            if (ffif):
                sta = ffif[0]
                cmt = ffif[1]
                if (cmt == comment and sta > 0): # the same error / failure msg
                    srvObj.getDb().updateSubscrQueueEntry(subscrId, fileId, fileVersion, diskId, sta + 1, ts) #increase # of failures by one
                else:
                    srvObj.getDb().updateSubscrQueueEntry(subscrId, fileId, fileVersion, diskId, status, ts, comment)
        else:
            srvObj.getDb().updateSubscrQueueEntry(subscrId, fileId, fileVersion, diskId, status, ts, comment)
    except Exception as eee:
        logger.error("Fail to update persistent queue: %s", str(eee))

def getSubscrQueueStatus(srvObj, subscrId, fileId, fileVersion, diskId):
    """
    Return both status and comment
    """
    try:
        return srvObj.getDb().getSubscrQueueStatus(subscrId, fileId, fileVersion, diskId)
    except Exception as ex:
        logger.error("Fail to query persistent queue: %s", str(ex))
        return None

def addToSubscrQueue(srvObj, subscrId, fileInfo, quChunks):
    """
    Insert into the persistent subscription queue,
    if successful, then add to the cache subscription queue

    fileInfo    file information (List) that has already been converted (see _convertFileInfo(fileInfo))
    """
    fileInfo = _convertFileInfo(fileInfo)
    fileId         = fileInfo[FILE_ID]
    filename       = fileInfo[FILE_NM]
    fileVersion    = fileInfo[FILE_VER]
    fileIngDate    = fileInfo[FILE_DATE]
    fileMimeType   = fileInfo[FILE_MIME]
    diskId = fileInfo[FILE_DISK_ID]
    try:
        ts = time.time()
        srvObj.getDb().addSubscrQueueEntry(subscrId, fileId, fileVersion, diskId, filename, fileIngDate, fileMimeType, -2, ts)
        quChunks.put(fileInfo)
    except Exception as ee:
        # most likely error - key duplication, that will prevent cache queue from adding this entry, which is correct
        logger.error('Subscriber %s failed to add to the persistent subscription queue file %s due to %s', subscrId, filename, str(ee))
        if (fileInfo[FILE_BL] == NGAMS_SUBSCR_BACK_LOG):
            quChunks.put(fileInfo)

def stageFile(srvObj, filename):
    fspi = srvObj.getCfg().getFileStagingPlugIn()
    if not fspi:
        return
    try:
        logger.debug("Invoking FSPI.isFileOffline: %s to check file: %s", fspi, filename)
        isFileOffline = loadPlugInEntryPoint(fspi, 'isFileOffline')
        if isFileOffline(filename) == 1:
            logger.debug("File %s is offline, staging for delivery...", filename)
            stageFiles = loadPlugInEntryPoint(fspi, 'stageFiles')
            stageFiles(filenames = [filename], serverObj = srvObj)
            logger.debug("File %s staging completed for delivery.", filename)
    except Exception as ex:
        logger.error("File staging error: %s", filename)
        raise ex

def subscriptionThread(srvObj,
                       dummy):
    """
    The Subscription Thread is normally suspended, but is woken up
    whenever there might be data to be delivered to Subscribers.

    srvObj:      Reference to server object (ngamsServer).

    dummy:       Needed by the thread handling ...

    Returns:     Void.
    """
    logger.debug("Data Subscription Thread initializing ...")
    dataMoverOnly = srvObj.getDataMoverOnlyActive()
    if (dataMoverOnly):
        dm_hosts = srvObj.getCfg().getDataMoverHostIds()
        if (dm_hosts == None):
            raise Exception("No data mover hosts are available!")
        else:
            dm_hosts = map(lambda x: x.strip(), dm_hosts.split(','))
            if (len(dm_hosts) < 1):
                raise Exception("Invalid data mover hosts configuration!")

    fileDicDbm = None
    fileDicDbmName = ngamsHighLevelLib.genTmpFilename(srvObj.getCfg(),
                                                      NGAMS_SUBSCRIPTION_THR +\
                                                      "_FILE_DIC")
    # Similar to Deliver Status Dictionary, the Schedule Status Dictionary
    # indicates for each Subscriber when the last file was scheduled (but
    # possibly not delivered yet)
    # this will be used across multiple iterations in the subscriptionThread
    # key subscriberId, value - date string
    scheduledStatus = srvObj._subscrScheduledStatus

    checkedStatus = srvObj._subscrCheckedStatus

    # key: subscriberId, value - a FIFO file queue, which is a list of fileInfo chunks, each chunk has a number of fileInfos
    queueDict = srvObj._subscrQueueDic

    # key: subscriberId, value - a List of deliveryThreads for that subscriber
    deliveryThreadDic = srvObj._subscrDeliveryThreadDic
    #deliverySuspendDic = srvObj._subscrSuspendDic

    # key: threadName (unique), value -  dummy 1
    # Threads for the same subscriber will have different thread names
    deliveryThreadRefDic = srvObj._subscrDeliveryThreadDicRef

    # key: threadName (unique), value -  None or FileInfo (the current file that is being transferred)
    # Threads for the same subscriber will have different thread names
    deliveryFileDic = srvObj._subscrDeliveryFileDic

    # key: file_id, value - the number of pending deliveries, should be > 0,
    # decreases by 1 upon a successful delivery
    fileDeliveryCountDic = srvObj._subscrFileCountDic
    fileDeliveryCountDic_Sem = srvObj._subscrFileCountDic_Sem

    # trigger all subscribers, so it can go ahead checking files when the server/subscriptionThread is just started
    srvObj.addSubscriptionInfo([], srvObj.getSubscriberDic().values()).triggerSubscriptionThread()

    while (1):
        # Incapsulate this whole block to avoid that the thread dies in
        # case a problem occurs, like e.g. a problem with the DB connection.
        try:
            fileRefs, subscrObjs = _waitForScheduling(srvObj)
            _checkStopSubscriptionThread(srvObj)
            #srvObj.resetSubcrBackLogCount()

            # If there are no Subscribers - don't do anything.
            if ((len(srvObj.getSubscriberDic()) == 0) and (subscrObjs == [])):
                continue

            # Dictionary used to keep track of what to deliver to each
            # Subscriber.
            # The dictionary is organized in the following way:
            # {<Subscriber ID>: {<File Info>:, <File Info>:,...}, ...}
            # The dictionary is first build up, and the files deliveried first
            # when everything to deliver has been identified. This is done in
            # order to be able to deliver the files in one go to each
            # Subscriber.
            deliverReqDic = {}

            # Generate dictionary to keep information about each file, which
            # might be a candidate for being delivered to Subscribers.
            # The format is:
            #
            # {<File Key>: [[<File Info>, ...], ...], ...}
            #
            # The key in this Dictionary is the File ID (pointing to lists
            # with the file information, one for each version.
            #
            # If no specific Subscribers are specified, we only query
            # information about the files specified, otherwise, we have to
            # query information about all files available on this host.
            rmFile(fileDicDbmName + "*")
            fileDicDbm = ngamsDbm.ngamsDbm(fileDicDbmName, writePerm=1)

            if (dataMoverOnly and srvObj.getSubcrBackLogCount() <= 1000): # do not bring in too many new files if back-logged files are piling up
                for subscrId in srvObj.getSubscriberDic().keys():
                    subscrObj = srvObj.getSubscriberDic()[subscrId]
                    start_date = None
                    """
                    if (subscrId in scheduledStatus and scheduledStatus[subscrId]):
                        start_date = scheduledStatus[subscrId]
                    """
                    if (subscrId in checkedStatus and checkedStatus[subscrId]):
                        start_date = checkedStatus[subscrId]
                    elif subscrObj.getLastFileIngDate():
                        start_date = subscrObj.getLastFileIngDate()
                    elif (subscrObj.getStartDate() is not None):
                        start_date = subscrObj.getStartDate()

                    logger.debug('Data mover %s start_date = %s', subscrId, start_date)
                    count = 0
                    logger.debug('Checking hosts %s for data mover %s', dm_hosts, subscrId)
                    files = srvObj.getDb().getFileSummary2(hostId = dm_hosts, ing_date = start_date, max_num_records = 1000, fetch_size=100)
                    lastIngDate = None
                    for fileInfo in files:
                        fileInfo = _convertFileInfo(fileInfo)
                        fileDicDbm.add(_fileKey(fileInfo[FILE_ID], fileInfo[FILE_VER]), fileInfo)
                        if (fileInfo[FILE_DATE] > lastIngDate): #just in case the cursor result is not sorted!
                            lastIngDate = fileInfo[FILE_DATE]
                        count += 1
                        _checkStopSubscriptionThread(srvObj)
                    if (lastIngDate):
                        # mark the "last" file that will be checked regardless if it will be delivered or not
                        checkedStatus[subscrId] = lastIngDate
                        pass
                    if (count == 0):
                        logger.debug('No new files for data mover %s', subscrId)
                    else:
                        logger.debug('Data mover %s will examine %d files for delivery', subscrId, count)
            elif (subscrObjs != []):

                min_date = None # the "earliest" last_ingestion_date or start_date amongst all explicitly referenced subscribers.
                # The min_date is used to exclude files that have been delivered (<= min_date) during previous NGAS sessions
                for subscriber in subscrObjs:
                    myMinDate = subscriber.getStartDate()

                    myIngDate = subscriber.getLastFileIngDate()
                    if myIngDate:
                        myMinDate = myIngDate

                    if min_date is None or min_date > myMinDate:
                        min_date = myMinDate

                files = srvObj.getDb().getFileSummary2(srvObj.getHostId(), ing_date = min_date, fetch_size=100)
                if min_date is not None:
                    logger.debug('Fetching files ingested after %s', toiso8601(min_date))
                else:
                    logger.debug('Fetching all ingested files')
                for fileInfo in files:
                    fileInfo = _convertFileInfo(fileInfo)
                    fileDicDbm.add(_fileKey(fileInfo[FILE_ID],
                                            fileInfo[FILE_VER]),
                                   fileInfo)
                    _checkStopSubscriptionThread(srvObj)
            elif (fileRefs != []): # this is still possible even for data mover (due to recovered subscriptionList during server start)
                # fileRefDic: Dictionary indicating which versions for each
                # file that are of interest.
                # debug_chen
                logger.debug('Count of fileRefs = %d', len(fileRefs))
                fileRefDic = {}
                fileIds = {}   # To generate a list with all File IDs
                for fileInfo in fileRefs:
                    fileId  = fileInfo[0]
                    fileVersion = fileInfo[1]
                    fileIds[fileId] = 1
                    if (fileId in fileRefDic):
                        fileRefDic[fileId].append(fileVersion)
                    else:
                        fileRefDic[fileId] = [fileVersion]

                files = srvObj.getDb().getFileSummary2(srvObj.getHostId(),
                                                       fileIds.keys(),
                                                       ignore=0, fetch_size=100)
                for fileInfo in files:
                    # Take only the file if the File ID + File Version are
                    # explicitly specified.
                    fileInfo = _convertFileInfo(fileInfo)
                    if fileInfo[FILE_VER] in fileRefDic[fileInfo[FILE_ID]]:
                        fileDicDbm.add(_fileKey(fileInfo[FILE_ID],
                                                fileInfo[FILE_VER]),
                                       fileInfo)
                    _checkStopSubscriptionThread(srvObj)

            # The Deliver Status Dictionary indicates for each Subscriber
            # when the last file was delivered. We first initialize the
            # Deliver Status Dictionary with None to indicate later if that
            # this Subscriber (apparently) didn't have any files delivered.
            deliveredStatus = {}
            for subscrId in srvObj.getSubscriberDic().keys():
                deliveredStatus[subscrId] = None
                if (subscrId not in scheduledStatus):
                    scheduledStatus[subscrId] = None
            subscrIds = srvObj.getSubscriberDic().keys()
            subscrStatus = srvObj.getDb().\
                           getSubscriberStatus(subscrIds, srvObj.getHostId(),
                                               srvObj.getCfg().getPortNo())
            for subscrStat in subscrStatus:
                subscrId      = subscrStat[0]
                subscrLastDel = subscrStat[1]
                deliveredStatus[subscrId] = subscrLastDel

            # Deliver file to a Subscriber if:
            #
            # 1. (File-Ingestion-Date >= Subscription-Date) and
            #    (Last-File-Ingestion-Date = None)
            # 2. (File-Ingestion-Date >= Subscription-Date) and
            #    (File-Ingestion-Date >= Last-File-Ingestion-Date)
            # 3. If the Filter Plug-In indicates a match (if a Filter Plug-In
            #    is specified).

            # First check for each file referenced explicitly (new files
            # archived since last run of Subscription Thread) if they should
            # be delivered to one or more of the Subscribers.
            for fileRef in fileRefs:
                fileId      = fileRef[0]
                fileVersion = fileRef[1]

                # Check that this file is contained in the File Dictionary
                # of possible candiate files, and resolve the reference to the
                # information for that file at the same time.
                if (fileDicDbm.hasKey(_fileKey(fileId, fileVersion))):
                    tmpFileInfo = fileDicDbm.get(_fileKey(fileId, fileVersion))
                else:
                    errMsg = "File Scheduled for delivery to Subscribers " +\
                             "(File ID: " + fileId + "/File Version: " +\
                             str(fileVersion) + ") not registered in the NGAS DB"
                    logger.warning(errMsg)
                    continue

                # Loop to determine for each Subscriber whether to deliver
                # the file or not to this or not.
                for subscrId in srvObj.getSubscriberDic().keys():
                    subscrObj = srvObj.getSubscriberDic()[subscrId]
                    _checkIfDeliverFile(srvObj, subscrObj, tmpFileInfo,
                                        deliverReqDic, deliveredStatus, scheduledStatus, fileDeliveryCountDic, fileDeliveryCountDic_Sem, explicitFileDelivery = True)

            # Then check if for each of the Subscribers referenced explicitly
            # (new Subscribers) for each file Online on this system, if we
            # should deliver data to these.
            if (not dataMoverOnly):
                for subscrObj in subscrObjs:
                    # Loop over each file and check if it should be delivered.
                    for fileKey in fileDicDbm.keys():
                        fileInfo = fileDicDbm.get(fileKey)
                        _checkIfDeliverFile(srvObj, subscrObj, fileInfo,
                                            deliverReqDic, deliveredStatus, scheduledStatus, fileDeliveryCountDic, fileDeliveryCountDic_Sem)
            else:  # Third, if datamover, add those files
                for subscrId in srvObj.getSubscriberDic().keys():
                    subscrObj = srvObj.getSubscriberDic()[subscrId]
                    logger.debug('Checking files for data mover %s', subscrId)
                    for fileKey in fileDicDbm.keys():
                        fileInfo = fileDicDbm.get(fileKey)
                        _checkIfDeliverFile(srvObj, subscrObj, fileInfo,
                                            deliverReqDic, deliveredStatus, scheduledStatus, fileDeliveryCountDic, fileDeliveryCountDic_Sem)

            # Then finally check if there are back-logged files to deliver.
            # selectDiskId = srvObj.getCachingActive()
            selectDiskId = True # always select Disk Id now so that alll back log files will have disk_id
            srvObj._subscrBlScheduledDic_Sem.acquire()
            try:
                subscrBackLog = srvObj.getDb().\
                                getSubscrBackLog(srvObj.getHostId(),
                                                 srvObj.getCfg().getPortNo(), selectDiskId)
                for backLogInfo in subscrBackLog:
                    subscrId = backLogInfo[0]

                    # HACK HACK HACK HACK
                    #
                    # Original comment follows:
                    # Note, it is signalled by adding an extra element (at the end)
                    # with the value of the constant NGAMS_SUBSCR_BACK_LOG, that
                    # this file is a back-logged file. This is done to make the
                    # handling more efficient.
                    #
                    # New comment follows
                    # This little creation here is related to the _checkFileInfo
                    # function defined above. The function was (and still is) a
                    # big hack that differentiates between two types of,
                    # returning always the second type of sequence. Its behavior
                    # was based on a database-defined data type, and therefore
                    # was not reliable across databases. We have (hopefully
                    # temporarily) changed it to rely on something more concrete
                    # (the length of the sequence), and thus we needed to create
                    # the monster below.
                    #
                    # TODO: read the TODO in _checkFileInfo about what to do
                    # with this
                    #
                    # HACK HACK HACK HACK
                    fileInfo = list(backLogInfo[2:]) + [NGAMS_SUBSCR_BACK_LOG, None]
                    #else:
                    #    fileInfo = list(backLogInfo[2:]) + [None] + [NGAMS_SUBSCR_BACK_LOG]

                    _addFileDeliveryDic(subscrId, fileInfo, deliverReqDic, fileDeliveryCountDic, fileDeliveryCountDic_Sem, srvObj)
            finally:
                srvObj._subscrBlScheduledDic_Sem.release()

            # Sort the files listed in the Delivery Dictionary for each
            # Subscriber so that files are sorted according to Ingestion Date.
            # This is done in order to prevent that a file with a more recent
            # Ingestion Date is registered in the Subscriber Status,
            # preventing other files with an older Ingestion Date, which
            # should have been delivered from being delivered.
            # Then deliver the data (if there is something to deliver).
            # Data Delivery Thread is spawned off per Subscriber, which should
            # receive data.
            for subscrId in srvObj.getSubscriberDic().keys():
            #for subscrId in deliverReqDic.keys():
                if (subscrId in deliverReqDic):
                    deliverReqDic[subscrId].sort(key=lambda x: _convertFileInfo(x)[FILE_DATE])
                    #get the ingest_date of the last file in the queue (list)
                    lastScheduleDate = _convertFileInfo(deliverReqDic[subscrId][-1])[FILE_DATE]
                    if (scheduledStatus[subscrId] is None or lastScheduleDate > scheduledStatus[subscrId]):
                        scheduledStatus[subscrId] = lastScheduleDate

                """
                This is not used since Priority Queue will sort the list
                if (dataMoverOnly):#
                    deliverReqDic[subscrId].sort(_compFctFileId)
                """

                # multi-threaded concurrent transfer, added by chen.wu@icrar.org
                if (subscrId not in srvObj.getSubscriberDic()): # this is possible since back log files can still use old subscriber names
                    continue
                num_threads = float(srvObj.getSubscriberDic()[subscrId].getConcurrentThreads())
                if subscrId in queueDict:
                    #debug_chen
                    logger.debug('Use existing queue for %s', subscrId)
                    quChunks = queueDict[subscrId]
                else:
                    """
                    if (dataMoverOnly): # for data movers, file ids (which is the first field of the fileInfo) close to one another are sent in sequence
                        quChunks = PriorityQueue()
                    else:
                        quChunks = Queue()
                    """
                    quChunks = buildSubscrQueue(srvObj, subscrId, dataMoverOnly)
                    queueDict[subscrId] = quChunks

                if (subscrId in deliverReqDic):
                    allFiles = deliverReqDic[subscrId]
                else:
                    allFiles = []
                #if (srvObj.getSubcrBackLogCount() > 0):
                logger.debug('Put %d new files in the queue for subscriber %s', len(allFiles), subscrId)
                for jdx in range(len(allFiles)):
                    ffinfo = allFiles[jdx]
                    addToSubscrQueue(srvObj, subscrId, ffinfo, quChunks)
                    #quChunks.put(allFiles[jdx])
                    # Deliver the data - spawn off a Delivery Thread to do this job
                logger.debug('Number of elements in Queue %s: %d', subscrId, quChunks.qsize())
                if subscrId not in deliveryThreadDic:
                    deliveryThreads = []
                    for tid in range(int(num_threads)):
                        args = (srvObj, srvObj.getSubscriberDic()[subscrId], quChunks, fileDeliveryCountDic, fileDeliveryCountDic_Sem, None)
                        thrdName = NGAMS_DELIVERY_THR + subscrId + str(tid)
                        deliveryThreadRefDic[thrdName] = 1
                        deliveryFileDic[thrdName] = None
                        deliveryThrRef = threading.Thread(None, _deliveryThread, thrdName, args)
                        deliveryThrRef.setDaemon(0)
                        deliveryThrRef.start()
                        deliveryThreads.append(deliveryThrRef)

                    deliveryThreadDic[subscrId] = deliveryThreads
        except Exception as e:
            try:
                del fileDicDbm
            except:
                pass
            rmFile(fileDicDbmName + "*")
            if (str(e).find("_STOP_SUBSCRIPTION_THREAD_") != -1): break
            errMsg = "Error occurred during execution of the Data " +\
                     "Subscription Thread."
            logger.exception(errMsg)