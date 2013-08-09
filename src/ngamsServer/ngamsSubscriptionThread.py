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

import thread, threading, time, commands, cPickle, types, math, sys, traceback, os
from Queue import Queue, Empty

from ngams import *
import ngamsDbm, ngamsDb, ngamsLib, ngamsStatus, ngamsHighLevelLib, ngamsCacheControlThread

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

def startSubscriptionThread(srvObj):
    """
    Start the Data Subscription Thread.

    srvObj:     Reference to server object (ngamsServer).
    
    Returns:    Void.
    """
    info(3,"Starting Subscription Thread ...")
    srvObj._subscriptionRunSync.set()
    args = (srvObj, None)
    srvObj._subscriptionThread = threading.Thread(None, subscriptionThread,
                                                  NGAMS_SUBSCRIPTION_THR, args)
    srvObj._subscriptionThread.setDaemon(0)
    srvObj._subscriptionThread.start()
    
    if (srvObj._deliveryStopSync.isSet()):
        srvObj._deliveryStopSync.clear() #revoke the shutdown (offline) setting
        
    info(3,"Subscription Thread started")


def stopSubscriptionThread(srvObj):
    """
    Stop the Data Subscription Thread.

    srvObj:     Reference to server object (ngamsServer).
    
    Returns:    Void.
    """
    info(3,"Stopping Subscription Thread ...")
    srvObj._subscriptionStopSyncConf.clear()
    srvObj._subscriptionStopSync.set()
    srvObj._deliveryStopSync.set()
    srvObj._subscriptionRunSync.set()
    srvObj._subscriptionStopSyncConf.wait(10)
    srvObj._subscriptionStopSync.clear()
    srvObj._subscriptionThread = None
    _backupQueueToBacklog(srvObj) # this should always occur after srvObj._deliveryStopSync.set() is called
    info(3,"Subscription Thread stopped")


def _checkStopSubscriptionThread(srvObj):
    """
    The function is used by the Subscription Thread when checking if it
    should stop execution. If this is the case, the function will terminate
    the thread.

    srvObj:     Reference to server object (ngamsServer).
    
    Returns:    Void.
    """
    if (srvObj._subscriptionStopSync.isSet()):
        info(2,"Stopping Subscription Thread ...")
        srvObj._subscriptionStopSyncConf.set()
        raise Exception, "_STOP_SUBSCRIPTION_THREAD_"


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
        (not deliveryThreadRefDic.has_key(tname)) or # this thread's reference has been removed by the USUBSCRIBE command, see ngamsPlugIns/ngamsCmd_USUBSCRIBE.changeNumThreads()
        (not srvObj.getSubscriberDic().has_key(subscrbId))): # the UNSUBSCRIBE command is issued
        info(2,"Stopping Data Delivery Thread ... %s" % tname)
        raise Exception, "_STOP_DELIVERY_THREAD_%s" % tname


def _waitForScheduling(srvObj):
    """
    Small function to let the Data Subscription Thread wait to be scheduled
    to check if data should be deliveried.

    srvObj:     Reference to server object (ngamsServer).
    
    Returns:    Tuple with list of files to check for delivery and Subscribers
                that should be checked to see if there is data to deliver
                (tuple/string, ngamsSubscriber).
    """
    info(4,"Data Subscription Thread suspending itself (waiting to " +\
         "be scheduled) ...")
    # If there are no pending deliveries in the Subscription Back-Log,
    # we suspend until the thread is woken up by another thread, e.g. when
    # new data is available.
    if (srvObj.getSubcrBackLogCount() > 0):
        suspTime = isoTime2Secs(srvObj.getCfg().getSubscrSuspTime())
        #debug_chen
        info(3, 'Subscription thread will suspend %s seconds before re-trying delivering back-logged files' % str(suspTime))
        srvObj._subscriptionRunSync.wait(suspTime)
    elif (srvObj.getDataMoverOnlyActive()):       
        tmout = isoTime2Secs(srvObj.getCfg().getDataMoverSuspenstionTime()) # in general, tmout > suspTime
        #debug_chen
        info(3, 'Data mover thread will suspend %s seconds before re-trying querying the db to get new files' % str(tmout))
        srvObj._subscriptionRunSync.wait(tmout)
    else:
        srvObj._subscriptionRunSync.wait()
    
    _checkStopSubscriptionThread(srvObj)
    info(4,"Data Subscription Thread received wake-up signal ...")
    try:
        srvObj._subscriptionSem.acquire()
        srvObj._subscriptionRunSync.clear()
        if (srvObj.getDataMoverOnlyActive()):
            return ([], [])
        filenames = srvObj._subscriptionFileList
        srvObj._subscriptionFileList = []
        subscrObjs = srvObj._subscriptionSubscrList
        srvObj._subscriptionSubscrList = []
        return (filenames, subscrObjs)
    except Exception, e:
        errMsg = "Error occurred in ngamsSubscriptionThread." +\
                  "_waitForScheduling(). Exception: " + str(e)
        alert(errMsg)
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
    if (deliverReqDic.has_key(subscrId)):
        for idx in range(len(deliverReqDic[subscrId])):
            tstFileInfo            = deliverReqDic[subscrId][idx]
            tstFilename            = tstFileInfo[FILE_NM]
            tstFileVersion         = tstFileInfo[FILE_VER]
            tstFileBackLogBuffered = tstFileInfo[FILE_BL]
            if ((tstFilename == filename) and
                (tstFileVersion == fileVersion)):
                if ((fileBackLogBuffered == NGAMS_SUBSCR_BACK_LOG) and
                    (tstFileBackLogBuffered != NGAMS_SUBSCR_BACK_LOG)):
                    # The new entry is back-log buffered, the old not,
                    # replace the old entry.
                    deliverReqDic[subscrId][idx] = fileInfo
                    add = 0
                    replaceWithBL = 1
                    break
    
    #Second, Check if the file is a back-logged file that has been previously registered                 
    if (fileBackLogBuffered == NGAMS_SUBSCR_BACK_LOG):
        if (not srvObj._subscrBlScheduledDic.has_key(subscrId)):
            srvObj._subscrBlScheduledDic[subscrId] = {}
        k = _fileKey(fileId, fileVersion) 
        if (srvObj._subscrBlScheduledDic[subscrId].has_key(k)):
            add = 0 # if this is an old entry, do not add again
        else:
            # if this is a new entry, maybe it will be added unless the first check set 'add' to 0. 
            # Must occupy the dic key space
            srvObj._subscrBlScheduledDic[subscrId][k] = None 
    if (add):
        if (deliverReqDic.has_key(subscrId)):
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
            if (fileDeliveryCountDic.has_key(fkey)):
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
    T = TRACE()
    
    deliverFile         = 0
    lastDelivery        = deliveredStatus[subscrObj.getId()]
    lastSchedule        = scheduledStatus[subscrObj.getId()]

    fileInfo            = _convertFileInfo(fileInfo)
    fileId              = fileInfo[FILE_ID]
    filename            = fileInfo[FILE_NM]
    fileVersion         = fileInfo[FILE_VER]
    fileIngDate         = fileInfo[FILE_DATE]
    fileBackLogBuffered = fileInfo[FILE_BL]
    
    if (lastSchedule != None and lastSchedule > lastDelivery):
        # assume what have been scheduled are already delivered, this avoids multiple schedules for the same file across multiple main thread iterations
        # (so that we do not have to block the main iteration anymore)
        # if a file is scheduled but fail to deliver, it will be picked up by backlog in the future
        lastDelivery = lastSchedule
    if (
        ((lastDelivery == None) and (subscrObj.getStartDate() == "")) or
        
        ((lastDelivery == None) and
         (fileIngDate >= subscrObj.getStartDate())) or
        
        ((lastDelivery != None) and
         (fileIngDate >= subscrObj.getStartDate()) and
         (fileIngDate >= lastDelivery)) or
        
        ((lastDelivery != None) and
         (fileIngDate >= subscrObj.getStartDate()) and
         explicitFileDelivery)
        
        ):
        # If a Filter Plug-In is specified, apply it.
        plugIn = subscrObj.getFilterPi()
        if (plugIn != ""):
            # Apply Filter Plug-In
            plugInPars = subscrObj.getFilterPiPars()
            exec "import " + plugIn
            info(3,"Invoking FPI: " + plugIn + " on file " +\
                 "(version/ID): " + fileId + "/" + str(fileVersion) +\
                 ". Subscriber: " + subscrObj.getId())
            fpiRes = eval(plugIn + "." + plugIn +\
                          "(srvObj, plugInPars, filename, fileId, " +\
                          "fileVersion)")
            if (fpiRes):
                info(4,"File (version/ID): " + fileId + "/" +\
                     str(fileVersion) + " accepted by the FPI: " + plugIn +\
                     " for Subscriber: " +  subscrObj.getId())
                deliverFile = 1
            else:
                info(4,"File (version/ID): " + fileId + "/" +\
                     str(fileVersion) + " not accepted by the FPI: " +\
                     plugIn + " for Subscriber: " +  subscrObj.getId())
        else:
            # If no file is specified, we always take the file.
            info(4,"No FPI specified, file (version/ID): " + fileId + "/" +\
                 str(fileVersion) + " selected for Subscriber: " +
                 subscrObj.getId())
            deliverFile = 1

    # Register the file if we should deliver this file to the Subscriber.
    if (deliverFile):
        deliverReqDic = _addFileDeliveryDic(subscrObj.getId(), fileInfo,
                                            deliverReqDic, fileDeliveryCountDic, fileDeliveryCountDic_Sem, srvObj)
        #debug_chen
        info(4, 'File %s is accepted to delivery list' % fileId)
    


def _compFct(fileInfo1,
             fileInfo2):
    """
    Sorter function to sort the elements in the file info list as returned by
    ngamsDb.getFileSummary2() (according to the File Ingestion Date).

    fileInfo1:    
    fileInfo2:    Lists with file information (list).

    Returns:      -1 if fileInfo1.IngestionDate  < fileInfo2.IngestionDate
                   0 if fileInfo1.IngestionDate == fileInfo2.IngestionDate
                   1 if fileInfo1.IngestionDate  > fileInfo2.IngestionDate
    """
    fileInfo1 = _convertFileInfo(fileInfo1)
    fileInfo2 = _convertFileInfo(fileInfo2)
    if (fileInfo1[FILE_DATE] < fileInfo2[FILE_DATE]):
        return -1
    elif (fileInfo1[FILE_DATE] == fileInfo2[FILE_DATE]):
        return 0
    else:
        return 1


def _convertFileInfo(fileInfo):
    """
    Convert the file info (in DB Summary 2 format) to the internal format
    reflecting the ngas_subscr_back_log table.

    If already in the right format, nothing is done.

    fileInfo:       File info in DB Summary 2 format or 
    """
    # If element #4 is an integr (=file version), convert to internal format.
    if (type(fileInfo[ngamsDb.ngamsDbCore.SUM2_VERSION]) == types.IntType):
        locFileInfo = 7 * [None]
        locFileInfo[FILE_ID]   = fileInfo[ngamsDb.ngamsDbCore.SUM2_FILE_ID]
        locFileInfo[FILE_NM]   = \
                             os.path.normpath(fileInfo[ngamsDb.ngamsDbCore.SUM2_MT_PT] +\
                                              os.sep +\
                                              fileInfo[ngamsDb.ngamsDbCore.SUM2_FILENAME])
        locFileInfo[FILE_VER]  = fileInfo[ngamsDb.ngamsDbCore.SUM2_VERSION]
        locFileInfo[FILE_DATE] = fileInfo[ngamsDb.ngamsDbCore.SUM2_ING_DATE]
        locFileInfo[FILE_MIME] = fileInfo[ngamsDb.ngamsDbCore.SUM2_MIME_TYPE]
        locFileInfo[FILE_DISK_ID]   = fileInfo[ngamsDb.ngamsDbCore.SUM2_DISK_ID]
    else:
        locFileInfo = fileInfo
    if ((len(locFileInfo) == FILE_BL)): locFileInfo.append(None)
    return locFileInfo


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
    srvObj._backLogAreaSem.acquire()
    try:
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
        srvObj.getDb().addSubscrBackLogEntry(getHostId(),
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
        srvObj._backLogAreaSem.release()
    except Exception, e:
        srvObj._backLogAreaSem.release()
        error("Error generating Subscription Back-Log File. " +\
              "Exception: " + str(e))
        raise e


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
    
    srvObj._backLogAreaSem.acquire()
    try:
        srvObj.decSubcrBackLogCount()
        # Delete the entry from the DB for that file/Subscriber.
        srvObj.getDb().delSubscrBackLogEntry(getHostId(),
                                             srvObj.getCfg().getPortNo(),
                                             subscrId, fileId, fileVersion)        
        # If there are no other Subscribers interested in this file, we
        # delete the file.
        # subscrIds = srvObj.getDb().getSubscrsOfBackLogFile(fileId, fileVersion)
        # if (subscrIds == []): commands.getstatusoutput("rm -f " + fileName)
        srvObj._backLogAreaSem.release()
    except Exception, e:
        srvObj._backLogAreaSem.release()
        raise e

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
    info(3, 'Started - backing up pending files from delivery queue to back logs ......')
    queueDict = srvObj._subscrQueueDic
    subscrbDict =srvObj.getSubscriberDic()
    for subscrbId, qu in queueDict.items():
        if (subscrbDict.has_key(subscrbId)):
            subscrObj = subscrbDict[subscrbId]
        else:
            alert('Cannot find the file queue for subscriber %s during backing up' % subscrbId)
            break
        while (1):
            fileInfo = None
            try:
                fileInfo = qu.get_nowait()
            except Empty, e:
                break
            if (fileInfo is None):
                break
            fileInfo = _convertFileInfo(fileInfo)
            _genSubscrBackLogFile(srvObj, subscrObj, fileInfo)
            info(3, 'File %s for subscriber %s is backed up to backlog' % (fileInfo[FILE_ID], subscrbId))
    info(3, 'Completed - backing up pending files from delivery queue to back logs')
    
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
                fileInfo = quChunks.get(timeout = 60)   
                srvObj._subscrDeliveryFileDic[tname] = fileInfo # once it is dequeued, it is no longer safe, so need to record it in case server shut down.
            except Empty, e:
                info(4, "Data delivery thread [" + str(thread.get_ident()) + "] block timeout")
                _checkStopDataDeliveryThread(srvObj, subscrbId) # Timeout allows it to check if the delivery thread should stop
                # if delivery thread is to continue, trigger the subscriptionThread to get more files in
                if (srvObj.getDataMoverOnlyActive() and remindMainThread and firstThread):                    
                    # But only the first thread does this to avoid repeated notifications
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
            fileIngDate    = fileInfo[FILE_DATE]
            fileMimeType   = fileInfo[FILE_MIME]
            fileBackLogged = fileInfo[FILE_BL]
            if (fileBackLogged == NGAMS_SUBSCR_BACK_LOG and (not os.path.isfile(filename))):# if this file is removed by an agent outside of NGAS (e.g. Cortex volunteer cleanup)
                alert('File %s is no longer available' %  filename)
                _delFromSubscrBackLog(srvObj, subscrObj.getId(), fileId, fileVersion, filename) 
                continue
            baseName = os.path.basename(filename)
            contDisp = "attachment; filename=\"" + baseName + "\""
            # TODO: Note should not have no_versioning hardcoded in the
            # request send to the client/subscriber.
            contDisp += "; no_versioning=1"
            info(3,"Thread [" + str(thread.get_ident()) + "] Delivering file: " + baseName + "/" +\
                 str(fileVersion) + " - to Subscriber with ID: " +\
                 subscrObj.getId() + " ...")
            ex = ""
            stat = ngamsStatus.ngamsStatus()       
            # Calculate the suspension time for this thread based on the priority of this subscriber. 
            # Dynamically calculated for each file so that the priority can be changed on the fly (no re-subscribe or server restart is needed)
            suspenTime = (0.005 * (subscrObj.getPriority() - 1)) # so that the top priority (level 1) does not suspend at all 
            try:
                reply, msg, hdrs, data = \
                       ngamsLib.httpPostUrl(subscrObj.getUrl(), fileMimeType,
                                            contDisp, filename, "FILE",
                                            blockSize=\
                                            srvObj.getCfg().getBlockSize(),
                                            suspTime = suspenTime)
                if (data.strip() != ""):
                    stat.clear().unpackXmlDoc(data)
                else:
                    # TODO: For the moment assume success in case no
                    #       exception was thrown.
                    stat.clear().setStatus(NGAMS_SUCCESS)
            except Exception, e:
                ex = str(e)
            if ((ex != "") or (reply != NGAMS_HTTP_SUCCESS) or
                (stat.getStatus() == NGAMS_FAILURE)):
                # If an error occurred during data delivery, we should not update
                # the Subscription Status table for this Subscriber, but should
                # instead make an entry in the Subscription Back-Log Table
                # (if the file is not an already back log buffered file, which
                # was attempted re-posted).                
                errMsg = "Error occurred while delivering file: " + baseName +\
                         "/" + str(fileVersion) +\
                         " - to Subscriber with ID/url: " + subscrObj.getId() + "/" + subscrObj.getUrl() + " by Data Delivery Thread [" + str(thread.get_ident()) + "]"
                if (ex != ""): errMsg += " Exception: " + ex + "."
                if (stat.getMessage() != ""):
                    errMsg += " Message: " + stat.getMessage()
                warning(errMsg)
                _genSubscrBackLogFile(srvObj, subscrObj, fileInfo)
                if (fileBackLogged == NGAMS_SUBSCR_BACK_LOG):
                    # remove bl record from the dict
                    if (srvObj._subscrBlScheduledDic.has_key(subscrbId)):
                        k = _fileKey(fileId, fileVersion)
                        srvObj._subscrBlScheduledDic_Sem.acquire()                    
                        try:
                            if (srvObj._subscrBlScheduledDic[subscrbId].has_key(k)):
                                del srvObj._subscrBlScheduledDic[subscrbId][k]
                        finally:
                            srvObj._subscrBlScheduledDic_Sem.release()
            else:
                if (srvObj.getCachingActive()):                   
                    fkey = fileId + "/" + str(fileVersion)
                    fileDeliveryCountDic_Sem.acquire()
                    try:
                        if (fileDeliveryCountDic.has_key(fkey)):
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
                            else:
                                alert("Fail to find %s/%d in the fileDeliveryCountDic" % (fileId, fileVersion))
                    finally:
                        fileDeliveryCountDic_Sem.release()
                info(3,"File: " + baseName + "/" + str(fileVersion) +\
                     " - delivered to Subscriber with ID: " + subscrObj.getId() + " by Data Delivery Thread [" + str(thread.get_ident()) + "]")
                
                # Update the Subscriber Status to avoid that this file
                # gets delivered again.
                try:
                    srvObj.getDb().updateSubscrStatus(subscrObj.getId(), fileIngDate)
                except Exception, e:
                    # continue with warning message. this means the database (i.e. last_ingestion_date) is not synchronised for this file, 
                    # but at least remaining files can be delivered continuously, the database may be back in sync upon delivering remaining files 
                    errMsg = "Error occurred during update the ngas_subscriber table " +\
                         "_devliveryThread [" + str(thread.get_ident()) + "] Exception: " + str(e)
                    alert(errMsg)
    
                # If the file is back-log buffered, we check if we can delete it.
                if (fileBackLogged == NGAMS_SUBSCR_BACK_LOG):
                    srvObj._subscrBlScheduledDic_Sem.acquire()
                    try: # the following block must be atomic
                        _delFromSubscrBackLog(srvObj, subscrObj.getId(), fileId,
                                          fileVersion, filename)
                        if (srvObj._subscrBlScheduledDic.has_key(subscrbId)):
                            k = _fileKey(fileId, fileVersion)
                            if (srvObj._subscrBlScheduledDic[subscrbId].has_key(k)):
                                del srvObj._subscrBlScheduledDic[subscrbId][k]
                    finally:
                        srvObj._subscrBlScheduledDic_Sem.release()                    
                    
            srvObj._subscrDeliveryFileDic[tname] = None
        except Exception, be:
            if (str(be).find("_STOP_DELIVERY_THREAD_") != -1): 
                # Stop delivery thread.
                info(3, 'Delivery thread [' + str(thread.get_ident()) + '] is exiting.')
                thread.exit()
            errMsg = "Error occurred during file delivery: " + str(be)
            alert(errMsg)


def _fileKey(fileId,
             fileVersion):
    """
    Generate file identifier.

    fileId:        File ID (string).
    
    fileVersion:   File Version (integer).

    Returns:       File identifier (string).
    """
    return fileId + "___" + str(fileVersion)

     
def subscriptionThread(srvObj,
                       dummy):
    """
    The Subscription Thread is normally suspended, but is woken up
    whenever there might be data to be delivered to Subscribers.

    srvObj:      Reference to server object (ngamsServer).

    dummy:       Needed by the thread handling ... 
    
    Returns:     Void.
    """
    info(3,"Data Subscription Thread initializing ...")
    dataMoverOnly = srvObj.getDataMoverOnlyActive()
    if (dataMoverOnly):
        dm_hosts = srvObj.getCfg().getDataMoverHostIds()
        if (dm_hosts == None):
            raise Exception, "No data mover hosts are available!"
        else:    
            dm_hosts = map(lambda x: x.strip(), dm_hosts.split(','))
            if (len(dm_hosts) < 1):
                raise Exception, "Invalid data mover hosts configuration!"
        
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
            # TODO - handle the abnormal shutdown situation, i.e. 
            # for each subscriber, find out the LastFileIngDate, and query db with: ingestion_date > 'LastFileIngDate'
            # if (not dataMoverOnly and abnormalShutdown): abnormalShutdown = False then: blah blah
            if (dataMoverOnly and srvObj.getSubcrBackLogCount() <= 0): # this ensures back-logged files to be sent before new files are brought in
                # data mover purposefully only supports exact one subscriber. 
                # To support multiple subscribers, run multiple data mover servers
                subscrId = srvObj.getSubscriberDic().keys()[0] 
                #debug_chen
                #info(3, 'Data mover subscriId = %s' % subscrId)
                subscrObj = srvObj.getSubscriberDic()[subscrId]
                start_date = None
                if (scheduledStatus.has_key(subscrId)):
                    if (scheduledStatus[subscrId]):
                        start_date = scheduledStatus[subscrId]
                elif (subscrObj.getLastFileIngDate() and '1970-01-01' != subscrObj.getLastFileIngDate().split('T')[0]):
                    start_date = subscrObj.getLastFileIngDate()
                elif (subscrObj.getStartDate()):
                    start_date = subscrObj.getStartDate()
                
                #debug_chen
                info(3, 'Data mover start_date = %s\n' % start_date)    
                count = 0
                for host in dm_hosts:         
                    cursorObj = srvObj.getDb().getFileSummary2(hostId = host, ing_date = start_date) # need to add file_version == 1 condition!!
                    while (1):
                        fileList = cursorObj.fetch(100)
                        if (fileList == []): break
                        for fileInfo in fileList:
                            fileInfo = _convertFileInfo(fileInfo)
                            fileDicDbm.add(_fileKey(fileInfo[FILE_ID], fileInfo[FILE_VER]), fileInfo)
                            count += 1
                        _checkStopSubscriptionThread(srvObj)
                        time.sleep(0.1)
                    del cursorObj
                if (count == 0):
                    #debug_chen
                    info(3, 'No files meet the data mover condition')
                    continue
                else:
                    info(3, 'Data mover will examine %d files for delivery' % count)
            elif (subscrObjs != []):
                cursorObj = srvObj.getDb().getFileSummary2(getHostId())
                while (1):
                    fileList = cursorObj.fetch(100)
                    if (fileList == []): break
                    for fileInfo in fileList:
                        fileInfo = _convertFileInfo(fileInfo)
                        fileDicDbm.add(_fileKey(fileInfo[FILE_ID],
                                                fileInfo[FILE_VER]),
                                       fileInfo)
                    _checkStopSubscriptionThread(srvObj)
                    time.sleep(0.1)
                del cursorObj
            elif (fileRefs != []):
                # fileRefDic: Dictionary indicating which versions for each
                # file that are of interest.
                # debug_chen
                info(4, 'Count of fileRefs = %d' % len(fileRefs))
                fileRefDic = {}
                fileIds = {}   # To generate a list with all File IDs
                for fileInfo in fileRefs:
                    fileId  = fileInfo[0]
                    fileVersion = fileInfo[1]
                    fileIds[fileId] = 1
                    if (fileRefDic.has_key(fileId)):
                        fileRefDic[fileId].append(fileVersion)
                    else:
                        fileRefDic[fileId] = [fileVersion]

                cursorObj = srvObj.getDb().getFileSummary2(getHostId(),
                                                           fileIds.keys(),
                                                           ignore=0)
                while (1):
                    fileList = cursorObj.fetch(100)
                    if (fileList == []): break
                    _checkStopSubscriptionThread(srvObj)
                    for fileInfo in fileList:
                        # Take only the file if the File ID + File Version are
                        # explicitly specified.
                        fileInfo = _convertFileInfo(fileInfo)
                        if (ngamsLib.elInList(fileRefDic[fileInfo[FILE_ID]],
                                              fileInfo[FILE_VER])):
                            fileDicDbm.add(_fileKey(fileInfo[FILE_ID],
                                                    fileInfo[FILE_VER]),
                                           fileInfo)
                    time.sleep(0.1) 
                del cursorObj

            # The Deliver Status Dictionary indicates for each Subscriber
            # when the last file was delivered. We first initialize the
            # Deliver Status Dictionary with None to indicate later if that
            # this Subscriber (apparently) didn't have any files delivered.
            deliveredStatus = {}            
            for subscrId in srvObj.getSubscriberDic().keys():
                deliveredStatus[subscrId] = None
                if (not scheduledStatus.has_key(subscrId)):
                    scheduledStatus[subscrId] = None
            subscrIds = srvObj.getSubscriberDic().keys()
            subscrStatus = srvObj.getDb().\
                           getSubscriberStatus(subscrIds, getHostId(),
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
                    warning(errMsg)
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
            for subscrObj in subscrObjs:
                # Loop over each file and check if it should be delivered.
                for fileKey in fileDicDbm.keys():
                    fileInfo = fileDicDbm.get(fileKey)
                    _checkIfDeliverFile(srvObj, subscrObj, fileInfo,
                                        deliverReqDic, deliveredStatus, scheduledStatus, fileDeliveryCountDic, fileDeliveryCountDic_Sem)

            # Third, if datamover, add those files
            if (dataMoverOnly):
                info(3, 'Checking data mover files')
                for fileKey in fileDicDbm.keys():
                    fileInfo = fileDicDbm.get(fileKey)
                    _checkIfDeliverFile(srvObj, subscrObj, fileInfo,
                                        deliverReqDic, deliveredStatus, scheduledStatus, fileDeliveryCountDic, fileDeliveryCountDic_Sem)
            
            # Then finally check if there are back-logged files to deliver.
            selectDiskId = srvObj.getCachingActive()
            srvObj._subscrBlScheduledDic_Sem.acquire()
            try:
                subscrBackLog = srvObj.getDb().\
                                getSubscrBackLog(getHostId(),
                                                 srvObj.getCfg().getPortNo(), selectDiskId)
                for backLogInfo in subscrBackLog:
                    subscrId = backLogInfo[0]
                    # Note, it is signalled by adding an extra element (at the end)
                    # with the value of the constant NGAMS_SUBSCR_BACK_LOG, that
                    # this file is a back-logged file. This is done to make the
                    # handling more efficient.
                    if (selectDiskId):
                        fileInfo = list(backLogInfo[2:]) + [NGAMS_SUBSCR_BACK_LOG]
                    else:
                        fileInfo = list(backLogInfo[2:]) + [None] + [NGAMS_SUBSCR_BACK_LOG]
    
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
            
            for subscrId in deliverReqDic.keys():
                deliverReqDic[subscrId].sort(_compFct)
                #get the ingest_date of the last file in the queue (list)
                lastScheduleDate = _convertFileInfo(deliverReqDic[subscrId][-1])[FILE_DATE]
                scheduledStatus[subscrId] = lastScheduleDate
                
                # multi-threaded concurrent transfer, added by chen.wu@icrar.org
                num_threads = float(srvObj.getSubscriberDic()[subscrId].getConcurrentThreads())
                if queueDict.has_key(subscrId):
                    #debug_chen
                    #info(4, 'Use existing queue for %s' % subscrId)
                    quChunks = queueDict[subscrId]
                else:
                    quChunks = Queue()
                    queueDict[subscrId] = quChunks
 
                allFiles = deliverReqDic[subscrId]         
                if (srvObj.getSubcrBackLogCount() > 0):
                    info(3, 'Put %d new files in the queue for subscriber %s' %(len(allFiles), subscrId))        
                for jdx in range(len(allFiles)):
                    quChunks.put(allFiles[jdx])   
                    # Deliver the data - spawn off a Delivery Thread to do this job                    
                if not deliveryThreadDic.has_key(subscrId):
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
        except Exception, e:
            try:
                del fileDicDbm
            except:
                pass
            rmFile(fileDicDbmName + "*")
            if (str(e).find("_STOP_SUBSCRIPTION_THREAD_") != -1): thread.exit()
            errMsg = "Error occurred during execution of the Data " +\
                     "Subscription Thread. Exception: " + str(e)
            alert(errMsg)    
            em = traceback.format_exc()    
            alert(em)             

# EOF
