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

import thread, threading, time, commands, cPickle, types
from ngams import *
import ngamsDbm, ngamsDb, ngamsLib, ngamsStatus, ngamsHighLevelLib

# TODO:
# - Should not hardcode no_versioning=1.
# - Should not back-log buffer data 'physically'.


# Some 'constant abbreviatins' used in this module.
FILE_ID   = 0
FILE_NM   = 1
FILE_VER  = 2
FILE_DATE = 3
FILE_MIME = 4
FILE_BL   = 5

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


def _checkStopDataDeliveryThread(srvObj):
    """
    Function used by the Data Delivery Threads to check if they should
    stop execution.

    srvObj:     Reference to server object (ngamsServer).
    
    Returns:    Void. 
    """
    if (srvObj._deliveryStopSync.isSet()):
        info(2,"Stopping Data Delivery Thread ...")
        raise Exception, "_STOP_SUBSCRIPTION_THREAD_"


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
    if (srvObj.getSubcrBackLogCount()):
        suspTime = isoTime2Secs(srvObj.getCfg().getSubscrSuspTime())
        srvObj._subscriptionRunSync.wait(suspTime)
    else:
        srvObj._subscriptionRunSync.wait()
    
    _checkStopSubscriptionThread(srvObj)
    info(4,"Data Subscription Thread received wake-up signal ...")
    try:
        srvObj._subscriptionSem.acquire()
        srvObj._subscriptionRunSync.clear()
        filenames = srvObj._subscriptionFileList
        srvObj._subscriptionFileList = []
        subscrObjs = srvObj._subscriptionSubscrList
        srvObj._subscriptionSubscrList = []
        srvObj._subscriptionSem.release()
        return (filenames, subscrObjs)
    except Exception, e:
         srvObj._subscriptionSem.release()
         errMsg = "Error occurred in ngamsSubscriptionThread." +\
                  "_waitForScheduling(). Exception: " + str(e)
         alert(errMsg)
         return ([], [])


def _addFileDeliveryDic(subscrId,
                        fileInfo,
                        deliverReqDic):
    """
    Add a file in the delivery dictionary. If file already registered,
    replace the existing entry if not a back-log buffered file, and the
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
    filename            = fileInfo[FILE_NM]
    fileVersion         = fileInfo[FILE_VER]
    fileBackLogBuffered = fileInfo[FILE_BL]
    if (deliverReqDic.has_key(subscrId)):
        add = 1
        # Check if the file is already registered for that Subscriber.
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
                    break
        if (add): deliverReqDic[subscrId].append(fileInfo)
    else:
        # It was a new entry, create new list for this Subscriber.
        deliverReqDic[subscrId] = [fileInfo]
    

def _checkIfDeliverFile(srvObj,
                        subscrObj,
                        fileInfo,
                        deliverReqDic,
                        deliveredStatus):
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

    fileInfo            = _convertFileInfo(fileInfo)
    fileId              = fileInfo[FILE_ID]
    filename            = fileInfo[FILE_NM]
    fileVersion         = fileInfo[FILE_VER]
    fileIngDate         = fileInfo[FILE_DATE]
    fileBackLogBuffered = fileInfo[FILE_BL]
    if (
        ((lastDelivery == None) and (subscrObj.getStartDate() == "")) or
        
        ((lastDelivery == None) and
         (fileIngDate >= subscrObj.getStartDate())) or
        
        ((lastDelivery != None) and
         (fileIngDate >= subscrObj.getStartDate()) and
         (fileIngDate >= lastDelivery))
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
                                            deliverReqDic)


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
        locFileInfo = 6 * [None]
        locFileInfo[FILE_ID]   = fileInfo[ngamsDb.ngamsDbCore.SUM2_FILE_ID]
        locFileInfo[FILE_NM]   = \
                             os.path.normpath(fileInfo[ngamsDb.ngamsDbCore.SUM2_MT_PT] +\
                                              os.sep +\
                                              fileInfo[ngamsDb.ngamsDbCore.SUM2_FILENAME])
        locFileInfo[FILE_VER]  = fileInfo[ngamsDb.ngamsDbCore.SUM2_VERSION]
        locFileInfo[FILE_DATE] = fileInfo[ngamsDb.ngamsDbCore.SUM2_ING_DATE]
        locFileInfo[FILE_MIME] = fileInfo[ngamsDb.ngamsDbCore.SUM2_MIME_TYPE]
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
    
    # Increase the Subscription Back-Log Counter to indicate to the Data
    # Subscription Thread that it should only suspend itself temporarily.
    srvObj.incSubcrBackLogCount()

    # If file was already back-logged, nothing is done.
    if (locFileInfo[FILE_BL] == NGAMS_SUBSCR_BACK_LOG): return

    # NOTE: The actions carried out by this function are critical and need
    #       to be semaphore protected (Back-Log Operations Semaphore).
    srvObj._backLogAreaSem.acquire()
    try:
        # Create copy of file in Subscription Back-Log Area + make entry in
        # the DB for the file.
        fileId        = locFileInfo[FILE_ID]
        filename      = locFileInfo[FILE_NM]
        fileVersion   = locFileInfo[FILE_VER]
        fileIngDate   = locFileInfo[FILE_DATE]
        fileMimeType  = locFileInfo[FILE_MIME]
        backLogName   = os.path.\
                        normpath(srvObj.getCfg().getBackLogBufferDirectory() +\
                                 "/" + NGAMS_SUBSCR_BACK_LOG_DIR + "/" +\
                                 fileId + "/" + str(fileVersion) +\
                                 "/" + os.path.basename(filename))
        if (not os.path.exists(backLogName)):
            checkCreatePath(os.path.dirname(backLogName))
            commands.getstatusoutput("cp " + filename +\
                                     " " + backLogName)
        srvObj.getDb().addSubscrBackLogEntry(getHostId(),
                                             srvObj.getCfg().getPortNo(),
                                             subscrObj.getId(),
                                             subscrObj.getUrl(),
                                             fileId,
                                             backLogName,
                                             fileVersion,
                                             fileIngDate,
                                             fileMimeType)

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
        # Delete the entry from the DB for that file/Subscriber.
        srvObj.getDb().delSubscrBackLogEntry(getHostId(),
                                             srvObj.getCfg().getPortNo(),
                                             subscrId, fileId, fileVersion)
        
        # If there are no other Subscribers interested in this file, we
        # delete the file.
        subscrIds = srvObj.getDb().getSubscrsOfBackLogFile(fileId, fileVersion)
        if (subscrIds == []): commands.getstatusoutput("rm -f " + fileName)
        srvObj._backLogAreaSem.release()
    except Exception, e:
        srvObj._backLogAreaSem.release()
        raise e


def _deliveryThread(srvObj,
                    subscrObj,
                    fileInfoList,
                    dummy):
    """
    Function to be executed as a thread to delivery data to a Data Subscriber.

    srvObj:        Reference to server object (ngamsServer).

    subscrObj:     Subscriber Object (ngamsSubscriber). 
    
    fileInfoList:  List with sub-lists with information about file
                   (sub-list generated by ngamsDb.getFileSummary2().
                   Note: In case the file was contained in the Subscription
                   Back-Log, it will have an extra element appended to the
                   file info list, with the value of NGAMS_SUBSCR_BACK_LOG
                   (list/list).
    
    dummy:         Needed by the thread handling ... 

    Returns:       Void.
    """
    info(3,"Data Delivery Thread preparing to deliver files to Subscriber "+\
         "with ID: " + subscrObj.getId() + " ...")
    
    # Calculate the suspension time for this thread based on the
    # priority of this thread.
    suspTime = (0.005 * subscrObj.getPriority())
    
    # Loop over the files to deliver and deliver these to the Subscribers.
    stat = ngamsStatus.ngamsStatus()
    for fileInfo in fileInfoList:
        fileInfo = _convertFileInfo(fileInfo)
        
        # Allow to break this loop.
        _checkStopDataDeliveryThread(srvObj)

        # Prepare info and POST the file.
        fileId         = fileInfo[FILE_ID]
        filename       = fileInfo[FILE_NM]
        fileVersion    = fileInfo[FILE_VER]
        fileIngDate    = fileInfo[FILE_DATE]
        fileMimeType   = fileInfo[FILE_MIME]
        fileBackLogged = fileInfo[FILE_BL]
        baseName = os.path.basename(filename)
        contDisp = "attachment; filename=\"" + baseName + "\""
        # TODO: Note should not have no_versioning hardcoded in the
        # request send to the client/subscriber.
        contDisp += "; no_versioning=1"
        info(3,"Delivering file: " + baseName + "/" +\
             str(fileVersion) + " - to Subscriber with ID: " +\
             subscrObj.getId() + " ...")
        ex = ""
        try:
            reply, msg, hdrs, data = \
                   ngamsLib.httpPostUrl(subscrObj.getUrl(), fileMimeType,
                                        contDisp, filename, "FILE",
                                        blockSize=\
                                        srvObj.getCfg().getBlockSize())
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
                     " - to Subscriber with ID: " + subscrObj.getId() + "."
            if (ex != ""): errMsg += " Exception: " + ex + "."
            if (stat.getMessage() != ""):
                errMsg += " Message: " + stat.getMessage()
            warning(errMsg)
            _genSubscrBackLogFile(srvObj, subscrObj, fileInfo)
        else:
            info(3,"File: " + baseName + "/" + str(fileVersion) +\
                 " - delivered to Subscriber with ID: " + subscrObj.getId())
            
            # Update the Subscriber Status to avoid that this file
            # gets delivered again.
            srvObj.getDb().updateSubscrStatus(subscrObj.getId(),
                                              fileIngDate)

            # If the file is back-log buffered, we check if we can delete it.
            if (fileBackLogged == NGAMS_SUBSCR_BACK_LOG):
                filename = os.path.normpath(filename)
                _delFromSubscrBackLog(srvObj, subscrObj.getId(), fileId,
                                      fileVersion, filename)
    
    # Stop this thread.
    thread.exit()


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
    fileDicDbm = None
    fileDicDbmName = ngamsHighLevelLib.genTmpFilename(srvObj.getCfg(),
                                                      NGAMS_SUBSCRIPTION_THR +\
                                                      "_FILE_DIC")
    while (1):
        # Incapsulate this whole block to avoid that the thread dies in
        # case a problem occurs, like e.g. a problem with the DB connection.
        try:
            fileRefs, subscrObjs = _waitForScheduling(srvObj)
            _checkStopSubscriptionThread(srvObj)
            srvObj.resetSubcrBackLogCount()

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
            if (subscrObjs != []):
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
                             fileVersion + ") not registered in the NGAS DB"
                    warning(errMsg)
                    continue

                # Loop to determine for each Subscriber whether to deliver
                # the file or not to this or not.
                for subscrId in srvObj.getSubscriberDic().keys():
                    subscrObj = srvObj.getSubscriberDic()[subscrId]
                    _checkIfDeliverFile(srvObj, subscrObj, tmpFileInfo,
                                        deliverReqDic, deliveredStatus)

            # Then check if for each of the Subscribers referenced explicitly
            # (new Subscribers) for each file Online on this system, if we
            # should deliver data to these.
            for subscrObj in subscrObjs:
                # Loop over each file and check if it should be delivered.
                for fileKey in fileDicDbm.keys():
                    fileInfo = fileDicDbm.get(fileKey)
                    _checkIfDeliverFile(srvObj, subscrObj, fileInfo,
                                        deliverReqDic, deliveredStatus)

            # Then finally check if there are back-logged files to deliver.
            subscrBackLog = srvObj.getDb().\
                            getSubscrBackLog(getHostId(),
                                             srvObj.getCfg().getPortNo())
            for backLogInfo in subscrBackLog:
                subscrId = backLogInfo[0]
                # Note, it is signalled by adding an extra element (at the end)
                # with the value of the constant NGAMS_SUBSCR_BACK_LOG, that
                # this file is a back-logged file. This is done to make the
                # handling more efficient.
                fileInfo = list(backLogInfo[2:]) + [NGAMS_SUBSCR_BACK_LOG]

                # If a Subscriber is no-longer subscribed, the back-logged
                # entry is simply deleted.
                if (not srvObj.getSubscriberDic().has_key(subscrId)):
                    fileId      = fileInfo[FILE_ID]
                    filename    = fileInfo[FILE_NM]
                    fileVersion = fileInfo[FILE_VER]
                    _delFromSubscrBackLog(srvObj, subscrId, fileId,
                                          fileVersion, fileId)
                else:
                    _addFileDeliveryDic(subscrId, fileInfo, deliverReqDic)

            # Sort the files listed in the Delivery Dictionary for each
            # Subscriber so that files are sorted according to Ingestion Date.
            # This is done in order to prevent that a file with a more recent
            # Ingestion Date is registered in the Subscriber Status,
            # preventing other files with an older Ingestion Date, which
            # should have been delivered from being delivered.
            # Then deliver the data (if there is something to deliver).
            # Data Delivery Thread is spawned off per Subscriber, which should
            # receive data.        
            deliveryThreads = []
            for subscrId in deliverReqDic.keys():
                deliverReqDic[subscrId].sort(_compFct)

                # Deliver the data - spawn off a Delivery Thread to do this job
                # per Subscriber.
                args = (srvObj, srvObj.getSubscriberDic()[subscrId],
                        deliverReqDic[subscrId], None)
                deliveryThrRef = threading.Thread(None, _deliveryThread,
                                                  NGAMS_DELIVERY_THR+subscrId,
                                                  args)
                deliveryThrRef.setDaemon(0)
                deliveryThrRef.start()
                deliveryThreads.append(deliveryThrRef)

            # Wait nicely until all files have been delivered. This is done
            # to prevent that another set of Data Deliveries are spawned off
            # before this is finished; this might create conflicts.
            while (len(deliveryThreads)):
                for idx in range(len(deliveryThreads)):
                    if (not deliveryThreads[idx].isAlive()):
                        # If a thread is no-longer active, we break the loop
                        # and start the checking all over again.
                        del deliveryThreads[idx]
                        break
                time.sleep(1.0)
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
                    

# EOF
