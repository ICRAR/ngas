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
# "@(#) $Id: ngamsStatusCmd.py,v 1.11 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  22/04/2003  Created
#

"""
Function + code to handle the STATUS command.
"""
import time, sys, types, gzip, glob

import pcc, PccUtTime
from ngams import *
import ngamsDbCore, ngamsDb, ngamsDbm, ngamsLib, ngamsStatus, ngamsDiskInfo
import ngamsDppiStatus
import ngamsFileInfo, ngamsHighLevelLib, ngamsFileUtils
import ngamsSrvUtils, ngamsRetrieveCmd


# Man-page for the command.
_help = loadDoc("ngamsServer/ngamsStatusCmd.doc")


def _checkFileAccess(srvObj,
                     reqPropsObj,
                     httpRef,
                     fileId,
                     fileVersion = -1,
                     diskId = ""):
    """
    Check if a file is accessible either on the local host or on a remotely
    located host.

    srvObj:         Instance of the server object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).

    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).
    
    fileId:         File ID (string).
    
    fileVersion:    File Version (integer).
    
    diskId:         Disk ID (string).

    Returns:        Returns message indicating if the file is available
                    (string).
    """
    T = TRACE()

    info(4,"Checking for access to file with ID: " + fileId + " ...")

    # Check if the file is located on this host, or if the request should be
    # forwarded (if this server should act as proxy).
    location, fileHost, ipAddress, filePortNo, mountPoint, filename, fileId,\
              fileVersion, mimeType =\
              ngamsFileUtils.locateArchiveFile(srvObj, fileId, fileVersion,
                                               diskId)
    if (location != NGAMS_HOST_LOCAL):
        httpStatCode, httpStatMsg, httpHdrs, data =\
                      srvObj.forwardRequest(reqPropsObj, httpRef, fileHost,
                                            filePortNo, autoReply = 0)
        tmpStat = ngamsStatus.ngamsStatus().unpackXmlDoc(data)
        return tmpStat.getMessage()
    else:
        # First check if this system allows for Retrieve Requests.
        if (not srvObj.getCfg().getAllowRetrieveReq()):
            msg = genLog("NGAMS_INFO_SERVICE_UNAVAIL", ["File Retrieval"])
        else:
            fileHost = "%s:%d" % (getHostName(), filePortNo)
            msg = genLog("NGAMS_INFO_FILE_AVAIL",
                         [fileId + "/Version: " + str(fileVersion), fileHost])
        return msg


def _getRefCounts():
    """
    Return information about all object allocated and the number of
    references pointing to each object. This can be used to check a running
    server for memory (object) leaks.

    Taken from: http://www.nightmare.com/medusa/memory-leaks.html.

    Returns:    
    """
    T = TRACE()
    
    d = {}
    sys.modules
    # Collect all classes
    for m in sys.modules.values():
        for sym in dir(m):
            o = getattr (m, sym)
            if type(o) is types.ClassType:
                d[o] = sys.getrefcount (o)
    # sort by refcount
    pairs = map(lambda x: (x[1],x[0]), d.items())
    pairs.sort()
    pairs.reverse()
    return pairs


def _genRefCountRep():
    """
    Generate a report with number of objects allocated of each type.
    The report is a list with a sublist for each object of the form:

      [[<No of Refs>, <Object>], ...]

    Returns:   Report (list).
    """
    T = TRACE()

    refDic = {}
    for count, obj in _getRefCounts():
        if (not refDic.has_key(obj)): refDic[obj] = 0
        refDic[obj] += count
    objList = []
    objects = refDic.keys()
    objects.sort()
    for obj in objects:
        objList.append([refDic[obj], obj])
    return objList

  
def _genObjectStatus():
    """
    Generate a report with information about objects allocated, numbers of
    references to each object and other information that can be used to
    track down memory (object) leaks in the server.
    
    Returns:  Object report (string).
    """
    T = TRACE()
    
    rep = "NG/AMS SERVER OBJECT STATUS REPORT\n\n"
    import gc
    gc.set_debug(gc.DEBUG_COLLECTABLE | gc.DEBUG_UNCOLLECTABLE |
                 gc.DEBUG_INSTANCES | gc.DEBUG_OBJECTS)
    rep += "=Garbage Collector Status:\n\n"
    rep += "Enabled:             %d\n" % gc.isenabled()
    rep += "Unreachable objects: %d\n" % gc.collect()
    rep += "Threshold:           %s\n" % str(gc.get_threshold())
    #rep += "Objects:\n"
    #rep += str(gc.get_objects()) + "\n"
    rep += "Garbage:\n"
    rep += str(gc.garbage) + "\n\n"

    # Dump refence count status of all objects allocated into file specified.
    rep += "=Object Reference Counts:\n\n"
    for objInfo in _genRefCountRep():
        rep += "%-4d %s\n" % (objInfo[0], objInfo[1])
        
    rep += "\n=EOF"
    return rep


_fileListXmlHdr = """<?xml version="1.0" ?> 

<NgamsStatus>
<FileList Id="%s" Status="REMAINING_DATA_OBJECTS: %s">

"""

_fileListXmlFooter = """

</FileList>
</NgamsStatus>
"""

STATUS_FILE_LIST_DBM_TAG = "STATUS_FILE_LIST_%s_DBM"

def _handleFileList(srvObj,
                    reqPropsObj,
                    httpRef):
    """
    Handle STATUS?file_list... Command.
        
    srvObj:         Reference to NG/AMS server class object (ngamsServer).
    
    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).
        
    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).
        
    Returns:        The File List ID allocated for this request (string).
    """
    T = TRACE()

    # Should a lower limit for the ingestion date be taken into account.
    fromIngDate = None
    if (reqPropsObj.hasHttpPar("from_ingestion_date")):
        tmpTromIngDate = reqPropsObj.getHttpPar("from_ingestion_date")
        # Ensure representation is ISO 8601.
        try:
            fromIngDate = timeRef2Iso8601(tmpTromIngDate)
        except Exception, e:
            msg = "from_ingestion_date should be given as an ISO 8601 time " +\
                  "stamp or seconds since epoch. Value given: %s. Error: %s"
            raise Exception, msg % (str(tmpTromIngDate), str(e))

    # Handle the unique flag. If this is specified, only information for unique
    # File ID/Version pairs are returned.
    unique = False
    if (reqPropsObj.hasHttpPar("unique")):
        unique = int(reqPropsObj.getHttpPar("unique"))

    # Dump the file information needed.
    fileListId = genUniqueId()
    dbmBaseName = STATUS_FILE_LIST_DBM_TAG % fileListId
    fileInfoDbmBaseName = ngamsHighLevelLib.genTmpFilename(srvObj.getCfg(),
                                                           dbmBaseName)

    # Dump the file info from the DB.
    try:
        fileInfoDbmName = srvObj.getDb().\
                          dumpFileInfo2(fileInfoDbmBaseName,
                                        hostId = getHostId(),
                                        ignore = 0,
                                        lowLimIngestDate = fromIngDate)
        
        # If requested, make the result set unique by inserting the elements
        # with File ID/Version as key.
        if (unique):
            fileInfoDbm = ngamsDbm.ngamsDbm(fileInfoDbmName)
            uniqueFileInfoDbmName = fileInfoDbmBaseName + "_UNIQUE"
            uniqueFileListDbm = ngamsDbm.ngamsDbm(uniqueFileInfoDbmName,
                                                  cleanUpOnDestr = 0,
                                                  writePerm = 1)
            while (True):
                key, fileInfo = fileInfoDbm.getNext()
                if (not key):
                    break
                fileKey = "%s_%s" %\
                          (fileInfo[ngamsDbCore.NGAS_FILES_FILE_ID],
                           fileInfo[ngamsDbCore.NGAS_FILES_FILE_VER])
                if (uniqueFileListDbm.hasKey(fileKey)):
                    # File with that ID/Version already registered.
                    continue
                uniqueFileListDbm.add(fileKey, fileInfo)
            fileInfoDbm.sync() 
            fileInfoDbmName = fileInfoDbm.getDbmName()
            del fileInfoDbm
            uniqueFileListDbm.sync()
            uniqueFileListDbmName = uniqueFileListDbm.getDbmName()
            del uniqueFileListDbm
            mvFile(uniqueFileListDbmName, fileInfoDbmName)
    except Exception, e:
        rmFile("%s*" % fileInfoDbmBaseName)
        msg = "Problem generating file list for STATUS Command. " +\
              "Parameters: from_ingestion_date=%s. Error: %s" %\
              (str(fromIngDate), str(e))
        error(msg)
        raise Exception, msg

    return fileListId


def _handleFileListReply(srvObj,
                         reqPropsObj,
                         httpRef,
                         fileListId,
                         maxElements = None):
    """
    Extracts file information from a previously dumped file information
    in connection with a STATUS?file_list request.
    
    srvObj:         Reference to NG/AMS server class object (ngamsServer).
    
    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).
        
    httpRef:        Reference to the HTTP request handler object
                    (ngamsHttpRequestHandler).

    fileListId:     File List ID allocated to this request (string).

    maxElements:    Maximum number of elements to extract and return to the
                    requestor (integer).

    Returns:        Void.
    """
    T = TRACE()

    # Get the name of the DBM in which the information is stored.
    dbmBaseName = STATUS_FILE_LIST_DBM_TAG % fileListId
    dbmPat = os.path.normpath("%s/*%s*" %\
                              (ngamsHighLevelLib.\
                               getNgasTmpDir(srvObj.getCfg()),
                               dbmBaseName))
    dbmMatches = glob.glob(dbmPat)
    if (len(dbmMatches) < 1):
        msg = "Referenced File List ID: %s in connection with " +\
              "STATUS/file_list request, is not (or no longer) known"
        raise Exception, msg % fileListId
    elif (len(dbmMatches) > 1):
        msg = "Inconsistencies encountered in locating result set for " +\
              "STATUS/file_list for referenced File List ID: %s"
        raise Exception, msg % fileListId    
    fileInfoDbmName = dbmMatches[0]
    
    # Generate the NG/AMS Status Document, with a File List in it.
    # Compress it on the fly.
    fileListXmlDoc = ngamsHighLevelLib.\
                     genTmpFilename(srvObj.getCfg(),
                                    "STATUS_FILE_LIST_XML.xml")
    try:
        tmpFileListDbm = ngamsDbm.ngamsDbm(fileInfoDbmName,
                                           cleanUpOnDestr = 0,
                                           writePerm = 1)
        rmFile(fileListXmlDoc)
        fo = open(fileListXmlDoc, "w")
        if (not maxElements):
            remainingObjects = 0
        else:
            remainingObjects = (tmpFileListDbm.getCount() - maxElements)
        if (remainingObjects < 0):
            remainingObjects = 0
        fo.write(_fileListXmlHdr % (fileListId, str(remainingObjects)))

        # Loop over the file info objects and write them into the file.
        # take max the number of elements specified.
        tmpFileListDbm.initKeyPtr()
        elCount = 0
        keyRefList = []
        while (True):
            # Have the requested number of elements been extracted?
            if (maxElements):
                if (elCount >= maxElements):
                   break

            # Get the next key (if there are more elements).
            key, fileInfo = tmpFileListDbm.getNext()
            if (not key):
                break
            try:
                # Write the File Status XML Element in the file.
                tmpFileInfoObj = ngamsFileInfo.ngamsFileInfo().\
                                 unpackSqlResult(fileInfo)
                fileInfoXml = tmpFileInfoObj.genXml(storeDiskId = 1).\
                              toprettyxml("  ", "\n")[:-1]
                fo.write("\n" + fileInfoXml)
            except Exception, e:
                msg = "Error creating STATUS/File List XML Document. " +\
                      "Error: %s" % str(e)
                error(msg)
                raise Exception, e
            keyRefList.append(key)
            elCount += 1
        # Finish up the XML document, close the file.
        fo.write(_fileListXmlFooter)
        fo.close()
        # Assume this type of file can always be compressed.
        fileListXmlDoc = compressFile(fileListXmlDoc)
    except Exception, e:
        rmFile("%s*" % fileInfoDbmName)
        rmFile("%s*" % fileListXmlDoc)
        raise e
    
    # Send the XML document back to the requestor.
    try:
        tmpDppiResult = ngamsDppiStatus.ngamsDppiResult(NGAMS_PROC_FILE).\
                        setMimeType(NGAMS_GZIP_XML_MT).\
                        setDataRef(fileListXmlDoc).\
                        setRefFilename(os.path.basename(fileListXmlDoc))
        tmpDppiStatus = ngamsDppiStatus.ngamsDppiStatus().\
                        addResult(tmpDppiResult)
        ngamsRetrieveCmd.genReplyRetrieve(srvObj, reqPropsObj, httpRef,
                                          [tmpDppiStatus])
        reqPropsObj.setSentReply(1)
        rmFile("%s*" % fileListXmlDoc)

        # Remove the reported entries.
        for key in keyRefList:
            tmpFileListDbm.rem(key)
        tmpFileListDbm.sync()
        del keyRefList
        keyRefList = []
        
        # If there are no more entries, delete the DBM.
        dbmCount = tmpFileListDbm.getCount()
        del tmpFileListDbm
        if (dbmCount == 0):
            rmFile("%s*" % fileInfoDbmName)

    except Exception, e:
        rmFile("%s*" % fileListXmlDoc)
        msg = "Error returning response to STATUS?file_list request. Error: %s"
        msg = msg % str(e)
        error(msg)
        raise Exception, msg
    
        
def handleCmdStatus(srvObj,
                    reqPropsObj,
                    httpRef):
    """
    Handle STATUS command.
        
    srvObj:         Reference to NG/AMS server class object (ngamsServer).
    
    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).
        
    httpRef:        Reference to the HTTP request handler object
                    (ngamsHttpRequestHandler).
        
    Returns:        Void.
    """
    T = TRACE()

    status = ngamsStatus.ngamsStatus()
    status.\
             setDate(PccUtTime.TimeStamp().getTimeStamp()).\
             setVersion(getNgamsVersion()).setHostId(getHostId()).\
             setStatus(NGAMS_SUCCESS).\
             setMessage("Successfully handled command STATUS").\
             setState(srvObj.getState()).setSubState(srvObj.getSubState())

    reqPropsObjRef = reqPropsObj
        
    # Get the information requested.
    diskId            = ""
    fileId            = ""
    fileVersion       = ""
    configurationFile = ""
    fileAccess        = ""
    hostId            = ""
    requestId         = ""
    dbTime            = ""
    dbTimeReset       = ""
    fileList          = ""
    fileListId        = ""
    maxElements       = 100000
    if (reqPropsObj.hasHttpPar("disk_id")):
        diskId = reqPropsObj.getHttpPar("disk_id")

    if (reqPropsObj.hasHttpPar("file_id")):
        fileId = reqPropsObj.getHttpPar("file_id")
    if (reqPropsObj.hasHttpPar("file_version")):
        fileVersion = reqPropsObj.getHttpPar("file_version")

    if (reqPropsObj.hasHttpPar("configuration_file")): configurationFile = "-"

    if (reqPropsObj.hasHttpPar("file_access")):
        fileAccess = reqPropsObj.getHttpPar("file_access")

    if (reqPropsObj.hasHttpPar("host_id")):
        hostId = reqPropsObj.getHttpPar("host_id")

    if (reqPropsObj.hasHttpPar("max_elements")):
        maxElements = int(reqPropsObj.getHttpPar("max_elements"))

    if (reqPropsObj.hasHttpPar("request_id")):
        requestId = reqPropsObj.getHttpPar("request_id")

    if (reqPropsObj.hasHttpPar("db_time")):
        dbTime = True
    if (reqPropsObj.hasHttpPar("db_time_reset")):
        dbTimeReset = True

    if (reqPropsObj.hasHttpPar("flush_log")):
        try:
            logFlush()
        except:
            pass

    if (reqPropsObj.hasHttpPar("file_list")):
        fileList = int(reqPropsObj.getHttpPar("file_list"))

    if (reqPropsObj.hasHttpPar("file_list_id")):
        fileListId = reqPropsObj.getHttpPar("file_list_id")

    if (reqPropsObj.hasHttpPar("dump_object_info")):
        # Dump status of all objects allocated into file specified.
        targFile = reqPropsObj.getHttpPar("dump_object_info")
        rmFile(targFile)
        fo = open(targFile, "w")
        fo.write(_genObjectStatus())
        fo.close()

    # Handle request for file info.
    genCfgStatus     = 0
    genDiskStatus    = 0
    genFileStatus    = 0
    genStatesStatus  = 1
    genRequestStatus = 0
    msg              = ""
    help             = 0
    if (reqPropsObj.hasHttpPar("help")):
        global _help
        msg = _help
        help = 1
    elif (hostId and (hostId != getHostId())):
        # Query the status for the host referenced.
        contactDic = ngamsHighLevelLib.\
                     resolveHostAddress(srvObj.getDb(), srvObj.getCfg(),
                                        [hostId])
        # Handle the request as follows:
        #
        # 1. Resolved host/port = local host/port:
        #    => Generate standard reply to STATUS command.
        #
        # 2. Resolved host/port != local host/port + !Proxy Mode:
        #    => Send back Re-Direction HTTP Response.
        #
        # 3. Resolved host/port != local host/port + Proxy Mode:
        #    => Forward the request to the host indicated, and send
        #       back the reply received from this.
        hostObj = contactDic[hostId]
        cfgObj = srvObj.getCfg()
        if ((hostObj.getHostId() == getHostId()) and 
            (hostObj.getSrvPort() == cfgObj.getPortNo())):
            info(2,"Send back status of this server/host to STATUS/host_id "+\
                 "request")
            msg = "Successfully handled command STATUS"
        elif (((hostObj.getHostId() != getHostId()) or
               (hostObj.getSrvPort() != cfgObj.getPortNo())) and
              (not cfgObj.getProxyMode())):
            info(2,"Sending back re-direction HTTP response for host/port: "+\
                 "%s/%d to STATUS/host_id request" %
                 (hostObj.getHostId(), hostObj.getSrvPort()))
            srvObj.httpRedirReply(reqPropsObj, httpRef, hostObj.getHostId(),
                                  hostObj.getSrvPort())
            return
        else:
            try:
                # Check if host is suspended, if yes, wake it up.
                if (srvObj.getDb().getSrvSuspended(hostObj.getHostId())):
                    info(3,"Status Request - Waking up suspended " +\
                         "NGAS Host: " + hostObj.getHostId())
                    ngamsSrvUtils.wakeUpHost(srvObj, hostObj.getHostId())
                srvObj.forwardRequest(reqPropsObj, httpRef,hostObj.getHostId(),
                                      hostObj.getSrvPort(), autoReply = 1)
            except Exception, e:
                ex = re.sub("<|>", "", str(e))
                errMsg = genLog("NGAMS_ER_COM",
                                [hostObj.getHostId(),hostObj.getSrvPort(),ex])
                raise Exception, errMsg
            return
    elif (fileList):
        if (not fileListId):
            # It's a new STATUS?file_list request.
             fileListId = _handleFileList(srvObj, reqPropsObj, httpRef)
        # Send back data from the request.
        _handleFileListReply(srvObj, reqPropsObj, httpRef, fileListId,
                             maxElements)
    elif (diskId):
        diskObj = ngamsDiskInfo.ngamsDiskInfo()
        diskObj.read(srvObj.getDb(), diskId)
        status.addDiskStatus(diskObj)
        genDiskStatus = 1
    elif (fileId):
        if (not fileVersion): fileVersion = -1
        fileObj = ngamsFileInfo.ngamsFileInfo()
        fileObj.read(srvObj.getDb(), fileId, fileVersion)
        diskObj = ngamsDiskInfo.ngamsDiskInfo()
        try:
            diskObj.read(srvObj.getDb(), fileObj.getDiskId())
        except:
            errMsg = "Illegal Disk ID found: %s for file with ID: %s" %\
                     (fileObj.getDiskId(), fileId)
            raise Exception, errMsg
        diskObj.addFileObj(fileObj)
        status.addDiskStatus(diskObj)
        genDiskStatus = 1
        genFileStatus = 1
    elif (requestId):
        info(3,"Checking status of request with ID: " + requestId)
        reqPropsObjRef = srvObj.getRequest(requestId)
        if (not reqPropsObjRef):
            errMsg = genLog("NGAMS_ER_ILL_REQ_ID", [requestId])
            raise Exception, errMsg
        genRequestStatus = 1
    elif (configurationFile):
        msg = "configuration_file=" + srvObj.getCfg().getCfg()
        genCfgStatus = 1
        status.setNgamsCfgObj(srvObj.getCfg())
    elif (fileAccess):
        if (not fileVersion): fileVersion = -1
        fileId = fileAccess
        msg = _checkFileAccess(srvObj, reqPropsObj, httpRef, fileId,
                               fileVersion, diskId)
    elif (dbTime):
        info(3, "Querying total DB time")
        msg = "Total DB time: %.6fs" % srvObj.getDb().getDbTime()
    elif (dbTimeReset):
        msg = "Resetting DB timer"
        info(3, msg)
        srvObj.getDb().resetDbTime()
    else:
        msg = "Successfully handled command STATUS"

    if (reqPropsObjRef == reqPropsObj):
        reqPropsObj.setCompletionTime()
        srvObj.updateRequestDb(reqPropsObj)
    if (genCfgStatus or genDiskStatus or genFileStatus or genRequestStatus):
        status.setReqStatFromReqPropsObj(reqPropsObjRef)
        
        # Generate XML reply.
        xmlStat = status.genXmlDoc(genCfgStatus, genDiskStatus, genFileStatus,
                                   genStatesStatus)
        xmlStat = ngamsHighLevelLib.\
                  addDocTypeXmlDoc(srvObj, xmlStat, NGAMS_XML_STATUS_ROOT_EL,
                                   NGAMS_XML_STATUS_DTD)
        srvObj.httpReply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, xmlStat,
                         NGAMS_XML_MT)
    elif (not reqPropsObjRef.getSentReply()):
        srvObj.reply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, NGAMS_SUCCESS,
                     msg)
        
    if (msg and (not help)):
        info(1, msg)
    else:
        info(1, "Successfully handled command STATUS")


# EOF
