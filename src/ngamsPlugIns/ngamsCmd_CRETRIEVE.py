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


"""
Function + code to handle the CRETRIEVE Command.
"""
from   ngams import *
import socket, re, glob, commands
import PccUtTime
import ngamsDb, ngamsLib, ngamsHighLevelLib, ngamsDbCore
import ngamsDb, ngamsPlugInApi, ngamsFileInfo, ngamsDiskInfo, ngamsFileList
import ngamsDppiStatus, ngamsStatus, ngamsDiskUtils
import ngamsSrvUtils, ngamsFileUtils, ngamsReqProps

def performProcessing(srvObj,
                      reqPropsObj,
                      filename,
                      mimeType):
    """
    Carry out the processing requested.

    srvObj:       Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:  Request Property object to keep track of actions done
                  during the request handling (ngamsReqProps).

    filename:     File to be processed (string).

    mimeType:     Mime-type of file (string).

    Returns:      List with ngamsDppiStatus object
                  (list/ngamsDppiStatus objects).
    """
    T = TRACE()

    statusObjList = []

    # Carry out the processing specified. If no processing is
    # specified, we simply set the source file as the file to be send.
    if (reqPropsObj.hasHttpPar("processing")):
        dppi = reqPropsObj.getHttpPar("processing")
        # Before starting to process, check if the specified DPPI
        # is supported by this NG/AMS.
        if (not srvObj.getCfg().hasDppiDef(dppi)):
            errMsg = genLog("NGAMS_ER_ILL_DPPI", [dppi])
            raise Exception, errMsg
        # Invoke the DPPI.
        exec "import " + dppi
        info(2,"Invoking DPPI: " + dppi + " to process file: " + filename)
        statusObj = eval(dppi + "." + dppi + "(srvObj, reqPropsObj, filename)")
    else:
        info(2,"No processing requested - sending back file as is")
        resultObj = ngamsDppiStatus.ngamsDppiResult(NGAMS_PROC_FILE, mimeType,
                                                    filename, filename)
        statusObj = ngamsDppiStatus.ngamsDppiStatus().addResult(resultObj)
    statusObjList.append(statusObj)

    return statusObjList


def cleanUpAfterProc(statusObjList):
    """
    Clean up after processing. I.e., remove the directories created for
    holding the files being processed.

    statusObjList:   List of status objects as returned by
                     ngamsCmdHandling.performProcessing()
                     (list/ngamsDppiStatus objects).

    Returns:         Void.
    """
    T = TRACE()

    for statObj in statusObjList:
        for resObj in statObj.getResultList():
            if (resObj.getProcDir() != ""):
                info(3,"Cleaning up processing directory: " +\
                     resObj.getProcDir() + " after completed processing")
                ngamsPlugInApi.execCmd("rm -rf " + resObj.getProcDir())


def genReplyRetrieve(srvObj,
                     reqPropsObj,
                     httpRef,
                     statusObjList,
		     container_name):
    """
    Function to send back a reply with the result queried with the
    RETRIEVE command. After having send back the result, the
    processing areas may be cleaned up.

    srvObj:          Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:     Request Property object to keep track of
                     actions done during the request handling
                     (ngamsReqProps).

    httpRef:         Reference to the HTTP request handler
                     object (ngamsHttpRequestHandler).

    statusObjList:   List of status objects as returned by
                     ngamsCmdHandling.performProcessing()
                     (list/ngamsDppiStatus objects).

    container_name:  Name of the container being retrieved.

    Returns:         Void.
    """
    T = TRACE()

    CRLF = '\r\n'

    # Send back reply with the result queried.
    # This is done by constructing a mutipart/mixed MIME message,
    # where each part of the message is a MIME message containing the
    # binary data for one file. Each of these individual MIME messages
    # also state the MIME type of the indiviual file and its filename
    #
    # Because we don't want to use that much memory we must feed the
    # multipart MIME message through the output socket as we create it,
    # instead of creating the whole beast and then sending it over the wire
    # In the latter case we would be able to use the assisting classes from
    # the email.message and email.mime modules
    try:

        dataSize = 0

        info(4, "Number of objects in container: {0}".format(len(statusObjList)))
        resObjList = [obj[0].getResultObject(0) for obj in statusObjList]

        # To send the initial response line to the client
        # want to know what is the size of the response.
        # We thus compute the header of the multipart MIME message,
        # its boundaries, their sizes and the sizes of each individual
        # MIME message containing each file
        from random import randint
        boundary = '===============' + str(randint(10**9,(10**10)-1)) + '=='
        multipartHeader = 'MIME-Version: 1.0' + CRLF +\
                          'Content-Type: multipart/mixed; boundary="' + boundary +\
                          '"; container_name="' + container_name + '"' + CRLF

        # These mark the boundaries between MIME messages (EOF)
        # and the end of the multipart message (EOC)
        EOF = '--' + boundary
        EOC = EOF + '--'

        # Pre-compute the headers for each file
        # and calculate how much space do they use
        from collections import deque
        headerDeque = deque()
        for obj in resObjList:

            mimeType = obj.getMimeType()
            contDisp = 'attachment; filename="{0}"'.format(obj.getRefFilename())
            headerDeque.append({'Content-Type': mimeType, 'Content-disposition': contDisp})

            relHeader = 'Content-Type: {0}\r\nContent-disposition: {1}"\r\n\n'.format(mimeType, contDisp)
            dataSize += obj.getDataSize()
            dataSize += len(relHeader) + len(EOF)

        # Now sum up the lenght of the multipart header and the
        # EOC, which marks the end of the multipart MIME message
        dataSize += len(multipartHeader)
        dataSize += len(EOC)

        # Let's send the status line reply
        srvObj.httpReplyGen(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, None, 0, 'ngams/container', dataSize)

        # ... and now all the rest: multipart MIME headers,
        # individual MIME messages for each file, and EOC line
        info(4, "Sending mainHeader:  " + multipartHeader)
        httpRef.wfile.write(CRLF + multipartHeader)

        blockSize = srvObj.getCfg().getBlockSize()
        for resObj in resObjList:
            #Send deliminater to reference end of section
            info(4, "Sending boundary: " + EOF)
            httpRef.wfile.write(CRLF + EOF + CRLF)

            #Get file information
            dataSize = resObj.getDataSize()
            headerDict = headerDeque.popleft()
            for header, value in headerDict.iteritems():
                info(4, "Sending header: {0}: {1}".format(header, value))
                httpRef.send_header(header, value)
            httpRef.wfile.write(CRLF)

            # Send back data from the memory buffer, from the result file, or
            # from HTTP socket connection.
            # TODO: An important improvement here would be to use sendfile instead
            # of doing the data copy at user-space level. sendfile is not officially
            # supported in python 2.7 though, but there are backported versions
            # that do support it (and it's been in Linux since 2.4)
            if (resObj.getObjDataType() == NGAMS_PROC_DATA):
                info(3,"Sending data in buffer to requestor ...")
                #httpRef.wfile.write(resObj.getDataRef())
                httpRef.wfile._sock.sendall(resObj.getDataRef())
            elif (resObj.getObjDataType() == NGAMS_PROC_FILE):
                info(3,"Reading data block-wise from file and sending to requestor ...")
                fd = open(resObj.getDataRef())
                dataSent = 0
                dataToSent = getFileSize(resObj.getDataRef())
                while (dataSent < dataToSent):
                    tmpData = fd.read(blockSize)
                    httpRef.wfile._sock.sendall(tmpData)
                    dataSent += len(tmpData)
                fd.close()
            else:
                # NGAMS_PROC_STREAM - read the data from the File Object in
                # blocks and send it directly to the requestor.
                info(3,"Routing data from foreign location to requestor ...")
                dataSent = 0
                dataToSent = dataSize
                while (dataSent < dataToSent):
                    tmpData = resObj.getDataRef().\
                              read(blockSize)
                    httpRef.wfile._sock.sendall(tmpData)
                    dataSent += len(tmpData)

        info(4,"Sending End of Container: " + EOC)
        httpRef.wfile.write(EOC)

        info(4,"HTTP reply sent to: " + str(httpRef.client_address))
        reqPropsObj.setSentReply(1)

        for obj in statusObjList:
            cleanUpAfterProc(obj)
    except Exception, e:
        for obj in statusObjList:
            cleanUpAfterProc(obj)
        raise e


def _handleRemoteIntFile(srvObj,
                         reqPropsObj,
                         httpRef):
    """
    Retrieve the remote, internal file and send it back to the requestor.

    srvObj:        Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:   Request Property object to keep track of actions done
                   during the request handling (ngamsReqProps).

    httpRef:       Reference to the HTTP request handler object
                   (ngamsHttpRequestHandler).

    Returns:       Void.
    """
    T = TRACE()

    forwardHost = reqPropsObj.getHttpPar("host_id")
    forwardPort = srvObj.getDb().getPortNoFromHostId(forwardHost)
    httpStatCode, httpStatMsg, httpHdrs, data =\
                  srvObj.forwardRequest(reqPropsObj, httpRef, forwardHost,
                                        forwardPort, autoReply = 1)


def _handleCmdCRetrieve(srvObj,
                       reqPropsObj,
                       httpRef):
    """
    Carry out the action of a CRETRIEVE command.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of
                    actions done during the request handling
                    (ngamsReqProps).

    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).

    Returns:        Void.
    """
    T = TRACE()

    # Get query information.
    if (reqPropsObj.hasHttpPar("ng_log")):
        if (reqPropsObj.hasHttpPar("host_id")):
            if (reqPropsObj.getHttpPar("host_id") != getHostId()):
                _handleRemoteIntFile(srvObj, reqPropsObj, httpRef)
                return

        # If there is a Local Log File, send it back.
        locLogFile = srvObj.getCfg().getLocalLogFile()
        if (os.path.exists(locLogFile)):
            mimeType = NGAMS_TEXT_MT
            srvObj.httpReplyGen(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS,
                                locLogFile, 1, mimeType)
            return
        else:
            errMsg = genLog("NGAMS_ER_UNAVAIL_FILE", ["ng_log: " + locLogFile])
            error(errMsg)
            raise Exception, errMsg
    elif (reqPropsObj.hasHttpPar("cfg")):
        if (reqPropsObj.hasHttpPar("host_id")):
            if (reqPropsObj.getHttpPar("host_id") != getHostId()):
                _handleRemoteIntFile(srvObj, reqPropsObj, httpRef)
                return

        # Send back the file.
        srvObj.httpReplyGen(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS,
                            srvObj.getCfg().getCfg(), 1, "text/xml")
        return
    elif (reqPropsObj.hasHttpPar("internal")):
        if (reqPropsObj.hasHttpPar("host_id")):
            if (reqPropsObj.getHttpPar("host_id") != getHostId()):
                _handleRemoteIntFile(srvObj, reqPropsObj, httpRef)
                return

        # Handle internal (local) non-archive file or send back directory
        # contents info.
        intPath = reqPropsObj.getHttpPar("internal")
        if (intPath.strip() == ""):
            raise Exception, "Illegal path specified for RETRIEVE?internal"

        # If specified path is a directory, return contents of the directory.
        if (os.path.isdir(intPath) or (intPath == "/")):
            info(2,"Querying info about directory: %s" % intPath)
            comment = "Info about folder: " + intPath
            fileListObj = ngamsFileList.ngamsFileList("DIR-INFO", comment,
                                                      NGAMS_SUCCESS)
            if (intPath[-1] != "/"): intPath += "/"
            globFileList = glob.glob(os.path.normpath(intPath + "*"))

            # To get the permissions, owner, group, access and modification
            # time we use 'ls -l' for now.
            # TODO: PORTABILITY ISSUE: Avoid usage of UNIX commands.
            lsCmd = "ls --full-time %s" % intPath
            stat, lsBuf = commands.getstatusoutput(lsCmd)
            dirInfoList = lsBuf.split("\n")
            dirDic = {}
            for dirInfo in dirInfoList[1:]:
                dirInfo = dirInfo.strip()
                dirEls = cleanList(dirInfo.split(" "))
                if (len(dirEls) != 9): continue
                # Example:
                # -rw-r----- 1 ngas ngas 102 2007-03-30 13:10:38.000 +0200 XX
                # -rw-rw-r-- 1 ngas ngas 488 Oct 26     14:50              YY
                entryName = os.path.normpath(intPath + dirEls[8])
                dirDic[entryName] = dirEls

            # Unpack the information about each entry.
            for filename in globFileList:
                if (filename[:-1] == intPath): continue
                statInfo = os.stat(filename)
                tmpFileObj = ngamsFileInfo.ngamsFileInfo().\
                             setFilename(os.path.normpath(filename)).\
                             setPermissions(dirDic[filename][0]).\
                             setOwner(dirDic[filename][2]).\
                             setGroup(dirDic[filename][3]).\
                             setAccDateFromSecs(statInfo[7]).\
                             setModDateFromSecs(statInfo[8]).\
                             setCreationDate(statInfo[9]).\
                             setFileSize(statInfo[6])
                fileListObj.addFileInfoObj(tmpFileObj)
            statObj = srvObj.genStatus(NGAMS_SUCCESS, "Successfully handled " +
                                       "CRETRIEVE Command").\
                                       addFileList(fileListObj)
            xmlStat = ngamsHighLevelLib.\
                      addDocTypeXmlDoc(srvObj, statObj.genXmlDoc(0, 0, 1),
                                       NGAMS_XML_STATUS_ROOT_EL,
                                       NGAMS_XML_STATUS_DTD)
            srvObj.httpReplyGen(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS,
                                xmlStat, 0, NGAMS_XML_MT, len(xmlStat), [], 1)
            return

        # Check that it is not tried to retrieve a data file in this way.
        # This is done by checking if the file is located in one of the
        # storage areas. Certain files like NgasDiskInfo, DB Snapshot Files,
        # ect., can be retrieved.
        complFilename = ngamsLib.locateInternalFile(intPath)
        diskIdsMtPts = srvObj.getDb().getDiskIdsMtPtsMountedDisks(getHostId())
        mountRtDir = srvObj.getCfg().getRootDirectory()
        for diskInfo in diskIdsMtPts:
            tmpDir   = os.path.normpath(diskInfo[1] + "/tmp/")
            cacheDir = os.path.normpath(diskInfo[1] + "/cache/")
            dbDir    = os.path.normpath(diskInfo[1] + "/.db/")
            if ((os.path.basename(complFilename) != NGAMS_DISK_INFO) and
                (os.path.basename(complFilename) != NGAMS_VOLUME_ID_FILE) and
                (os.path.basename(complFilename) != NGAMS_VOLUME_INFO_FILE) and
                (complFilename.find(tmpDir) == -1) and
                (complFilename.find(cacheDir) == -1) and
                (complFilename.find(dbDir) == -1) and
                (complFilename.find(diskInfo[1]) == 0)):
                errMsg = genLog("NGAMS_ER_ILL_RETRIEVE_REQ",
                                ["File requested appears to be an archived " +\
                                "data file. Retrieve these using the " +\
                                "RETRIEVE command + a combination of " +\
                                "File ID, File Version and Disk ID"])
                raise Exception, errMsg

        # OK, get the file and send it back.
        if ((complFilename.find(".xml") != -1) or
            (complFilename.find(".dtd") != -1) or
            (complFilename.find(NGAMS_DISK_INFO) != -1)):
            mimeType = "text/xml"
        elif (complFilename.find(".html") != -1):
            mimeType = "text/html"
        else:
            mimeType = ngamsHighLevelLib.\
                       determineMimeType(srvObj.getCfg(), complFilename, 1)
            if (mimeType == NGAMS_UNKNOWN_MT):
                # ".py", ...
                mimeType = NGAMS_TEXT_MT

        # Send back the file.
        srvObj.httpReplyGen(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS,
                            complFilename, 1, mimeType)
        return

    # For data files, retrieval must be enabled otherwise the request is
    # rejected.
    if (not srvObj.getCfg().getAllowRetrieveReq()):
        errMsg = genLog("NGAMS_ER_ILL_REQ", ["Retrieve"])
        error(errMsg)
        raise Exception, errMsg

    # At least container_id or container_name must be specified
    #if not an internal file has been requested.
    issueRetCmdErr = 0
    hasId = 0
    containerName, containerId = "", ""
    if (not reqPropsObj.hasHttpPar("container_id")):
        issueRetCmdErr = 1
    elif (reqPropsObj.getHttpPar("container_id").strip() == ""):
            issueRetCmdErr = 1
    else:
        containerId = reqPropsObj.getHttpPar("container_id")
        hasId = 1
    if(not hasId):
        if (not reqPropsObj.hasHttpPar("container_name")):
            issueRetCmdErr = 1
        elif (reqPropsObj.getHttpPar("container_name").strip() == ""):
                issueRetCmdErr = 1
        else:
                containerName = reqPropsObj.getHttpPar("container_name")
                issueRetCmdErr = 0
    if (issueRetCmdErr):
        errMsg = genLog("NGAMS_ER_RETRIEVE_CMD")
        error(errMsg)
        raise Exception, errMsg

    info(4,"Handling request for file with CID: " + containerId)
    fileVer = -1
    if (reqPropsObj.hasHttpPar("file_version")):
        fileVer = int(reqPropsObj.getHttpPar("file_version"))
    diskId = ""
    if (reqPropsObj.hasHttpPar("disk_id")):
        diskId = reqPropsObj.getHttpPar("disk_id")
    hostId = ""
    if (reqPropsObj.hasHttpPar("host_id")):
        hostId = reqPropsObj.getHttpPar("host_id")
    domain = ""
    if (reqPropsObj.hasHttpPar("domain")):
        domain = reqPropsObj.getHttpPar("domain")
    quickLocation = False
    if (reqPropsObj.hasHttpPar("quick_location")):
        quickLocation = int(reqPropsObj.getHttpPar("quick_location"))

    # First try the quick retrieve attempt, just try to get the first
    # (and best?) suitable file which is online and located on a node in the
    # same domain as the contacted node.
    ipAddress = None
#    if (quickLocation):
#        location, host, ipAddress, port, mountPoint, filename,\
#                  fileVersion, mimeType =\
#                  ngamsFileUtils.quickFileLocate(srvObj, reqPropsObj, fileId,
#                                                 hostId, domain, diskId,
#                                                 fileVer)
    if(not containerName):
        SQL = ("SELECT container_name FROM ngas_containers nc" +
               " WHERE nc.container_id='" + containerId + "'")
        cursor = srvObj.getDb().query(SQL)
        containerName = cursor[0][0][0]

    if(not containerId):
        SQL = ("SELECT container_id FROM ngas_containers nc" +
               " WHERE nc.container_name='" + containerName + "'")
        cursor = srvObj.getDb().query(SQL)
        info(4, "#################cursor: " + str(cursor))
        if (cursor != [[]]):
            containerId = cursor[0][0][0]
        else:
            errMsg = genLog("NGAMS_ER_RETRIEVE_CMD")
            error(errMsg)
            raise Exception, errMsg

    SQL = ("SELECT " + ngamsDbCore.getNgasFilesCols() +
                   " FROM ngas_files nf WHERE nf.container_id='" + containerId + "'")
    cursor = srvObj.getDb().query(SQL)

    procResultList = []
    for files in cursor[0]:
        fileId = files[2]
        # If not located the quick way try the normal way.
        ipAddress = None
        if (not ipAddress):
            # Locate the file best suiting the query and send it back if possible.
            location, host, ipAddress, port, mountPoint, filename, fileId,\
                      fileVersion, mimeType =\
                      ngamsFileUtils.locateArchiveFile(srvObj, fileId, fileVer,
                                                       diskId, hostId, reqPropsObj)

        # If still not located, try to contact associated NGAS sites to query
        # if the file is available there.
        # TODO:
        if (not ipAddress):
            pass

        if (location == NGAMS_HOST_LOCAL):
            # Get the file and send back the contents from this NGAS host.
            srcFilename = os.path.normpath(mountPoint + "/" + filename)
            # Perform the possible processing requested.
            procResult = performProcessing(srvObj,reqPropsObj,srcFilename,mimeType)
            procResultList.append(procResult)
        elif (((location == NGAMS_HOST_CLUSTER) or \
               (location == NGAMS_HOST_REMOTE)) and \
               srvObj.getCfg().getProxyMode()):

            info(3,"NG/AMS Server acting as proxy - requesting file with ID: " +\
                 fileId + " from NG/AMS Server on host/port: " + host + "/" +\
                 str(port) + " ...")

            # Act as proxy - get the file from the NGAS host specified and
            # send back the contents. The file is temporarily stored in the
            # Processing Area.
            procDir = ngamsHighLevelLib.genProcDirName(srvObj.getCfg())
            checkCreatePath(procDir)
            pars = []
            for par in reqPropsObj.getHttpParNames():
                if (par != "initiator"):
                    pars.append([par, reqPropsObj.getHttpPar(par)])
            authHdr = ngamsSrvUtils.genIntAuthHdr(srvObj)
            httpStatCode, httpStatMsg, httpHdrs, data =\
                          ngamsLib.httpGet(ipAddress, port, NGAMS_RETRIEVE_CMD, 1,
                                           pars,"",srvObj.getCfg().getBlockSize(),
                                           timeOut = None, returnFileObj = 1,
                                           authHdrVal = authHdr)
            httpHdrDic = ngamsLib.httpMsgObj2Dic(httpHdrs)
            dataSize = int(httpHdrDic["content-length"])

            # Check that the Retrieve Request was successful.
            try:
                tmpStatObj = ngamsStatus.ngamsStatus().\
                             unpackXmlDoc(data, getStatus=1)
            except Exception, e:
                # Data was not a NG/AMS XML Status Document.
                tmpStatObj = None
                pass
            if (tmpStatObj):
                if (tmpStatObj.getStatus() == NGAMS_FAILURE):
                    raise Exception, tmpStatObj.getMessage()

            tmpPars = ngamsLib.parseHttpHdr(httpHdrDic["content-disposition"])
            dataFilename = tmpPars["filename"]

            # Generate fake ngamsDppiStatus object.
            resultObj = ngamsDppiStatus.ngamsDppiResult(NGAMS_PROC_STREAM,
                                                        mimeType, data,
                                                        dataFilename, procDir,
                                                        dataSize)
            procResult = [ngamsDppiStatus.ngamsDppiStatus().addResult(resultObj)]
            procResultList.append(procResult)
        else:
            # No proxy mode: A redirection HTTP response is generated.
            srvObj.httpRedirReply(reqPropsObj, httpRef, ipAddress, port)
            return

    # Send back reply with the result(s) queried and possibly processed.
    genReplyRetrieve(srvObj, reqPropsObj, httpRef, procResultList, containerName)


def handleCmd(srvObj,
                      reqPropsObj,
                      httpRef):
    """
    Handle a RETRIEVE command.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of
                    actions done during the request handling
                    (ngamsReqProps).

    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).

    Returns:        Void.
    """
    T = TRACE()

    # If an internal file is retrieved we allow to handle the request also
    # when the system is Offline (for trouble-shooting purposes).
    if ((not reqPropsObj.hasHttpPar("internal")) and
        (not reqPropsObj.hasHttpPar("ng_log")) and
        (not reqPropsObj.hasHttpPar("cfg"))):
        srvObj.checkSetState("Command CRETRIEVE", [NGAMS_ONLINE_STATE],
                             [NGAMS_IDLE_SUBSTATE, NGAMS_BUSY_SUBSTATE],
                             "", NGAMS_BUSY_SUBSTATE)

    # Check if processing is requested if this systems allows processing.
    if (reqPropsObj.hasHttpPar("processing") and \
        (not srvObj.getCfg().getAllowProcessingReq())):
        errMsg = genLog("NGAMS_ER_ILL_REQ", ["Retrieve+Processing"])
        error(errMsg)
        srvObj.setSubState(NGAMS_IDLE_SUBSTATE)
        raise Exception, errMsg
    try:
        _handleCmdCRetrieve(srvObj, reqPropsObj, httpRef)
        srvObj.setSubState(NGAMS_IDLE_SUBSTATE)
    except Exception, e:
        raise Exception, e


# EOF
