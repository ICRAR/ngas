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
import os
from ngams import TRACE, genLog, info, error, rmFile, getFileSize, checkCreatePath
from ngams import NGAMS_PROC_FILE, NGAMS_PROC_DATA, NGAMS_PROC_STREAM
from ngams import NGAMS_CONT_MT, NGAMS_HTTP_SUCCESS, NGAMS_FAILURE
from ngams import NGAMS_HOST_LOCAL, NGAMS_HOST_REMOTE, NGAMS_HOST_CLUSTER
from ngams import NGAMS_RETRIEVE_CMD, NGAMS_ONLINE_STATE, NGAMS_IDLE_SUBSTATE, NGAMS_BUSY_SUBSTATE
import ngamsLib, ngamsHighLevelLib, ngamsDbCore
import ngamsDppiStatus, ngamsStatus
import ngamsSrvUtils, ngamsFileUtils
import ngamsMIMEMultipart

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

    statusObjList      = statusObjList[1]
    resObjList         = [obj[0].getResultObject(0) for obj in statusObjList if isinstance(obj[0], ngamsDppiStatus.ngamsDppiStatus)]
    childStatusObjList = [obj                       for obj in statusObjList if isinstance(obj[0], str)]
    for childStatusObj in childStatusObjList:
        cleanUpAfterProc(childStatusObj)

    for resObj in resObjList:
        if (resObj.getProcDir() != ""):
            info(3,"Cleaning up processing directory: " + resObj.getProcDir() + " after completed processing")
            rmFile(resObj.getProcDir())


def genReplyRetrieve(srvObj,
                     reqPropsObj,
                     httpRef,
                     statusObjList):
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

    Returns:         Void.
    """
    T = TRACE()

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

        # Build the structure needed by the MIMEMultipartWriter to
        # do its work; that is:
        # [contName1, [[contName2, [[mime1, filename1, size2], [mime2, filename2, size2], ...]]]]]
        def buildFileInformation(statusObjList):

            containerName      = statusObjList[0]
            statusObjList      = statusObjList[1]
            resObjList         = [obj[0].getResultObject(0) for obj in statusObjList if isinstance(obj[0], ngamsDppiStatus.ngamsDppiStatus)]
            childStatusObjList = [obj                       for obj in statusObjList if isinstance(obj[0], str)]

            filesInformation = [[obj.getMimeType(), obj.getRefFilename(), obj.getDataSize(), obj.getObjDataType(), obj.getDataRef()] for obj in resObjList]
            for childStatusObj in childStatusObjList:
                filesInformation.append(buildFileInformation(childStatusObj))
            return [containerName, filesInformation]

        fileInformation = buildFileInformation(statusObjList)
        writer = ngamsMIMEMultipart.MIMEMultipartWriter(fileInformation, httpRef.wfile)
        dataSize = writer.getTotalSize()

        # Let's send the status line reply and the
        # extra CLRF to start the body
        srvObj.httpReplyGen(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, None, 0, NGAMS_CONT_MT, dataSize)
        httpRef.end_headers()

        # ... and now all the rest: multipart MIME headers,
        # individual MIME messages for each file, and EOC line
        blockSize = srvObj.getCfg().getBlockSize()

        def writeContainer(writer, fileInformation, blockSize):

            writer.startContainer()
            for fileInfo in fileInformation:
                if isinstance(fileInfo[1], list):
                    writeContainer(writer, fileInfo[1], blockSize)
                    continue;

                writer.startNextFile()

                dataSize = fileInfo[2]
                dataType = fileInfo[3]
                dataRef  = fileInfo[4]

                # Send back data from the memory buffer, from the result file, or
                # from HTTP socket connection.
                # TODO: An important improvement here would be to use sendfile instead
                # of doing the data copy at user-space level. sendfile is not officially
                # supported in python 2.7 though, but there are backported versions
                # that do support it (and it's been in Linux since 2.4)
                if (dataType == NGAMS_PROC_DATA):
                    info(3,"Sending data in buffer to requestor ...")
                    writer.writeData(dataRef)
                elif (dataType == NGAMS_PROC_FILE):
                    info(3,"Reading data block-wise from file and sending to requestor ...")
                    fd = open(dataRef)
                    dataSent = 0
                    dataToSent = getFileSize(dataRef)
                    while (dataSent < dataToSent):
                        tmpData = fd.read(blockSize)
                        writer.writeData(tmpData)
                        dataSent += len(tmpData)
                    fd.close()
                else:
                    # NGAMS_PROC_STREAM - read the data from the File Object in
                    # blocks and send it directly to the requestor.
                    info(3,"Routing data from foreign location to requestor ...")
                    dataSent = 0
                    dataToSent = dataSize
                    while (dataSent < dataToSent):
                        tmpData = dataRef.read(blockSize)
                        writer.writeData(tmpData)
                        dataSent += len(tmpData)

            writer.endContainer()

        writeContainer(writer, fileInformation[1], blockSize)

        info(4,"HTTP reply sent to: " + str(httpRef.client_address))
        reqPropsObj.setSentReply(1)

    finally:
        cleanUpAfterProc(statusObjList)


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


def collectProcResults(srvObj, reqPropsObj, fileVer, diskId, hostId, container):

    # Collect inner containers
    procResultList = []
    for childCont in container.getContainers():
        procResultList.append(collectProcResults(srvObj, reqPropsObj, fileVer, diskId, hostId, childCont))

    # Collect individual files
    for fileInfo in container.getFilesInfo():
        fileId = fileInfo.getFileId()
        fileVer = fileInfo.getFileVersion()

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

        if location == NGAMS_HOST_LOCAL:
            # Get the file and send back the contents from this NGAS host.
            # TODO: pass down the fileId to get it at the container build time
            srcFilename = os.path.normpath(mountPoint + "/" + filename)
            # Perform the possible processing requested.
            procResult = performProcessing(srvObj,reqPropsObj,srcFilename,mimeType)
            procResultList.append(procResult)
        elif location == NGAMS_HOST_CLUSTER or location == NGAMS_HOST_REMOTE:

            if srvObj.getCfg().getProxyMode():
                info(3, 'Ignoring proxy mode, still collecting whole contents of containers locally')
                pass

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

    return [container.getContainerName(), procResultList]


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

    # For data files, retrieval must be enabled otherwise the request is
    # rejected.
    if (not srvObj.getCfg().getAllowRetrieveReq()):
        errMsg = genLog("NGAMS_ER_ILL_REQ", ["Retrieve"])
        error(errMsg)
        raise Exception, errMsg

    # At least container_id or container_name must be specified
    containerName = containerId = None
    if reqPropsObj.hasHttpPar("container_id") and reqPropsObj.getHttpPar("container_id").strip():
        containerId = reqPropsObj.getHttpPar("container_id").strip()
    if not containerId and reqPropsObj.hasHttpPar("container_name") and reqPropsObj.getHttpPar("container_name").strip():
        containerName = reqPropsObj.getHttpPar("container_name").strip()
    if not containerId and not containerName:
        errMsg = genLog("NGAMS_ER_RETRIEVE_CMD")
        error(errMsg)
        raise Exception, errMsg

    # If container_name is specified, and maps to more than one container,
    # an error is issued
    if not containerId:
        containerId = srvObj.getDb().getContainerIdForUniqueName(containerName)

    info(4,"Handling request for file with containerId: " + containerId)
    fileVer = -1
    if (reqPropsObj.hasHttpPar("file_version")):
        fileVer = int(reqPropsObj.getHttpPar("file_version"))
    diskId = ""
    if (reqPropsObj.hasHttpPar("disk_id")):
        diskId = reqPropsObj.getHttpPar("disk_id")
    hostId = ""
    if (reqPropsObj.hasHttpPar("host_id")):
        hostId = reqPropsObj.getHttpPar("host_id")

    # Build the container hierarchy, get all file references and send back the results
    container = srvObj.getDb().readHierarchy(containerId, True)
    procResultList = collectProcResults(srvObj, reqPropsObj, fileVer, diskId, hostId, container)
    genReplyRetrieve(srvObj, reqPropsObj, httpRef, procResultList)


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


    _handleCmdCRetrieve(srvObj, reqPropsObj, httpRef)
    srvObj.setSubState(NGAMS_IDLE_SUBSTATE)


# EOF
