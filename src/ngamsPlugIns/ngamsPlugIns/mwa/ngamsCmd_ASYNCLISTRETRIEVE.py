#
#    ICRAR - International Centre for Radio Astronomy Research
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
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu      11/12/2012  Created
#
"""
NGAS Command Plug-In, implementing asynchronous retrieval file list. It supports the following basic features

1. request an asynchlist retrieval
2. cancel an existing asynclistretrieval
3. suspend an existing asynclistretrieval
4. resume an existing asynclistretrieval

Main motivation is to deal with files offline (on Tapes), which often block and then timeout the requested HTTP session.

This command also has a persistent queue containing files pending to be sent.
During ngamsServer shutdown, this queue will be updated and saved by an offlinePlugin(ngamsMWAOfflinePlugIn.py)
During ngamsServer startup, this command's associated service (e.g. startAsyncQService) will be invoked by an onlinePlugin (e.g. ngamsMWAOnlinePlugIn.py).

To use this command, a python client can run the following code to
have the server with url #serverUrl# to push a list of file #file_id_list# to another server with a url #pushUrl#

    myReq = AsyncListRetrieveRequest(file_id_list, pushUrl) #construct the asynclistretrieverequest object
    strReq = pickle.dumps(myReq) #pack it into a post message
    strRes = urllib.urlopen(serverUrl, strReq).read() #send the message through post
    myRes = pickle.loads(strRes) # unpack the result

    # print the reply, which shows error and return code, session id
    print("session_uuid = %s" % myRes.session_uuid)
    print("errorcode = %d" % myRes.errorcode)
    print("file_info length = %d" % len(myRes.file_info))

    # print detailed file information on the server (serverUrl)
    for fileinfo in myRes.file_info:
        print("\tfile id: %s" % fileinfo.file_id)
        print("\tfile size: %d" % fileinfo.filesize)
        print("\tfile status: %d" % fileinfo.status)

For detailed examples on how to write all other features, please read
src/ngamsTest/ngamsTestAsyncListRetrieve.py

"""

import httplib
import logging
import os
import thread
import threading
import time
import urllib2

import cPickle as pickle
from ngamsLib.ngamsCore import NGAMS_HTTP_SUCCESS, NGAMS_TEXT_MT, TRACE, \
    NGAMS_HTTP_POST, getFileSize, getHostName, NGAMS_SUCCESS, NGAMS_FAILURE
from ngamsLib import ngamsDbCore, ngamsStatus, ngamsPlugInApi
import ngamsMWACortexTapeApi
from ngamsPlugIns.mwa.ngamsMWAAsyncProtocol import AsyncListRetrieveResponse, \
    AsyncListRetrieveProtocolError, AsyncListRetrieveCancelResponse, \
    AsyncListRetrieveSuspendResponse, AsyncListRetrieveResumeResponse, \
    AsyncListRetrieveStatusResponse, FileInfo


logger = logging.getLogger(__name__)

asyncReqDic = {} #key - uuid, value - AsyncListRetrieveRequest (need to remember the original request in case of cancel/suspend/resume or server shutting down)
statusResDic = {} #key - uuid, value - AsyncListRetrieveStatusResponse
nextFileDic = {} #key - uuid, value - next file_id to be delivered
threadDic = {} #key - uuid, value - the threadref
threadRunDic = {} #key - uuid, value - 1/0, 1: run 0: stop
ASYNC_DELIVERY_THR = "Asyn-delivery-thrd-"
fileMimeType = "application/octet-stream"
THREAD_STOP_TIME_OUT = 8

def handleCmd(srvObj, reqPropsObj, httpRef):
    """
    Handle the Asynchronously Retrieve Command.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).

    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).

    Returns:        Void.
    """
    httpMethod = reqPropsObj.getHttpMethod()
    if (httpMethod == 'POST'):
        postContent = _getPostContent(srvObj, reqPropsObj)
        # postContent = urllib.unquote(postContent)
        #info(3,"decoded getPostContent: %s" % postContent)
        # unpack AsyncListRetrieveRequest
        #exec "import ngamsMWAAsyncProtocol"
        #exec "from ngamsMWAAsyncProtocol import *"
        asyncListReqObj = pickle.loads(postContent)
        """
        lit = "(ingamsMWAAsyncProtocol\nAsyncListRetrieveRequest\np1\n(dp2\nS'url'\np3\nS'http://localhost:7777/QARCHIVE'\np4\nsS'file_id'\np5\n(lp6\nS'91_20120914164909_71.fits'\np7\naS'8875_20120914160053_32.fits'\np8\naS'110028_20120914130909_12.fits'\np9\nasS'session_uuid'\np10\nS'2e277a42-1f70-4975-859a-776926a26255'\np11\nsb."

        if (postContent != lit):
            info(3, "they are not the same!")
            info(3, "len postContent = %d" % len(postContent))
            info(3, "len literal = %d" % len(lit))
            d = difflib.Differ()
            re = d.compare(postContent, lit)
            s = ''.join(re)
            info(3, s)
            asyncListReqObj = pickle.loads(lit)
        else:
            asyncListReqObj = pickle.loads(postContent)
        """
        logger.debug("push url: %s", asyncListReqObj.url)
        filelist = list(set(asyncListReqObj.file_id)) #remove duplicates
        asyncListReqObj.file_id = filelist

        sessionId = asyncListReqObj.session_uuid
        logger.debug("uuid : %s", sessionId)
        asyncReqDic[sessionId] = asyncListReqObj

        # 2. generate response (i.e. status reports)
        res = genInstantResponse(srvObj, asyncListReqObj)
        logger.debug("response uuid : %s", res.session_uuid)

        # 3. launch a thread to process the list
        _startThread(srvObj, sessionId)
        srvObj.httpReply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, pickle.dumps(res), NGAMS_TEXT_MT)
    else:
        # extract parameters
        sessionId = None
        resp = None

        if (reqPropsObj.hasHttpPar("ngassystem")):
            syscmd = reqPropsObj.getHttpPar("ngassystem")
            if (syscmd == "start"):
                resp = startAsyncQService(srvObj, reqPropsObj)
            elif (syscmd == "stop"):
                resp = stopAsyncQService(srvObj, reqPropsObj)
            else:
                resp = "Unknown system command '%s'." % syscmd
                #raise Exception, msg
        elif (reqPropsObj.hasHttpPar("uuid")):
            sessionId = reqPropsObj.getHttpPar("uuid")
            if (reqPropsObj.hasHttpPar("cmd")):
                cmd = reqPropsObj.getHttpPar("cmd")
                if (cmd == "cancel"):
                    resp = cancelHandler(srvObj, reqPropsObj, sessionId)
                elif (cmd == "suspend"):
                    resp = suspendHandler(srvObj, reqPropsObj, sessionId)
                elif (cmd == "resume"):
                    resp = resumeHandler(srvObj, reqPropsObj, sessionId)
                elif (cmd == "status"):
                    resp = statusHandler(srvObj, reqPropsObj, sessionId)
                else:
                    resp = AsyncListRetrieveResponse(None, AsyncListRetrieveProtocolError.UNKNOWN_COMMAND_IN_REQUEST, [])
                    #msg = "Unknown command '%s' in the GET request." % cmd
                    #raise Exception, msg            else:
            else:
                resp = AsyncListRetrieveResponse(None, AsyncListRetrieveProtocolError.NO_COMMAND_IN_REQUEST, [])
                #msg = "No command (cancel|suspend|resume|status) in the GET request."
                #raise Exception, msg
        else:
            resp = AsyncListRetrieveResponse(None, AsyncListRetrieveProtocolError.NO_UUID_IN_REQUEST, [])
            #msg = "No UUID in the GET request."
            #raise Exception, msg

        srvObj.httpReply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, pickle.dumps(resp), NGAMS_TEXT_MT)

def cancelHandler(srvObj, reqPropsObj, sessionId):
    resp = AsyncListRetrieveCancelResponse()
    resp.session_uuid = sessionId;
    resp.errorcode = AsyncListRetrieveProtocolError.OK

    if (not asyncReqDic.has_key(sessionId) or not threadDic.has_key(sessionId)):
        resp.errorcode = AsyncListRetrieveProtocolError.INVALID_UUID
    else:
        re = _stopThread(sessionId)
        resp.errorcode = re

    if (asyncReqDic.has_key(sessionId)):
        v = asyncReqDic.pop(sessionId)
        del v
    if (threadDic.has_key(sessionId)):
        t = threadDic.pop(sessionId)
        if (not t.isAlive()):
            del t
    if (threadRunDic.has_key(sessionId)):
        threadRunDic.pop(sessionId)

    if (statusResDic.has_key(sessionId)):
        st = statusResDic.pop(sessionId)
        del st

    return resp


def suspendHandler(srvObj, reqPropsObj, sessionId):
    resp = AsyncListRetrieveSuspendResponse()
    resp.session_uuid = sessionId;
    resp.errorcode = AsyncListRetrieveProtocolError.OK

    if (not asyncReqDic.has_key(sessionId) or not threadDic.has_key(sessionId)):
        resp.errorcode = AsyncListRetrieveProtocolError.INVALID_UUID
    else:
        re = _stopThread(sessionId)
        resp.errorcode = re
        if (nextFileDic.has_key(sessionId)):
            resp.current_fileid = nextFileDic[sessionId]  # this is not accurate given the complex situation where the "next" file might be migrated to tape after sometime
        else:
            resp.current_fileid = None

    if (threadDic.has_key(sessionId)):
        t = threadDic.pop(sessionId)
        if (not t.isAlive()):
            del t
    if (threadRunDic.has_key(sessionId)):
        threadRunDic.pop(sessionId)

    return resp

def resumeHandler(srvObj, reqPropsObj, sessionId):
    resp = AsyncListRetrieveResumeResponse()
    resp.session_uuid = sessionId
    resp.errorcode = AsyncListRetrieveProtocolError.OK
    #info(3, "length of asyncReqDic = %d" % len(asyncReqDic.keys()))
    if (not asyncReqDic.has_key(sessionId)):
        resp.errorcode = AsyncListRetrieveProtocolError.INVALID_UUID
    else:
        if (nextFileDic.has_key(sessionId)):
            resp.current_fileid = nextFileDic[sessionId]  # this is not accurate given the complex situation where the "next" file might be migrated to tape after sometime
        else:
            resp.current_fileid = None
        _startThread(srvObj, sessionId)

    return resp

def statusHandler(srvObj, reqPropsObj, sessionId):
    """
    Handles the status handling
    """
    res = None
    if (statusResDic.has_key(sessionId)):
        res = statusResDic[sessionId]
    else:
        res = AsyncListRetrieveStatusResponse()
        res.errorcode = AsyncListRetrieveProtocolError.INVALID_UUID
        res.session_uuid = sessionId
    return res

def _getPostContent(srvObj, reqPropsObj):
    """
    Get the actual asynchlist request content from the HTTP Post
    """
    remSize = reqPropsObj.getSize()
    #info(3,"Post Data size: %d" % remSize)
    buf = reqPropsObj.getReadFd().read(remSize) #TODO - use proper loop on read here! given remSize is small, should be okay for now
    sizeRead = len(buf)
    #info(3,"Read buf size: %d" % sizeRead)
    #info(3,"Read buf: %s" % buf)
    if (sizeRead == remSize):
        reqPropsObj.setBytesReceived(sizeRead)
    return buf

def _httpPostUrl(url,
                mimeType,
                contDisp = "",
                dataRef = "",
                dataSource = "BUFFER",
                dataTargFile = "",
                blockSize = 65536,
                suspTime = 0.0,
                timeOut = None,
                authHdrVal = "",
                dataSize = -1,
                session_uuid = ""):
    """
    Post the the data referenced on the given URL. This function is adapted from
    ngamsLib.httpPostUrl, which does not support block-level suspension and cancelling for file transfer

    The data send back from the remote server + the HTTP header information
    is return in a list with the following contents:

      [<HTTP status code>, <HTTP status msg>, <HTTP headers (list)>, <data>]

    url:          URL to where data is posted (string).

    mimeType:     Mime-type of message (string).

    contDisp:     Content-Disposition of the data (string).

    dataRef:      Data to post or name of file containing data to send
                  (string).

    dataSource:   Source where to pick up the data (string/BUFFER|FILE|FD).

    dataTargFile: If a filename is specified with this parameter, the
                  data received is stored into a file of that name (string).

    blockSize:    Block size (in bytes) used when sending the data (integer).

    suspTime:     Time in seconds to suspend between each block (double).

    timeOut:      Timeout in seconds to wait for replies from the server
                  (double).

    authHdrVal:   Authorization HTTP header value as it should be sent in
                  the query (string).

    dataSize:     Size of data to send if read from a socket (integer).

    Returns:      List with information from reply from contacted
                  NG/AMS Server (reply, msg, hdrs, data) (list).
    """
    T = TRACE()

    # Separate the URL from the command.
    idx = (url[7:].find("/") + 7)
    tmpUrl = url[7:idx]
    cmd    = url[(idx + 1):]
    http = httplib.HTTP(tmpUrl)
    logger.debug("Sending HTTP header ...")
    logger.debug("HTTP Header: %s: %s", NGAMS_HTTP_POST, cmd)
    http.putrequest(NGAMS_HTTP_POST, cmd)
    logger.debug("HTTP Header: Content-Type: %s", mimeType)
    http.putheader("Content-Type", mimeType)
    if (contDisp != ""):
        logger.debug("HTTP Header: Content-Disposition: %s", contDisp)
        http.putheader("Content-Disposition", contDisp)
    if (authHdrVal):
        if (authHdrVal[-1] == "\n"): authHdrVal = authHdrVal[:-1]
        logger.debug("HTTP Header: Authorization: %s", authHdrVal)
        http.putheader("Authorization", authHdrVal)
    if (dataSource == "FILE"):
        dataSize = getFileSize(dataRef)
    elif (dataSource == "BUFFER"):
        dataSize = len(dataRef)

    if (dataSize != -1):
        logger.debug("HTTP Header: Content-Length: %s", str(dataSize))
        http.putheader("Content-Length", str(dataSize))
    logger.debug("HTTP Header: Host: %s", getHostName())
    http.putheader("Host", getHostName())
    http.endheaders()
    logger.debug("HTTP header sent")

    http._conn.sock.settimeout(timeOut)

    # Send the data.
    logger.debug("Sending data ...")
    if (dataSource == "FILE"):
        fdIn = open(dataRef)
        block = "-"
        blockAccu = 0
        while (block != ""):
            if (threadRunDic.has_key(session_uuid) and threadRunDic[session_uuid] == 0):
                logger.debug("Received cancel/suspend request, discard remaining blocks")
                break
            block = fdIn.read(blockSize)
            blockAccu += len(block)
            http._conn.sock.sendall(block)
            if (suspTime > 0.0): time.sleep(suspTime)
        fdIn.close()
    elif (dataSource == "FD"):
        fdIn = dataRef
        dataRead = 0
        while (dataRead < dataSize):
            if ((dataSize - dataRead) < blockSize):
                rdSize = (dataSize - dataRead)
            else:
                rdSize = blockSize
            block = fdIn.read(rdSize)
            http._conn.sock.sendall(block)
            dataRead += len(block)
            if (suspTime > 0.0): time.sleep(suspTime)
    else:
        # dataSource == "BUFFER"
        http.send(dataRef)
    logger.debug("Data sent")
    if (threadRunDic.has_key(session_uuid) and threadRunDic[session_uuid] == 0):
        logger.debug("Received cancel/suspend request, close HTTP connection and return None values")
        if (http != None):
            http.close()
            del http
        return [None, None, None, None]
    # Receive + unpack reply.
    logger.debug("Waiting for reply ...")

    reply, msg, hdrs = http.getreply()

    if (hdrs == None):
        errMsg = "Illegal/no response to HTTP request encountered!"
        raise Exception, errMsg

    if (hdrs.has_key("content-length")):
        dataSize = int(hdrs["content-length"])
    else:
        dataSize = 0
    if (dataTargFile == ""):
        data = http.getfile().read(dataSize)
    else:
        fd = None
        try:
            data = dataTargFile
            fd = open(dataTargFile, "w")
            fd.write(http.getfile().read(dataSize))
            fd.close()
        except Exception, e:
            if (fd != None): fd.close()
            raise e

    # Dump HTTP headers if Verbose Level >= 4.
    logger.debug("HTTP Header: HTTP/1.0 %s %s". str(reply), msg)
    if logger.isEnabledFor(logging.DEBUG):
        for hdr in hdrs.keys():
            logger.debug("HTTP Header: %s: %s", hdr, hdrs[hdr])

    if (http != None):
        http.close()
        del http

    return [reply, msg, hdrs, data]


def _httpPost(srvObj, url, filename, sessionId):
    """
    A wrapper for _httpPostUrl
    return success 0 or failure 1

    filename     full path for the file to be sent
    sessionId    session uuid
    """
    #info(3, "xxxxxx ---- filename %s" % filename)
    stat = ngamsStatus.ngamsStatus()
    baseName = os.path.basename(filename)
    contDisp = "attachment; filename=\"" + baseName + "\""
    contDisp += "; no_versioning=1"
    logger.debug("Async Delivery Thread [%s] Delivering file: %s - to: %s",
                 str(thread.get_ident()), baseName, url)
    ex = ""
    try:
        reply, msg, hdrs, data = \
        _httpPostUrl(url, fileMimeType,
                                        contDisp, filename, "FILE",
                                        blockSize=\
                                        srvObj.getCfg().getBlockSize(), session_uuid = sessionId)
        if (reply == None and msg == None and hdrs == None and data == None): # transfer cancelled/suspended
            return 1

        if (data.strip() != ""):
            stat.clear().unpackXmlDoc(data)
        else:
            stat.clear().setStatus(NGAMS_SUCCESS)
    except Exception, e:
            ex = str(e)
    if ((ex != "") or (reply != NGAMS_HTTP_SUCCESS) or
        (stat.getStatus() == NGAMS_FAILURE)):
        errMsg = "Error occurred while async delivering file: " + baseName +\
                     " - to url: " + url + " by Data Delivery Thread [" + str(thread.get_ident()) + "]"
        if (ex != ""): errMsg += " Exception: " + ex + "."
        if (stat.getMessage() != ""):
            errMsg += " Message: " + stat.getMessage()
        logger.warning(errMsg)
        jobManHost = srvObj.getCfg().getNGASJobMANHost()
        if (jobManHost):
            try:
                if (not ex):
                    ex = ''
                rereply = urllib2.urlopen('http://%s/failtodeliverfile?file_id=%s&to_url=%s&err_msg=%s' % (jobManHost, baseName, urllib2.quote(url), urllib2.quote(ex)), timeout = 15).read()
                logger.debug('Reply from sending file %s failtodeliver event to server %s - %s',
                             baseName, jobManHost, rereply)
            except Exception, err:
                logger.error('Fail to send fail-to-deliver event to server %s, Exception: %s', jobManHost, str(err))

        return 1
    else:
        logger.debug("File: %s - delivered to url: %s by Async Delivery Thread [%s]",
                     baseName, url, str(thread.get_ident()))
        return 0

def genInstantResponse(srvObj, asyncListReqObj):
    """
    Generate instance response this is why the command is called "asynch" because
    it instantly return to users and launch threads to handle the file retrieval behind the scene
    Major motivation is to deal with files offline (i.e. on Tapes)
    """
    clientUrl = asyncListReqObj.url
    sessionId = asyncListReqObj.session_uuid
    res = AsyncListRetrieveResponse(sessionId, 0, [])
    statuRes = AsyncListRetrieveStatusResponse()
    statuRes.errorcode = AsyncListRetrieveProtocolError.OK
    statuRes.session_uuid = sessionId

    if (clientUrl is None or sessionId is None):
        res.errorcode = -1
        return res
    cursorObj = srvObj.getDb().getFileSummary1(None, [], asyncListReqObj.file_id, None, [], None, 0)
    fileInfoList = cursorObj.fetch(1000)
    baseNameDic = {}
    for f in fileInfoList:
        file_id = f[ngamsDbCore.SUM1_FILE_ID]
        if (baseNameDic.has_key(file_id)):
            #info(3, "duplication detected %s" % basename)
            continue #get rid of multiple versions
        else:
            baseNameDic[file_id] = 1
        file_size = f[ngamsDbCore.SUM1_FILE_SIZE]
        filename  = f[ngamsDbCore.SUM1_MT_PT] + "/" + f[ngamsDbCore.SUM1_FILENAME]
        status = AsyncListRetrieveProtocolError.OK #online
        if (ngamsMWACortexTapeApi.isFileOnTape(filename) == 1):
            status = AsyncListRetrieveProtocolError.FILE_NOT_ONLINE #offline
        finfo = FileInfo(file_id, file_size, status)
        res.file_info.append(finfo)
        statuRes.number_bytes_to_be_delivered += file_size
        statuRes.number_files_to_be_delivered += 1
    del cursorObj
    statusResDic[sessionId] = statuRes
    for ff in asyncListReqObj.file_id:
        if (not baseNameDic.has_key(ff)):
            finfo = FileInfo(ff, 0, AsyncListRetrieveProtocolError.FILE_NOT_FOUND)
            res.file_info.append(finfo)

    return res

def _deliveryThread(srvObj, asyncListReqObj):
    """
    this is where files get pushed
    do not use thread suspend/resume to implement push suspend and resume
    for suspend request, simply kill the thread, which is harder to do actually

    keep reducing the elements in the list upon files delivery
    """
    # clone the original list to for loop
    # use the original list for elements reduction
    #fileList = list(asyncListReqObj.file_id)
    logger.debug("* * * entering the _deliveryThread")
    clientUrl = asyncListReqObj.url
    sessionId = asyncListReqObj.session_uuid
    logger.debug("clientUrl = %s, sessionId = %s", clientUrl, sessionId)
    if (clientUrl is None or sessionId is None):
        return
    filesOnDisk = []
    filesOnTape = []
    logger.debug("file_id length = %d", len(asyncListReqObj.file_id))
    fileHost = None
    if (asyncListReqObj.one_host):
        fileHost = srvObj.getHostId()
    cursorObj = srvObj.getDb().getFileSummary1(fileHost, [], asyncListReqObj.file_id, None, [], None, 0)
    fileInfoList = cursorObj.fetch(1000)
    logger.debug("fileIninfList length = %d", len(fileInfoList))
    baseNameDic = {} # key - basename, value - file size

    statusRes = None
    if (statusResDic.has_key(sessionId)):
        statusRes = statusResDic[sessionId]

    for fileInfo in fileInfoList:
        # recheck the file status, this is necessary
        # because, in addition to the initial request, this thread might be started under the "resume" command or when NGAS is started.
        # And no file status has been probed in either of these two conditions. (e.g. some files might have been migrated to tapes between "cancel" and "resume")
        # so remembering the old AsyncListRetrieveResponse is useless

        basename = fileInfo[ngamsDbCore.SUM1_FILE_ID] #e.g. 110024_20120914132151_12.fits
        logger.debug("------basename %s", basename)
        if (baseNameDic.has_key(basename)):
            #info(3, "duplication detected %s" % basename)
            continue #get rid of multiple versions
        else:
            file_size = fileInfo[ngamsDbCore.SUM1_FILE_SIZE]
            baseNameDic[basename] = file_size

        filename  = fileInfo[ngamsDbCore.SUM1_MT_PT] + "/" + fileInfo[ngamsDbCore.SUM1_FILENAME] #e.g. /home/chen/proj/mwa/testNGAS/NGAS2/volume1/afa/2012-10-26/2/110024_20120914132151_12.fits

        if (ngamsMWACortexTapeApi.isFileOnTape(filename) == 1):
            filesOnTape.append(filename)
            if (statusRes != None):
                statusRes.number_files_to_be_staged += 1
                statusRes.number_bytes_to_be_staged += file_size
        else:
            filesOnDisk.append(filename)
            logger.debug("add %s in the queue", filename)
    del cursorObj
    logger.debug(" * * * middle of the _deliveryThread")
    stageRet = 0
    if (len(filesOnTape) > 0):
        stageRet = ngamsMWACortexTapeApi.stageFiles(filesOnTape) # TODO - this should be done in another thread very soon! then the thread synchronisation issues....

    if (statusRes != None):
        statusRes.number_files_to_be_staged = 0
        statusRes.number_bytes_to_be_staged = 0

    allfiles = filesOnDisk
    if (stageRet != -1):
        allfiles = filesOnDisk + filesOnTape

    for filename in allfiles:
        basename = os.path.basename(filename)
        nextFileDic[sessionId] = basename
        if (threadRunDic.has_key(sessionId) and threadRunDic[sessionId] == 0):
            logger.debug("transfer cancelled/suspended before transferring file '%s'", basename)
            break
        logger.debug("About to deliver %s", filename)
        ret = _httpPost(srvObj, clientUrl, filename, sessionId)
        if (ret == 0):
            asyncListReqObj.file_id.remove(basename) #once it is delivered successfully, it is removed from the list
            if (statusRes != None):
                statusRes.number_files_delivered += 1
                statusRes.number_files_to_be_delivered -= 1
                statusRes.number_bytes_delivered += baseNameDic[basename]
                statusRes.number_bytes_to_be_delivered -= baseNameDic[basename]
        elif (threadRunDic.has_key(sessionId) and threadRunDic[sessionId] == 0):
            logger.debug("transfer cancelled/suspended while transferring file '%s'", basename)
            break

    for ff in asyncListReqObj.file_id:
        if (not baseNameDic.has_key(ff)):
            asyncListReqObj.file_id.remove(ff) #remove files that cannot be found

    if (len(asyncListReqObj.file_id) == 0): # if delivery is completed
        if (asyncReqDic.has_key(sessionId)):
            v = asyncReqDic.pop(sessionId)
            del v
            threadDic.pop(sessionId) # cannot del threadRef itself, TODO - this should be moved to a monitoring thread
            v = threadRunDic.pop(sessionId)
        if (nextFileDic.has_key(sessionId)):
            v = nextFileDic.pop(sessionId)
        if (statusResDic.has_key(sessionId)):
            st = statusResDic.pop(sessionId)
            del st

    thread.exit()

def startAsyncQService(srvObj, reqPropsObj):
    """
    when the server is started, this is called to spawn threads that process persistent queues
    manages a thread pool
    get a thread running for each uncompleted persistent queue
    """
    ngas_root_dir =  srvObj.getCfg().getRootDirectory()

    myDir = ngas_root_dir + "/AsyncQService"
    if (not os.path.exists(myDir)):
        return "no directory is created, cannot start the service"

    saveFile = myDir + "/AsyncRetrieveListObj"
    if (not os.path.exists(saveFile)):
        return "no file is found, skip starting the service"

    saveObj = None
    try:
        pkl_file = open(saveFile, 'rb')
        saveObj = pickle.load(pkl_file)
        pkl_file.close()
    except Exception, e:
        ex = str(e)
        return ex

    if (saveObj == None or len(saveObj) != 3):
        return "SaveObj is corrupted."

    for sessionId in saveObj[0].keys():
        asyncReqDic[sessionId] = saveObj[0][sessionId]

    for sessionId in saveObj[1].keys():
        statusResDic[sessionId] = saveObj[1][sessionId]

    for sessionId in saveObj[2].keys():
        nextFileDic[sessionId] = saveObj[2][sessionId]

    uuids = asyncReqDic.keys()
    if (len(uuids) == 0):
        return "len of uuid = 0"

    for sessionId in uuids:
        resp = resumeHandler(srvObj, reqPropsObj, sessionId)

    return "ok, queue length = %d" % len(uuids)

def stopAsyncQService(srvObj, reqPropsObj):
    """
    # when the server is shutdown, this is called to stop threads, and save uncompleted list back to database
    """
    ngas_root_dir =  srvObj.getCfg().getRootDirectory()
    myDir = ngas_root_dir + "/AsyncQService"
    saveFile = myDir + "/AsyncRetrieveListObj"

    uuids = asyncReqDic.keys()
    if (len(uuids) == 0):
        if (os.path.exists(saveFile)):
            cmd = "rm " + saveFile
            ngamsPlugInApi.execCmd(cmd, -1)
        return "ok after deleting the file"

    for sessionId in uuids:
        suspendHandler(srvObj, reqPropsObj, sessionId)

    #info(3, "Stopping - root dir = %s" % ngas_root_dir)

    if (not os.path.exists(myDir)):
        os.makedirs(myDir)

    """
    asyncReqDic = {} #key - uuid, value - AsyncListRetrieveRequest (need to remember the original request in case of cancel/suspend/resume or server shutting down)
    statusResDic = {} #key - uuid, value - AsyncListRetrieveStatusResponse
    nextFileDic = {} #key - uuid, value - next file_id to be delivered
    threadDic = {} #key - uuid, value - the threadref
    threadRunDic = {} #key - uuid, value - 1/0, 1: run 0: stop
    """
    saveObj = [asyncReqDic, statusResDic, nextFileDic]
    try:
        output = open(saveFile, 'wb')
        pickle.dump(saveObj, output)
        output.close()
    except Exception, e:
        ex = str(e)
        return ex

    return "ok"

def _stopThread(sessionId):
    """
    sessionId    uuid representing the file id list

    this will be called under any one of the three conditions:
    1. cancel Cmd
    2. suspend Command
    3. NGAS server is being shutting down (called by stopAsyncQService)
    """
    deliveryThrRef = threadDic[sessionId]
    threadRunDic[sessionId] = 0 # don't care about the race condition for this flag
    counter = 0
    while (counter <= THREAD_STOP_TIME_OUT and deliveryThrRef.isAlive()):
        time.sleep(1.0)
        counter = counter + 1

    if (counter > THREAD_STOP_TIME_OUT and deliveryThrRef.isAlive()):
        logger.debug("thread stopping timeout for session %s", sessionId)
        return AsyncListRetrieveProtocolError.THREAD_STOP_TIMEOUT
    else:
        logger.debug("thread stopped successfully for session %s", sessionId)
        return AsyncListRetrieveProtocolError.OK


def _startThread(srvObj, sessionId):
    """
    sessionId    uuid representing the file id list

    this will be called under any one of the three conditions:
    1. handleCmd
    2. resume command
    3. NGAS server is started (called by startAsyncQService)
    """
    if (threadDic.has_key(sessionId)):
        #TODO - stop the thread first!!!
        del threadDic[sessionId]

    args = (srvObj, asyncReqDic.get(sessionId))
    logger.debug("starting thread for uuid : %s", sessionId)
    deliveryThrRef = threading.Thread(None, _deliveryThread, ASYNC_DELIVERY_THR+sessionId, args)
    threadDic[sessionId] = deliveryThrRef
    deliveryThrRef.setDaemon(0)
    deliveryThrRef.start()
    threadRunDic[sessionId] = 1

    return
