#******************************************************************************
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu      11/12/2012  Created
#

"""
NGAS Command Plug-In, implementing asynchronous retrieval file list.
"""
import cPickle as pickle
import thread, threading, urllib, httplib
import os

from ngams import *
import ngamsDbCore, ngamsLib, ngamsStatus
import ngamsMWACortexTapeApi
import ngamsMWAAsyncProtocol
from ngamsMWAAsyncProtocol import *
#import difflib

asyncReqDic = {} #key - uuid, value - AsyncListRetrieveRequest (need to remember the original request in case of cancel/suspend/resume or server shutting down)
threadDic = {} #key - uuid, value - the threadref
threadRunDic = {} #key - uuid, value - 1/0, 1: run 0: stop
ASYNC_DELIVERY_THR = "Asyn-delivery-thrd-"
fileMimeType = "application/octet-stream"

def handleCmd(srvObj, reqPropsObj, httpRef):
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
        info(3,"push url: %s" % asyncListReqObj.url)
        filelist = list(set(asyncListReqObj.file_id)) #remove duplicates
        asyncListReqObj.file_id = filelist
        
        sessionId = asyncListReqObj.session_uuid
        info(3,"uuid : %s" % sessionId)
        asyncReqDic[sessionId] = asyncListReqObj
        
        # 2. generate response (i.e. status reports)
        res = genInstantResponse(srvObj, asyncListReqObj)
        info(3,"response uuid : %s" % res.session_uuid)
        
        # 3. launch a thread to process the list
        _startThread(srvObj, sessionId)
        srvObj.httpReply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, pickle.dumps(res), NGAMS_TEXT_MT)
    else:
        # extract parameters
        sessionId = None
        if (reqPropsObj.hasHttpPar("uuid")):
            sessionId = reqPropsObj.getHttpPar("uuid")
        else:
            msg = "No UUID in the GET request."
            raise Exception, msg
        if (reqPropsObj.hasHttpPar("cmd")):
            cmd = reqPropsObj.getHttpPar("cmd")
            if (cmd == "cancel"):
                cancelHandler(srvObj, reqPropsObj, sessionId)
            elif (cmd == "suspend"):
                suspendHandler(srvObj, reqPropsObj, sessionId)
            elif (cmd == "resume"):
                resumeHandler(srvObj, reqPropsObj, sessionId)
            elif (cmd == "status"):
                statusHandler(srvObj, reqPropsObj, sessionId)
            else:
                msg = "Unknown command in the GET request."
                raise Exception, msg
        else:
            msg = "No command (cancel|suspend|resume|status) in the GET request."
            raise Exception, msg
    return    
    

def cancelHandler(srvObj, reqPropsObj, sessionId):   
    pass 

def suspendHandler(srvObj, reqPropsObj, sessionId):
    pass

def resumeHandler(srvObj, reqPropsObj, sessionId):
    pass

def statusHandler(srvObj, reqPropsObj, sessionId):
    pass

def _getPostContent(srvObj, reqPropsObj):
    """
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
                dataSize = -1):
    """
    Post the the data referenced on the given URL. This function is adapted from
    ngamsLib.httpPostUrl, which does not support block-level suspension and cancelling for file transfer 

    The data send back from the remote server + the HTTP header information
    is return in a list with the following contents:

      [<HTTP status code>, <HTTP status msg>, <HTTP headers (list)>, <data>]

    url:          URL to where data is posted (string).
    
    mimeType:     Mime-type of message (string).

    contDisp:     Content-disposition of the data (string).
    
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
    info(4,"Sending HTTP header ...")
    info(4,"HTTP Header: %s: %s" % (NGAMS_HTTP_POST, cmd))
    http.putrequest(NGAMS_HTTP_POST, cmd)
    info(4,"HTTP Header: %s: %s" % ("Content-type", mimeType))
    http.putheader("Content-type", mimeType)
    if (contDisp != ""):
        info(4,"HTTP Header: %s: %s" % ("Content-disposition", contDisp))
        http.putheader("Content-disposition", contDisp)
    if (authHdrVal):
        if (authHdrVal[-1] == "\n"): authHdrVal = authHdrVal[:-1]
        info(4,"HTTP Header: %s: %s" % ("Authorization", authHdrVal))
        http.putheader("Authorization", authHdrVal)
    if (dataSource == "FILE"):
        dataSize = getFileSize(dataRef)
    elif (dataSource == "BUFFER"):
        dataSize = len(dataRef)

    if (dataSize != -1):
        info(4,"HTTP Header: %s: %s" % ("Content-length", str(dataSize)))
        http.putheader("Content-length", str(dataSize))
    info(4,"HTTP Header: %s: %s" % ("Host", getHostName()))
    http.putheader("Host", getHostName())
    http.endheaders()
    info(4,"HTTP header sent")

    # Send the data.
    info(4,"Sending data ...")
    if (dataSource == "FILE"):
        fdIn = open(dataRef)
        block = "-"
        blockAccu = 0
        while (block != ""):
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
    info(4,"Data sent")

    # Receive + unpack reply.
    info(4,"Waiting for reply ...")
    ngamsLib._setSocketTimeout(timeOut, http)
    reply, msg, hdrs = http.getreply()

    if (hdrs == None):
        errMsg = "Illegal/no response to HTTP request encountered!"
        raise Exception, errMsg
    
    if (hdrs.has_key("content-length")):
        dataSize = int(hdrs["content-length"])
    else:
        dataSize = 0
    if (dataTargFile == ""):
        ngamsLib._waitForResp(http.getfile(), timeOut)
        data = http.getfile().read(dataSize)
    else:
        fd = None
        try:
            data = dataTargFile
            fd = open(dataTargFile, "w")
            ngamsLib._waitForResp(http.getfile(), timeOut)
            fd.write(http.getfile().read(dataSize))
            fd.close()
        except Exception, e:
            if (fd != None): fd.close()
            raise e

    # Dump HTTP headers if Verbose Level >= 4.
    info(4,"HTTP Header: HTTP/1.0 " + str(reply) + " " + msg)
    for hdr in hdrs.keys():
        info(4,"HTTP Header: " + hdr + ": " + hdrs[hdr])
        
    if (http != None):
        http.close()
        del http    

    return [reply, msg, hdrs, data]


def _httpPost(srvObj, url, filename):
    """
    return success 0 or failure 1
    """
    #info(3, "xxxxxx ---- filename %s" % filename)
    stat = ngamsStatus.ngamsStatus()
    baseName = os.path.basename(filename)
    contDisp = "attachment; filename=\"" + baseName + "\""
    contDisp += "; no_versioning=1"
    info(3,"Async Delivery Thread [" + str(thread.get_ident()) + "] Delivering file: " + baseName + " - to: " + url + " ...")
    ex = ""
    try:
        reply, msg, hdrs, data = \
        _httpPostUrl(url, fileMimeType,
                                        contDisp, filename, "FILE",
                                        blockSize=\
                                        srvObj.getCfg().getBlockSize())
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
        warning(errMsg)
        return 1
    else:
        info(3,"File: " + baseName +\
                " - delivered to url: " + url + " by Async Delivery Thread [" + str(thread.get_ident()) + "]")
        return 0
    
def genInstantResponse(srvObj, asyncListReqObj):
    clientUrl = asyncListReqObj.url
    sessionId = asyncListReqObj.session_uuid
    res = AsyncListRetrieveResponse(sessionId, 0, [])
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
        status = 1 #online
        if (ngamsMWACortexTapeApi.isFileOnTape(filename) == 1):
            status = 0 #offline
        finfo = FileInfo(file_id, file_size, status)
        res.file_info.append(finfo)
    del cursorObj
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
    #info(3, "* * * entering the _deliveryThread")
    clientUrl = asyncListReqObj.url
    sessionId = asyncListReqObj.session_uuid
    if (clientUrl is None or sessionId is None):
        return
    filesOnDisk = []
    filesOnTape = []
    cursorObj = srvObj.getDb().getFileSummary1(None, [], asyncListReqObj.file_id, None, [], None, 0)
    fileInfoList = cursorObj.fetch(1000)
    baseNameDic = {}
    for fileInfo in fileInfoList:
        # recheck the file status, this is necessary 
        # because, in addition to the initial request, this thread might be started under the "resume" command or when NGAS is started. 
        # And no file status has been probed in either of these two conditions. (e.g. some files might have been migrated to tapes between "cancel" and "resume")
        # so remembering the old AsyncListRetrieveResponse is useless
        
        basename = fileInfo[ngamsDbCore.SUM1_FILE_ID] #e.g. 110024_20120914132151_12.fits
        #info(3, "------basename %s" % basename)
        if (baseNameDic.has_key(basename)):
            #info(3, "duplication detected %s" % basename)
            continue #get rid of multiple versions
        else:
            baseNameDic[basename] = 1
        filename  = fileInfo[ngamsDbCore.SUM1_MT_PT] + "/" + fileInfo[ngamsDbCore.SUM1_FILENAME] #e.g. /home/chen/proj/mwa/testNGAS/NGAS2/volume1/afa/2012-10-26/2/110024_20120914132151_12.fits
        
        if (ngamsMWACortexTapeApi.isFileOnTape(filename) == 1):
            filesOnTape.append(filename)
        else:
            filesOnDisk.append(filename)        
    del cursorObj
    #info(3, " * * * middle of the _deliveryThread")
    if (len(filesOnTape) > 0):
        ngamsMWACortexTapeApi.stageFiles(filesOnTape) # TODO - this should be done in another thread very soon! then the thread synchronisation issues....
    
    allfiles = filesOnDisk + filesOnTape
    
    for filename in allfiles:
        ret = _httpPost(srvObj, clientUrl, filename)
        if (ret == 0):
            basename = os.path.basename(filename)
            #info(3, "Removing %s" % basename)
            asyncListReqObj.file_id.remove(basename) #once it is delivered successfully, it is removed from the list
    #info(3, " * * * end the _deliveryThread")
    
    if (len(asyncListReqObj.file_id) == 0 and asyncReqDic.has_key(sessionId)):
        v = asyncReqDic.pop(sessionId)
        del v
    
    thread.exit()
      
def startAsyncQService():
    """
    when the server is started, this is called to spawn threads that process persistent queues
    manages a thread pool
    get a thread running for each uncompleted persistent queue
    """
    return

def stopAsyncQService():
    """
    # when the server is shutdown, this is called to stop threads, and save uncompleted list back to database
    """
    return

def _stopThread(srvObj, sessionId):
    """
    sessionId    uuid representing the file id list
    
    this will be called under any one of the three conditions:
    1. cancel Cmd
    2. suspend Command
    3. NGAS server is being shutting down (called by stopAsyncQService)
    """


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
    info(3,"starting thread for uuid : %s" % sessionId)
    deliveryThrRef = threading.Thread(None, _deliveryThread, ASYNC_DELIVERY_THR+sessionId, args)
    threadDic[sessionId] = deliveryThrRef
    deliveryThrRef.setDaemon(0)
    deliveryThrRef.start()
        
    
    return
