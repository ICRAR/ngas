

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
# "@(#) $Id: ngasRetrieveFiles.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  13/10/2005  Created
#

_doc =\
"""
TEMPORARY IMPLEMENTATION - COULD BE IMPROVED IF DECIDED TO KEEP THIS.

The defined input parameters to the tool are:

%s

"""

import sys, os, time, getpass, random, thread, threading

import pcc, PccUtTime

from ngams import *
import ngamsDbm, ngamsFileInfo
import ngamsLib
import ngamsPClient
import ngasUtils
from ngasUtilsLib import *

BLOCK_SIZE = 131072

# Definition of predefined command line parameters.
_options = [\
    ["_startTime", [], time.time(), NGAS_OPT_INT, "",
     "Internal Parameter: Start time for running the tool."],
    ["servers", [], None, NGAS_OPT_MAN, "=<Server List>",
     "Comma separated list of servers to contact (<Host>:<Port>,...)"],
    ["file-list", [], None, NGAS_OPT_MAN, "=<File List>",
     "write"],
    ["threads", [], None, NGAS_OPT_MAN, "=<Threads>", 
     "write"]]
_optDic, _optDoc = genOptDicAndDoc(_options)
__doc__ = _doc % _optDoc


class testClient(ngamsPClient.ngamsPClient):
    """
    Class derived from the ngamsPClient, providing a method to submit a
    Retrieve Request, but where a file object is returned rather than the
    data itself. I.e., the data must be received by the client.
    """

    def __init__(self,
                 host = "",
                 port = -1,
                 timeOut = None):
        """
        Constructor method.
        """
        ngamsPClient.ngamsPClient.__init__(self, host, port, timeOut)


    def retrieveFileObj(self,
                        fileId,
                        diskId = None,
                        fileVersion = None):
        """
        Submit a Retrieve Request. Rather then receiving the data, a file
        object is returned.

        fileId:          NG/AMS File ID of file to retrieve (string).

        diskId:          Disk ID of the disk hosting the file (string).

        fileVersion:     Specific version of the file to retrieve (integer)

        Returns:         Tuple with file size and a File Object from which the
                         data can be read (tuple/integer, file object).
        """
        cmdPars = [["file_id", fileId]]
        if (diskId): cmdPars.append(["disk_id", diskId])
        if (fileVersion): cmdPars.append(["file_version", fileVersion])
        code, msg, hdrs, fileObj = self._httpGet("", -1, NGAMS_RETRIEVE_CMD,
                                                 pars=cmdPars, returnFileObj=1)
        hdrDic = ngamsLib.httpMsgObj2Dic(hdrs)
        return int(hdrDic["content-length"]), fileObj
        

def getOptDic():
    """
    Return reference to command line options dictionary.

    Returns:  Reference to dictionary containing the command line options
              (dictionary).
    """
    return _optDic


def correctUsage():
    """
    Return the usage/online documentation in a string buffer.

    Returns:  Man-page (string).
    """
    return __doc__


class taskControl:

    def __init__(self):
        self.fileIds = []
        self.fileIdListSem = threading.Semaphore(1)
        self.runPermission = True
        self.bytesReceived = 0


    def getNextFileId(self):
        """
        """
        self.fileIdListSem.acquire()
        if (len(self.fileIds)):
            nextId = self.fileIds.pop()
        else:
            nextId = ""
        self.fileIdListSem.release()
        return nextId

    def incBytesRecv(self,
                     size):
        self.fileIdListSem.acquire()
        self.bytesReceived += size
        self.fileIdListSem.release()


def __retrieveThread(optDic,
                     taskCtrl,
                     dummy):
    """
    """
    info(4,"Entering retrieveThread(%s) ..." % getThreadName())
    client = ngamsPClient.ngamsPClient().\
             parseSrvList(optDic["servers"][NGAS_OPT_VAL])
    while (1):
        nextFileId = taskCtrl.getNextFileId()
        if (not nextFileId): thread.exit()
        nextFileId = nextFileId[:-1]
        stat = client.retrieve2File(nextFileId, targetFile=nextFileId)
        fileSize = getFileSize(nextFileId)
        taskCtrl.incBytesRecv(fileSize)
        info(1,"Next File ID: %s" % nextFileId)
 
  
def retrieveThread(optDic,
                   taskCtrl,
                   dummy):
    """
    """
    info(4,"Entering retrieveThread(%s) ..." % getThreadName())
    client = testClient().parseSrvList(optDic["servers"][NGAS_OPT_VAL])
    while (1):
        nextFileId = taskCtrl.getNextFileId()
        if (not nextFileId): thread.exit()
        nextFileId = nextFileId[:-1]
        info(1,"Next File ID: %s" % nextFileId)
        try:
            fileSize, fileObj = client.retrieveFileObj(nextFileId)
            sizeRemain = fileSize
            while (sizeRemain):
                if (sizeRemain < BLOCK_SIZE):
                    reqSize = sizeRemain
                else:
                    reqSize = BLOCK_SIZE
                buf = fileObj.read(reqSize)
                sizeRemain -= len(buf)
            taskCtrl.incBytesRecv(fileSize)
        except Exception, e:
            error("Error retrieving file with ID: %s - skipping. Error: %s" %\
                  (nextFileId, str(e)))
 
 
def execTest(optDic):
    """
    Carry out the tool execution.

    optDic:    Dictionary containing the options (dictionary).

    Returns:   Void.
    """
    info(4,"Entering execTest() ...")
    if (optDic["HELP"][NGAS_OPT_VAL]):
        print correctUsage()
        sys.exit(0)

    taskCtrl = taskControl()
    taskCtrl.fileIds = open(optDic["FILE-LIST"][NGAS_OPT_VAL]).readlines()
    random.shuffle(taskCtrl.fileIds)
    thrHandleDic = {}
    noOfThreads = int(optDic["THREADS"][NGAS_OPT_VAL])
    for n in range (1, (noOfThreads + 1)):
        threadId = "RETRIEVE-THREAD-" + str(n)
        args = (optDic, taskCtrl, None)
        info(4,"Starting Retrieve Sub-Thread: %s" % threadId)
        thrHandleDic[n] = threading.Thread(None, retrieveThread,
                                           threadId, args)
        thrHandleDic[n].setDaemon(0)
    for n in range (1, (noOfThreads + 1)):
        thrHandleDic[n].start()

    startTime = time.time()

    # Wait for threads to finish
    thrFinishCount = 0
    while (thrFinishCount < int(optDic["threads"][NGAS_OPT_VAL])):
        time.sleep(0.100)
        for n in thrHandleDic.keys():
            if (not thrHandleDic[n].isAlive()):
                del thrHandleDic[n]
                thrFinishCount += 1
                
    stopTime = time.time()
    statMsg = "Total time: %.3fs. Total rate: %.6f MB/s" %\
              ((stopTime - startTime),
               ((taskCtrl.bytesReceived / (1024.*1024))/(stopTime-startTime)))
    info(1,statMsg)

    info(4,"Leaving execTest()")


if __name__ == '__main__':
    """
    Main function to execute the tool.
    """
    optDic = parseCmdLine(sys.argv, _optDic)
    setLogCond(0, "", 0, "", 1)
    execTest(optDic)
    
    #try:
    #    optDic = parseCmdLine(sys.argv, _optDic)
    #except Exception, e:
    #    print "\nProblem executing the tool:\n\n%s\n" % str(e)
    #    sys.exit(1)
    #execTest(optDic)

# EOF
