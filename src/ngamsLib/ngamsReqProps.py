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
# "@(#) $Id: ngamsReqProps.py,v 1.7 2009/06/02 07:41:23 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  18/10/2001  Created
#

"""
Contains definition of class to handle the properties (parameters) in
connection with a request.
"""

import time, urllib, types, copy

from   ngams import *
import ngamsLib


class ngamsReqProps:
    """
    Class used to keep track of the properties in connection with an HTTP
    request. This class is passed on to the various method handling
    the request.
    """
    
    def __init__(self):
        """
        Constructor method.
        """
        # Parameters from the HTTP request.
        self.__httpMethod      = ""
        # Used to store the HTTP header in 'raw' condition.
        self.__httpHdrDic      = {}      
        self.__cmd             = ""
        self.__wait            = 1
        self.__mimeType        = ""
        self.__size            = -1
        self.__fileUri         = ""
        self.__safeFileUri     = ""
        self.__httpPars        = {}
        self.__authorization   = None

        # Other parameters to keep track of the request handling.
        self.__sentReply       = 0
        self.__bytesReceived   = 0
        self.__stagingFilename = ""
        self.__ioTime          = 0
        self.__targDiskInfoObj = None
        self.__readFd          = None
        self.__writeFd         = None
        self.__noReplication   = 0

        # To handle the request status.
        self.__requestId             = None
        self.__requestTime           = time.time()
        self.__completionPercent     = None
        self.__expectedCount         = None
        self.__actualCount           = None
        self.__estTotalTime          = None
        self.__remainingTime         = None
        self.__lastRequestStatUpdate = None
        self.__completionTime        = None


    def getObjStatus(self):
        """
        Return a list with the current status of the object. The format
        of the list is:

          [[<attribute name>, <value>, ...], ...]

        Returns:    List with object status (list/list).
        """
        return [["HTTP Method", self.getHttpMethod()],
                ["Cmd", self.getCmd()],
                ["MimeType", self.getMimeType()],
                ["Size", self.getSize()],
                ["Wait", self.getWait()],
                ["FileUri", self.getFileUri()],
                ["SafeFileUri", self.getSafeFileUri()],
                ["HttpParsDic", self.getHttpParsDic()],
                ["HttpParNames", self.getHttpParNames()],
                ["SentReply", self.getSentReply()],
                ["BytesReceived", self.getBytesReceived()],
                ["StagingFilename", self.getStagingFilename()],
                ["IoTime", self.getIoTime()],
                ["TargDiskInfo", self.getTargDiskInfo()],
                ["ReadFd", self.getReadFd()],
                ["WriteFd", self.getWriteFd()],
                ["NoReplication", self.getNoReplication()],
                ["RequestId", self.getRequestId()],
                ["RequestTime", self.getRequestTime()],
                ["CompletionPercent", self.getCompletionPercent()],
                ["ExpectedCount", self.getExpectedCount()],
                ["ActualCount", self.getActualCount()],
                ["EstTotalTime", self.getEstTotalTime()],
                ["RemainingTime", self.getRemainingTime()],
                ["LastRequestStatUpdate", self.getLastRequestStatUpdate()],
                ["CompletionTime", self.getCompletionTime()],
                ["Authorization", self.getAuthorization()]]


    def unpackHttpInfo(self,
                       ngamsCfgObj,
                       httpMethod,
                       path,
                       headers):
        """
        Unpack the information from an HTTP request and set the
        members of the class accordingly.

        httpMethod:   HTTP request method (GET/POST) (string).
        
        path:         Path of HTTP request (string).
        
        headers:      Dictionary containing the information for the
                      headers of the HTTP query (dictionary).

        Returns:      Reference to object itself.
        """
        T = TRACE()

        self.setHttpMethod(httpMethod)
        
        # Handle the HTTP headers.
        for key in headers.keys():
            keyTmp = key.lower()
            val = urllib.unquote(headers[key])
            info(5, "Parsing HTTP header key: %s with value: %s" % (key, val))
            self.__httpHdrDic[key] = val
            if (keyTmp == "content-disposition"):
                # NOTE: Parse on URL encoded value!
                pars = ngamsLib.parseHttpHdr(headers[key])
                for par in pars.keys():
                    # For some reason '+' is not converted back to ' ' ...
                    tmpVal = pars[par].replace("+", " ")
                    uncVal = urllib.unquote(tmpVal)
                    if (par == "filename"):
                        self.setFileUri(os.path.basename(uncVal))
                    elif (par == "wait"):
                        self.setWait(uncVal)
                    elif (par == "mime_type"):
                        if (self.getMimeType() == ""):
                            self.setMimeType(uncVal)
                    else:
                        if (par.strip() != ""): self.addHttpPar(par, uncVal)
            elif (keyTmp == "content-type"):
                if (self.getMimeType() == ""):
                    self.setMimeType(trim(val.split(";"), " \""))
            elif (keyTmp == "content-length"):
                self.setSize(trim(val, " \""))
            elif (keyTmp == "authorization"):
                self.setAuthorization(urllib.unquote(trim(val, " ")))

        # Handle the information in the path.
        if (path):
            parList = ngamsLib.parseUrlRequest(path)
            for el in parList:
                # For some reason '+' is not converted back to ' ' ...
                tmpVal = el[1].replace("+", " ")    
                val = urllib.unquote(str(tmpVal))
                info(4, "Found parameter: " + el[0] + " with value: " + val)
                if (el[0] == "initiator"): self.setCmd(val)
                if (httpMethod in [NGAMS_HTTP_GET, NGAMS_HTTP_PUT, NGAMS_HTTP_POST]): 
                    # Subscription file delivery is always POST, but sometimes we want it behave like GET (e.g. proxy qrchive) to pass on parametres in url string.
                    if (el[0] == "filename"):
                        self.setFileUri(val)
                    elif (el[0] == "wait"):
                        self.setWait(val)
                    elif (el[0] == "mime_type"):
                        self.setMimeType(val)
                    elif (el[0] == "authorization"):
                        self.setAuthorization(val)
                    else:
                        self.addHttpPar(el[0], val)

        # Small trick to set the mime-type in case not defined by the
        # Content-type HTTP header.
        if ((self.getCmd() == NGAMS_ARCHIVE_CMD) and
            ((self.getMimeType() == "") or
             ((self.getMimeType() == NGAMS_ARCH_REQ_MT)))):
            if (self.getFileUri().strip() == ""):
                raise Exception, genLog("NGAMS_ER_CMD_EXEC",
                                        [NGAMS_ARCHIVE_CMD,
                                         "Missing parameter: filename"])
            mimeType = ngamsLib.detMimeType(ngamsCfgObj.getMimeTypeMappings(),
                                            self.getFileUri(), 1)
            if (mimeType == NGAMS_UNKNOWN_MT):
                errMsg = genLog("NGAMS_ER_UNKNOWN_MIME_TYPE1",
                                [self.getFileUri()])
                raise Exception, errMsg
            else:
                self.setMimeType(mimeType)

        # Convert a timeout of -1 to None or if set from string to float.
        if (not self.hasHttpPar("time_out")):
            self.addHttpPar("time_out", "-1")
                
        # In the case of an Archive Request, check that the mime-type is
        # known to this NGAS installation.
        if (self.getCmd() == NGAMS_ARCHIVE_CMD):
            # - To do this, we check if there is a Stream defined for
            # this kind of data.
            if (not ngamsCfgObj.getStreamFromMimeType(self.getMimeType())):
                errMsg = genLog("NGAMS_ER_UNKNOWN_MIME_TYPE2",
                                [self.getMimeType(), self.getFileUri()])
                raise Exception, errMsg

        return self

    
    def hasHttpHdr(self,
                   httpHdr):
        """
        Return one if the given HTTP header was contained in the request.

        httpHdr:      Name of HTTP header (string).

        Returns:      1 if referenced HTTP header was contained in the
                      request otherwise 0 (integer/0|1).
        """
        return self.__httpHdrDic.has_key(httpHdr.lower())


    def getHttpHdr(self,
                   httpHdr):
        """
        Returns the value of the HTTP header referenced or None if the
        HTTP header was not contained in the request.

        httpHdr:    Name of HTTP header (string).

        Returns:    Value of HTTP header (raw) or None (string|None).
        """
        if (self.hasHttpHdr(httpHdr)):
            return self.__httpHdrDic[httpHdr.lower()]
        else:
            return None


    def getHttpHdrs(self):
        """
        Return list with HTTP header keys (all lower cased).

        Returns:   List with HTTP header keys (list).
        """
        return self.__httpHdrDic.keys()
        

    def setHttpMethod(self,
                      httpMethod):
        """
        Set HTTP request method.

        httpMethod:   HTTP request method (string).

        Returns:      Reference to object itself.
        """
        self.__httpMethod = str(httpMethod)
        return self


    def getHttpMethod(self):
        """
        Get the HTTP request method (GET, POST, ...).

        Returns:    HTTP request method (string).
        """
        return self.__httpMethod


    def setCmd(self,
               cmd):
        """
        Set NG/AMS command.

        cmd:       Set NG/AMS command (string).

        Returns:   Reference to object itself.
        """
        self.__cmd = str(cmd)
        return self


    def getCmd(self):
        """
        Get the NG/AMS command.

        Returns:  NG/AMS command (string).
        """
        return  self.__cmd
   

    def setMimeType(self,
                    mimeType):
        """
        Set the mime-type.

        mimeType:  Mime-type (string).

        Returns:   Reference to object itself.
        """
        self.__mimeType = str(mimeType)
        return self


    def getMimeType(self):
        """
        Get the mime-type.

        Returns:  Mime-type (string).
        """
        return self.__mimeType


    def setSize(self,
                size):
        """
        Set the size of the data in the HTTP request.

        size:     Size of data in bytes (integer).
        
        Returns:  Reference to object itself.
        """
        self.__size = int(size)
        return self


    def getSize(self):
        """
        Get the size of the data in the HTTP request.

        Returns:  Data size in bytes (integer).
        """
        return self.__size


    def setWait(self,
                wait):
        """
        Set the Wait Flag.
        
        wait:      1 = wait, 0 = immediate reply (integer).

        Returns:   Reference to object itself.
        """
        self.__wait = int(wait)
        return self


    def getWait(self):
        """
        Get Wait Flag.

        Returns:  Wait Flag (integer).
        """
        return self.__wait


    def setFileUri(self,
                   fileUri):
        """
        Set URI referring to the data to archive.

        fileUri:    File URI (string).

        Returns:    Reference to object itself.
        """
        if fileUri == "(null)":
            fileUri = "null"
        self.__fileUri = urllib.unquote(str(fileUri))
        self.__safeFileUri = ngamsLib.hidePassword(self.__fileUri)
        return self


    def getFileUri(self):
        """
        Get the file URI.

        Returns:  File URI (string).
        """
        return self.__fileUri


    def getSafeFileUri(self):
        """
        Get a safe file URI, i.e. a URI where a possible password has
        been cleared out.
        
        Returns:    Safe URI (string).
        """
        return self.__safeFileUri


    def addHttpPar(self,
                   httpPar,
                   val):
        """
        Add a parameter consisting of a parameter name and a value.

        httpPar:   Parameter name (string).

        val:       Parameter value (string).

        Returns:   Reference to object itself.
        """
        self.__httpPars[httpPar] = urllib.unquote(str(val))
        return self


    def checkGetHttpPar(self,
                        httpPar,
                        retVal):
        """
        Returns the value of the HTTP parameter given if defined. Otherwise,
        it returns the specified return value.

        httpPar:      Parameter name (string).
        
        retVal:       Value to return in case parameter is not found
                      (<user definable>).

        Returns:      Value of HTTP parameter or specified return value if
                      not defined (string|<return value>).
        """
        if (self.hasHttpPar(httpPar)):
            return self.getHttpPar(httpPar)
        else:
            return retVal


    def hasHttpPar(self,
                   httpPar):
        """
        Return 1 if the object contains the referenced parameter.
        
        httpPar:    Parameter name of parameter to probe for (string).

        Returns:    1 if parameter is contained in object,
                    otherwise 0 (integer).
        """
        return self.__httpPars.has_key(httpPar)
    

    def getHttpPar(self,
                   httpPar):
        """
        Get the value of a parameter.

        Returns:    Name of parameter (string).
        """
        return self.__httpPars[httpPar]


    def getHttpParsDic(self):
        """
        Get reference to the dictionary containing the HTTP parameters.

        Returns:  Dictionary with the HTTP parameters (dictionary).
        """
        return self.__httpPars


    def getHttpParNames(self):
        """
        Get list of parameter names contained in the object.

        Returns:  List of parameter names (list).
        """
        return self.__httpPars.keys()


    def setSentReply(self,
                     state = 1):
        """
        Set the Sent Reply Flag.

        state:     1 = reply sent (integer).
        
        Returns:   Reference to object itself.
        """
        self.__sentReply = state
        return self


    def getSentReply(self):
        """
        Get the Sent Reply Flag.
        
        Returns:    Sent Reply Flag (integer).
        """
        return self.__sentReply


    def setBytesReceived(self,
                         bytes):
        """
        Set the number of bytes received from the HTTP request data.

        bytes:     Number of bytes received (integer).
        
        Returns:   Reference to object itself.
        """
        self.__bytesReceived = bytes
        return self


    def getBytesReceived(self):
        """
        Get the number of bytes received from the HTTP request data.
        
        Returns:    Number of bytes received (integer).
        """
        return self.__bytesReceived


    def setStagingFilename(self,
                           filename):
        """
        Set the staging (temporary) filename under which the file was stored.

        filename:   Staging Area Filename (string).

        Returns:    Reference to object itself.
        """
        self.__stagingFilename = str(filename)
        return self


    def getStagingFilename(self):
        """
        Return the staging (temporary) filename.

        Returns:    Temporary filename (string).
        """
        return self.__stagingFilename


    def incIoTime(self,
                  incrValue):
        """
        Increment the IO Time of the object with the given value.

        incrValue:   Value in seconds with which to increase
                     the IO Time (float).

        Returns:     Reference to object itself.
        """
        self.__ioTime += float(incrValue)
        return self


    def getIoTime(self):
        """
        Return the IO Time of the object.

        Returns:   IO Time (float).
        """
        return self.__ioTime 


    def setTargDiskInfo(self,
                        targDiskInfoObj):
        """
        Set the Target Disk Info Object for the request (Main Disk Info
        Object).

        targDiskInfoObj:  Target Disk Info Object (ngamsDiskInfo).

        Returns:          Reference to object itself.
        """
        self.__targDiskInfoObj = targDiskInfoObj
        return self


    def getTargDiskInfo(self):
        """
        Get the Target Disk Info Object for the request (Main Disk Info
        Object).

        Returns:     Target Disk Info Object (ngamsDiskInfo)
        """
        return self.__targDiskInfoObj


    def setAuthorization(self,
                         authString):
        """
        Set the authorization string (raw) of the object.

        authString:  Value of authorization string (string).
 
        Returns:     Reference to object itself.
        """
        self.__authorization = authString
        return self

    def getAuthorization(self):
        """
        Get the authorization string (raw) of the object.
 
        Returns:   Value of authorization string (string).  
        """
        return self.__authorization


    def setReadFd(self,
                  readFd):
        """
        Set the HTTP read file descriptor.

        readFd:    Read file descriptor (file object).

        Returns:   Reference to object itself.
        """
        self.__readFd = readFd
        return self


    def getReadFd(self):
        """
        Return the HTTP read file descriptor.

        Returns:   HTTP read file descriptor (file object).
        """
        return self.__readFd


    def setWriteFd(self,
                   writeFd):
        """
        Set the HTTP write file descriptor.

        readFd:    Write file descriptor (file object).

        Returns:   Reference to object itself.
        """
        self.__writeFd = writeFd
        return self


    def getWriteFd(self):
        """
        Return the HTTP write file descriptor.

        Returns:   HTTP write file descriptor (file object).
        """
        return self.__writeFd


    def setNoReplication(self,
                         noRep):
        """
        Set the No Replication Flag.
        
        noRep:      1 = no replication, 0 = replication (integer).

        Returns:   Reference to object itself.
        """
        self.__noReplication = int(noRep)
        return self


    def getNoReplication(self):
        """
        Get the No Replication Flag.

        Returns:  No Replication Flag (integer).
        """
        return self.__noReplication


    def clone(self):
        """
        Create a clone (exact copy) of this object.

        Returns:   New instance (clone) of the obejct (ngamsReqProps).
        """
        clone = ngamsReqProps().\
                setHttpMethod(self.getHttpMethod()).\
                setCmd(self.getCmd()).\
                setMimeType(self.getMimeType()).\
                setSize(self.getSize()).\
                setWait(self.getWait()).\
                setFileUri(self.getFileUri()).\
                setSentReply(self.getSentReply()).\
                setBytesReceived(self.getBytesReceived()).\
                setStagingFilename(self.getStagingFilename()).\
                setTargDiskInfo(self.getTargDiskInfo()).\
                setReadFd(self.getReadFd()).\
                setWriteFd(self.getWriteFd()).\
                setNoReplication(self.getNoReplication()).\
                setRequestId(self.getRequestId()).\
                setCompletionPercent(self.getCompletionPercent()).\
                setExpectedCount(self.getExpectedCount()).\
                setActualCount(self.getActualCount()).\
                setEstTotalTime(self.getEstTotalTime()).\
                setRemainingTime(self.getRemainingTime()).\
                setCompletionTime(self.getCompletionTime())
        self.__lastRequestStatUpdate = self.getLastRequestStatUpdate()
        for httpPar in self.getHttpParNames():
            clone.addHttpPar(httpPar, self.getHttpPar(httpPar))

        return clone


    def dumpBuf(self,
                ignoreUndefFields = 0):
        """
        Dump contents of the object to a string buffer (to the extent
        possible).

        ignoreUndefFields:     Don't take fields, which have a length of 0
                               (integer/0|1).
                               
        Returns:               String buffer with ASCII output (string).
        """
        format = prFormat1()
        buf = "Request Properties Object:\n"
        objStat = self.getObjStatus()
        for fieldName, val in objStat:
            if (not ignoreValue(ignoreUndefFields, val)):
                if (type(val) == types.DictType):
                    val2 = createSortDicDump(val)
                elif (type(val) == types.ListType):
                    val2 = str(val.sort())
                    val2 = copy.deepcopy(val)
                    val2.sort()
                else:
                    val2 = val
                buf += format % (fieldName + ":", str(val2))
        return buf


    def setRequestId(self,
                     requestId,
                     updateTime = 0):
        """
        Set the Request ID of the object.

        requestId:    Request ID allocated to the request (string).

        updateTime:   Update time for this update of the status information
                      in case this is set to 1 (integer/0|1).

        Returns:      Reference to object itself.
        """
        self.__requestId = requestId
        if (updateTime): self.setLastRequestStatUpdate()
        return self


    def getRequestId(self):
        """
        Return the Request ID allocated to the request.

        Returns:      Request ID (string).
        """
        return self.__requestId


    def getRequestTime(self):
        """
        Return the time for receiving the request (in seconds since epoch).

        Returns:    Time for receiving request in seconds since epoch (float).
        """
        return self.__requestTime


    def setCompletionPercent(self,
                             complPercent,
                             updateTime = 0):
        """
        Set the degree of completion of the request (in percen).

        complPercent:   Degree of completion in percent (float).

        updateTime:     Update time for this update of the status information
                        in case this is set to 1 (integer/0|1).
                      
        Returns:        Reference to object itself.
        """
        if (complPercent): self.__completionPercent = float(complPercent)
        if (updateTime): self.setLastRequestStatUpdate()
        return self


    def getCompletionPercent(self):
        """
        Get the degree of completion of the request (in percen).

        Returns:        Degree of completion in percent (float). 
        """
        return self.__completionPercent


    def setExpectedCount(self,
                         expCount,
                         updateTime = 0):
        """
        Set the expected number of iterations to be carried out. Could 
        e.g. be the number of files to handle.

        expCount:    Expected number of iterations (integer).

        updateTime:  Update time for this update of the status information
                     in case this is set to 1 (integer/0|1).

        Returns:     Reference to object itself.
        """
        if (expCount): self.__expectedCount = int(expCount)
        if (updateTime): self.setLastRequestStatUpdate()
        return self


    def getExpectedCount(self):
        """
        Return the expected number of iterations to be carried out. Could 
        e.g. be the number of files to handle.

        Returns:  Expected number of iterations (integer).
        """
        return self.__expectedCount


    def setActualCount(self,
                       actCount,
                       updateTime = 0):
        """
        Set the actual number of iterations completed.

        actCount:    Current number of iterations done (integer).

        updateTime:   Update time for this update of the status information
                      in case this is set to 1 (integer/0|1).
       
        Returns:     Reference to object itself.
        """
        if (actCount): self.__actualCount = int(actCount)
        if (updateTime): self.setLastRequestStatUpdate()
        return self 


    def incActualCount(self,
                       updateTime = 0):
        """
        Increase the Actual Count counter by one.

        updateTime:   Update time for this update of the status information
                      in case this is set to 1 (integer/0|1).

        Returns:      Reference to object itself.
        """
        if (self.__actualCount):
            self.__actualCount += 1
        else:
            self.__actualCount = 1
        if (updateTime): self.setLastRequestStatUpdate()
        return self
        
        
    def getActualCount(self):
        """
        Return the number of iterations carried out.

        Returns:    Current number of iterations done (integer). 
        """
        return self.__actualCount


    def setEstTotalTime(self,
                        estTime,
                        updateTime = 0):
        """
        Set the estimated total time for handling the request.

        estTime:      Estimated time for completing the request in seconds
                      (integer).

        updateTime:   Update time for this update of the status information
                      in case this is set to 1 (integer/0|1).

        Returns:      Reference to object itself.
        """
        if (estTime): self.__estTotalTime = int(estTime)
        if (updateTime): self.setLastRequestStatUpdate()
        return self


    def getEstTotalTime(self):
        """
        Return the estimated total time for handling the request.

        Returns:    Estimated time for completing the request in seconds
                    (integer).
        """
        return self.__estTotalTime


    def setRemainingTime(self,
                         remainingTime,
                         updateTime = 0):
        """
        Set the estimated remaining time for handling the request.

        remainingTime:    The remaining time in seconds (integer).

        updateTime:       Update time for this update of the status information
                          in case this is set to 1 (integer/0|1).

        Returns:          Current number of iterations done (integer). 
        """
        if (remainingTime): self.__remainingTime = int(remainingTime)
        if (updateTime): self.setLastRequestStatUpdate()
        return self


    def getRemainingTime(self):
        """
        Get the estimated remaining time for handling the request.

        Returns:     The remaining time in seconds (integer).
        """
        return self.__remainingTime


    def setLastRequestStatUpdate(self):
        """
        Set the time for performing the last update of the request
        handling status.

        Returns:    Reference to object itself.
        """
        self.__lastRequestStatUpdate = time.time()
        return self
        

    def getLastRequestStatUpdate(self):
        """
        Get the time for performing the last update of the request
        handling status.

        Returns:   Last time for updating the request handling status.
                   Given in seconds since epoch (float).
        """
        return self.__lastRequestStatUpdate


    def setCompletionTime(self,
                          updateTime = 0):
        """
        Set the time for completing the request to the present time.

        updateTime:   Update time for this update of the status information
                      in case this is set to 1 (integer/0|1).

        Returns:      Reference to object itself.
        """
        self.__completionTime = time.time()
        if (updateTime): self.setLastRequestStatUpdate()
        return self
    

    def getCompletionTime(self):
        """
        Get the time for completing the request. Given in seconds since epoch.

        Returns:    Time in seconds since epoch for when the request handling
                    finished (float).
        """
        return self.__completionTime


# EOF
