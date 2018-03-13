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

import copy
import logging
import os
import time
import urllib

from six.moves.urllib import parse as urlparse  # @UnresolvedImport

from . import ngamsLib
from .ngamsCore import TRACE, NGAMS_HTTP_GET, NGAMS_HTTP_PUT,\
    NGAMS_HTTP_POST, NGAMS_ARCHIVE_CMD, NGAMS_ARCH_REQ_MT, genLog,\
    NGAMS_UNKNOWN_MT, createSortDicDump, ignoreValue, prFormat1


logger = logging.getLogger(__name__)

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
        self.__mimeType        = ""
        self.__size            = -1
        self.__fileUri         = ""
        self.__safeFileUri     = ""
        self.__httpPars        = {}
        self.__authorization   = None

        # Other parameters to keep track of the request handling.
        self.__bytesReceived   = 0
        self.__stagingFilename = ""
        self.__ioTime          = 0
        self.__targDiskInfoObj = None
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
                ["FileUri", self.getFileUri()],
                ["SafeFileUri", self.getSafeFileUri()],
                ["HttpParsDic", self.getHttpParsDic()],
                ["HttpParNames", self.getHttpParNames()],
                ["BytesReceived", self.getBytesReceived()],
                ["StagingFilename", self.getStagingFilename()],
                ["IoTime", self.getIoTime()],
                ["TargDiskInfo", self.getTargDiskInfo()],
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
            logger.debug("Parsing HTTP header key: %s with value: %s", key, val)
            self.__httpHdrDic[key] = val
            if (keyTmp == "content-disposition"):
                pars = ngamsLib.parseHttpHdr(headers[key])
                for name, val in pars.items():
                    val = urllib.unquote(val)
                    if name == "filename":
                        self.setFileUri(os.path.basename(val))
                    elif name == "mime_type":
                        if (self.getMimeType() == ""):
                            self.setMimeType(val)
                    elif name.strip():
                        self.addHttpPar(name, val)

            elif (keyTmp == "content-type"):
                if (self.getMimeType() == ""):
                    self.setMimeType(val.split(";")[0].strip(" \""))
            elif (keyTmp == "content-length"):
                self.setSize(val.strip(" \""))
            elif (keyTmp == "authorization"):
                self.setAuthorization(urllib.unquote(val.strip()))

        # Handle the information in the path.
        path,query = urllib.splitquery(path)
        self.setCmd(path.lstrip('/'))
        if (query):
            parList = urlparse.parse_qsl(query)
            for name,val in parList:
                logger.debug("Found parameter: %s with value: %s", name, val)
                if (httpMethod in [NGAMS_HTTP_GET, NGAMS_HTTP_PUT, NGAMS_HTTP_POST]):
                    self.addHttpPar(name, val)
                    # Subscription file delivery is always POST, but sometimes we want it behave like GET (e.g. proxy qrchive) to pass on parametres in url string.
                    if name == "filename":
                        self.setFileUri(val)
                    elif name == "mime_type":
                        self.setMimeType(val)
                    elif name == "authorization":
                        self.setAuthorization(val)

        # Small trick to set the mime-type in case not defined by the
        # Content-Type HTTP header.
        if ((self.getCmd() == NGAMS_ARCHIVE_CMD) and
            ((self.getMimeType() == "") or
             ((self.getMimeType() == NGAMS_ARCH_REQ_MT)))):
            if (self.getFileUri().strip() == ""):
                raise Exception(genLog("NGAMS_ER_CMD_EXEC",
                                        [NGAMS_ARCHIVE_CMD,
                                         "Missing parameter: filename"]))
            mimeType = ngamsLib.detMimeType(ngamsCfgObj.getMimeTypeMappings(),
                                            self.getFileUri(), 1)
            if (mimeType == NGAMS_UNKNOWN_MT):
                errMsg = genLog("NGAMS_ER_UNKNOWN_MIME_TYPE1",
                                [self.getFileUri()])
                raise Exception(errMsg)
            else:
                self.setMimeType(mimeType)

        # In the case of an Archive Request, check that the mime-type is
        # known to this NGAS installation.
        if (self.getCmd() == NGAMS_ARCHIVE_CMD):
            # - To do this, we check if there is a Stream defined for
            # this kind of data.
            if (not ngamsCfgObj.getStreamFromMimeType(self.getMimeType())):
                errMsg = genLog("NGAMS_ER_UNKNOWN_MIME_TYPE2",
                                [self.getMimeType(), self.getFileUri()])
                raise Exception(errMsg)

        return self

    # Container interface
    def __contains__(self, k):
        return k in self.__httpPars

    def __getitem__(self, k):
        return self.__httpPars[k]

    def hasHttpHdr(self,
                   httpHdr):
        """
        Return one if the given HTTP header was contained in the request.

        httpHdr:      Name of HTTP header (string).

        Returns:      1 if referenced HTTP header was contained in the
                      request otherwise 0 (integer/0|1).
        """
        return httpHdr.lower() in self.__httpHdrDic


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

    def is_GET(self):
        return self.__httpMethod == "GET"

    def is_POST(self):
        return self.__httpMethod == "POST"

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
        self.__httpPars[httpPar] = val
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
        return httpPar in self.__httpPars


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
        T = TRACE()
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
                setFileUri(self.getFileUri()).\
                setBytesReceived(self.getBytesReceived()).\
                setStagingFilename(self.getStagingFilename()).\
                setTargDiskInfo(self.getTargDiskInfo()).\
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
                if isinstance(val, dict):
                    val2 = createSortDicDump(val)
                elif isinstance(val, list):
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
        self.__estTotalTime = estTime
        if updateTime:
            self.setLastRequestStatUpdate()
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
        self.__remainingTime = remainingTime
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
