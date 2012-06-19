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
# "@(#) $Id: ngamsMirroringRequest.py,v 1.11 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  14/03/2008  Created
#

"""
Class to handle the information in connection with one Mirroring Request.
"""

import types
import xml.dom.minidom
import PccUtTime
from   ngams import *
import ngamsLib


import ngamsDbCore


# TODO: Add writeMirroringReq() in ngamsDbJoin.py

# Define IDs for the status of a Mirroring Request.
NGAMS_MIR_REQ_STAT_SCHED          = "SCHEDULED"
NGAMS_MIR_REQ_STAT_SCHED_NO       = 1
NGAMS_MIR_REQ_STAT_ACTIVE         = "ACTIVE"
NGAMS_MIR_REQ_STAT_ACTIVE_NO      = 2
NGAMS_MIR_REQ_STAT_MIR            = "MIRRORED"
NGAMS_MIR_REQ_STAT_MIR_NO         = 3
NGAMS_MIR_REQ_STAT_REP            = "REPORTED"
NGAMS_MIR_REQ_STAT_REP_NO         = 4
NGAMS_MIR_REQ_STAT_ERR_ABANDON    = "ERROR-ABANDON"
NGAMS_MIR_REQ_STAT_ERR_ABANDON_NO = 5
NGAMS_MIR_REQ_STAT_ERR_RETRY      = "ERROR-RETRY"
NGAMS_MIR_REQ_STAT_ERR_RETRY_NO   = 6

# Dictionary to convert from string to number representation.
_mirReqStatStr2NoDic = {NGAMS_MIR_REQ_STAT_SCHED:
                        NGAMS_MIR_REQ_STAT_SCHED_NO,
                        NGAMS_MIR_REQ_STAT_ACTIVE:
                        NGAMS_MIR_REQ_STAT_ACTIVE_NO,
                        NGAMS_MIR_REQ_STAT_MIR:
                        NGAMS_MIR_REQ_STAT_MIR_NO,
                        NGAMS_MIR_REQ_STAT_REP:
                        NGAMS_MIR_REQ_STAT_REP_NO,
                        NGAMS_MIR_REQ_STAT_ERR_ABANDON:
                        NGAMS_MIR_REQ_STAT_ERR_ABANDON_NO,
                        NGAMS_MIR_REQ_STAT_ERR_RETRY:
                        NGAMS_MIR_REQ_STAT_ERR_RETRY_NO}

# Dictionary to convert from number to string representation.
_mirReqStatNo2StrDic = {NGAMS_MIR_REQ_STAT_SCHED_NO:
                        NGAMS_MIR_REQ_STAT_SCHED,
                        NGAMS_MIR_REQ_STAT_ACTIVE_NO:
                        NGAMS_MIR_REQ_STAT_ACTIVE,
                        NGAMS_MIR_REQ_STAT_MIR_NO:
                        NGAMS_MIR_REQ_STAT_MIR,
                        NGAMS_MIR_REQ_STAT_REP_NO:
                        NGAMS_MIR_REQ_STAT_REP,
                        NGAMS_MIR_REQ_STAT_ERR_ABANDON_NO:
                        NGAMS_MIR_REQ_STAT_ERR_ABANDON,
                        NGAMS_MIR_REQ_STAT_ERR_RETRY_NO:
                        NGAMS_MIR_REQ_STAT_ERR_RETRY}


class ngamsMirroringRequest:
    """
    Class to handle the information in connection with one Mirroring Request.
    """
    
    def __init__(self):
        """
        Constructor method.
        """
        timeNow = time.time()
        self.__instanceId       = getHostId()
        self.__fileId           = None
        self.__fileVersion      = -1
        self.__ingestionDate    = None
        self.__srvListId        = None
        self.__xmlFileInfo      = None
        self.__status           = None
        self.__message          = None
        self.__lastActivityTime = timeRef2Iso8601(timeNow)
        self.__schedulingTime   = timeRef2Iso8601(timeNow)


    def statusStr2No(self,
                     statusStr):
        """
        Convert a Mirroring Request Status from string to integer
        representation.
        
        An exception is raised if the status is illegal.

        statusStr:   Mirroring Request Status as string (string).

        Return:      Mirroring Request Status as number (integer).
        """
        try:
            statusInt = _mirReqStatStr2NoDic[statusStr]
            return statusNo
        except:
            msg = "Error converting Mirroring Request Status from text " +\
                  "to number representation. Status given: %s/type: %s"
            raise Exception, msg % (str(statusStr), str(type(statusStr)))


    def statusNo2Str(self,
                     statusNo):
        """
        Convert a Mirroring Request Status from integer to string
        representation.
        
        An exception is raised if the status is illegal.

        statusNo:   Mirroring Request Status as number (integer).

        Return:     Mirroring Request Status as string (string).
        """
        try:
            statusStr = _mirReqStatNo2StrDic[statusNo]
            return statusStr
        except:
            msg = "Error converting Mirroring Request Status from number " +\
                  "to string representation. Status given: %s/type: %s."
            raise Exception, msg % (str(statusNo), str(type(statusNo)))


    def setInstanceId(self,
                      id):
        """
        Set Instance ID, this is the identification for the NGAS Node in
        charge of controlling this NGAS Mirroring Scenario.

        This will simply be the NGAS Node name or NGAS Node + port number
        if multiple instances of NGAS is running on the given node.
        
        id:         Instance ID (string).
        
        Returns:    Reference to object itself.
        """
        self.__instanceId = str(trim(id, "\" "))
        return self


    def getInstanceId(self):
        """
        Get Instance ID, this is the identification for the NGAS Node in
        charge of controlling this NGAS Mirroring Scenario.

        This will simply be the NGAS Node name or NGAS Node + port number
        if multiple instances of NGAS is running on the given node.
        
        Returns:   Instance ID (string).
        """
        return self.__instanceId


    def setFileId(self,
                  id):
        """
        Set File ID.
        
        id:         File ID (string).
        
        Returns:    Reference to object itself.
        """
        self.__fileId = str(trim(id, "\" "))
        return self


    def getFileId(self):
        """
        Get File ID.
        
        Returns:   File ID (string).
        """
        return self.__fileId


    def setFileVersion(self,
                       version):
        """
        Set File Version.
        
        version:    File Version (integer).
        
        Returns:    Reference to object itself.
        """
        if (str(version).strip()): self.__fileVersion = int(version)
        return self

  
    def getFileVersion(self):
        """
        Get File Version.
        
        Returns:   File Version (string).
        """
        return self.__fileVersion


    def setIngestionDate(self,
                         date):
        """
        Set the ingestion date for the file (in the ISO 8601 format).

        date:       Ingestion date for file (string/ISO 8601|float/secs).
        
        Returns:    Reference to object itself.
        """
        if (not date): return self
        self.__ingestionDate = timeRef2Iso8601(date)
        return self


    def getIngestionDate(self):
        """
        Get the ingestion date.
         
        Returns:   Ingestion data in ISO 8601 format (string).
        """
        return self.__ingestionDate


    def setSrvListId(self,
                     id):
        """
        Set server list ID.
        
        id:         Server list ID (integer).
        
        Returns:    Reference to object itself.
        """
        self.__srvListId = int(str(id).replace(" ", ""))
        return self


    def getSrvListId(self):
        """
        Get server list ID.
        
        Returns:   Server list ID (integer).
        """
        return self.__srvListId


    def setXmlFileInfo(self,
                       xmlFileInfo):
        """
        Set the XML file information for the file.
        
        xmlFileInfo:  XML file information (string/XML).
        
        Returns:      Reference to object itself.
        """
        self.__xmlFileInfo = xmlFileInfo
        return self


    def getXmlFileInfo(self):
        """
        Get the XML file information.
        
        Returns:   XML file information (string/XML).
        """
        return self.__xmlFileInfo


    def setStatus(self,
                  status):
        """
        Set the status of the request in text representation or as a number.
        
        status:       Status represented as text or number (string | integer).
        
        Returns:      Reference to object itself.
        """
        # Check/convert, stored internally in number representation.
        try:
            status = _mirReqStatStr2NoDic[status]
        except:
            try:
                _mirReqStatNo2StrDic[int(status)]
                status = int(status)
            except:
                msg = "Illegal Mirroring Request Status specified: %s"
                raise Exception, msg % str(status)
        self.__status = status
        return self
    

    def getStatusAsNo(self):
        """
        Set the status of the request in number representation.
        
        Returns:      Mirroring Request Status as number (integer).
        """
        return self.__status
    

    def getStatusAsStr(self):
        """
        Get the status of the Mirroring Request as a string.
        
        Returns:  Status represented as string (string). 
        """
        return self.statusNo2Str(self.__status)


    def setMessage(self,
                   msg):
        """
        Set message field of the Mirroring Request.
        
        msg:        Message (string).
        
        Returns:    Reference to object itself.
        """
        self.__message = msg
        return self


    def getMessage(self):
        """
        Get message in connection with the Mirroring Request.
        
        Returns:   Message (string).
        """
        return self.__message


    def setLastActivityTime(self,
                            timeStamp):
        """
        Set the last activity field of the Mirroring Request.

        timeStamp:   Time for last activity as seconds since epoch or as an
                     ISO 8601 time stamp (string/integer).

        Returns:     Reference to object itself.
        """
        self.__lastActivityTime = timeRef2Iso8601(timeStamp)
        return self


    def getLastActivityTime(self):
        """
        Return the time for the last activity as an ISO 8601 timestamp.

        Returns:   Last time for activity/ISO 8601 or None
                   (string/ISO 8601|None).
        """
        return  self.__lastActivityTime


    def getLastActivityTimeSecs(self):
        """
        Return the time for the last activity in seconds since epoch.

        Returns:   Last time for activity in seconds (integer|None).
        """
        if (not self.__lastActivityTime):
            return None
        else:
            return iso8601ToSecs(self.__lastActivityTime)


    def setSchedulingTime(self,
                          timeStamp):
        """
        Set the scheduling of the Mirroring Request.

        timeStamp:   Time for scheduling as seconds since epoch or as an
                     ISO 8601 time stamp (string/integer).

        Returns:     Reference to object itself.
        """
        self.__schedulingTime = timeRef2Iso8601(timeStamp)
        return self


    def getSchedulingTime(self):
        """
        Return the time for scheduling the request as an ISO 8601 timestamp.

        Returns:   Last time for scheduling the request/ISO 8601 or None
                   (string/ISO 8601|None).
        """
        return  self.__schedulingTime


    def getSchedulingTimeSecs(self):
        """
        Return the time for scheduling the request in seconds since epoch.

        Returns:   Last time for scheduling the request in seconds
                   (integer|None).
        """
        if (not self.__schedulingTime):
            return None
        else:
            return iso8601ToSecs(self.__schedulingTime)


    def dump(self):
        """
        Create an ASCII dump of the contents in a string buffer and return
        a reference to the buffer.

        Returns:   String buffer containing the ASCII dump (string).
        """
        T = TRACE()
        
        buf = "Contents of ngamsMirroringRequest:%s:\n" % str(self)
        buf += "Instance ID:         %s\n" % self.getInstanceId()
        buf += "File ID:             %s\n" % self.getFileId()
        buf += "File Version:        %d\n" % self.getFileVersion()
        buf += "Ingestion Date:      %s\n" % self.getIngestionDate()
        buf += "Server List ID:      %d\n" % self.getSrvListId()
        buf += "XML File Info:\n%s\n" % self.getXmlFileInfo()
        buf += "Status:              %s\n" % self.getStatusAsStr()
        buf += "Message:             %s\n" % str(self.getMessage())
        buf += "Last Activity Date:  %s\n" % self.getLastActivityTime()
        return buf


    def genSummary(self):
        """
        Generate a summary of the contents of the object in one line.

        Returns:   Reference to string buffer with the object summary (string).
        """
        T = TRACE(5)
        
        buf = "Summary of Mirroring Request Object: "
        buf += "Instance ID: %s" % self.getInstanceId()
        buf += ". File ID: %s" % self.getFileId()
        buf += ". File Version: %d" % self.getFileVersion()
        buf += ". Status: %s" % self.getStatusAsStr()
        buf += ". Message: %s" % str(self.getMessage())
        buf += ". Last Activity Date: %s" % self.getLastActivityTime()
        return buf


    def genFileKey(self):
        """
        Generate a file key from the contents of the object using the
        File ID/Version and return this.

        Returns:  Generated file key (string).
        """
        return ngamsLib.genFileKey(None, self.getFileId(),
                                   self.getFileVersion())


if __name__ == '__main__':
    """
    Main function.
    """
    setLogCond(0, 0, "", 5)
        

# EOF
