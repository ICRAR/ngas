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
# "@(#) $Id: ngamsHostInfo.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  25/03/2002  Created
#

"""
Contains definition of class for handling information in connection with
one NGAS host.
"""

import pcc, PccUtTime
from ngams import *


class ngamsHostInfo:
    """
    Contains information about one host from the NGAS DB.
    """
    
    def __init__(self):
        """
        Constructor method.
        """
        self.__hostId           = ""
        self.__domain           = ""
        self.__ipAddress        = ""
        self.__macAddress       = ""
        self.__nSlots           = -1
        self.__clusterName      = ""
        self.__installationDate = ""

        self.__srvVersion       = ""
        self.__srvPort          = -1
        self.__srvArchive       = -1
        self.__srvRetrieve      = -1
        self.__srvProcess       = -1
        self.__srvRemove        = -1
        self.__srvDataChecking  = -1
        self.__srvState         = ""

        self.__srvSuspended     = -1
        self.__srvReqWakeUpSrv  = ""
        self.__srvReqWakeUpTime = ""
 
        # Type of host NGAMS_HOST_LOCAL, NGAMS_HOST_CLUSTER, NGAMS_HOST_DOMAIN,
        # NGAMS_HOST_REMOTE.
        self.__hostType         = ""


    def getObjStatus(self):
        """
        Return a list with the current status of the object. The format
        of the list is:

          [[<xml attribute name>, <value>, ...], ...]

        Returns:    List with object status (list/list).
        """
        # Fields in XML document/ASCII dump
        return [["Host ID",                 self.getHostId()],
                ["Domain",                  self.getDomain()],
                ["IP Address",              self.getIpAddress()],
                ["MAC Address",             self.getMacAddress()],
                ["Number of Slots",         self.getNSlots()],
                ["Cluster Name",            self.getClusterName()],
                ["Installation Date",       self.getInstallationDate()],
                ["Server Version",          self.getSrvVersion()],
                ["Server Port",             self.getSrvPort()],
                ["Server Allow Archiving",  self.getSrvArchive()],
                ["Server Allow Retrieving", self.getSrvRetrieve()],
                ["Server Allow Processing", self.getSrvProcess()],
                ["Server Allow Remove",     self.getSrvRemove()],
                ["Server Data Checking",    self.getSrvDataChecking()],
                ["Server State",            self.getSrvState()],
                ["Server Suspended",        self.getSrvSuspended()],
                ["Server Wake-Up Request",  self.getSrvReqWakeUpSrv()],
                ["Server Wake-Up Time",     self.getSrvReqWakeUpTime()]]


    def unpackFromSqlQuery(self,
                           sqlQueryResult):
        """
        Unpack the host information from an SQL query result and set the
        members of the class.

        sqlQueryResult:   SQL query result (list).

        Returns:          Reference to object itself.
        """
        res = sqlQueryResult

        # Static host information.
        self.setHostId(res[0])
        self.setDomain(res[1])
        self.setIpAddress(res[2])
        self.setMacAddress(res[3])
        self.setNSlots(res[4])
        self.setClusterName(res[5])
        self.setInstallationDate(res[6])

        # Host information set by server.
        self.setSrvVersion(res[7])
        self.setSrvPort(res[8])
        self.setSrvArchive(res[9])
        self.setSrvRetrieve(res[10])
        self.setSrvProcess(res[11])
        self.setSrvRemove(res[12])
        self.setSrvDataChecking(res[13])
        self.setSrvState(res[14])

        # Parameters in connection with suspension
        self.setSrvSuspended(res[15])
        self.setSrvReqWakeUpSrv(res[16])
        self.setSrvReqWakeUpTime(res[17])

        return self


    def setHostId(self,
                  hostId):
        """
        Set the host ID.

        hostId:   Host ID (string).

        Returns:  Reference to object itself.
        """
        if (hostId == None): return self
        self.__hostId = hostId
        return self


    def getHostId(self):
        """
        Return the host ID.

        Returns:  Host ID (string).
        """
        return self.__hostId


    def setDomain(self,
                  domain):
        """
        Set the Domain Name.
        
        domain:   Domain name (string).

        Returns:  Reference to object itself.
        """
        if (domain == None): return self
        self.__domain = domain
        return self


    def getDomain(self):
        """
        Return the Domain Name.

        Returns:  Domain Name (string).
        """
        return self.__domain


    def setIpAddress(self,
                     ipAddress):
        """
        Set the IP Addess of the NGAS host.

        ipAddress:    IP address (string).

        Returns:  Reference to object itself.
        """
        if (ipAddress == None): return self
        self.__ipAddress = ipAddress
        return self


    def getIpAddress(self):
        """
        Return the IP address.

        Returns:  IP address (string).
        """
        return self.__ipAddress


    def setMacAddress(self,
                      macAddress):
        """
        Set the Mac address.

        macAddress:  Mac Address (string).

        Returns:     Reference to object itself.
        """
        if (macAddress == None): return self
        self.__macAddress = macAddress
        return self


    def getMacAddress(self):
        """
        Return the Mac Address.

        Returns:   Mac address (string).
        """
        return self.__macAddress


    def setNSlots(self,
                  number):
        """
        Set the number of slots.

        number:   Number of slots (integer).

        Returns:  Reference to object itself.
        """
        if (number == None): return self
        self.__nSlots = int(number)
        return self


    def getNSlots(self):
        """
        Return the number of slots.

        Returns:  Number of slots (integer).
        """
        return int(self.__nSlots)


    def setClusterName(self,
                       name):
        """
        Set the NGAS Cluster Name.

        name:     Name of the cluster (string).

        Returns:  Reference to object itself.
        """
        if (name == None): return self
        self.__clusterName = name
        return self


    def getClusterName(self):
        """
        Return the name of the NGAS Cluster.

        Returns:   NGAS Cluster Name (string).
        """
        return self.__clusterName


    def setInstallationDate(self,
                            date):
        """
        Set the date for installing this NGAS system.

        date:    Installation date (string/ISO 8601).

        Returns:  Reference to object itself.
        """
        if (not date): return self
        self.__installationDate = timeRef2Iso8601(date)
        return self


    def setInstallationDateFromSecs(self,
                                    dateSecs):
        """
        Set the installation date from seconds since epoch.

        dateSecs:  Installation date in seconds since epoch (integer).
 
        Returns:   Reference to object itself.
        """
        if (dateSecs == None): return self
        self.__installationDate = PccUtTime.TimeStamp().\
                                  initFromSecsSinceEpoch(dateSecs).\
                                  getTimeStamp()
        return self


    def getInstallationDate(self):
        """
        Return the installation date

        Returns:    Installation date (string/ISO 8601).
        """
        return self.__installationDate


    def setSrvVersion(self,
                      version):
        """
        Set the version ID of the server.

        version:  Version ID of server running (string).      
        
        Returns:  Reference to object itself.
        """
        if (version == None): return self
        self.__srvVersion = version
        return self


    def getSrvVersion(self):
        """
        Return the server version ID.

        Returns:   Server version ID (string).
        """
        return self.__srvVersion


    def setSrvPort(self,
                   portNo):
        """
        Set the server port number.

        portNo:   Server port number (integer).
        
        Returns:  Reference to object itself.
        """
        if (portNo == None): return self
        self.__srvPort = int(portNo)
        return self


    def getSrvPort(self):
        """
        Return the server port number. 

        Returns:   Server port number (integer).
        """
        return self.__srvPort


    def setSrvArchive(self,
                      allowArchive):
        """
        Set the allow archiving flag.

        allowArchive:  A value of 1 indicates that archiving is
                       allowed (integer).
        
        Returns:       Reference to object itself.
        """
        if (allowArchive == None): return self
        self.__srvArchive = int(allowArchive)
        return self


    def getSrvArchive(self):
        """
        Return the server archiving flag.

        Returns:       Server archiving flag (integer/0|1).
        """
        return self.__srvArchive


    def setSrvRetrieve(self,
                       allowRetrieve):
        """
        Set the server retrieve flag.

        allowRetrieve:   A value of 1 indicates that file retrieval is
                         supported (integer/0|1).
        
        Returns:         Reference to object itself.
        """
        if (allowRetrieve == None): return self
        self.__srvRetrieve = int(allowRetrieve)
        return self


    def getSrvRetrieve(self):
        """
        Return the server retrieve flag.

        Returns:        Server retrieve flag (integer/0|1).
        """
        return self.__srvRetrieve


    def setSrvProcess(self,
                      allowProc):
        """
        Set the server allow processing flag.

        allowProc:   A value of 1 indicates that file processing is
                     supported (integer/0|1).       
        
        Returns:     Reference to object itself.
        """
        if (allowProc == None): return self
        self.__srvProcess = int(allowProc)
        return self


    def getSrvProcess(self):
        """
        Return the server allow processing flag.

        Returns:    Server processing flag (integer/0|1).
        """
        return self.__srvProcess


    def setSrvRemove(self,
                     allowRem):
        """
        Set the server Allow Remove Requests Flag.

        allowRem:    A value of 1 indicates that file processing is
                     supported (integer/0|1).       
        
        Returns:     Reference to object itself.
        """
        if (allowRem == None): return self
        self.__srvRemove = int(allowRem)
        return self


    def getSrvRemove(self):
        """
        Return the server Allow Remove Requests Flag.

        Returns:    Server Allow Remove Requests Flag (integer/0|1).
        """
        return self.__srvRemove


    def setSrvDataChecking(self,
                           dataChecking):
        """
        Set the server data checking flag.

        dataChecking:  A value of 1 indicates that data checking is
                       running (integer/0|1).            
        
        Returns:       Reference to object itself.
        """
        if (dataChecking == None): return self
        self.__srvDataChecking = int(dataChecking)
        return self


    def getSrvDataChecking(self):
        """
        Return the server data checking flag.

        Returns:     Server data checking flag (integer/0|1).
        """
        return self.__srvDataChecking


    def setSrvState(self,
                    state):
        """
        Set the server state.

        state:         Server state: 'NOT-RUNNING', 'OFFLINE',
                       'ONLINE' (string).
        
        Returns:       Reference to object itself.
        """
        if (state == None): return self
        self.__srvState = state
        return self


    def getSrvState(self):
        """
        Return the server state

        Returns:   Server state (string).
        """
        return self.__srvState


    def setSrvSuspended(self,
                        suspended):
        """
        Set the Server Suspended Flag.

        suspended:     Server Suspended Flag (integer/0|1).
        
        Returns:       Reference to object itself.
        """
        if (suspended == None): return self
        self.__srvSuspended = int(suspended)
        return self


    def getSrvSuspended(self):
        """
        Return the Server Suspended Flag.

        Returns:       Server Suspended Flag (integer/0|1).   
        """
        return self.__srvSuspended


    def setSrvReqWakeUpSrv(self,
                           srv):
        """
        Set name of Requested Wake-Up Server.

        srv:           Name of server on which the NG/AMS Server requested
                       for the wake-up call is running (string).
        
        Returns:       Reference to object itself.
        """
        if (srv == None): return self
        self.__srvReqWakeUpSrv = srv
        return self


    def getSrvReqWakeUpSrv(self):
        """
        Return name of Requested Wake-Up Server.

        Returns:       Name of server on which the NG/AMS Server requested
                       for the wake-up call is running (string).
        """
        return self.__srvReqWakeUpSrv


    def setSrvReqWakeUpTime(self,
                            wakeUpTime):
        """
        Set the wake-up time.

        wakeUpTime:    Requested Wake-Up Time (string/ISO 8601).
        
        Returns:       Reference to object itself.
        """
        if (not wakeUpTime): return self
        self.__srvReqWakeUpTime = timeRef2Iso8601(wakeUpTime)
        return self


    def setSrvReqWakeUpTimeFromSecs(self,
                                    dateSecs):
        """
        Set the wake-up time date from seconds since epoch.

        dateSecs:  Wake-up time in seconds since epoch (integer).
 
        Returns:   Reference to object itself.
        """
        if (dateSecs == None): return self
        self.__srvReqWakeUpTime = PccUtTime.TimeStamp().\
                                  initFromSecsSinceEpoch(dateSecs).\
                                  getTimeStamp()
        return self


    def getSrvReqWakeUpTime(self):
        """
        Return the wake-up time.

        Returns:      Requested Wake-Up Time (string/ISO 8601).
        """
        return self.__srvReqWakeUpTime

 
    def setHostType(self,
                    type):
        """
        Set the host type (or location). Possible values are NGAMS_HOST_LOCAL,
        NGAMS_HOST_CLUSTER, NGAMS_HOST_DOMAIN and NGAMS_HOST_REMOTE.

        type:     Type of host (string).
        
        Returns:  Reference to object itself.
        """
        if (type == None): return self
        self.__hostType = type
        return self


    def getHostType(self):
        """
        Return the type of host.

        Returns:   Type of host (string).
        """
        return self.__hostType


    def dumpBuf(self,
                ignoreUndefFields = 0):
        """
        Dump contents of object into a string buffer.

        ignoreUndefFields:     Don't take fields, which have a length of 0
                               (integer/0|1).
                            
        Returns:               String buffer with contents of object (string).
        """        
        format = prFormat1()
        buf = "HostStatus:\n"
        objStat = self.getObjStatus()
        for fieldName, val in objStat:
            if (not ignoreValue(ignoreUndefFields, val)):
                buf += format % (fieldName + ":", val)
        return buf
   

    def clone(self):
        """
        Create a clone of this object (+ contents).

        Returns:    Copy of this object (ngamsHostInfo).
        """
        return ngamsHostInfo().\
               setHostId(self.getHostId()).\
               setDomain(self.getDomain()).\
               setIpAddress(self.getIpAddress()).\
               setMacAddress(getMacAddress()).\
               setNSlots(self.getNSlots()).\
               setClusterName(self.getClusterName()).\
               setInstallationDate(self.getInstallationDate()).\
               setSrvVersion(self.getSrvVersion()).\
               setSrvPort(self.getSrvPort()).\
               setSrvArchive(self.getSrvArchive()).\
               setSrvRetrieve(self.getSrvRetrieve()).\
               setSrvProcess(self.getSrvProcess()).\
               setSrvRemove(self.getSrvRemove()).\
               setSrvDataChecking(self.getSrvDataChecking()).\
               setSrvState(self.getSrvState()).\
               setHostType(self.getHostType()).\
               setSrvSuspended(self.getSrvSuspended()).\
               setSrvLastHost(self.getSrvLastHost()).\
               setSrvReqWakeUpSrv(self.getSrvReqWakeUpSrv()).\
               setSrvReqWakeUpTime(self.getSrvReqWakeUpTime())


# EOF
