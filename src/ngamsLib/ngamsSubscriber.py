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
# "@(#) $Id: ngamsSubscriber.py,v 1.6 2009/11/26 11:46:57 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  04/11/2002  Created
#

"""
Contains classes to handle the information about each Subscriber and
the complete set of Subscribers.
"""
import xml.dom.minidom

import pcc, PccUtTime

from ngams import *


class ngamsSubscriber:
    """
    Class to handle/contain information about one Subscriber.
    """
    
    def __init__(self,
                 hostId = "",
                 portNo = 0,
                 priority = 10,
                 url = "",
                 startDate = "",
                 filterPi = "",
                 filterPiPars = "",
                 lastFileIngDate = "",
                 subscrId = ""):
        """
        Constructor method.
        """
        if subscrId:  # if an explicit ID is specified in the element then use it
                      # else use the URL.
            self.\
               setHostId(hostId).setPortNo(portNo).setPriority(priority).\
               setUrl(url).setStartDate(startDate).\
               setFilterPi(filterPi).setFilterPiPars(filterPiPars).\
               setLastFileIngDate(lastFileIngDate).setId(subscrId)
        else:
            self.\
               setHostId(hostId).setPortNo(portNo).setPriority(priority).\
               setUrl(url).setId(url).setStartDate(startDate).\
               setFilterPi(filterPi).setFilterPiPars(filterPiPars).\
               setLastFileIngDate(lastFileIngDate)


    def setHostId(self,
                  id):
        """
        Set the Host ID.

        id:         Host ID (string).
        
        Returns:    Reference to object itself.
        """
        info(6,"Executing ngamsSubscriber.setHostId(): " + str(id))
        self.__hostId = id.strip()
        return self


    def getHostId(self):
        """
        Get the Host ID.
        
        Returns:    Host ID (string).
        """
        return self.__hostId


    def setPortNo(self,
                  portNo):
        """
        Set the Port Number of the Data Provider.

        portNo:     Port number of Data Provider (integer).

        Returns:    Reference to object itself.
        """
        info(6,"Executing ngamsSubscriber.setPortNo(): " + str(portNo))
        self.__portNo = int(portNo)
        return self


    def getPortNo(self):
        """
        Return port number of Data Provider.

        Returns:    Port number of Data Provider (integer).
        """
        return self.__portNo


    def setPriority(self,
                    prio):
        """
        Set the Subscriber Priority.

        prio:       Priori (low number = high priority) (integer).
        
        Returns:    Reference to object itself.
        """
        info(6,"Executing ngamsSubscriber.setPriority(): " + str(prio))
        self.__priority = int(prio)
        return self


    def getPriority(self):
        """
        Get the Priority.
        
        Returns:    Priority (integer).
        """
        return self.__priority


    def setId(self,
              id):
        """
        Set the Subscriber ID. If a complete URL is given, only the part
        up to the possible '?' is taken.

        id:         Subscriber ID or Subscriber URL (string).
        
        Returns:    Reference to object itself.
        """
        info(6,"Executing ngamsSubscriber.setId(): " + str(id))
        self.__id = id.split("?")[0].strip()
        return self


    def getId(self):
        """
        Get the Subscriber ID.
        
        Returns:    Subscriber ID (string).
        """
        return self.__id


    def setUrl(self,
               url):
        """
        Set the Subscriber URL. 

        url:        Subscriber URL (string).
        
        Returns:    Reference to object itself.
        """
        info(6,"Executing ngamsSubscriber.setUrl(): " + str(url))
        self.__url = url.strip()
        return self


    def getUrl(self):
        """
        Get the Subscriber URL.
        
        Returns:    Subscriber URL (string).
        """
        return self.__url


    def setStartDate(self,
                     startDate):
        """
        Set the Subscription Start Date (ISO 8601).

        startDate:   Subscription start date (string).
        
        Returns:     Reference to object itself.
        """
        info(6,"Executing ngamsSubscriber.setStartDate(): " + str(startDate))
        if (not startDate):
            self.__startDate = ""
        else:
            self.__startDate = timeRef2Iso8601(startDate)
        return self


    def setStartDateFromSecs(self,
                             startDateSecs):
        """
        Set the  Subscription Start Date from seconds since epoch.

        startDateSecs:   Date in seconds since epoch (integer).
 
        Returns:         Reference to object itself.
        """
        info(6,"Executing ngamsSubscriber.setStartDateFromSecs(): " +\
             str(startDateSecs))
        self.__startDate = PccUtTime.TimeStamp().\
                           initFromSecsSinceEpoch(startDateSecs).getTimeStamp()
        return self


    def getStartDate(self):
        """
        Get the Subscription Start Date.
        
        Returns:    Subscription Start Date (string).
        """
        return self.__startDate


    def setFilterPi(self,
                    plugIn):
        """
        Set the Filter Plug-In.

        plugIn:      Filter Plug-In (string).
        
        Returns:     Reference to object itself.
        """
        info(6,"Executing ngamsSubscriber.setFilterPi(): " + str(plugIn))
        if (not plugIn):
            self.__plugIn = ""
        else:
            self.__plugIn = plugIn.strip()
        return self


    def getFilterPi(self):
        """
        Get the Subscription Filter Plug-In.
        
        Returns:    Name of Subscription Filter Plug-In (string).
        """
        return self.__plugIn


    def setFilterPiPars(self,
                        plugInPars):
        """
        Set the Filter Plug-In Parameters. These should be given as
        '<par>=<val>,<par>=<val>,...'.

        plugInPars:    Filter Plug-In Parameters (string).
        
        Returns:       Reference to object itself.
        """
        info(6,"Executing ngamsSubscriber.setFilterPiPars(): "+str(plugInPars))
        if (not plugInPars):
            self.__plugInPars = ""
        else:
            self.__plugInPars = plugInPars.strip()
        return self


    def getFilterPiPars(self):
        """
        Get the Subscription Filter Plug-In Parameters.
        
        Returns:    Name of Subscription Filter Plug-In Parameters (string).
        """
        return self.__plugInPars


    def setLastFileIngDate(self,
                           lastFileIngDate):
        """
        Set the File Ingestion Date of last file delivered.

        lastFileIngDate:  File Ingestion Date of last file delivered
                          (string/ISO 8601).
        
        Returns:          Reference to object itself.
        """
        info(6,"Executing ngamsSubscriber.setLastFileIngDate(): " +\
             str(lastFileIngDate))
        if (not lastFileIngDate):
            self.__lastFileIngDate = ""
        else:
            ###self.__lastFileIngDate = lastFileIngDate.strip()
            self.__lastFileIngDate = timeRef2Iso8601(lastFileIngDate)
        return self


    def setLastFileIngDateFromSecs(self,
                                   dateSecs):
        """
        Set the Last File Ingestion Date from seconds since epoch.

        dateSecs:  Date in seconds since epoch (integer).
 
        Returns:   Reference to object itself.
        """
        info(6,"Executing ngamsSubscriber.setLastFileIngDateFromSecs(): " +\
             str(dateSecs))
        if (not dateSecs):
            self.__lastFileIngDate = ""
        else:
            self.__lastFileIngDate = PccUtTime.TimeStamp().\
                                     initFromSecsSinceEpoch(dateSecs).\
                                     getTimeStamp()
        return self


    def getLastFileIngDate(self):
        """
        Get the the File Ingestion Date.
        
        Returns:     File Ingestion Date of last file delivered
                     (string/ISO 8601).
        """
        return self.__lastFileIngDate

    
    def unpackSqlResult(self,
                        sqlResult):
        """
        Unpack the result from an SQL query, whereby the columns of one row in
        the ngas_subscribers table is queried with ngamsDb.getSubscriberInfo()
        (specific Subscriber specified).

        sqlResult:   List with elements from the ngas_subscribers table (list).

        Returns:     Reference to object itself.
        """
        T = TRACE()
        
        self.\
               setHostId(sqlResult[0]).\
               setPortNo(sqlResult[1]).\
               setPriority(sqlResult[2]).\
               setId(sqlResult[3]).\
               setUrl(sqlResult[4]).\
               setStartDate(sqlResult[5]).\
               setFilterPi(sqlResult[6]).\
               setFilterPiPars(sqlResult[7]).\
               setLastFileIngDate(sqlResult[8])
        return self


    def read(self,
             dbConObj,
             subscrId,
             hostId = "",
             portNo = -1):
        """
        Query information about a specific Subscriber and set the
        class member variables accordingly.

        dbConObj:  Reference to DB connection object (ngamsDb).

        subscrId:  Subscriber ID (string).

        hostId:    Limit the query to Subscribers in connection with one
                   host (Data Provider) (string).

        portNo:    Limit the query to Subscribers in connection with one
                   host (Data Provider) (integer).

        Returns:   Reference to object itself.
        """
        T = TRACE()

        res = dbConObj.getSubscriberInfo(subscrId, hostId, portNo)
        self.unpackSqlResult(res)
        return self


    def write(self,
              dbConObj):
        """
        Write the information contained in the object about a Subscriber
        into the DB specified by the DB connection object.

        dbConObj:     DB connection object (ngamsDb).

        Returns:      Returns 1 if a new entry was created in the DB
                      and 0 if an existing entry was updated (integer/0|1).
        """
        addedNewEntry = dbConObj.\
                        writeSubscriberEntry(self.getHostId(),
                                             self.getPortNo(),
                                             self.getId(),
                                             self.getUrl(),
                                             self.getPriority(),
                                             self.getStartDate(),
                                             self.getFilterPi(),
                                             self.getFilterPiPars(),
                                             self.getLastFileIngDate())
        return addedNewEntry


    def dumpBuf(self):
        """
        Dump contents of object into a string buffer.

        Returns:    String buffer with disk info (string).
        """
        format = prFormat1()
        buf = "Subscriber Info:\n"
        buf += format % ("Host ID:", self.getHostId())
        if (self.getPortNo() > 0):
            buf += format % ("Port Number:", str(self.getPortNo()))
        buf += format % ("Priority:", str(self.getPriority()))
        buf += format % ("Subscriber ID:", self.getId())
        buf += format % ("Subscriber URL:", self.getUrl())
        buf += format % ("Start Date:", self.getStartDate())
        buf += format % ("Filter Plug-In:", self.getFilterPi())
        buf += format % ("Filter Plug-In Par.s:", self.getFilterPiPars())
        buf += format % ("Last File Ing. Date:",str(self.getLastFileIngDate()))
        return buf


    def genXml(self):
        """
        Generate an XML DOM Node and with the information in the object
        and return this.

        Returns:         DOM node object (Node).
        """
        tmpSubscrEl = xml.dom.minidom.Document().createElement("Subscriber")
        tmpSubscrEl.setAttribute("HostId", self.getHostId())
        if (self.getPortNo() > 0):
            tmpSubscrEl.setAttribute("PortNo", str(self.getPortNo()))
        tmpSubscrEl.setAttribute("Priority", str(self.getPriority()))
        tmpSubscrEl.setAttribute("SubscriberUrl", self.getUrl())
        if (self.getStartDate() != ""):
            tmpSubscrEl.setAttribute("StartDate", self.getStartDate())
        tmpSubscrEl.setAttribute("FilterPlugIn", self.getFilterPi())
        tmpSubscrEl.setAttribute("FilterPlugInPars", self.getFilterPiPars())
        return tmpSubscrEl
        

# EOF
