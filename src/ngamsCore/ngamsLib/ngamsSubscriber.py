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
import random
import xml.dom.minidom

from ngamsCore import fromiso8601, toiso8601, TRACE, prFormat1


class ngamsSubscriber:
    """
    Class to handle/contain information about one Subscriber.
    """

    def __init__(self,
                 hostId = "",
                 portNo = 0,
                 priority = 10,
                 url = "",
                 startDate = None,
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

        self.setConcurrentThreads(1) # by default only uses 1 thread for each subscriber
        self._AND_DELIMIT = '____' # urllib.quote('&&')
        self._OR_DELIMIT = '----' # urllib.quote('||')


    def setHostId(self,
                  id):
        """
        Set the Host ID.

        id:         Host ID (string).

        Returns:    Reference to object itself.
        """
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
        if not url:
            raise Exception('Invalid subscriber url: %s' % url)
        self.__url = url.strip()
        return self


    def getUrl(self):
        """
        Get the Subscriber URL.

        Returns:    Subscriber URL (string).
        """
        return self.__url

    def getUrlList(self):
        """
        Get a list of URLs from self.__url, the order of list items depends on
        the logical relationships between these URLs
        for 'AND' (i.e. &&), the original order must be maintained
        for 'OR' (i.e. ||), the order must be randomised
        """
        url = self.getUrl()
        if (url.find(self._AND_DELIMIT) > -1):
            urlList = url.split(self._AND_DELIMIT)
        elif (url.find(self._OR_DELIMIT) > -1):
            urlList = url.split(self._OR_DELIMIT)
            random.shuffle(urlList)
        else:
            urlList = [url]

        return urlList


    def setStartDate(self, startDate):
        """
        Set the Subscription Start Date

        startDate:   Subscription start date (number).

        Returns:     Reference to object itself.
        """
        self.__startDate = startDate
        return self


    def getStartDate(self):
        """
        Get the Subscription Start Date.

        Returns:    Subscription Start Date (string).
        """
        return self.__startDate

    def setConcurrentThreads(self, num_threads):
        if not num_threads:
            return self

        num_threads = int(num_threads)
        if num_threads < 0:
            raise Exception('Invalid concurrent threads: %s' % num_threads)

        self.__concurthrds = num_threads
        return self

    def getConcurrentThreads(self):
        return self.__concurthrds


    def setFilterPi(self,
                    plugIn):
        """
        Set the Filter Plug-In.

        plugIn:      Filter Plug-In (string).

        Returns:     Reference to object itself.
        """
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


    def setLastFileIngDate(self, lastFileIngDate):
        """
        Set the File Ingestion Date of last file delivered.

        lastFileIngDate:  File Ingestion Date of last file delivered (number).

        Returns:          Reference to object itself.
        """
        self.__lastFileIngDate = lastFileIngDate
        return self


    def getLastFileIngDate(self):
        """
        Get the the File Ingestion Date.

        Returns:     File Ingestion Date of last file delivered (number).
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

        self.setHostId(sqlResult[0]).\
               setPortNo(sqlResult[1]).\
               setPriority(sqlResult[2]).\
               setId(sqlResult[3]).\
               setUrl(sqlResult[4]).\
               setFilterPi(sqlResult[6]).\
               setFilterPiPars(sqlResult[7]).\
               setLastFileIngDate(fromiso8601(sqlResult[8], local=True)).\
               setConcurrentThreads(sqlResult[9])
        if sqlResult[5]:
            self.setStartDate(fromiso8601(sqlResult[5], local=True))
        else:
            self.setStartDate(None)
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
        if not res:
            raise Exception('%s %s %s not found in DB' % (subscrId, hostId, portNo))
        self.unpackSqlResult(res[0])
        return self


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
        if self.__startDate is not None:
            buf += format % ("Start Date:", toiso8601(self.__startDate))
        buf += format % ("Filter Plug-In:", self.getFilterPi())
        buf += format % ("Filter Plug-In Par.s:", self.getFilterPiPars())
        if self.__lastFileIngDate is not None:
            buf += format % ("Last File Ing. Date:", toiso8601(self.__lastFileIngDate))
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
        if self.__startDate is not None:
            tmpSubscrEl.setAttribute("StartDate", toiso8601(self.__startDate))
        tmpSubscrEl.setAttribute("FilterPlugIn", self.getFilterPi())
        tmpSubscrEl.setAttribute("FilterPlugInPars", self.getFilterPiPars())
        return tmpSubscrEl


# EOF
