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
# "@(#) $Id: ngamsDbNgasSubscribers.py,v 1.5 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  07/03/2008  Created
#

"""
Contains queries for accessing the NGAS Subscribers Table.

This class is not supposed to be used standalone in the present implementation.
It should be used as part of the ngamsDbBase parent classes.
"""

from   ngams import *
import ngamsDbCore


class ngamsDbNgasSubscribers(ngamsDbCore.ngamsDbCore):
    """
    Contains queries for accessing the NGAS Subscribers Table.
    """

    def subscriberInDb(self,
                       subscrId):
        """
        Check if the Subscriber with the given ID is registered in the DB.
    
        subscrId:    Subscriber ID (string).
        
        Returns:     1 = Subscriber registered, 0 = Subscriber not
                     registered (integer).
        """
        T = TRACE()
        
        sqlQuery = "SELECT subscr_id FROM ngas_subscribers WHERE " +\
                   "subscr_id='" + subscrId + "'"
        res = self.query(sqlQuery)
        if (res == [[]]):
            return 0
        else:
            if (res[0][0][0] == subscrId):
                return 1
            else:
                return 0


    def getSubscriberInfo(self,
                          subscrId = "",
                          hostId = "",
                          portNo = -1):
        """
        Get the information for one or more Subcribers from the
        ngas_subscribers table and return the contents in a list. The format
        of this list is formatted as follows:

          [<Host ID>, <Port No>, <Priority>, <Subscriber ID>, <Subscriber URL>,
           <Subscription Start Date>, <Subscription Filter Plug-In>,
           <Subscription Filter Plug-In Parameters>,
           <Last File Ingestion Date>]

        subscrId:   ID of the Subcriber (string).

        hostId:     Limit the query to Subscribers in connection with one
                    host (Data Provider) (string).

        portNo:     Limit the query to Subscribers in connection with one
                    host (Data Provider) (integer).

        Returns:    If a Subscriber ID is specified: List with information
                    about the Subscriber (if found). Otherwise [] is returned
                    (list).

                    If no Subscriber ID is given: List with sub-lists with
                    information for all Subscribers. Otherwise [] is returned
                    (list/list).
        """
        T = TRACE()
        
        sqlQuery = "SELECT " + ngamsDbCore.getNgasSubscribersCols() +\
                   " FROM ngas_subscribers ns"
        if ((subscrId != "") or (hostId != "")): sqlQuery += " WHERE "
        if (subscrId != ""): sqlQuery += "subscr_id='" + subscrId + "'"
        if ((hostId != "") and (subscrId != "")):
            sqlQuery += " AND host_id='" + hostId + "'"
        if ((hostId != "") and (subscrId == "")):
            sqlQuery += " ns.host_id='" + hostId + "'"
        if ((portNo != -1) and ((hostId != ""))):
            sqlQuery += " AND ns.srv_port=" + str(portNo)
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (res == [[]]):
            return []
        else:
            if (subscrId != ""):
                return res[0][0]
            else:
                return res[0]


    def writeSubscriberEntry(self,
                             hostId,
                             portNo,
                             subscrId,
                             subscrUrl,
                             priority = 10,
                             startDate = "",
                             filterPlugIn = "",
                             filterPlugInPars = "",
                             lastFileIngDate = ""):
        """
        The method writes the information in connection with a Subscriber
        in the NGAS DB. If an entry already exists for that disk, it is updated
        with the information given as input parameters. Otherwise, a new
        entry is created.

        hostId:
        ...
        filterPlugInPars:    Parameters for the Subscriber (string).

        priority:            Priority of Subscriber (integer).

        startDate:           Date the subscription should start from
                             (string/ISO 8601).

        lastFileIngDate:     Ingestion dtae of last file delivered
                             (string/ISO 8601).

        Returns:             Returns 1 if a new entry was created in the DB
                             and 0 if an existing entry was updated
                             (integer/0|1).
        """
        T = TRACE()
        try:
            try:
                self.takeDbSem()
                if ((startDate == "") or (startDate == None)):
                    startDate = "None"
                else:
                    startDate = "'" + self.convertTimeStamp(startDate) + "'"
                if ((filterPlugIn == "") or (filterPlugIn == None)):
                    filterPlugIn = "''"
                else:
                    filterPlugIn = "'" + filterPlugIn + "'"
                if ((filterPlugInPars == "") or (filterPlugInPars == None)):
                    filterPlugInPars = "''"
                else:
                    filterPlugInPars = "'" + filterPlugInPars + "'"
                if ((lastFileIngDate == "") or (lastFileIngDate == None)):
                    lastFileIngDate = "'" + self.convertTimeStamp(0) + "'"
                else:
                    lastFileIngDate = "'" +\
                                      self.convertTimeStamp(lastFileIngDate) +\
                                      "'"
                self.relDbSem()
            except Exception, e:
                self.relDbSem()
                raise Exception, e                  

            # Check if the entry already exists. If yes update it, otherwise
            # insert a new element.
            if (self.subscriberInDb(subscrId)):
                sqlQuery = "UPDATE ngas_subscribers SET " +\
                           "host_id='" + hostId + "', " +\
                           "srv_port=" + str(portNo) + ", " +\
                           "subscr_prio=" + str(priority) + ", " +\
                           "subscr_id='" + subscrId + "', " +\
                           "subscr_url='" + subscrUrl + "', " +\
                           "subscr_start_date=" + startDate + ", " +\
                           "subscr_filter_plugin=" + filterPlugIn + ", " +\
                           "subscr_filter_plugin_pars="+filterPlugInPars+", "+\
                           "last_file_ingestion_date=" + lastFileIngDate+" " +\
                           "WHERE subscr_id='" + subscrId + "' AND " +\
                           "host_id='" + hostId + "' AND " +\
                           "srv_port=" + str(portNo)
                addedEntry = 0
            else:
                sqlQuery = "INSERT INTO ngas_subscribers " +\
                           "(host_id, srv_port, subscr_prio, subscr_id," +\
                           " subscr_url, subscr_start_date," +\
                           " subscr_filter_plugin,"+\
                           " subscr_filter_plugin_pars," +\
                           " last_file_ingestion_date) " +\
                           " VALUES " +\
                           "('" + hostId + "', " + str(portNo) + ", " +\
                           str(priority) + ", '"+subscrId + "', '"+subscrUrl+\
                           "', " + startDate + ", " + filterPlugIn + ", " +\
                           filterPlugInPars + ", " + lastFileIngDate + ")"
                addedEntry = 1
            res = self.query(sqlQuery)
            self.triggerEvents()
            return addedEntry
        except Exception, e:   
            raise e


    def deleteSubscriber(self,
                         subscrId):
        """
        Delete the information for one Subscriber from the NGAS DB.

        subscrId:   Subscriber ID (string).

        Returns:    Reference to object itself.
        """
        T = TRACE()
        
        try:
            sqlQuery = "DELETE FROM ngas_subscribers WHERE subscr_id='" +\
                       subscrId + "'"
            self.query(sqlQuery)
            self.triggerEvents()
            return self
        except Exception, e:   
            raise e


    def getSubscriberStatus(self,
                            subscrIds,
                            hostId = "",
                            portNo = -1):
        """
        Method to query the information about the Ingestion Date of the
        last file delivered to the Subscriber. A list is returned, which
        contains the following:

          [(<Subscriber ID>, <Last File Ingestion Date (ISO 8601)>), ...]

        subscrIds:      List of Subscriber ID to query (list/string).

        hostId:         Host name of Subscriber host (string).

        portNo:         Port number used by Subscriber host (integer).
                            
        Returns:        List with Subscriber status (list/tuple/string).
        """
        T = TRACE()
        
        if (not subscrIds):
            return []
        sqlQuery = "SELECT subscr_id, last_file_ingestion_date " +\
                   "FROM ngas_subscribers WHERE subscr_id IN (" +\
                   str(subscrIds)[1:-1] + ")"
        if (hostId != ""):
            sqlQuery += " AND host_id='" + hostId + "'"
            if (portNo != -1):
                sqlQuery += " AND srv_port=" + str(portNo)
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (res == [[]]):
            return []
        else:
            subscrStatus = []
            for subscrInfo in res[0]:
                if (subscrInfo[1]):
                    if ((subscrInfo[1].find(":") != -1) and
                        (subscrInfo[1].find("T") != -1)):
                        lastIngDate = subscrInfo[1]
                    else:
                        lastIngDate = PccUtTime.TimeStamp().\
                                      initFromSecsSinceEpoch(subscrInfo[1]).\
                                      getTimeStamp()
                else:
                    lastIngDate = None
                subscrStatus.append((subscrInfo[0], lastIngDate))
            return subscrStatus


    def subscrBackLogEntryInDb(self,
                               hostId,
                               portNo,
                               subscrId,
                               fileId,
                               fileVersion):
        """
        Check if there is an entry in the Subscription Back-Log for that
        file/Subscriber.

        hostId:          Host ID for NGAS host where Data Provider concerned
                         is running (string).

        portNo:          Port number used by Data Provider concerned (integer).

        subscrId:        Subscriber ID (string).
        
        fileId:          File ID (string).
        
        fileVersion:     File Version (string).

        Returns:         1 = file found, 0 = file no found (integer).
        """
        T = TRACE()
        
        sqlQuery = "SELECT file_id FROM ngas_subscr_back_log " +\
                   "WHERE host_id='" + hostId + "' " +\
                   "AND srv_port=" + str(portNo) + " " +\
                   "AND subscr_id='" + subscrId + "' " +\
                   "AND file_id='" + fileId + "' " +\
                   "AND file_version=" + str(fileVersion)
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (len(res[0]) == 1):
            if (res[0][0][0] == fileId):
                return 1
            else:
                return 0
        else:
            return 0


    def addSubscrBackLogEntry(self,
                              hostId,
                              portNo,
                              subscrId,
                              subscrUrl,
                              fileId,
                              fileName,
                              fileVersion,
                              ingestionDate,
                              format):
        """
        Adds a Back-Log Entry in the DB. If there is already an entry
        for that file/Subscriber, a new entry is not created.

        hostId:          Host ID for NGAS host where Data Provider concerned
                         is running (string).

        portNo:          Port number used by Data Provider concerned (integer).

        subscrUrl:       Susbcriber URl to where the files are delivered
                         (string).
        
        subscrId:        Subscriber ID (string).
        
        fileId:          File ID (string).

        fileName:        Filename, i.e., name of file as stored in the
                         Subscription Back-Log Area (string).
        
        fileVersion:     File Version (integer).

        ingestionDate:   File Ingestion Date (string/ISO 8601).

        format:          Mime-type of file (string).

        Returns:         Void.
        """
        T = TRACE()

        try:
            try:
                self.takeDbSem()
                ingDate = self.convertTimeStamp(ingestionDate)
                self.relDbSem()
            except Exception, e:
                self.relDbSem()
                raise Exception, e             
            if (not self.subscrBackLogEntryInDb(hostId, portNo, subscrId,
                                                fileId, fileVersion)):
                sqlQuery = "INSERT INTO ngas_subscr_back_log " +\
                           "(host_id, srv_port, subscr_id, subscr_url, " +\
                           "file_id, file_name, file_version, " +\
                           "ingestion_date, format) " +\
                           "VALUES " +\
                           "('" + hostId + "', " + str(portNo) + ", '" +\
                           subscrId + "', '" + subscrUrl + "', '" +\
                           fileId + "', '" + fileName + "', " +\
                           str(fileVersion) +\
                           ", '" + ingDate + "', '" + format + "')"
                self.query(sqlQuery)
                self.triggerEvents()
        except Exception, e:   
            raise e


    def delSubscrBackLogEntry(self,
                              hostId,
                              portNo,
                              subscrId,
                              fileId,
                              fileVersion):
        """
        Delete an entry in the Subscription Back-Log Table.

        hostId:          Host ID for NGAS host where Data Provider concerned
                         is running (string).

        portNo:          Port number used by Data Provider concerned (integer).
 
        subscrId:        Subscriber ID (string).
        
        fileId:          File ID (string).
        
        fileVersion:     File Version (string).
        
        fileName:        Filename, i.e., name of file as stored in the
                         Subscription Back-Log Area (string).

        Returns:         Void.
        """
        T = TRACE()
        
        try:
            if (self.subscrBackLogEntryInDb(hostId, portNo, subscrId, fileId,
                                            fileVersion)):
                sqlQuery = "DELETE FROM ngas_subscr_back_log " +\
                           "WHERE host_id='" + hostId + "' " +\
                           "AND srv_port=" + str(portNo) + " " +\
                           "AND subscr_id='" + subscrId + "' " +\
                           "AND file_id='" + fileId + "' " +\
                           "AND file_version=" + str(fileVersion)
                res = self.query(sqlQuery)
                self.triggerEvents()
        except Exception, e:   
            raise e


    def updateSubscrStatus(self,
                           subscrId,
                           fileIngDate):
        """
        Update the Subscriber Status so that it reflects the File Ingestion
        Date of the last file ingested.

        subscrId:       Subscriber ID (string).
        
        fileIngDate:    File Ingestion Date (string/ISO 8601).

        Returns:        Void.
        """
        T = TRACE()

        try:
            try:
                self.takeDbSem()
                ingDate = self.convertTimeStamp(fileIngDate)
                self.relDbSem()
            except Exception, e:
                self.relDbSem()
                raise Exception, e
            sqlQuery = "UPDATE ngas_subscribers SET " +\
                       "last_file_ingestion_date='" +\
                       ingDate + "' WHERE subscr_id='" + subscrId + "' AND " +\
                       "last_file_ingestion_date < '" + ingDate + "'"
            self.query(sqlQuery)
            self.triggerEvents()
        except Exception, e:   
            raise e


    def getSubscrBackLog(self,
                         hostId,
                         portNo):
        """
        Read all entries in the Subscriber Back-Log Table 'belonging'
        to a specific Data Provider, and return these in a list with sub-lists.

        hostId:      Host ID of Data Provider (string).
        
        portNo:      Port number used by Data Provider (integer).

        Returns:     List containing sub-list with the following information:

                       [[<Subscr. ID>, <Subscr. URL>, <File ID>, <Filename>,
                         <File Version>, <Ingestion Date>,
                         <Format <Mime-Type>], ...]

                     Note that the part of the list after the Subscriber URL
                     is the same as generated by ngamsDbBase.getFileSummary2()
                     (list/list).
        """
        T = TRACE()
        
        sqlQuery = "SELECT subscr_id, subscr_url, file_id, file_name, " +\
                   "file_version, ingestion_date, format " +\
                   "FROM ngas_subscr_back_log WHERE " +\
                   "host_id='" + hostId + "' AND srv_port=" + str(portNo)
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (res == [[]]):
            return []
        else:
            # Convert the time into ISO 8601.
            procList = []
            for fi in res[0]:
                if ((fi[5].find(":") != -1) and
                    (fi[5].find("T") != -1)):
                    ingDate = fi[5]
                else:
                    ingDate = PccUtTime.TimeStamp().\
                              initFromSecsSinceEpoch(fi[5]).getTimeStamp()
                newItem = list(fi[0:5]) + [ingDate] + [fi[6]]
                procList.append(newItem)
            return procList


    def getSubscrsOfBackLogFile(self,
                                fileId,
                                fileVersion):
        """
        Get a list of Subscribers (their IDs) interested in this Back-Log
        Buffered file.

        fileId:       File ID (string).
        
        fileVersion:  File Version (string).

        Returns:      List with Subscriber IDs (if any) (list/string).
        """
        T = TRACE()
        
        sqlQuery = "SELECT subscr_id FROM ngas_subscr_back_log " +\
                   "WHERE file_id='" + fileId + "' AND " +\
                   "file_version=" + str(fileVersion)
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (res == [[]]):
            return []
        else:
            return res[0]


# EOF
