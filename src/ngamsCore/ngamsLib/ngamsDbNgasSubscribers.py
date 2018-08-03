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

from . import ngamsDbCore
from .ngamsCore import fromiso8601


class ngamsDbNgasSubscribers(ngamsDbCore.ngamsDbCore):
    """
    Contains queries for accessing the NGAS Subscribers Table.
    """

    def comment_colname(self):
        if 'oracle' in self.module_name.lower():
            return '"comment"'
        return 'comment'

    def subscriberInDb(self,
                       subscrId):
        """
        Check if the Subscriber with the given ID is registered in the DB.

        subscrId:    Subscriber ID (string).

        Returns:     1 = Subscriber registered, 0 = Subscriber not
                     registered (integer).
        """
        sql = "SELECT subscr_id FROM ngas_subscribers WHERE subscr_id={0}"
        res = self.query2(sql, args = (subscrId,))
        if res:
            return 1
        return 0


    def getSubscriberInfo(self,
                          subscrId = None,
                          hostId = None,
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
        where = False
        vals = []
        sql = []
        sql.append("SELECT %s FROM ngas_subscribers ns" % ngamsDbCore.getNgasSubscribersCols())

        if subscrId:
            where = True
            sql.append(" WHERE ")
            sql.append("subscr_id = {}")
            vals.append(subscrId)

        if hostId:
            if where == False:
                where = True
                sql.append(" WHERE ")
            else:
                sql.append(" AND ")
            sql.append("host_id = {}")
            vals.append(hostId)

        if portNo != -1:
            if where == False:
                sql.append(" WHERE ")
            else:
                sql.append(" AND ")
            sql.append("srv_port = {}")
            vals.append(portNo)

        return self.query2(''.join(sql), args = vals)


    def insertSubscriberEntry(self, sub_obj):

        hostId = sub_obj.getHostId()
        portNo = sub_obj.getPortNo()
        subscrId = sub_obj.getId()
        subscrUrl = sub_obj.getUrl()
        priority = sub_obj.getPriority()
        startDate = self.asTimestamp(sub_obj.getStartDate())
        filterPlugIn = sub_obj.getFilterPi()
        filterPlugInPars = sub_obj.getFilterPiPars()
        lastFileIngDate = self.asTimestamp(sub_obj.getLastFileIngDate())
        concurrent_threads = sub_obj.getConcurrentThreads()

        sql = ("INSERT INTO ngas_subscribers"
               " (host_id, srv_port, subscr_prio, subscr_id,"
               " subscr_url, subscr_start_date,"
               " subscr_filter_plugin,"
               " subscr_filter_plugin_pars,"
               " last_file_ingestion_date, concurrent_threads) "
               " VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9})")

        vals = (hostId, portNo, priority, \
                subscrId, subscrUrl, startDate, \
                filterPlugIn, filterPlugInPars, \
                lastFileIngDate, concurrent_threads)

        self.query2(sql, args = vals)
        self.triggerEvents()


    def updateSubscriberEntry(self, sub_obj):
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
        hostId = sub_obj.getHostId()
        portNo = sub_obj.getPortNo()
        subscrId = sub_obj.getId()
        subscrUrl = sub_obj.getUrl()
        priority = sub_obj.getPriority()
        startDate = self.asTimestamp(sub_obj.getStartDate())
        filterPlugIn = sub_obj.getFilterPi()
        filterPlugInPars = sub_obj.getFilterPiPars()
        lastFileIngDate = self.asTimestamp(sub_obj.getLastFileIngDate())
        concurrent_threads = sub_obj.getConcurrentThreads()

        sql = ("UPDATE ngas_subscribers SET "
               "host_id={0}"
               ", srv_port={1}"
               ", subscr_prio={2}"
               ", subscr_id={3}"
               ", subscr_url={4}"
               ", subscr_start_date={5}"
               ", subscr_filter_plugin={6}"
               ", subscr_filter_plugin_pars={7}"
               ", last_file_ingestion_date={8}"
               ", concurrent_threads={9} "
               "WHERE subscr_id={10} AND host_id={11} AND srv_port={12}")
        vals = (hostId, portNo, priority, subscrId, subscrUrl, \
                startDate, filterPlugIn, filterPlugInPars, lastFileIngDate, \
                concurrent_threads, subscrId, hostId, portNo)
        self.query2(sql, args = vals)
        self.triggerEvents()


    def deleteSubscriber(self,
                         subscrId):
        """
        Delete the information for one Subscriber from the NGAS DB.

        subscrId:   Subscriber ID (string).

        Returns:    Reference to object itself.
        """
        sql = "DELETE FROM ngas_subscribers WHERE subscr_id={0}"
        self.query2(sql, args = (subscrId,))
        self.triggerEvents()
        return self


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
        if not subscrIds:
            return []

        sql = []
        vals = []
        sql_tmp = ("SELECT subscr_id, last_file_ingestion_date "
                    "FROM ngas_subscribers WHERE subscr_id IN (%s)")

        params = []
        for i in subscrIds:
            params.append('{}')
            vals.append(i)

        sql_tmp = sql_tmp % ','.join(params)
        sql.append(sql_tmp)

        if hostId:
            sql.append(" AND host_id = {}")
            vals.append(hostId)

        if portNo != -1:
            sql.append(" AND srv_port = {}")
            vals.append(portNo)

        res = self.query2(''.join(sql), args = vals)
        if not res:
            return []

        subscrStatus = []
        for subscrInfo in res:
            if subscrInfo[1]:
                lastIngDate = fromiso8601(subscrInfo[1], local=True)
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
        sql = ("SELECT file_id FROM ngas_subscr_back_log "
               "WHERE host_id={} "
               "AND srv_port={} "
               "AND subscr_id={} "
               "AND file_id={} "
               "AND file_version={}")
        vals = (hostId, portNo, subscrId, fileId, fileVersion)
        res = self.query2(sql, args = vals)
        if res:
            return 1
        return 0

    def updateSubscrQueueEntry(self,
                            subscrId,
                            fileId,
                            fileVersion,
                            diskId,
                            status,
                            status_date,
                            comment = None):
        """
        Update the status (and comment) of a file in the persistent queue
        given its primary key
        """
        sql = []
        sql.append("UPDATE ngas_subscr_queue SET status={}, status_date={} ")
        vals = [status, self.convertTimeStamp(status_date)]
        if comment:
            sql.append(", %s={} " % (self.comment_colname(),))
            vals.append(comment)
        sql.append("WHERE subscr_id={} AND file_id={} AND file_version={} AND disk_id={}")
        vals += [subscrId, fileId, fileVersion, diskId]
        self.query2(''.join(sql), args = vals)

    def updateSubscrQueueEntryStatus(self, subscrId, oldStatus, newStatus):
        """
        change the status from old to new for files belonging to a subscriber
        """
        sql = ("UPDATE ngas_subscr_queue SET status={} "
                "WHERE subscr_id={} AND status={}")
        self.query2(sql, args = (newStatus, subscrId, oldStatus))

    def addSubscrQueueEntry(self,
                            subscrId,
                            fileId,
                            fileVersion,
                            diskId,
                            fileName,
                            ingestionDate,
                            format,
                            status,
                            status_date,
                            comment = None
                            ):

        sql = ("INSERT INTO ngas_subscr_queue "
                "(subscr_id, file_id, file_version, "
                "disk_id, file_name, ingestion_date, "
                "format, status, status_date, %s) "
                "VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {})") % (self.comment_colname(),)
        vals = (subscrId, fileId, fileVersion, diskId, fileName, \
                ingestionDate, format, status, self.convertTimeStamp(status_date), comment)
        self.query2(sql, args = vals)

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

        ingDate = self.convertTimeStamp(ingestionDate)

        if self.subscrBackLogEntryInDb(hostId, portNo, subscrId, fileId, fileVersion):
            return

        sql = ("INSERT INTO ngas_subscr_back_log "
                "(host_id, srv_port, subscr_id, subscr_url, "
                "file_id, file_name, file_version, ingestion_date, format) "
                "VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {})")
        vals = (hostId, portNo, subscrId, subscrUrl, \
                fileId, fileName, fileVersion, ingDate, format)
        self.query2(sql, args = vals)
        self.triggerEvents()


    def delSubscrBackLogEntries(self, hostId, portNo, subscrId):
        """
        Delete all entries to be delivered to a subscriber with subscrId

        hostId:        Host ID for NGAS host where Data Provider concerned
                       is running (string).
        portNo:        Port number used by Data Provider concerned (integer).
        subscrId:      Subscriber ID (string).

        """
        sql = ("DELETE FROM ngas_subscr_back_log WHERE subscr_id = {}"
                " AND host_id = {} AND srv_port = {}")
        self.query2(sql, args = (subscrId, hostId, portNo))
        self.triggerEvents()

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
        if not self.subscrBackLogEntryInDb(hostId, portNo, subscrId, fileId,fileVersion):
            return

        sql = ("DELETE FROM ngas_subscr_back_log "
               "WHERE host_id={} "
               "AND srv_port={} "
               "AND subscr_id={} "
               "AND file_id={} "
               "AND file_version={} ")
        vals = (hostId, portNo, subscrId, fileId, fileVersion)
        self.query2(sql, args = vals)
        self.triggerEvents()


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
        ingDate = self.convertTimeStamp(fileIngDate)

        sql = ("UPDATE ngas_subscribers SET "
               "last_file_ingestion_date = {} "
               "WHERE subscr_id = {} AND last_file_ingestion_date < {}")
        vals = (ingDate, subscrId, ingDate)
        self.query2(sql, args = vals)
        self.triggerEvents()


    def getSubscrBackLogBySubscrId(self, subscrId):
        """
        Get all entries in the Susbscriber Back-log Table
        to be delivered to a specific subscriber

        subscrId    Subscriber Id

        Returns     List containing sublist with the following information:
                    [[<file_id>, <file_version>], ...]
        """
        # need to join ngas_file table to get the disk id!!!
        sql = ("SELECT a.file_id, a.file_version, b.disk_id "
                "FROM ngas_subscr_back_log a, ngas_files b "
                "WHERE a.subscr_id = {} AND a.file_id = b.file_id "
                "AND a.file_version = b.file_version")
        res = self.query2(sql, args = (subscrId,))
        if not res:
            return []

        procList = []
        for fi in res:
            newItem = [fi[0]] + [fi[1]] + [fi[2]]
            procList.append(newItem)

        return procList

    def getSubscrBackLogCount(self, hostId, portNo):
        """
        Read the number of entries in the Subscriber Back-Log Table 'belonging'
        to a specific Data Provider/Mover

        hostId:      Host ID of Data Provider (string).

        portNo:      Port number used by Data Provider (integer).

        Returns:     The number of records (integer)
        """
        sql = ("SELECT COUNT(*) FROM ngas_subscr_back_log "
                "WHERE host_id = {} AND srv_port = {}")
        res = self.query2(sql, args = (hostId, portNo))
        return int(res[0][0])


    def getSubscrQueueStatus(self, subscrId, fileId, fileVersion, diskId):
        sql = ("SELECT status, %s FROM ngas_subscr_queue "
                "WHERE subscr_id = {} AND file_id = {} AND "
                "file_version = {} AND disk_id = {}") % (self.comment_colname(),)
        vals = (subscrId, fileId, fileVersion, diskId)
        res = self.query2(sql, args = vals)
        if not res:
            return None
        return res[0] #get the first row only


    def getSubscrQueue(self, subscrId, status = None):
        """
        Read all entries in the ngas_subscr_queue table 'belonging' to a
        specific subscriber, and where the status meets the "status" condition

        subscrId:    subscriber Id (string)
        status:      the status of current file delivery (int or None)
        """
        sql = []
        vals = [subscrId]
        sql.append(("SELECT a.file_id, a.file_name, a.file_version, a.ingestion_date,"
                    "a.format, a.disk_id FROM ngas_subscr_queue a "
                    "WHERE a.subscr_id={}"))
        if status:
            sql.append(" AND a.status={}")
            vals.append(status)

        res = self.query2(''.join(sql), args = vals)
        if not res:
            return []
        return res

    def getSubscrBackLog(self,
                         hostId,
                         portNo,
                         selectDiskId = False):
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
        vals = [hostId, portNo]

        if selectDiskId:
            sql = ("SELECT a.subscr_id, a.subscr_url, a.file_id, a.file_name, "
                    "a.file_version, a.ingestion_date, a.format, b.disk_id "
                    "FROM ngas_subscr_back_log a, ngas_files b "
                    "WHERE a.host_id={} AND a.srv_port={} AND a.file_id = "
                    "b.file_id AND a.file_version = b.file_version")
        else:
            sql = ("SELECT subscr_id, subscr_url, file_id, file_name, "
                   "file_version, ingestion_date, format "
                   "FROM ngas_subscr_back_log WHERE "
                   "host_id={} AND srv_port={}")

        res = self.query2(sql, args = vals)
        if not res:
            return []

        procList = []
        for fi in res:
            if not selectDiskId:
                fi = fi[0:7]
            procList.append(fi)
        return procList
