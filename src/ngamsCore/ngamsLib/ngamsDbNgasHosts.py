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
# "@(#) $Id: ngamsDbNgasHosts.py,v 1.13 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  07/03/2008  Created
#
"""
Contains queries for accessing the NGAS Hosts Table.

This class is not supposed to be used standalone in the present implementation.
It should be used as part of the ngamsDbBase parent classes.
"""

import collections

from   ngamsCore import TRACE, timeRef2Iso8601, iso8601ToSecs
import ngamsDbCore


class ngamsDbNgasHosts(ngamsDbCore.ngamsDbCore):
    """
    Contains queries for accessing the NGAS Hosts Table.
    """

    def getHostInfoFromHostIds(self,
                               hostList):
        """
        Return a dictionary with the information in connection with each host.
        If for a host ID given, no information is found in the NGAS Hosts
        Table, the value for this wil be None.

        hostList:    List of host IDs (list/string).

        Returns:     List with sub-lists containing the information about the
                     hosts from the NGAS Hosts Table (list).
        """
        T = TRACE()

        sqlQuery = ["SELECT %s FROM ngas_hosts nh WHERE host_id IN (" % ngamsDbCore.getNgasHostsCols()]
        for x in xrange(len(hostList)):
            sqlQuery.append("{}")
            if x < len(hostList) - 1:
                sqlQuery.append(", ")
        sqlQuery.append(")")
        res = self.query2(''.join(sqlQuery), args=[str(h) for h in hostList])

        # TODO: Check that this is the real intention here. Maybe it's OK if we
        # return an empty result
        # This applies to the rest of the methods in this class as well
        if not res:
            raise Exception("No host info found for host list %r" % (hostList,))
        return res


    def getIpFromHostId(self,
                        hostId):
        """
        Get the IP Address of a host from its Host ID.

        hostId:     Host ID (string).

        Returns:    IP Address (string).
        """
        sqlQuery = "SELECT ip_address FROM ngas_hosts WHERE host_id={0}"
        res = self.query2(sqlQuery, args=(hostId,))
        if len(res) == 1:
            return res[0][0]
        else:
            errMsg = "Error retrieving IP Address for host: " + hostId
            raise Exception, errMsg


    def getClusterNameFromHostId(self,
                                 hostId):
        """
        Get the Cluster Name to which a node belongs from its Host ID.

        hostId:     Host ID (string).

        Returns:    Cluster Name (string).
        """
        sqlQuery = "SELECT cluster_name FROM ngas_hosts WHERE host_id={0}"
        res = self.query2(sqlQuery, args=(hostId,))
        if len(res) == 1:
            return res[0][0]
        else:
            errMsg = "Error retrieving Cluster Name for host: " + hostId
            raise Exception, errMsg

    def getSrvSuspended(self,
                        contactAddr,
                        ngasHostId = None):
        """
        Return flag indicating if the server is suspended.

        contactAddr:  Host ID or IP address (string).

        ngasHostId:   NGAS Host ID, e.g. myhost:8888 (string).

        Returns:      Server suspension flag (integer/0|1).
        """
        if (ngasHostId):
            sqlQuery = "SELECT srv_suspended FROM ngas_hosts WHERE host_id={0}"
            args = (ngasHostId,)
        else:
            sqlQuery = "SELECT srv_suspended FROM ngas_hosts WHERE host_id={0} OR ip_address={1}"
            args = (contactAddr, contactAddr)
        res = self.query2(sqlQuery, args)
        if len(res) == 1:
            try:
                return int(res[0][0])
            except:
                return 0
        else:
            errMsg = "Error retrieving Server Suspended Flag for host: " +\
                     str(ngasHostId)
            raise Exception, errMsg


    def getSrvDataChecking(self,
                           hostId):
        """
        Return flag indicating if server is executing a Data Consistency Check.

        hostId:     Host ID (string).

        Returns:    Server suspension flag (integer/0|1).
        """
        sqlQuery = "SELECT srv_data_checking FROM ngas_hosts WHERE host_id={0}"
        res = self.query2(sqlQuery, args=(hostId,))
        if len(res) == 1:
            if res[0][0]:
                return int(res[0][0])
            else:
                return 0
        else:
            # If we get to this point, the entry was not found.
            errMsg = "Error retrieving Data Checking Flag - host: " + hostId
            raise Exception, errMsg


    def writeHostInfo(self,
                      hostInfoObj):
        """
        Create an entry in the NGAS Hosts Table

        hostInfoObj:   ngamsHostInfo object containing the information for the
                       new entry (ngamsHostInfo).

        Returns:       Reference to object itself.
        """
        T = TRACE()

        # Key: column index; value: value to be INSERTed
        # It's ordered so we can iterate over it in the correct order later
        args = collections.OrderedDict()
        if hostInfoObj.getHostId():
            args[ngamsDbCore.NGAS_HOSTS_HOST_ID] = hostInfoObj.getHostId()
        if hostInfoObj.getDomain():
            args[ngamsDbCore.NGAS_HOSTS_DOMAIN] = hostInfoObj.getDomain()
        if hostInfoObj.getIpAddress():
            args[ngamsDbCore.NGAS_HOSTS_ADDRESS] = hostInfoObj.getIpAddress()
        if hostInfoObj.getMacAddress():
            args[ngamsDbCore.NGAS_HOSTS_MAC_ADDRESS] = hostInfoObj.getMacAddress()
        if hostInfoObj.getNSlots() and hostInfoObj.getNSlots() != -1:
            args[ngamsDbCore.NGAS_HOSTS_N_SLOTS] = hostInfoObj.getNSlots()
        if hostInfoObj.getClusterName():
            args[ngamsDbCore.NGAS_HOSTS_CLUSTER_NAME] = hostInfoObj.getClusterName()
        if hostInfoObj.getInstallationDate():
            args[ngamsDbCore.NGAS_HOSTS_INST_DATE] = self.asTimestamp(hostInfoObj.getInstallationDate())

        # Get column names and placeholder values to put into the SQL statement
        table_columns = ngamsDbCore.getNgasHostsMap()
        cols = ", ".join([table_columns[x] for x in args.keys()])
        params = ", ".join("{%d}" % (i) for i in xrange(len(args)))
        sql = "INSERT INTO ngas_hosts (%s) VALUES (%s)" % (cols, params)

        self.query2(sql, args=args.values())


    def updateSrvHostInfo(self,
                          hostId,
                          srvInfo):
        """
        Update the information in the DB, which is managed by the server
        itself. All columns starting with 'srv_' in the ngas_hosts tables
        are defined. The values can be taken from an instance of the
        ngamsHostInfo class.

        srvInfo:    List containing all information about the host. These are
                    all fields starting with 'srv_' from 'srv_version' to
                    'srv_state' (list).

        ignoreErr:  If set to 1, a possible exception thrown will be
                    caught, and this error ignored. Otherwise the
                    method will throw an exception itself (integer/0|1).

        Returns:    Void.
        """
        T = TRACE(5)

        sql = "UPDATE ngas_hosts SET " +\
              "srv_version={0}, srv_port={1}, srv_archive={2}, " +\
              "srv_retrieve={3}, srv_process={4}, srv_remove={5}, " +\
              "srv_data_checking={6}, srv_state={7} WHERE host_id={8}"
        args = list(srvInfo)
        args.append(hostId)
        self.query2(sql, args=args)

        self.triggerEvents()


    def reqWakeUpCall(self,
                      localHostId,
                      wakeUpHostId,
                      wakeUpTime):
        """
        Request a Wake-Up Call via the DB.

        wakeUpHostId:  Name of host where the NG/AMS Server requested for
                       the Wake-Up Call is running (string).

        wakeUpTime:    Absolute time for being woken up (seconds since
                       epoch) (integer).

        Returns:       Reference to object itself.
        """
        T = TRACE()

        wakeUpTimeLoc = self.asTimestamp(wakeUpTime)
        sqlQuery = "UPDATE ngas_hosts SET srv_suspended=1, " +\
                   "srv_req_wake_up_srv={0}, srv_req_wake_up_time={1} " +\
                   "WHERE host_id={2}"
        self.query2(sqlQuery, args=(wakeUpHostId, wakeUpTimeLoc, localHostId))
        self.triggerEvents()
        return self


    def markHostSuspended(self, hostId):
        """
        Mark a host as being suspended in the NGAS DB.

        hostId:    Name of host to mark as suspended. If not given the
                   local host name is used (string).

        Returns:   Reference to object itself.
        """
        T = TRACE()

        sql = "UPDATE ngas_hosts SET srv_suspended=1 WHERE host_id={0}"
        self.query2(sql, args=(hostId,))
        self.triggerEvents()
        return self


    def resetWakeUpCall(self,
                        hostId,
                        resetSrvSusp = 0):
        """
        Cancel/reset the Wake-Up Call parameters.

        hostId:        If specified, another host ID than the one where
                       this NG/AMS Server is running can be indicated (string).

        Returns:       Reference to object itself.
        """
        T = TRACE()

        sql = ["UPDATE ngas_hosts SET srv_req_wake_up_srv=''"]
        if (resetSrvSusp):
            sql.append(", srv_suspended=0")
        sql.append(" WHERE host_id={0}")
        self.query2(''.join(sql), args=(hostId,))

        self.triggerEvents()
        return self


    def getHostIdsFromClusterName(self,
                                  clusterName):
        """
        Return the list of host IDs within the context of a given cluster.

        clusterName:   Name of cluster to consider (string).

        Returns:       List with nodes in the cluster (list/string).
        """
        T = TRACE()

        sql = "SELECT host_id FROM ngas_hosts WHERE cluster_name={0}"
        res = self.query2(sql, args=(clusterName,))
        return [x[0] for x in res]


    def getWakeUpRequests(self, hostId):
        """
        Generates a tuple with suspended NGAS Hosts that have requested
        to be woken up by this host.

        Returns:  Tuple containing sub-tuples with information about hosts
                  to be woken up:

                    (({host id}, {wake-up time (secs since epoch)}), ...)

                                                                (list/tuple)
        """
        T = TRACE(5)

        sql = "SELECT host_id, srv_req_wake_up_time from ngas_hosts WHERE " +\
              "srv_req_wake_up_srv={0} AND srv_suspended=1"
        res = self.query2(sql, args=(hostId,))

        def pack(row):
            suspHost = row[0]
            tmpWakeUpTime = timeRef2Iso8601(row[1])
            wakeUpTime = iso8601ToSecs(tmpWakeUpTime)
            return (suspHost, wakeUpTime)
        return [pack(r) for r in res]


    def getPortNoFromHostId(self,
                            hostId):
        """
        Return the port number corresponding to the host ID.

        hostId:    Host ID (string).

        Return:    Port number (integer).
        """
        sql = "SELECT srv_port from ngas_hosts where host_id={0}"
        res = self.query2(sql, args=(hostId,))
        if len(res) == 1:
            return int(res[0][0])
        else:
            errMsg = "Error retrieving port number for host: " + hostId
            raise Exception, errMsg


    def updateDataCheckStat(self,
                            hostId,
                            start,
                            remain,
                            estimTime,
                            rate,
                            checkMb,
                            checkedMb,
                            checkFiles,
                            checkedFiles):
        """
        Update the statistics for the Data Checking Thread.

        hostId:          ID of NGAS Host to update statistics for (string).

        start:           Start of checking in seconds since epoch (integer).

        remain:          Estimated remaining time in seconds (integer).

        estimTime:       Estimated total time in seconds to complete the check
                         cycle (integer)

        rate:            Rate of checking in MB/s (float).

        checkMb:         Amount of data to check in MB (float).

        checkedMb:       Amount checked in MB (float).

        checkFiles:      Number of files to check (integer).

        checkedFiles:    Number of files checked (integer).

        Returns:         Reference to object itself.
        """
        T = TRACE()

        startDbTime = self.asTimestamp(start)
        endDbTime = self.asTimestamp(start + estimTime)
        sql = "UPDATE ngas_hosts SET " +\
              "srv_check_start={0}, srv_check_remain={1}, srv_check_end={2}, " +\
              "srv_check_rate={3}, srv_check_mb={4}, srv_checked_mb={5}, " +\
              "srv_check_files={6}, srv_check_count={7} WHERE host_id={8}"
        args = (startDbTime, remain, endDbTime,
                rate, checkMb, checkedMb,
                checkFiles, checkedFiles, hostId)
        self.query2(sql, args=args)
        return self


# EOF
