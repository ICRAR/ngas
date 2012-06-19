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

from   ngams import *
import ngamsLib, ngamsDbCore


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

        hostDic = {}
        sqlQuery = "SELECT " + ngamsDbCore.getNgasHostsCols() +\
                   " FROM ngas_hosts nh WHERE host_id IN ("
        for host in hostList:
            sqlQuery += "'" + host + "', "
        sqlQuery = sqlQuery[0:-2] + ")"
        res = self.query(sqlQuery, ignoreEmptyRes = 0)
        return res[0]


    def getIpFromHostId(self,
                        hostId):
        """
        Get the IP Address of a host from its Host ID.

        hostId:     Host ID (string).

        Returns:    IP Address (string).
        """
        sqlQuery = "SELECT ip_address FROM ngas_hosts " +\
                   "WHERE host_id='" + hostId + "'"
        res = self.query(sqlQuery, ignoreEmptyRes = 0)
        if (len(res[0]) == 1):
            return res[0][0][0]
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
        sqlQuery = "SELECT cluster_name FROM ngas_hosts " +\
                   "WHERE host_id='" + hostId + "'"
        res = self.query(sqlQuery, ignoreEmptyRes = 0)
        if (len(res[0]) == 1):
            return res[0][0][0]
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
            sqlQuery = "SELECT srv_suspended FROM ngas_hosts " +\
                       "WHERE host_id='" + ngasHostId + "'"
        else:
            sqlQuery = "SELECT srv_suspended FROM ngas_hosts " +\
                       "WHERE host_id='" + contactAddr + "' " +\
                       "OR ip_address='" + contactAddr + "'"
        res = self.query(sqlQuery, ignoreEmptyRes = 0)
        if (len(res[0]) == 1):
            try:
                return int(res[0][0][0])
            except:
                return 0
        else:
            errMsg = "Error retrieving Server Suspended Flag for host: " +\
                     hostId
            raise Exception, errMsg


    def getSrvDataChecking(self,
                           hostId):
        """
        Return flag indicating if server is executing a Data Consistency Check.

        hostId:     Host ID (string).

        Returns:    Server suspension flag (integer/0|1).
        """
        sqlQuery = "SELECT srv_data_checking FROM ngas_hosts " +\
                   "WHERE host_id='" + hostId + "'"
        res = self.query(sqlQuery, ignoreEmptyRes = 0)
        if (len(res[0]) == 1):
            if (res[0][0][0]):
                return int(res[0][0][0])
            else:
                return 0
        else:
            # If we get to this point, the entry was not found.
            errMsg = "Error retrieving Data Checking Flag - host: " + hostId
            raise Exception, errMsg


    def getHostContactInfo(self,
                           hostId):
        """
        Get the information needed to be able to contact a host. If the
        host is located within a cluster the Host ID/IP + Port Number of the
        Cluster Main Node is returned. In addition is returned whether or not
        the contact host is suspended.

        hostId:   ID of host to analyze (string).
        
        Returns:  Returns a tuple with the information:

                  (<Contact Host ID>, <Contact Host IP>, <Contact Port>,
                   <Suspended>)  (tuple).
        """
        T = TRACE()

        sqlQuery = "SELECT host_id, ip_address, srv_port, srv_suspended " +\
                   "FROM ngas_hosts WHERE host_id=(SELECT cluster_name " +\
                   "FROM ngas_hosts WHERE host_id='" + hostId + "')"
        res = self.query(sqlQuery, ignoreEmptyRes = 0)
        if (len(res[0]) == 1):
            # If the contact host corresponds to the local host, the contact
            # information for the host specified in the method invocation
            # must be queried and returned (since it can contact the
            # specified host directly).
            if (res[0][0][0] == getHostId()):
                sqlQuery = "SELECT host_id, ip_address, srv_port, " +\
                           "srv_suspended FROM ngas_hosts " +\
                           "WHERE host_id='" + hostId + "'"
                res = self.query(sqlQuery, ignoreEmptyRes = 0)
                if (len(res[0]) != 1):
                    errMsg = "Error retrieving info for host: " + hostId
                    raise Exception, errMsg 
            return (res[0][0][0], res[0][0][1], int(res[0][0][2]),res[0][0][3])
        else:
            errMsg = "Error retrieving info for Cluster Main Node " +\
                     "in connection with host: " + hostId
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
        
        sqlQuery = "INSERT INTO ngas_hosts (%s) VALUES (%s)"
        columns = ""
        values = ""
        if (hostInfoObj.getHostId()):
            columns += "%s, " % ngamsDbCore.\
                       getNgasHostsMap()[ngamsDbCore.NGAS_HOSTS_HOST_ID]
            values += "'%s', " % hostInfoObj.getHostId()
        if (hostInfoObj.getDomain()):
            columns += "%s, " % ngamsDbCore.\
                       getNgasHostsMap()[ngamsDbCore.NGAS_HOSTS_DOMAIN]
            values += "'%s', " % hostInfoObj.getDomain()
        if (hostInfoObj.getIpAddress()):
            columns += "%s, " % ngamsDbCore.\
                       getNgasHostsMap()[ngamsDbCore.NGAS_HOSTS_ADDRESS]
            values += "'%s', " % hostInfoObj.getIpAddress()
        if (hostInfoObj.getMacAddress()):
            columns += "%s, " % ngamsDbCore.\
                       getNgasHostsMap()[ngamsDbCore.NGAS_HOSTS_MAC_ADDRESS]
            values += "'%s', " % hostInfoObj.getMacAddress()
        if (hostInfoObj.getNSlots()):
            if (hostInfoObj.getNSlots() == -1):
                noOfSlots = 0
            else:
                noOfSlots = hostInfoObj.getNSlots()
                columns += "%s, " % ngamsDbCore.\
                           getNgasHostsMap()[ngamsDbCore.NGAS_HOSTS_N_SLOTS]
                values += "%d, " % noOfSlots
        if (hostInfoObj.getClusterName()):
            columns += "%s, " % ngamsDbCore.\
                       getNgasHostsMap()[ngamsDbCore.NGAS_HOSTS_CLUSTER_NAME]
            values += "'%s', " % hostInfoObj.getClusterName()
        if (hostInfoObj.getInstallationDate()):
            columns += "%s, " % ngamsDbCore.\
                       getNgasHostsMap()[ngamsDbCore.NGAS_HOSTS_INST_DATE]
            values += "'%s', " % self.\
                      convertTimeStamp(hostInfoObj.getInstallationDate())
        sqlQuery = sqlQuery % (columns[:-2], values[:-2])
        self.query(sqlQuery)


    def updateSrvHostInfo(self,
                          hostId,
                          srvInfo,
                          ignoreErr = 0):
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

        try:
            sqlQuery = "UPDATE ngas_hosts SET " +\
                       "srv_version='" + srvInfo[0] + "', " +\
                       "srv_port=" + str(srvInfo[1]) + ", " +\
                       "srv_archive=" + str(srvInfo[2]) + ", " +\
                       "srv_retrieve=" + str(srvInfo[3])+ ", " +\
                       "srv_process=" + str(srvInfo[4]) + ", " +\
                       "srv_remove=" + str(srvInfo[5]) + ", " +\
                       "srv_data_checking=" + str(srvInfo[6]) + ", " +\
                       "srv_state='" + srvInfo[7] + "' " +\
                       "WHERE host_id='" + hostId + "'"
            if (not ignoreErr):
                self.query(sqlQuery)
            else:
                try:
                    self.query(sqlQuery)
                except:
                    pass
            self.triggerEvents()
        except Exception, e:   
            raise e
            

    def reqWakeUpCall(self,
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

        try:
            try:
                self.takeDbSem()
                wakeUpTimeLoc = self.convertTimeStamp(wakeUpTime)
                self.relDbSem()
            except Exception, e:
                self.relDbSem()
                raise Exception, e
            sqlQuery = "UPDATE ngas_hosts SET " +\
                       "srv_suspended=1, " +\
                       "srv_req_wake_up_srv='" + wakeUpHostId + "', " +\
                       "srv_req_wake_up_time='" + wakeUpTimeLoc + "' " +\
                       "WHERE host_id='" + getHostId() + "'"
            self.query(sqlQuery)
            self.triggerEvents()
            return self
        except Exception, e:   
            raise e


    def markHostSuspended(self,
                          hostId = None):
        """
        Mark a host as being suspended in the NGAS DB.

        hostId:    Name of host to mark as suspended. If not given the
                   local host name is used (string).

        Returns:   Reference to object itself.
        """
        T = TRACE()

        try:
            if (hostId == None): hostId = getHostId()
            sqlQuery = "UPDATE ngas_hosts SET " + "srv_suspended=1 " +\
                       "WHERE host_id='" + hostId + "'"
            self.query(sqlQuery)
            self.triggerEvents()
            return self
        except Exception, e:   
            raise e


    def resetWakeUpCall(self,
                        hostId = None,
                        resetSrvSusp = 0):
        """
        Cancel/reset the Wake-Up Call parameters.

        hostId:        If specified, another host ID than the one where
                       this NG/AMS Server is running can be indicated (string).

        Returns:       Reference to object itself.
        """
        T = TRACE()

        try:
            if (hostId == None): hostId = getHostId()
            sqlQuery = "UPDATE ngas_hosts SET srv_req_wake_up_srv=''"
            if (resetSrvSusp): sqlQuery += ", srv_suspended=0"
            sqlQuery += " WHERE host_id='" + hostId + "'"
            self.query(sqlQuery)
            self.triggerEvents()
            return self
        except Exception, e:   
            raise e


    def getHostIdsFromClusterName(self,
                                  clusterName):
        """
        Return the list of host IDs within the context of a given cluster.

        clusterName:   Name of cluster to consider (string).

        Returns:       List with nodes in the cluster (list/string).
        """
        T = TRACE()

        sqlQuery = "SELECT host_id FROM ngas_hosts WHERE cluster_name='%s'" %\
                   clusterName
        res = self.query(sqlQuery)
        if (res == [[]]):
            return []
        else:
            hostIds = []
            for hostId in res[0]:
                hostIds.append(hostId[0])
            return hostIds


    def getWakeUpRequests(self):
        """
        Generates a tuple with suspended NGAS Hosts that have requested
        to be woken up by this host.

        Returns:  Tuple containing sub-tuples with information about hosts
                  to be woken up:

                    (({host id}, {wake-up time (secs since epoch)}), ...)
                    
                                                                (list/tuple)
        """
        T = TRACE(5)
        
        sqlQuery = "SELECT host_id, srv_req_wake_up_time from ngas_hosts " +\
                   "WHERE srv_req_wake_up_srv='" + getHostId() + "' " +\
                   "AND srv_suspended=1"
        res = self.query(sqlQuery, ignoreEmptyRes = 0)
        if (len(res[0]) >= 1):
            wakeUpReqs = []
            for req in res[0]:
                suspHost = req[0]
                tmpWakeUpTime = timeRef2Iso8601(req[1])
                wakeUpTime = iso8601ToSecs(tmpWakeUpTime)
                wakeUpReqs.append((suspHost, wakeUpTime))
            return wakeUpReqs
        else:
            return []


    def getPortNoFromHostId(self,
                            hostId):
        """
        Return the port number corresponding to the host ID.

        hostId:    Host ID (string).

        Return:    Port number (integer).
        """
        sqlQuery = "SELECT srv_port from ngas_hosts where host_id='" +\
                   hostId + "'"
        res = self.query(sqlQuery, ignoreEmptyRes = 0)
        if (len(res[0]) == 1):
            return int(res[0][0][0])
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
        
        try:
            self.takeDbSem()
            startDbTime = self.convertTimeStamp(start)
            endDbTime = self.convertTimeStamp(start + estimTime)
            self.relDbSem()
        except Exception, e:
            self.relDbSem()
            raise Exception, e
        sqlQuery = "UPDATE ngas_hosts SET " +\
                   "srv_check_start='" + startDbTime + "', " +\
                   "srv_check_remain=" + str(remain) + ", " +\
                   "srv_check_end='" + endDbTime + "', " +\
                   "srv_check_rate=" + str(rate) + ", " +\
                   "srv_check_mb=" + str(checkMb) + ", " +\
                   "srv_checked_mb=" + str(checkedMb) + ", " +\
                   "srv_check_files=" + str(checkFiles) + ", " +\
                   "srv_check_count=" + str(checkedFiles) + " " +\
                   "WHERE host_id='" + hostId + "'"
        self.query(sqlQuery)
        return self


# EOF
