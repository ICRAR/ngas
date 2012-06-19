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
# "@(#) $Id: ngamsDbMirroring.py,v 1.12 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  07/03/2008  Created
#

"""
Contains queries for accessing the NGAS Mirroring Tables.

This class is not supposed to be used standalone in the present implementation.
It should be used as part of the ngamsDbBase parent classes.
"""

import time, random

from   ngams import *
import ngamsDbCore
import ngamsMirroringRequest


class ngamsDbMirroring(ngamsDbCore.ngamsDbCore):
    """
    Contains queries for accessing the NGAS Mirroring Tables.
    """

    def mirReqInQueue(self,
                      fileId,
                      fileVersion,
                      instanceId = getHostId()):
        """
        Probe if the a Mirroring Request with the given ID is in the
        associated Mirroring Queue.

        fileId:       File ID (string).

        fileVersion:  File Version (integer).

        instanceId:   Identification of the NGAS instance taking care of
                      coordinating the mirroring (string).

        Returns:      Indication if the request is in the queue (boolean).
        """
        T = TRACE()
        
        sqlQuery = "SELECT file_id FROM %s WHERE file_id='%s' " +\
                   "AND file_version=%d AND instance_id='%s'"
        sqlQuery = sqlQuery %\
                   (ngamsDbCore.NGAS_MIR_QUEUE, fileId, fileVersion,
                    instanceId)
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        return (len(res[0]) == 1)


    def updateMirReq(self,
                     mirReqObj):
        """
        Update the referenced Mirroring Request in the DB. The request is
        defined by a the set of File ID and File Version.

        mirReqObj:   Instance of the Mirroring Request Object containing the
                     information about the Mirroring Request
                     (ngamsMirroringRequest).

        Returns:     Reference to object itself.
        """
        T = TRACE()

        sqlQuery = "UPDATE %s SET srv_list_id=%d, xml_file_info='%s', " +\
                   "status=%d, message='%s', last_activity_time='%s', " +\
                   "scheduling_time='%s' WHERE file_id='%s' " +\
                   "AND file_version=%d"        
        sqlQuery = sqlQuery % (ngamsDbCore.NGAS_MIR_QUEUE,
                               mirReqObj.getSrvListId(),
                               mirReqObj.getXmlFileInfo(),
                               mirReqObj.getStatusAsNo(),
                               mirReqObj.getMessage(),
                               mirReqObj.getLastActivityTime(),
                               mirReqObj.getSchedulingTime(),
                               mirReqObj.getFileId(),
                               mirReqObj.getFileVersion())
        res = self.query(sqlQuery, ignoreEmptyRes = 0)
        return self


    def updateStatusMirReq(self,
                           fileId,
                           fileVersion,
                           newStatus):
        """
        Update the status of the Mirroring Request.

        fileId:       File ID (string).

        fileVersion:  File Version (integer).
        
        newStatus:    New status for the Mirroring Request to write to the DB
                      (ngamsMirroringRequest.NGAMS_MIR_REQ_STAT_SCHED, ...)

        Returns:      Reference to object itself.
        """
        T = TRACE()

        sqlQuery = "UPDATE %s SET status=%d WHERE file_id='%s' " +\
                   "AND file_version=%d"
        sqlQuery = sqlQuery % (ngamsDbCore.NGAS_MIR_QUEUE,
                               newStatus, fileId, fileVersion)
        res = self.query(sqlQuery, ignoreEmptyRes = 0)
        return self


    def writeMirReq(self,
                    mirReqObj,
                    check = True):
        """
        Write the referenced Mirroring Request in the DB. The request is
        defined by a the set of File ID and File Version.

        mirReqObj:   Instance of the Mirroring Request Object containing the
                     information about the Mirroring Request
                     (ngamsMirroringRequest).

        check:       Check if the entry is already in the queue. In case yes,
                     just update it (boolean).

        Returns:     Reference to object itself.
        """
        T = TRACE()

        insertRow = True
        if (check):
            if (self.mirReqInQueue(mirReqObj.getFileId(),
                                   mirReqObj.getFileVersion())):
                self.updateMirReq(mirReqObj)
                insertRow = False
        if (insertRow):
            sqlQuery =\
                     "INSERT INTO %s " +\
                     "(instance_id, file_id, file_version, ingestion_date, " +\
                     "srv_list_id, xml_file_info, status, message, " +\
                     "last_activity_time, scheduling_time) " +\
                     "VALUES ('%s', '%s', %d, '%s', %d, '%s', %d, '%s', " +\
                     "'%s', '%s')"
            sqlQuery = sqlQuery % (ngamsDbCore.NGAS_MIR_QUEUE,
                                   mirReqObj.getInstanceId(),
                                   mirReqObj.getFileId(),
                                   mirReqObj.getFileVersion(),      
                                   mirReqObj.getIngestionDate(),      
                                   mirReqObj.getSrvListId(),      
                                   mirReqObj.getXmlFileInfo(),      
                                   mirReqObj.getStatusAsNo(),      
                                   str(mirReqObj.getMessage()),
                                   mirReqObj.getLastActivityTime(),
                                   mirReqObj.getSchedulingTime())
            res = self.query(sqlQuery, ignoreEmptyRes = 0)
            
        return self


    def getMirReq(self,
                  fileId,
                  fileVersion,
                  instanceId = getHostId()):
        """
        Read the information for a mirroring request and return the raw result.

        fileId:       File ID (string).

        fileVersion:  File Version (integer).

        instanceId:   Identification of the NGAS instance taking care of
                      coordinating the mirroring (string).

        Returns:      Raw result (list|[]).
        """
        T = TRACE()

        sqlQuery = "SELECT %s FROM %s mq WHERE file_id='%s' AND " +\
                   "file_version=%d AND instance_id='%s'"
        sqlQuery = sqlQuery % (ngamsDbCore.getNgasMirQueueCols(),
                               ngamsDbCore.NGAS_MIR_QUEUE, fileId, fileVersion,
                               instanceId)
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (len(res[0]) == 0):
            res = []
        else:
            res = res[0][0] 

        return res


    def unpackMirReqSqlResult(self,
                              sqlResult):
        """
        Unpack a SQL result for one row in the DB Mirroring Table.
        The columns in the result must be ordered according to the sequence
        given by ngamsDbCore.getNgasMirQueueCols().

        sqlResult:   List with elements resulting from the query for one
                     row (list).

        Returns:     Mirroring Request Object (ngamsMirroringRequest).
        """
        T = TRACE()
        
        res = sqlResult
        mirReqObj = \
                  ngamsMirroringRequest.ngamsMirroringRequest().\
                  setInstanceId(res[ngamsDbCore.NGAS_MIR_Q_INST_ID]).\
                  setFileId(res[ngamsDbCore.NGAS_MIR_Q_FILE_ID]).\
                  setFileVersion(res[ngamsDbCore.NGAS_MIR_Q_FILE_VERSION]).\
                  setIngestionDate(res[ngamsDbCore.NGAS_MIR_Q_ING_DATE]).\
                  setSrvListId(res[ngamsDbCore.NGAS_MIR_Q_SRV_LIST_ID]).\
                  setXmlFileInfo(res[ngamsDbCore.NGAS_MIR_Q_XML_FILE_INFO]).\
                  setStatus(res[ngamsDbCore.NGAS_MIR_Q_STATUS]).\
                  setMessage(res[ngamsDbCore.NGAS_MIR_Q_MESSAGE]).\
                  setLastActivityTime(res[ngamsDbCore.\
                                          NGAS_MIR_Q_LAST_ACT_TIME]).\
                  setSchedulingTime(res[ngamsDbCore.NGAS_MIR_Q_SCHED_TIME])

        return mirReqObj


    def getMirReqObj(self,
                     fileId,
                     fileVersion,
                     instanceId = getHostId()):
        """
        Read the information about a mirroring request and return the
        a Mirroring Request Object.

        fileId:       File ID (string).

        fileVersion:  File Version (integer).

        instanceId:   Identification of the NGAS instance taking care of
                      coordinating the mirroring (string).

        Returns:      Mirroring Request Object with information for that
                      request (ngamsMirroringRequest|None).                
        """
        T = TRACE()
        
        res = self.getMirReq(fileId, fileVersion, instanceId)
        if (len(res[0]) == 0): return None            
        mirReqObj = unpackSqlResult(res[0][0])

        return mirReqObj


    def deleteMirReq(self,
                     fileId,
                     fileVersion,
                     instanceId = getHostId()):
        """
        Delete an entry in the NGAS Mirroring Queue.

        fileId:       File ID (string).

        fileVersion:  File Version (integer).

        instanceId:   Identification of the NGAS instance taking care of
                      coordinating the mirroring (string).

        Returns:      Reference to object itself.
        """
        T = TRACE()

        # TODO: The two queries in this method should be carried out in one
        # transaction, so that a roll-back is possible in case of failure.

        # Copy the entry to the NGAS Mirroring Queue History.
        sqlQuery = "SELECT INTO %s SELECT %s FROM " +\
                   "%s WHERE file_id='%s' AND file_version=%d AND " +\
                   "instance_id='%s'"
        sqlQuery = sqlQuery % (ngamsDbCore.NGAS_MIR_QUEUE, 
                               ngamsDbCore.getNgasMirQueueCols2(),
                               ngamsDbCore.NGAS_MIR_HIST, fileId, fileVersion,
                               instanceId)
        self.query(sqlQuery)

        # Delete the old entry.
        sqlQuery = "DELETE FROM %s WHERE file_id='%s' AND file_version=%d " +\
                   "AND instance_id='%s'"
        sqlQuery = sqlQuery % (ngamsDbCore.NGAS_MIR_QUEUE, fileId, fileVersion,
                               instanceId)
        self.query(sqlQuery)
        
        return self


    def dumpMirroringQueue(self,
                           instanceId = getHostId()):
        """
        Dump the entire contents of the DB Mirroring Queue into a DBM
        in raw format.

        Returns:   Name of DBM hosting the contents of the DB Mirroring
                   Queue (string).
        """
        T = TRACE()
        
        sqlQuery = "SELECT %s FROM %s mq WHERE instance_id='%s'"
        sqlQuery = sqlQuery % (ngamsDbCore.getNgasMirQueueCols(),
                               ngamsDbCore.NGAS_MIR_QUEUE, instanceId)
        curObj = self.dbCursor(sqlQuery)

        return curObj


# EOF
