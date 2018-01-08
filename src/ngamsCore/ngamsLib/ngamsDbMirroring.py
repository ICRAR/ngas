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

import ngamsDbCore
import ngamsMirroringRequest


class ngamsDbMirroring(ngamsDbCore.ngamsDbCore):
    """
    Contains queries for accessing the NGAS Mirroring Tables.
    """

    def mirReqInQueue(self,
                      fileId,
                      fileVersion,
                      instanceId):
        """
        Probe if the a Mirroring Request with the given ID is in the
        associated Mirroring Queue.

        fileId:       File ID (string).

        fileVersion:  File Version (integer).

        instanceId:   Identification of the NGAS instance taking care of
                      coordinating the mirroring (string).

        Returns:      Indication if the request is in the queue (boolean).
        """
        sqlQuery = "SELECT file_id FROM ngas_mirroring_queue WHERE file_id={0} " +\
                   "AND file_version={1} AND instance_id={2}"
        res = self.query2(sqlQuery, args=(fileId, fileVersion,instanceId))
        return (len(res) == 1)


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
        sqlQuery = "UPDATE ngas_mirroring_queue SET " +\
                    "srv_list_id={0}, xml_file_info={1}, " +\
                   "status={2}, message={3}, last_activity_time={4}, " +\
                   "scheduling_time={5} WHERE file_id={6} " +\
                   "AND file_version={7}"
        args = (mirReqObj.getSrvListId(),  mirReqObj.getXmlFileInfo(),
                mirReqObj.getStatusAsNo(), mirReqObj.getMessage(),
                self.asTimestamp(mirReqObj.getLastActivityTime()),
                self.asTimestamp(mirReqObj.getSchedulingTime()),
                mirReqObj.getFileId(), mirReqObj.getFileVersion())
        self.query2(sqlQuery, args=args)
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
        sqlQuery = "UPDATE ngas_mirroring_queue SET status={0} WHERE file_id={1} " +\
                   "AND file_version={2}"
        self.query2(sqlQuery, args=(newStatus, fileId, fileVersion))
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
        insertRow = True
        if (check):
            if (self.mirReqInQueue(mirReqObj.getFileId(),
                                   mirReqObj.getFileVersion(),
                                   mirReqObj.getInstanceId())):
                self.updateMirReq(mirReqObj)
                insertRow = False
        if (insertRow):
            sqlQuery =\
                     "INSERT INTO ngas_mirroring_queue " +\
                     "(instance_id, file_id, file_version, ingestion_date, " +\
                     "srv_list_id, xml_file_info, status, message, " +\
                     "last_activity_time, scheduling_time) " +\
                     "VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9})"
            args = (mirReqObj.getInstanceId(),       mirReqObj.getFileId(),
                    mirReqObj.getFileVersion(),      self.asTimestamp(mirReqObj.getIngestionDate()),
                    mirReqObj.getSrvListId(),        mirReqObj.getXmlFileInfo(),
                    mirReqObj.getStatusAsNo(),       str(mirReqObj.getMessage()),
                    self.asTimestamp(mirReqObj.getLastActivityTime()),
                    self.asTimestamp(mirReqObj.getSchedulingTime()))
            self.query2(sqlQuery, args=args)

        return self


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
        res = sqlResult
        mirReqObj = \
                  ngamsMirroringRequest.ngamsMirroringRequest().\
                  setInstanceId(res[ngamsDbCore.NGAS_MIR_Q_INST_ID]).\
                  setFileId(res[ngamsDbCore.NGAS_MIR_Q_FILE_ID]).\
                  setFileVersion(res[ngamsDbCore.NGAS_MIR_Q_FILE_VERSION]).\
                  setIngestionDate(self.fromTimestamp(res[ngamsDbCore.NGAS_MIR_Q_ING_DATE])).\
                  setSrvListId(res[ngamsDbCore.NGAS_MIR_Q_SRV_LIST_ID]).\
                  setXmlFileInfo(res[ngamsDbCore.NGAS_MIR_Q_XML_FILE_INFO]).\
                  setStatus(res[ngamsDbCore.NGAS_MIR_Q_STATUS]).\
                  setMessage(res[ngamsDbCore.NGAS_MIR_Q_MESSAGE]).\
                  setLastActivityTime(self.fromTimestamp(res[ngamsDbCore.\
                                          NGAS_MIR_Q_LAST_ACT_TIME])).\
                  setSchedulingTime(self.fromTimestamp(res[ngamsDbCore.NGAS_MIR_Q_SCHED_TIME]))

        return mirReqObj


    def dumpMirroringQueue(self, instanceId):
        """
        Dump the entire contents of the DB Mirroring Queue into a DBM
        in raw format.

        Returns:   Name of DBM hosting the contents of the DB Mirroring
                   Queue (string).
        """
        sql = "SELECT %s FROM %s mq WHERE instance_id={0}" % \
                (ngamsDbCore.getNgasMirQueueCols(), ngamsDbCore.NGAS_MIR_QUEUE)
        with self.dbCursor(sql, args=(instanceId,)) as cursor:
            for res in cursor.fetch(1000):
                yield self.unpackMirReqSqlResult(res)


# EOF
