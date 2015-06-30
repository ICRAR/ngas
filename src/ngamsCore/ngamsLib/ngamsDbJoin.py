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
# "@(#) $Id: ngamsDbJoin.py,v 1.16 2009/12/02 23:17:56 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  07/03/2008  Created
#

"""
Contains queries for accessing the NGAS DB, which involves joining tables.

This class is not supposed to be used standalone in the present implementation.
It should be used as part of the ngamsDbBase parent classes.
"""

import time
from ngamsCore import TRACE, warning, error, notice, getHostId, rmFile, getTestMode
from ngamsCore import NGAMS_FILE_STATUS_OK, NGAMS_FILE_CHK_ACTIVE,NGAMS_DB_CH_FILE_UPDATE, NGAMS_DB_CH_FILE_INSERT
import ngamsDbm, ngamsDbCore
import ngamsLib

# TODO: Avoid using these classes in this module (mutual dependency):
import ngamsFileInfo


class ngamsDbJoin(ngamsDbCore.ngamsDbCore):
    """
    Contains queries for accessing the NGAS DB, which involves joining tables.
    """

    def getFileSummary1(self,
                        hostId = None,
                        diskIds = [],
                        fileIds = [],
                        ignore = None,
                        fileStatus = [NGAMS_FILE_STATUS_OK],
                        lowLimIngestDate = None,
                        order = 1):

        """
        Return summary information about files. The information is returned
        in a list containing again sub-lists with contents as defined
        by ngamsDbCore.getNgasSummary1Cols() (see general documentation of
        the ngamsDbBase Class).

        hostId:            Name of NGAS host on which the files reside
                           (string).

        diskIds:           Used to limit the query to certain disks
                           (list/string).

        fileIds:           List of file IDs for which to query information.
                           If not specified, all files of the referenced
                           host will be chosen (list/string|[]).

        ignore:            If set to 0 or 1, this value of ignore will be
                           queried for. If set to None, ignore is not
                           considered (None|0|1).

        fileStatus:        With this parameter it is possible to indicate which
                           files to consider according to their File Status
                           (list).

        lowLimIngestDate:  Lower limit in time for which files are taken into
                           account. Only files with an Ingestion Date after
                           this date, are taken into account (string/ISO 8601).

        order:             Used to trigger ordering by Slot ID + Ingestion Date
                           (integer/0|1).

        Returns:           Cursor object (<NG/AMS DB Cursor Object API>).
        """
        T = TRACE(5)

        sqlQuery = self.\
                   buildFileSummary1Query(hostId, diskIds, fileIds,
                                          ignore, fileStatus,
                                          lowLimIngestDate,
                                          order) %\
                                          ngamsDbCore.getNgasSummary1Cols()
        # Create a cursor and perform the query.
        curObj = self.dbCursor(sqlQuery)
        return curObj


    def getFileSummary1SingleFile(self,
                                  diskId,
                                  fileId,
                                  fileVersion):
        """
        Same as getFileSummary1() but for a single (specific) file.

        Returns:   List with information from query (list).
        """
        T = TRACE()

        sqlQuery = "SELECT %s FROM ngas_disks nd, ngas_files nf " +\
                   "WHERE nd.disk_id=nf.disk_id AND " +\
                   "nd.disk_id='%s' AND nf.file_id='%s' AND " +\
                   "nf.file_version=%d"
        sqlQuery = sqlQuery % (ngamsDbCore.getNgasSummary1Cols(), diskId,
                               fileId, fileVersion)
        res = self.query(sqlQuery)
        if ((res == [[]]) or (res == [])):
            return []
        else:
            return res[0][0]


    def getFileSummary2(self,
                        hostId = None,
                        fileIds = [],
                        diskId = None,
                        ignore = None,
                        ing_date = None,
                        max_num_records = None,
                        upto_ing_date = None):
        """
        Return summary information about files. An NG/AMS DB Cursor Object
        is created, which can be used to query the information sequentially.

        The information is returned in a list containing again sub-lists
        with contents as defined by ngamsDbBase._sum2Cols (see general
        documentation of the ngamsDbBase Class.

        This method returns all the files stored on an NGAS system also
        the ones with a File Status indicating that it is bad.

        hostId:            Name of NGAS host on which the files reside. If
                           None is specified, the host is not taken into
                           account (string or a list of string).

        fileIds:           List of file IDs for which to query information.
                           If not specified, all files of the referenced
                           host will be chosen (list/string|[]).

        diskId:            Used to refer to all files on a given disk
                           (string|None).

        ignore:            If set to 0 or 1, this value of ignore will be
                           queried for. If set to None, ignore is not
                           considered (None|0|1).

        max_num_records:   The maximum number of returned records (if presented) (int)

        Returns:           Cursor object (<NG/AMS DB Cursor Object API>).
        """
        T = TRACE()

        sqlQuery = "SELECT " + ngamsDbCore.getNgasSummary2Cols() + " " +\
                   "FROM ngas_disks nd, ngas_files nf " +\
                   "WHERE nd.disk_id=nf.disk_id "
        if (ignore != None): sqlQuery += "AND nf.file_ignore=%d" % int(ignore)
        if (hostId):
            if (type(hostId) is list):
                sqlQuery += " AND ("
                cc = 0
                for ho in hostId:
                    if (cc > 0):
                        sqlQuery += " OR "
                    sqlQuery += "nd.host_id='" + ho + "'"
                    cc += 1
                sqlQuery += ") "
            else: #assume it is string
                sqlQuery += " AND nd.host_id='" + hostId + "'"
        if (diskId): sqlQuery += " AND nf.disk_id='" + diskId + "'"
        if (fileIds != []):
            sqlQuery += " AND nf.file_id IN (" + str(fileIds)[1:-1] + ")"
        if (ing_date): sqlQuery += " AND nf.ingestion_date > '" + ing_date + "'"
        if (upto_ing_date): sqlQuery += " AND nf.ingestion_date < '" + upto_ing_date + "'"
        sqlQuery += " ORDER BY nf.ingestion_date"
        if (max_num_records): sqlQuery += " LIMIT %d" % max_num_records

        # Create a cursor and perform the query.
        curObj = self.dbCursor(sqlQuery)

        return curObj


    def getFileSummary3(self,
                        fileId,
                        hostId = None,
                        domain = None,
                        diskId = None,
                        fileVersion = -1,
                        cursor = True):
        """
        Return information about files matching the conditions which are not
        in ignore and which are not marked as bad.

        Files are ordered by the File Version (descending).

        The resulting file information will be:

          <Host ID>, <Ip Address>, <Port>, <Mountpoint>, <Filename>,
          <File Version>, <format>


        fileId:            ID of file to retrieve (string).

        hostId:            Host ID of node hosting file (string|None).

        domain:            Domain in which the node is residing (string|None).

        diskId:            Disk ID of disk hosting file (string|None).

        fileVersion:       Version of file to retrieve (integer).

        cursor:            Return DB cursor rather than the results (boolean).

        Returns:           Cursor object (<NG/AMS DB Cursor Object API>).
        """
        T = TRACE(5)

        sqlQuery = "SELECT nh.host_id, nh.ip_address, nh.srv_port, " +\
                   "nd.mount_point, nf.file_name, nf.file_version, " +\
                   "nf.format " +\
                   "FROM ngas_files nf, ngas_disks nd, ngas_hosts nh " +\
                   "WHERE nf.file_id='%s' AND nf.disk_id=nd.disk_id AND " +\
                   "nd.host_id=nh.host_id AND nf.file_ignore=0 AND " +\
                   "nf.file_status='00000000'"
        sqlQuery = sqlQuery % fileId
        if (hostId): sqlQuery += " AND nh.host_id='%s'" % hostId
        if (domain): sqlQuery += " AND nh.domain='%s'" % domain
        if (diskId): sqlQuery += " AND nd.disk_id='%s'" % diskId
        if (fileVersion > 0):
            sqlQuery += " AND nf.file_version=%d" % fileVersion
        sqlQuery += " ORDER BY nf.file_version DESC"

        if (cursor):
            # Create a cursor and perform the query.
            retVal = self.dbCursor(sqlQuery)
        else:
            retVal = self.query(sqlQuery)
        return retVal


    def getFileSummarySpuriousFiles1(self,
                                     hostId = None,
                                     diskId = None,
                                     fileId = None,
                                     fileVersion = None):
        """
        Return summary information about spurious files, i.e. files registered
        in the DB as to be ignored and/or having a status indicating that
        they're not OK. The information is returned in a list containing
        again sub-lists with contents as defined by
        ngamsDbBase.getNgasSummary1Cols() (see general documentation of the
        ngamsDbBase Class.

        hostId:            Name of NGAS host on which the files reside
                           (string).

        diskId:            Disk ID of disk to take into account (string|None).

        fileId:            File ID of file(s) to take into account
                           (string|None).

        fileVersion:       Version of file(s) to take into account
                           (integer|None).

        Returns:           Cursor object (<NG/AMS DB Cursor Object API>).
        """
        T = TRACE(5)

        fileStatusList = [NGAMS_FILE_STATUS_OK, NGAMS_FILE_CHK_ACTIVE]
        sqlQuery = "SELECT " + ngamsDbCore.getNgasSummary1Cols() + " " +\
                   "FROM ngas_disks nd, ngas_files nf " +\
                   "WHERE nd.disk_id=nf.disk_id " +\
                   "AND (nf.file_ignore=1 OR " +\
                   "nf.file_status NOT IN (" + str(fileStatusList)[1:-1]+ "))"

        if (hostId): sqlQuery += " AND nd.host_id='" + hostId + "'"
        if (diskId): sqlQuery += " AND nd.disk_id='" + diskId + "'"
        if (fileId): sqlQuery += " AND nf.file_id='" + fileId + "'"
        if (fileVersion):
            sqlQuery += " AND nf.file_version=" + str(fileVersion)

        # Create a cursor and perform the query.
        curObj = self.dbCursor(sqlQuery)

        return curObj


    def setFileChecksum(self,
                        fileId,
                        fileVersion,
                        diskId,
                        checksum,
                        checksumPlugIn):
        """
        Set the checksum value in the ngas_files table.

        fileId:          ID of file (string).

        fileVersion:     Version of file (integer).

        diskId:          ID of disk where file is stored (string).

        checksum:        Checksum of file (string).

        checksumPlugIn:  Name of plug-in used to generate the
                         checksum (string).

        Returns:         Reference to object itself.
        """
        T = TRACE()

        try:
            sqlQuery = "UPDATE ngas_files SET checksum='" + checksum + "', " +\
                       "checksum_plugin='" + checksumPlugIn + "' " +\
                       "WHERE file_id='" + fileId + "' AND file_version=" +\
                       str(fileVersion) + " AND disk_id='" + diskId + "'"
            res = self.query(sqlQuery)

            # Create a File Removal Status Document.
            if (self.getCreateSnapshot()):
                dbFileInfo = self.getFileInfoFromFileIdHostId(getHostId(),
                                                              fileId,
                                                              fileVersion,
                                                              diskId)
                tmpFileObj = ngamsFileInfo.ngamsFileInfo().\
                             unpackSqlResult(dbFileInfo)
                self.createDbFileChangeStatusDoc(NGAMS_DB_CH_FILE_UPDATE,
                                                 [tmpFileObj])

            self.triggerEvents()
            return self
        except Exception, e:
            raise e


    def getSummary1NoOfFiles(self,
                             hostId = None,
                             diskIds = [],
                             fileIds = [],
                             ignore = None,
                             fileStatus = [NGAMS_FILE_STATUS_OK],
                             lowLimIngestDate = None):
        """
        Get the theoretical number of files that a Summary 1 Query would
        return with the given parameters.

        For a description of the input parameters, check the man-page of
        ngamsDbBase.getFileSummary1().

        Returns:   Number of files query would return (integer).
        """
        T = TRACE()

        sqlQuery = self.buildFileSummary1Query(hostId, diskIds, fileIds,
                                               ignore, fileStatus,
                                               lowLimIngestDate,
                                               order=0) % "count(*)"
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (len(res[0]) == 1):
            if (res[0][0][0]):
                return res[0][0][0]
            else:
                return 0
        else:
            return 0


    def buildFileSummary1Query(self,
                               hostId = None,
                               diskIds = [],
                               fileIds = [],
                               ignore = None,
                               fileStatus = [NGAMS_FILE_STATUS_OK],
                               lowLimIngestDate = None,
                               order = 1):
        """
        Builds the SQL query for a File Summary1 query. The fields to be
        selected are left open (specified as %s).

        For a description of the input parameters, check the man-page of
        ngamsDbBase.getFileSummary1().

        Returns:    SQL query for a File Summary 1 Query (string).
        """
        T = TRACE(5)

        sqlQuery = "SELECT %s FROM ngas_disks nd, " +\
                   "ngas_files nf " +\
                   "WHERE nd.disk_id=nf.disk_id"

        # Additional WHERE clauses.
        if (ignore != None): sqlQuery += " AND nf.file_ignore=%d" % int(ignore)
        if (hostId): sqlQuery += " AND nd.host_id='" + hostId + "'"
        if (diskIds != []):
            sqlQuery += " AND nd.disk_id IN (" + str(diskIds)[1:-1] + ")"
        if (fileIds != []):
            sqlQuery += " AND nf.file_id IN (" + str(fileIds)[1:-1] + ")"
        if (fileStatus != []):
            sqlQuery += " AND nf.file_status IN (" + str(fileStatus)[1:-1]+ ")"
        if (lowLimIngestDate):
            try:
                self.takeDbSem()
                lowLimDate = self.convertTimeStamp(lowLimIngestDate)
                self.relDbSem()
            except Exception, e:
                self.relDbSem()
                raise Exception, e
            sqlQuery += " AND nf.ingestion_date >= '" + lowLimDate + "'"
        if (order): sqlQuery += " ORDER BY nd.slot_id, nf.ingestion_date"

        return sqlQuery


    def getFileInfoFromFileIdHostId(self,
                                    hostId,
                                    fileId,
                                    fileVersion = 1,
                                    diskId = None,
                                    ignore = None):
        """
        Return list with information about a certain file referenced
        by its File ID. A list is returned with the following elements:

          [<Disk ID>, <Filename>, <File ID>, <File Version>, <Format>,
           <File Size>, <Uncompressed File Size>, <Compression>,
           <Ingestion Date>, <Ignore>, <Checksum>, <Checksum Plug-In>,
           <File Status>, <Creation Date>]

        hostId:           Name of host where the disk is mounted on
                          which the file is stored (string).

        fileId:           ID for file to acquire information for (string).

        fileVersion:      Version of the file to query information
                          for (integer).

        diskId:           Used to refer to a specific disk (string).

        ignore:           If set to 0 or 1, this value of ignore will be
                          queried for. If set to None, ignore is not
                          considered (None|0|1).

        Returns           List with information about file, or [] if
                          no file(s) was found (list).
        """
        T = TRACE()

        sqlQuery = "SELECT " + ngamsDbCore.getNgasFilesCols() + " " +\
                   "FROM ngas_files nf, ngas_disks nd WHERE " +\
                   "nf.file_id='" + fileId + "' AND " +\
                   "nf.disk_id=nd.disk_id AND " +\
                   "nd.host_id='" + hostId + "' AND nd.mounted=1 AND " +\
                   "nf.file_version=" + str(fileVersion)
        if (diskId): sqlQuery += " AND nd.disk_id='%s'" % diskId
        if (ignore != None): sqlQuery += " AND nf.file_ignore=%d" % int(ignore)
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (len(res[0]) == 0):
            return []
        else:
            return res[0][0]


    def getFileInfoFromFileId(self,
                              fileId,
                              fileVersion = -1,
                              diskId = None,
                              ignore = None,
                              dbCursor = 1,
                              order = 1):
        """
        The method queries the file information for the files with the given
        File ID and returns the information found in a list containing
        sub-lists each with a list with the information for the file from the
        ngas_files table, host ID and mount point. The following rules are
        applied when determining which files to return:

          o All files are considered, also files which are Offline.

 	  o Files marked to be ignored are ignored.

          o Latest version - first priority.

        It is possible to indicate if files marked as being 'bad' in the
        DB should be taken into account with the 'ignoreBadFiles' flag.

        If a specific File Version is specified only that will be
        taken into account.

        The data can be retrieved via the DB Cursor returned by this object.
        The format of each sub-result is:

          [<see getFileInfoFromFileIdHostId()>, <host ID>, <mnt pt>]


        fileId:          File ID for file to be retrieved (string).

        fileVersion:     If a File Version is specified only information
                         for files with that version number and File ID
                         are taken into account. The version must be a
                         number in the range [1; oo[ (integer).

        diskId:          ID of disk where file is residing. If specified
                         to None (or empty string) the Disk ID is not taken
                         into account (string).

        ignore:          If set to 0 or 1, this value of ignore will be
                         queried for. If set to None, ignore is not
                         considered (None|0|1).

        dbCursor:        If set to 1, a DB cursor is returned from which
                         the files can be retrieved. Otherwise the result
                         is queried and returned in a list (0|1/integer).

        order:           If set to 0, the list of matching file information
                         will not be order according to the file version
                         (integer/0|1).

        Returns:         Cursor object or list with results
                         (<NG/AMS DB Cursor Object API>|list).
        """
        T = TRACE()

        try:
            int(fileVersion)
        except:
            raise Exception, "Illegal value for File Version specified: " +\
                  str(fileVersion)

        # Query for files being online.
        sqlQuery = "SELECT " + ngamsDbCore.getNgasFilesCols() +\
                   ", nd.host_id, " +\
                   "nd.mount_point FROM ngas_files nf, ngas_disks nd, "+\
                   "ngas_hosts nh WHERE nh.host_id=nd.host_id AND " +\
                   "nf.disk_id=nd.disk_id"
        if (ignore != None): sqlQuery != " AND nf.file_ignore=%d" % int(ignore)
        # File ID specified.
        if (fileId): sqlQuery += " AND nf.file_id='%s'" % fileId
        # Do we want a specific File Version?
        if (fileVersion != -1):
            sqlQuery += " AND nf.file_version=" + str(fileVersion)
        # Is a special disk referred?
        if (diskId): sqlQuery += " AND nf.disk_id='%s'" % diskId

        # Order the files according to the version.
        if (order): sqlQuery += " ORDER BY nf.file_version desc, " +\
                                "nd.disk_id desc"

        if (dbCursor):
            # Carry out query and return the DB cursor object.
            curObj = self.dbCursor(sqlQuery)
            return curObj
        else:
            # Execute the query directly and return the result.
            res = self.query(sqlQuery)
            if (len(res) > 0):
                return res[0]
            else:
                return []


    def _dumpFileInfo(self,
                      fileId,
                      fileVersion,
                      diskId,
                      ignore,
                      fileInfoDbmName,
                      expMatches,
                      order):
        """
        See ngamsDbJoin.dumpFileInfo().
        """
        T = TRACE()

        if (not fileInfoDbmName):
            fileInfoDbmName = "/tmp/" +\
                              ngamsLib.genUniqueFilename("FILE_INFO_DB")
        rmFile(fileInfoDbmName + "*")
        fileInfoDbm = None
        dbCount = 0
        dbCur = None
        try:
            fileInfoDbm = ngamsDbm.ngamsDbm(fileInfoDbmName, cleanUpOnDestr=0,
                                            writePerm = 1)

            # If there are more than a 100 expected matches, a cursor is used
            # otherwise the result is queried directly.
            if (expMatches > 100):
                dbCur = self.getFileInfoFromFileId(fileId, fileVersion, diskId,
                                                   ignore, 1, order)
                while (1):
                    tmpFileInfoList = dbCur.fetch(1000)
                    if (tmpFileInfoList == []): break
                    for tmpFileInfo in tmpFileInfoList:
                        tmpFileInfoObj =\
                                       ngamsFileInfo.ngamsFileInfo().\
                                       unpackSqlResult(tmpFileInfo)
                        newEl = [tmpFileInfoObj, tmpFileInfo[-2],
                                 tmpFileInfo[-1]]
                        fileInfoDbm.add(str(dbCount), newEl)
                        dbCount += 1
                    fileInfoDbm.sync()
                    time.sleep(0.050)
                del dbCur
            else:
                res = self.getFileInfoFromFileId(fileId, fileVersion, diskId,
                                                 ignore, 0, order)
                for tmpFileInfo in res:
                    tmpFileInfoObj = ngamsFileInfo.ngamsFileInfo().\
                                     unpackSqlResult(tmpFileInfo)
                    newEl = [tmpFileInfoObj, tmpFileInfo[-2],
                             tmpFileInfo[-1]]
                    fileInfoDbm.add(str(dbCount), newEl)
                    dbCount += 1
                fileInfoDbm.sync()
            del fileInfoDbm
            return fileInfoDbmName
        except Exception, e:
            error("Exception: " + str(e))
            del fileInfoDbm
            del dbCur
            rmFile(fileInfoDbmName + "*")
            raise e


    def dumpFileInfo(self,
                     fileId,
                     fileVersion = -1,
                     diskId = "",
                     ignore = None,
                     fileInfoDbmName = "",
                     order = 1):
        """
        The method queries the file information for the files with the given
        File ID and returns the information found in a list containing
        sub-lists each with a ngamsFileInfo object, host ID and mount point.
        The following rules are applied when determining which files to return:

            o Files marked to be ignored are ignored.
            o Only files that are marked as being hosted on hosts,
              which are Online or on suspended disks/hosts, are considered.
            o Latest version - first priority.

        It is possible to indicate if files marked as being 'bad' in the
        DB should be taken into account with the 'ignoreBadFiles' flag.

        The file information is referred to by a string key in the interval
        [0; oo[ (integer in string format).

        If a specific File Version is specified only that will be taken into
        account.

        fileId:          File ID for file to be retrieved (string).

        fileVersion:     If a File Version is specified only information
                         for files with that version number and File ID
                         are taken into account. The version must be a
                         number in the range [1; oo[ (integer).

        diskId:          ID of disk where file is residing. If specified
                         to '' (empty string) the Disk ID is not taken
                         into account (string).

        ignore:          If set to 0 or 1, this value of ignore will be
                         queried for. If set to None, ignore is not
                         considered (None|0|1).

        fileInfoDbmName: If given, this will be used as name for the file info
                         DB (string).

        order:           If set to 0, the list of matching file information
                         will not be order according to the file version
                         (integer/0|1).

        Returns:         Name of a BSD DB file in which the File Info Objects
                         are stored. This DB contains pickled objects pointed
                         to by an index number ([0; oo[). The contents of each
                         of these pickled objects is:

                           [<File Info Obj>, <Host ID>, <Mount Point>]

                         An element NGAMS_FILE_DB_COUNTER indicates the
                         number of files stored in the DB (string/filename).
        """
        T = TRACE()

        # Try first to get the expected number of files, which will be returned
        expNoOfFiles = -1
        if (self.getDbVerify()):
            for n in range(1):
                noOfFiles = self.getNumberOfFiles(diskId, fileId, fileVersion,
                                                  ignore, onlyOnlineFiles=1)
                if (noOfFiles > expNoOfFiles): expNoOfFiles = noOfFiles
                time.sleep(0.050)

        # Try up to 5 times to dump the files.
        for n in range(5):
            fileInfoDbmName = self._dumpFileInfo(fileId, fileVersion, diskId,
                                                 ignore, fileInfoDbmName,
                                                 expNoOfFiles, order)
            # Check the number of files for which info was dumped.
            tmpFileInfoDbm = ngamsDbm.ngamsDbm(fileInfoDbmName)
            dumpedNoOfFiles = tmpFileInfoDbm.getCount()
            del tmpFileInfoDbm

            # Print out DB Verification Warning if actual number of files
            # differs from expected number of files.
            if (self.getDbVerify() and (dumpedNoOfFiles != expNoOfFiles)):
                errMsg = "Problem dumping file info! Expected number of "+\
                         "files: %d, actual number of files: %d"
                errMsg = errMsg % (expNoOfFiles, dumpedNoOfFiles)
                warning(errMsg)

            # Try to Auto Recover if requested.
            if ((self.getDbVerify() and self.getDbAutoRecover()) and
                (dumpedNoOfFiles != expNoOfFiles)):
                rmFile(fileInfoDbmName + "*")
                if (n < 4):
                    # We try again, after a small pause.
                    if (getTestMode()):
                        time.sleep(0.5)
                    else:
                        time.sleep(5)
                    notice("Retrying to dump file info ...")
                else:
                    errMsg = "Giving up to auto recover dumping of file info!"
                    error(errMsg)
                    raise Exception, errMsg
            else:
                # All files were dumped, we stop.
                break

        return fileInfoDbmName


    def dumpFileInfo2(self,
                      fileInfoDbmName = None,
                      hostId = None,
                      diskIds = [],
                      fileIds = [],
                      ignore = None,
                      fileStatus = [NGAMS_FILE_STATUS_OK],
                      lowLimIngestDate = None,
                      order = 1):
        """
        Dump the info of the files defined by the parameters. The file info is
        dumped into a ngamsDbm DB.

        For the parameters check man-page for: ngamsDbBase.getFileSummary1().

        Returns:        Name of the DBM DB containing the info about the files
                        (string).
        """
        T = TRACE()

        # Create a temporay File Info DBM.
        if (not fileInfoDbmName):
            fileInfoDbmName = "/tmp/" +\
                              ngamsLib.genUniqueFilename("FILE-SUMMARY1")

        # Build query, create cursor.
        sqlQuery = self.\
                   buildFileSummary1Query(getHostId(), ignore=0,
                                          lowLimIngestDate=lowLimIngestDate,
                                          order=0)
        sqlQuery = sqlQuery % ngamsDbCore.getNgasFilesCols()
        curObj = None
        try:
            fileInfoDbm = ngamsDbm.ngamsDbm(fileInfoDbmName, 0, 1)
            curObj = self.dbCursor(sqlQuery)
            fileCount = 1
            while (1):
                fileList = curObj.fetch(1000)
                if (not fileList): break
                for fileInfo in fileList:
                    fileInfoDbm.add(str(fileCount), fileInfo)
                    fileCount += 1
            del curObj
            del fileInfoDbm
        except Exception, e:
            rmFile(fileInfoDbmName)
            if (curObj): del curObj
            msg = "dumpFileInfo2(): Failed in dumping file info. Error: %s" %\
                  str(e)
            error(msg)
            raise Exception, msg

        return fileInfoDbmName


    def dumpFileSummary1(self,
                         fileInfoDbmName = None,
                         hostId = None,
                         diskIds = [],
                         fileIds = [],
                         ignore = None,
                         fileStatus = [NGAMS_FILE_STATUS_OK],
                         lowLimIngestDate = None,
                         order = 1):
        """
        Dump the summary of the files defined by the parameters. This is done
        in a safe manner (or at least attempted) such that in case of problems
        with the DB interaction it is retried to dump the info.

        The file info is dumped into a ngamsDbm DB.

        fileInfoDbmName: Name of DBM, which will contain the info about the
                         files (string).

        For the other parameters check man-page for:
        ngamsDbBase.getFileSummary1().

        Returns:         Name of the DBM DB containing the info about the files
                        (string).
        """
        T = TRACE()

        # Try first to get the expected number of files, which will be returned
        expNoOfFiles = -1
        if (self.getDbVerify()):
            for n in range(1):
                noOfFiles = self.getSummary1NoOfFiles(hostId, diskIds, fileIds,
                                                      ignore, fileStatus,
                                                      lowLimIngestDate)
                if (noOfFiles > expNoOfFiles): expNoOfFiles = noOfFiles
                time.sleep(0.050)

        # Create a temporay File Info DBM.
        if (not fileInfoDbmName):
            fileInfoDbmName = "/tmp/" +\
                              ngamsLib.genUniqueFilename("FILE-SUMMARY1")

        # Try up to 5 times to dump the files. A retry is done if the number
        # of records dumped differs from the expected number.
        for n in range(5):
            fileInfoDbm = ngamsDbm.ngamsDbm(fileInfoDbmName, 0, 1)
            curObj = self.getFileSummary1(hostId, diskIds, fileIds, ignore,
                                          fileStatus, lowLimIngestDate, order)
            fileCount = 0
            while (1):
                fileList = curObj.fetch(1000)
                if (not fileList): break
                for fileInfo in fileList:
                    fileInfoDbm.add(str(fileCount), fileInfo)
                    fileCount += 1
            del curObj

            # Print out DB Verification Warning if actual number of files
            # differs from expected number of files.
            if (self.getDbVerify() and (fileCount != expNoOfFiles)):
                errMsg = "Problem dumping file info! Expected number of "+\
                         "files: %d, actual number of files: %d"
                errMsg = errMsg % (expNoOfFiles, fileCount)
                warning(errMsg)

            # Try to Auto Recover if requested.
            if ((self.getDbVerify() and self.getDbAutoRecover()) and
                (fileCount != expNoOfFiles)):
                del fileInfoDbm
                rmFile(fileInfoDbmName + "*")
                # Not all files were dumped.
                if (n < 4):
                    # - retry after a small pause.
                    if (getTestMode()):
                        time.sleep(0.5)
                    else:
                        time.sleep(5)
                    notice("Retrying to dump file info ...")
                else:
                    errMsg = "Giving up to auto recover dumping of file info!"
                    error(errMsg)
                    raise Exception, errMsg
            else:
                del fileInfoDbm
                break

        return fileInfoDbmName


    def getNumberOfFiles(self,
                         diskId = "",
                         fileId = "",
                         fileVersion = -1,
                         ignore = None,
                         onlyOnlineFiles = 0):
        """
        Get the number of files stored on a disk.

        diskId:          Disk ID of disk to get the number of files for
                         (string).

        fileId:          File ID for file to be retrieved (string).

        fileVersion:     If a File Version is specified only information
                         for files with that version number and File ID
                         are taken into account. The version must be a
                         number in the range [1; oo[ (integer).

        ignore:          If set to 0 or 1, this value of ignore will be
                         queried for. If set to None, ignore is not
                         considered (None|0|1).

        onlyOnlineFiles: If specified, only files which are Online or on
                         suspended nodes are considered (integer/0|1).

        Return:          Number of files stored on the disk (integer).
        """
        T = TRACE()

        sqlQuery = "SELECT count(file_id) from ngas_files nf"

        # Build up the query, take only Online/Suspended files into
        # account if requested.
        if (diskId or fileId or (fileVersion != -1) or (ignore != None) or
            onlyOnlineFiles):
            sqlQuery += ", ngas_disks nd WHERE nf.disk_id=nd.disk_id"
        if (diskId): sqlQuery += " AND nf.disk_id='%s'" % diskId
        if (fileId): sqlQuery += " AND nf.file_id='%s'" % fileId
        if (fileVersion > 0): sqlQuery += " AND nf.file_version=%d" %\
                                          int(fileVersion)
        if (ignore != None): sqlQuery += " AND nf.file_ignore=%d" % int(ignore)
        if (onlyOnlineFiles):
            # We assume here that either Disk ID, File ID, File Version
            # or ignore=1 specified so that we can append and AND clause.
            sqlQuery += " AND nf.disk_id IN (SELECT nd.disk_id " +\
                        "FROM ngas_disks nd, ngas_hosts nh " +\
                        "WHERE (nd.host_id=nh.host_id) OR " +\
                        "((nd.last_host_id=nh.host_id) AND " +\
                        "(nh.srv_suspended=1)))"

        # Now, do the query.
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (len(res[0]) == 1):
            if (res[0][0][0]):
                return res[0][0][0]
            else:
                return 0
        else:
            return 0


    def dumpFileInfoCluster(self,
                            clusterName,
                            fileInfoDbmName = None,
                            useFileKey = False,
                            count = False):
        """
        Dump the info for the files registered in the name space of the
        referenced cluster.

        Note, all files in the cluster are taken, also the ones marked
        as bad or to be ignored.

        clusterName:       Name of cluster to consider (string).

        fileInfoDbmName:   Base name of the DBM in which the file info will be
                           stored. If not given, a name will be generated
                           automatically (string).

        useFileKey:        Use a file key (<File ID>_<Version>) as key as
                           opposed to just an integer key. NOTE: Multiple
                           occurrences of a given File ID/Version will only
                           appear once (boolean).

        count:             When useFileKey == True, if count is True, the
                           number of ocurrences of each File ID + Version
                           is counted and an entry added in the DBM:

                             <File Key>__COUNTER

                           - pointing to a counter indicating the number of
                           occurrences. Note, the usage of '__' in the name of
                           the counter for each file, means it will be skipped
                           when doing a ngamsDbm.getNext(), scanning through
                           the contents of the DBM (boolean).

        Returns:           Final name of the DBM DB containing the info about
                           the files (string).
        """
        T = TRACE()

        # Create a temporay File Info DBM.
        if (not fileInfoDbmName):
            fileInfoDbmName = "/tmp/" +\
                              ngamsLib.genUniqueFilename("CLUSTER-FILE-INFO")
        rmFile("%s*" % fileInfoDbmName)

        # Get list of hosts in the cluster.
        clusterHostList = self.getHostIdsFromClusterName(clusterName)
        if (clusterHostList == []):
            msg = "No hosts registered for cluster with name: %s" % clusterName
            raise Exception, msg
        clusterHostList = str(clusterHostList).strip()[1:-1]
        if (clusterHostList[-1] == ","): clusterHostList = clusterHostList[:-1]

        # Build query, create cursor.
        sqlQuery = "SELECT %s FROM ngas_files nf WHERE disk_id IN " +\
                   "(SELECT disk_id FROM ngas_disks WHERE " +\
                   "host_id IN (%s) OR last_host_id IN (%s))"
        sqlQuery = sqlQuery % (ngamsDbCore.getNgasFilesCols(),
                               clusterHostList, clusterHostList)
        curObj = None
        try:
            fileInfoDbm = ngamsDbm.ngamsDbm(fileInfoDbmName, 0, 1)
            curObj = self.dbCursor(sqlQuery)
            fileCount = 1
            while (1):
                fileList = curObj.fetch(10000)
                if (not fileList): break
                for fileInfo in fileList:
                    if (not useFileKey):
                        fileInfoDbm.add(str(fileCount), fileInfo)
                    else:
                        fileId = fileInfo[ngamsDbCore.NGAS_FILES_FILE_ID]
                        fileVersion = fileInfo[ngamsDbCore.NGAS_FILES_FILE_VER]
                        fileKey = ngamsLib.genFileKey(None, fileId,
                                                      fileVersion)
                        if (count):
                            countKey = "%s__COUNTER" % fileKey
                            if (not fileInfoDbm.hasKey(countKey)):
                                fileInfoDbm.add(countKey, 0)
                            fileInfoDbm.add(countKey,
                                            (fileInfoDbm.get(countKey) + 1))
                        fileInfoDbm.add(fileKey, fileInfo)

                    fileCount += 1
            del curObj
            fileInfoDbm.sync()
            del fileInfoDbm
        except Exception, e:
            rmFile(fileInfoDbmName)
            if (curObj): del curObj
            msg = "dumpFileInfoCluster(): Failed in dumping file info. " +\
                  "Error: %s" % str(e)
            raise Exception, msg

        return fileInfoDbmName


    def writeFileEntry(self,
                       diskId,
                       filename,
                       fileId,
                       fileVersion,
                       format,
                       fileSize,
                       uncompressedFileSize,
                       compression,
                       ingestionDate,
                       ignore,
                       checksum,
                       checksumPlugIn,
                       fileStatus,
                       creationDate,
                       iotime,
                       ingestionRate,
                       genSnapshot = 1,
                       updateDiskInfo = 0):
        """
        The method writes the information in connection with a file in the
        NGAS DB. If an entry already exists for that file, it is updated
        with the information contained in the File Info Object. Otherwise,
        a new entry is created.

        diskId           Values for the columns in the ngas_disks
        ...              table (use values returned from ngamsFileInfo).

        genSnapshot:     Generate a snapshot file (integer/0|1).

        updateDiskInfo:  Update automatically the disk info for the
                         disk hosting this file (integer/0|1).

        Returns:         Void.
        """
        T = TRACE(5)

        # Check if the entry already exists. If yes update it, otherwise
        # insert a new element.
        if (ignore == -1): ignore = 0
        try:
            self.takeDbSem()
            ingDate = self.convertTimeStamp(ingestionDate)
            creDate = self.convertTimeStamp(creationDate)
        finally:
            self.relDbSem()

        if (self.fileInDb(diskId, fileId, fileVersion)):
            # We only allow to modify a limited set of columns.
            sqlQuery = "UPDATE ngas_files SET " +\
                       "file_name='" + filename + "', " +\
                       "format='" + format + "', " +\
                       "file_size=" + str(fileSize) + ", " +\
                       "uncompressed_file_size=" +\
                       str(uncompressedFileSize) + ", " +\
                       "compression='" + compression + "', " +\
                       "file_ignore=" + str(ignore) + ", " +\
                       "checksum='" + checksum + "', " +\
                       "checksum_plugin='" + checksumPlugIn + "', " +\
                       "file_status='" + fileStatus + "', " +\
                       "creation_date='" + creDate + "', " +\
                       "io_time=" + str(int(iotime*1000)) + ", " +\
                       "ingestion_rate=" + str(ingestionRate) + " " +\
                       "WHERE file_id='" + fileId + "' AND " +\
                       "disk_id='" + diskId + "'"
            if (int(fileVersion) != -1):
                sqlQuery += " AND file_version=" + str(fileVersion)
            dbOperation = NGAMS_DB_CH_FILE_UPDATE
        else:
            sqlQuery = "INSERT INTO ngas_files " +\
                       "(disk_id, file_name, file_id, file_version, " +\
                       "format, file_size, " +\
                       "uncompressed_file_size, compression, " +\
                       "ingestion_date, file_ignore, checksum, " +\
                       "checksum_plugin, file_status, creation_date, io_time, " +\
                       "ingestion_rate) "+\
                       "VALUES " +\
                       "('" + diskId + "', " +\
                       "'" + filename + "', " +\
                       "'" + fileId + "', " +\
                       "" + str(fileVersion) + ", " +\
                       "'" + format + "', " +\
                       str(fileSize) + ", " +\
                       str(uncompressedFileSize) + ", " +\
                       "'" + compression + "', " +\
                       "'" + ingDate + "', " +\
                       str(ignore) + ", " +\
                       "'" + checksum + "', " +\
                       "'" + checksumPlugIn + "', " +\
                       "'" + fileStatus + "', " +\
                       "'" + creDate + "', " +\
                       str(int(iotime*1000)) + ", " +\
                       str(ingestionRate) +\
                       ")"
            dbOperation = NGAMS_DB_CH_FILE_INSERT

        # Perform the main query.
        self.query(sqlQuery)

        # Update the Disk Info of the disk concerned if requested and
        # if a new entry was added.
        #
        # Note: In case of an update the columns ngas_disks.avail_mb
        #       and ngas_disks.bytes_stored should in principle be
        #       updated according to the actual size of the new
        #       version of the file.
        if (updateDiskInfo and (dbOperation == NGAMS_DB_CH_FILE_INSERT)):
            self.updateDiskFileStatus(diskId, fileSize)

        # Create a Temporary DB Change Snapshot Document if requested.
        if (self.getCreateDbSnapshot() and genSnapshot):
            tmpFileObj = ngamsFileInfo.ngamsFileInfo().\
                         unpackSqlResult([diskId, filename, fileId,
                                          fileVersion, format, fileSize,
                                          uncompressedFileSize,compression,
                                          ingestionDate, ignore, checksum,
                                          checksumPlugIn, fileStatus, creationDate,
                                          iotime, ingestionRate, None])
            self.createDbFileChangeStatusDoc(dbOperation, [tmpFileObj])
            del tmpFileObj

        self.triggerEvents([diskId, None])


    def getClusterReadyArchivingUnits(self,
                                      clusterName):
        """
        Return list of NAUs in the local cluster with archiving capability
        (archiving enabled + have capacity).

        The resulting list of nodes will be formatted as:

          [<Node>:<Port>, ...]                                (list).

        clusterName:   Name of cluster to consider (string).

        Returns:       List with ready NAU nodes in the cluster (list/string).
        """
        T = TRACE()

        sqlQuery = "SELECT host_id, srv_port FROM ngas_hosts " +\
                   "WHERE cluster_name='%s' AND host_id in " +\
                   "(SELECT host_id FROM ngas_disks WHERE completed=0 " +\
                   "AND mounted=1) ORDER BY host_id"
        sqlQuery = sqlQuery % clusterName
        res = self.query(sqlQuery)
        if (res == [[]]):
            return []
        else:
            hostList = []
            for node in res[0]:
                hostList.append("%s:%s" % (node[0], node[1]))
            return hostList


    def getSpaceAvailForHost(self,
                             hostId = None):
        """
        Return the amount of free disk space for the given host. This is
        calculated as the total sum of free space for all non-completed volumes
        mounted in this node, according to the DB.

        hostId:     Name of host (string).

        Returns:    Amount of free disk space in MB bytes (float)
        """
        T = TRACE()

        if hostId is None:
            hostId = getHostId()

        sqlQuery = "SELECT sum(available_mb) FROM ngas_disks WHERE " +\
                   "host_id='%s'"
        sqlQuery = sqlQuery % hostId
        res = self.query(sqlQuery, ignoreEmptyRes = 0)
        if (len(res[0]) == 1):
            if (res[0][0][0]):
                return float(res[0][0][0])
            else:
                return 0.0
        else:
            return 0.0


    def getCacheContents(self,
                         hostId = None):
        """
        Execute query by means of a cursor, with which the entire contents
        of the cache can be downloaded.

        hostId:    Name of host to consider (string).

        Returns:   Cursor object with which the contents can be retrieved
                   Cursor object (<NG/AMS DB Cursor Object API>).
        """
        T = TRACE()

        if hostId is None:
            hostId = getHostId()

        sqlQuery = "SELECT disk_id, file_id, file_version, " +\
                   "cache_time, cache_delete FROM ngas_cache " +\
                   "WHERE disk_id IN (SELECT disk_id FROM ngas_disks WHERE " +\
                   "host_id = '%s') ORDER BY cache_time"
        curObj = self.dbCursor(sqlQuery)
        return curObj

# EOF
