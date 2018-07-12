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

import logging

from . import ngamsDbm, ngamsDbCore, ngamsLib, ngamsFileInfo
from .ngamsCore import TRACE, rmFile
from .ngamsCore import NGAMS_FILE_STATUS_OK, NGAMS_FILE_CHK_ACTIVE,NGAMS_DB_CH_FILE_UPDATE, NGAMS_DB_CH_FILE_INSERT

logger = logging.getLogger(__name__)

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

        sql, vals = self.buildFileSummary1Query(ngamsDbCore.getNgasSummary1Cols(self._file_ignore_columnname),
                                                hostId, diskIds, fileIds,
                                                ignore, fileStatus,
                                                lowLimIngestDate, order)

        with self.dbCursor(sql, args=vals) as cursor:
            for x in cursor.fetch(1000):
                yield x


    def getFileSummary1SingleFile(self,
                                  diskId,
                                  fileId,
                                  fileVersion):
        """
        Same as getFileSummary1() but for a single (specific) file.

        Returns:   List with information from query (list).
        """
        T = TRACE()

        sql = ("SELECT %s FROM ngas_disks nd, ngas_files nf "
                "WHERE nd.disk_id=nf.disk_id AND "
                "nd.disk_id={} AND nf.file_id={} AND "
                "nf.file_version={}") % ngamsDbCore.getNgasSummary1Cols(self._file_ignore_columnname)
        res = self.query2(sql, args = (diskId, fileId, fileVersion))
        if not res:
            return []
        return res[0]


    def getFileSummary2(self,
                        hostId = None,
                        fileIds = [],
                        diskId = None,
                        ignore = None,
                        ing_date = None,
                        max_num_records = None,
                        upto_ing_date = None,
                        fetch_size=1000):
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

        sql = []
        vals = []
        sql.append(("SELECT %s FROM ngas_disks nd, ngas_files nf "
                    "WHERE nd.disk_id=nf.disk_id ") % ngamsDbCore.getNgasSummary2Cols())

        if ignore:
            sql.append(" AND nf.%s={}" % (self._file_ignore_columnname))
            vals.append(ignore)

        if hostId:
            if type(hostId) is list:
                params = []
                for i in hostId:
                    params.append('{}')
                    vals.append(i)
                sql.append(" AND nd.host_id IN (%s)" % ','.join(params))
            else:
                sql.append(" AND nd.host_id={}")
                vals.append(hostId)

        if diskId:
            sql.append(" AND nf.disk_id={}")
            vals.append(diskId)

        if fileIds:
            params = []
            for i in fileIds:
                params.append('{}')
                vals.append(i)
            sql.append(" AND nf.file_id IN (%s)" % ','.join(params))

        if ing_date:
            sql.append(" AND nf.ingestion_date > {}")
            vals.append(self.convertTimeStamp(ing_date))

        if upto_ing_date:
            sql.append(" AND nf.ingestion_date < {}")
            vals.append(self.convertTimeStamp(upto_ing_date))

        sql.append(" ORDER BY nf.ingestion_date")

        if max_num_records:
            sql.append(" LIMIT {}")
            vals.append(max_num_records)

        with self.dbCursor(''.join(sql), args=vals) as cursor:
            for res in cursor.fetch(fetch_size):
                yield res


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

        sql = []
        vals = []
        sql.append(("SELECT nh.host_id, nh.ip_address, nh.srv_port, "
                   "nd.mount_point, nf.file_name, nf.file_version, "
                   "nf.format FROM ngas_files nf, ngas_disks nd, ngas_hosts nh "
                   "WHERE nf.file_id={} AND nf.disk_id=nd.disk_id AND "
                   "nd.host_id=nh.host_id AND nf.%s=0 AND "
                   "nf.file_status='00000000'") % (self._file_ignore_columnname,))
        vals.append(fileId)
        if hostId:
            sql.append(" AND nh.host_id={}")
            vals.append(hostId)
        if domain:
            sql.append(" AND nh.domain={}")
            vals.append(domain)
        if diskId:
            sql.append(" AND nd.disk_id={}")
            vals.append(diskId)
        if fileVersion > 0:
            sql.append(" AND nf.file_version={}")
            vals.append(fileVersion)
        sql.append(" ORDER BY nf.file_version DESC")

        return self.query2(''.join(sql), args = vals)


    def getFileSummarySpuriousFiles1(self,
                                     hostId = None,
                                     diskId = None,
                                     fileId = None,
                                     fileVersion = None,
                                     fetch_size = 1000):
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
        sql = []
        vals = []
        sql.append(("SELECT %s FROM ngas_disks nd, ngas_files nf "
                    "WHERE nd.disk_id=nf.disk_id AND (nf.%s=1 OR "
                    "nf.file_status NOT IN (%s))") \
                    % (ngamsDbCore.getNgasSummary1Cols(self._file_ignore_columnname), self._file_ignore_columnname, str(fileStatusList)[1:-1]))

        if hostId:
            sql.append(" AND nd.host_id={}")
            vals.append(hostId)
        if diskId:
            sql.append(" AND nd.disk_id={}")
            vals.append(diskId)
        if fileId:
            sql.append(" AND nf.file_id={}")
            vals.append(fileId)
        if fileVersion:
            sql.append(" AND nf.file_version={}")
            vals.append(fileVersion)

        with self.dbCursor(''.join(sql), args = vals) as cursor:
            for res in cursor.fetch(fetch_size):
                yield res


    def setFileChecksum(self,
                        hostId,
                        fileId,
                        fileVersion,
                        diskId,
                        checksum,
                        checksumPlugIn):
        """
        Set the checksum value in the ngas_files table.

        hostId:          ID of this NGAS host

        fileId:          ID of file (string).

        fileVersion:     Version of file (integer).

        diskId:          ID of disk where file is stored (string).

        checksum:        Checksum of file (string).

        checksumPlugIn:  Name of plug-in used to generate the
                         checksum (string).

        Returns:         Reference to object itself.
        """
        T = TRACE()

        sql = ("UPDATE ngas_files SET checksum={}, checksum_plugin={}"
               " WHERE file_id={} AND file_version={} AND disk_id={}")
        vals = (checksum, checksumPlugIn, fileId, fileVersion, diskId)
        self.query2(sql, args = vals)

        # Create a File Removal Status Document.
        if (self.getCreateDbSnapshot()):
            dbFileInfo = self.getFileInfoFromFileIdHostId(hostId,
                                                          fileId,
                                                          fileVersion,
                                                          diskId)
            tmpFileObj = ngamsFileInfo.ngamsFileInfo().\
                         unpackSqlResult(dbFileInfo)
            self.createDbFileChangeStatusDoc(hostId,
                                             NGAMS_DB_CH_FILE_UPDATE,
                                             [tmpFileObj])

        self.triggerEvents()
        return self


    def buildFileSummary1Query(self,
                               columns,
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

        sql = []
        vals = []
        sql.append(("SELECT %s FROM ngas_disks nd, ngas_files nf "
                    "WHERE nd.disk_id=nf.disk_id") % columns)

        if ignore is not None:
            sql.append(" AND nf.%s={}" % (self._file_ignore_columnname,))
            vals.append(ignore)

        if hostId:
            sql.append(" AND nd.host_id={}")
            vals.append(hostId)

        if diskIds:
            params = []
            for i in diskIds:
                params.append('{}')
                vals.append(i)
            sql.append(" AND nd.disk_id IN (%s)" % ','.join(params))

        if fileIds:
            params = []
            for i in fileIds:
                params.append('{}')
                vals.append(i)
            sql.append(" AND nf.file_id IN (%s)" % ','.join(params))

        if fileStatus:
            params = []
            for i in fileStatus:
                params.append('{}')
                vals.append(i)
            sql.append(" AND nf.file_status IN (%s)" % ','.join(params))

        if lowLimIngestDate:
            sql.append(" AND nf.ingestion_date >= {}")
            vals.append(self.convertTimeStamp(lowLimIngestDate))

        if order:
            sql.append(" ORDER BY nd.slot_id, nf.ingestion_date")

        return ''.join(sql), vals


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

        sql = []
        sql.append(("SELECT %s FROM ngas_files nf, ngas_disks nd WHERE "
                   "nf.file_id={} AND nf.disk_id=nd.disk_id AND "
                   "nd.host_id={} AND nd.mounted=1 AND nf.file_version={}") \
                   % ngamsDbCore.getNgasFilesCols(self._file_ignore_columnname))

        vals = [fileId, hostId, int(fileVersion)]
        if diskId:
            sql.append(" AND nd.disk_id={}")
            vals.append(diskId)
        if ignore:
            sql.append(" AND nf.%s={}" % (self._file_ignore_columnname,))
            vals.append(ignore)
        res = self.query2(''.join(sql), args = vals)
        if not res:
            return []
        return res[0]

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
            raise Exception("Illegal value for File Version specified: " +\
                  str(fileVersion))
        sql = []
        vals = []
        # Query for files being online.
        sql.append(("SELECT %s, nd.host_id, "
                   "nd.mount_point FROM ngas_files nf, ngas_disks nd, "
                   "ngas_hosts nh WHERE nh.host_id=nd.host_id AND "
                   "nf.disk_id=nd.disk_id") % ngamsDbCore.getNgasFilesCols(self._file_ignore_columnname))

        if ignore:
            sql.append(" AND nf.%s={}" % (self._file_ignore_columnname,))
            vals.append(ignore)
        # File ID specified.
        if fileId:
            sql.append(" AND nf.file_id={}")
            vals.append(fileId)
        # Do we want a specific File Version?
        if fileVersion != -1:
            sql.append(" AND nf.file_version={}")
            vals.append(int(fileVersion))
        # Is a special disk referred?
        if diskId:
            sql.append(" AND nf.disk_id={}")
            vals.append(diskId)
        # Order the files according to the version.
        if order:
            sql.append(" ORDER BY nf.file_version desc, nd.disk_id desc")

        if dbCursor:
            return self.dbCursor(''.join(sql), args=vals)
        res = self.query2(''.join(sql), args = vals)
        if not res:
            return []
        return res


    def files_in_host(self, hostId, from_date=None):
        """
        Dump the info of the files defined by the parameters. The file info is
        dumped into a ngamsDbm DB.

        For the parameters check man-page for: ngamsDbBase.getFileSummary1().

        Returns:        Name of the DBM DB containing the info about the files
                        (string).
        """

        sql, vals = self.buildFileSummary1Query(ngamsDbCore.getNgasFilesCols(self._file_ignore_columnname),
                                                hostId, ignore = 0,
                                                lowLimIngestDate = from_date,
                                                order = 0)
        with self.dbCursor(sql, args=vals) as cursor:
            for res in cursor.fetch(1000):
                yield res


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

        sql = []
        vals = []

        sql.append("SELECT count(file_id) from ngas_files nf")

        # Build up the query, take only Online/Suspended files into
        # account if requested.
        if diskId or fileId or fileVersion != -1 or ignore or onlyOnlineFiles:
            sql.append(", ngas_disks nd WHERE nf.disk_id=nd.disk_id")
        if diskId:
            sql.append(" AND nf.disk_id={}")
            vals.append(diskId)
        if fileId:
            sql.append(" AND nf.file_id={}")
            vals.append(fileId)
        if fileVersion > 0:
            sql.append(" AND nf.file_version={}")
            vals.append(int(fileVersion))
        if ignore:
            sql.append(" AND nf.%s={}" % (self._file_ignore_columnname,))
            vals.append(int(ignore))
        if onlyOnlineFiles:
            # We assume here that either Disk ID, File ID, File Version
            # or ignore=1 specified so that we can append and AND clause.
            sql.append((" AND nf.disk_id IN (SELECT nd.disk_id "
                        "FROM ngas_disks nd, ngas_hosts nh "
                        "WHERE (nd.host_id=nh.host_id) OR "
                        "((nd.last_host_id=nh.host_id) AND "
                        "(nh.srv_suspended=1)))"))
        res = self.query2(''.join(sql), args = vals)
        return res[0][0]


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
            fileInfoDbmName = self.genTmpFile("CLUSTER-FILE-INFO")
        rmFile("%s*" % fileInfoDbmName)

        # Get list of hosts in the cluster.
        clusterHostList = self.getHostIdsFromClusterName(clusterName)
        if (clusterHostList == []):
            msg = "No hosts registered for cluster with name: %s" % clusterName
            raise Exception(msg)
        clusterHostList = str(clusterHostList).strip()[1:-1]
        if (clusterHostList[-1] == ","): clusterHostList = clusterHostList[:-1]

        sql = ("SELECT %s FROM ngas_files nf WHERE disk_id IN "
               "(SELECT disk_id FROM ngas_disks WHERE "
               "host_id IN ({}) OR last_host_id IN ({}))") \
               % ngamsDbCore.getNgasFilesCols(self._file_ignore_columnname)

        try:
            fileInfoDbm = ngamsDbm.ngamsDbm(fileInfoDbmName, 0, 1)
            cursor = self.dbCursor(sql, args=(clusterHostList, clusterHostList))
            fileCount = 1
            with cursor:
                for fileInfo in cursor.fetch(1000):
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
            fileInfoDbm.sync()
            del fileInfoDbm
        except Exception as e:
            rmFile(fileInfoDbmName)
            msg = "dumpFileInfoCluster(): Failed in dumping file info. " +\
                  "Error: %s" % str(e)
            raise Exception(msg)

        return fileInfoDbmName


    def writeFileEntry(self,
                       hostId,
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
        if ignore == -1:
            ignore = 0

        checksum = str(checksum) if checksum else None
        ingDate = self.convertTimeStamp(ingestionDate)
        creDate = self.convertTimeStamp(creationDate)

        sql = []
        vals = []
        sql_str = None

        if (self.fileInDb(diskId, fileId, fileVersion)):
            # We only allow to modify a limited set of columns.
            sql.append(("UPDATE ngas_files SET "
                       "file_name={}, format={}, file_size={}, "
                       "uncompressed_file_size={}, compression={}, "
                       "%s={}, checksum={}, checksum_plugin={}, "
                       "file_status={}, creation_date={}, io_time={}, "
                       "ingestion_rate={} WHERE file_id={} AND disk_id={}" % (self._file_ignore_columnname,)))
            vals = [filename, format, fileSize, uncompressedFileSize, compression,\
                    ignore, checksum, checksumPlugIn, fileStatus, creDate,\
                    int(iotime*1000), ingestionRate, fileId, diskId]

            if int(fileVersion) != -1:
                sql.append(" AND file_version={}")
                vals.append(fileVersion)

            dbOperation = NGAMS_DB_CH_FILE_UPDATE
            sql_str = ''.join(sql)
        else:
            sql_str = ("INSERT INTO ngas_files (disk_id, file_name, file_id,"
                        "file_version, format, file_size, uncompressed_file_size,"
                        " compression, ingestion_date, %s, checksum, "
                        "checksum_plugin, file_status, creation_date, io_time, "
                        "ingestion_rate) VALUES ({}, {}, {}, {}, {}, {}, {}, {},"
                        " {}, {}, {},{}, {}, {}, {}, {})" % (self._file_ignore_columnname,))
            vals = (diskId, filename, fileId, fileVersion, format, fileSize,\
                    uncompressedFileSize, compression, ingDate, ignore,\
                    checksum, checksumPlugIn, fileStatus, creDate,\
                    int(iotime*1000), ingestionRate)
            dbOperation = NGAMS_DB_CH_FILE_INSERT

        self.query2(sql_str, args = vals)

        # Update the Disk Info of the disk concerned if requested and
        # if a new entry was added.
        #
        # Note: In case of an update the columns ngas_disks.avail_mb
        #       and ngas_disks.bytes_stored should in principle be
        #       updated according to the actual size of the new
        #       version of the file.
        #print "writeFileEntry".upper(), updateDiskInfo, dbOperation
        if (updateDiskInfo and (dbOperation == NGAMS_DB_CH_FILE_INSERT)):
            self.updateDiskFileStatus(diskId, fileSize)

        # Create a Temporary DB Change Snapshot Document if requested.
        if (self.getCreateDbSnapshot() and genSnapshot):
            tmpFileObj = ngamsFileInfo.ngamsFileInfo().\
                            setDiskId(diskId).setFilename(filename).setFileId(fileId).\
                            setFileVersion(fileVersion).setFormat(format).setFileSize(fileSize).\
                            setUncompressedFileSize(uncompressedFileSize).setCompression(compression).\
                            setIngestionDate(ingestionDate).setIgnore(ignore).setChecksum(checksum).\
                            setChecksumPlugIn(checksumPlugIn).setFileStatus(fileStatus).setCreationDate(creationDate).\
                            setIoTime(iotime).setIngestionRate(ingestionRate)
            self.createDbFileChangeStatusDoc(hostId, dbOperation, [tmpFileObj])
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

        sql = ("SELECT host_id, srv_port FROM ngas_hosts "
               "WHERE cluster_name={} AND host_id in "
               "(SELECT host_id FROM ngas_disks WHERE completed=0 "
               "AND mounted=1) ORDER BY host_id")
        res = self.query2(sql, args = (clusterName,))
        if not res:
            return []
        hostList = []
        for node in res:
            hostList.append("%s:%s" % (node[0], node[1]))
        return hostList


    def getSpaceAvailForHost(self, hostId):
        """
        Return the amount of free disk space for the given host. This is
        calculated as the total sum of free space for all non-completed volumes
        mounted in this node, according to the DB.

        hostId:     Name of host (string).

        Returns:    Amount of free disk space in MB bytes (float)
        """
        T = TRACE()

        sql = "SELECT sum(available_mb) FROM ngas_disks WHERE host_id={}"
        res = self.query2(sql, args = (hostId,))
        return float(res[0][0])


    def getCacheContents(self, hostId):
        """
        Execute query by means of a cursor, with which the entire contents
        of the cache can be downloaded.

        hostId:    Name of host to consider (string).

        Returns:   Cursor object with which the contents can be retrieved
                   Cursor object (<NG/AMS DB Cursor Object API>).
        """
        T = TRACE()

        sql = ("SELECT disk_id, file_id, file_version, "
               "cache_time, cache_delete FROM ngas_cache "
               "WHERE disk_id IN (SELECT disk_id FROM ngas_disks WHERE "
               "host_id = {}) ORDER BY cache_time")

        with self.dbCursor(sql, args=(hostId,)) as cursor:
            for res in cursor.fetch(1000):
                yield res