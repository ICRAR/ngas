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
# "@(#) $Id: ngamsDbNgasDisks.py,v 1.8 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  07/03/2008  Created
#

"""
Contains queries for accessing the NGAS Disks Table.

This class is not supposed to be used standalone in the present implementation.
It should be used as part of the ngamsDbBase parent classes.
"""

import logging

from ngamsCore import TRACE, getDiskSpaceAvail, rmFile, NGAMS_DB_CH_FILE_DELETE, fromiso8601
import ngamsDbm, ngamsDbCore


logger = logging.getLogger(__name__)

# TODO: Avoid using these classes in this module (mutual dependency):
import ngamsFileInfo, ngamsDiskInfo


class ngamsDbNgasDisks(ngamsDbCore.ngamsDbCore):
    """
    Contains queries for accessing the NGAS Disks Table.
    """

    def updateDiskFileStatus(self, diskId, fileSize):
        """
        Update the NGAS Disks Table according to a new file archived.

        diskId:       Disk ID (string).

        fileSize:     Size of file as stored on disk (integer).

        Returns:      Reference to object itself.
        """
        T = TRACE()

        try:
            self.takeGlobalDbSem()
            sql = ("SELECT number_of_files, available_mb, "
                    "bytes_stored, mount_point "
                    "FROM ngas_disks WHERE disk_id={}")
            res = self.query2(sql, args = (diskId,))
            if not res:
                errMsg = "Cannot find entry for disk with ID: %s." % diskId
                raise Exception(errMsg)

            numberOfFiles = res[0][0]
            bytesStored = res[0][2]
            mountPoint = res[0][3]
            newNumberOfFiles = numberOfFiles + 1
            newAvailMb = getDiskSpaceAvail(mountPoint)
            newBytesStored = bytesStored + fileSize

            sql = ("UPDATE ngas_disks SET number_of_files={}, available_mb={},"
                   " bytes_stored={} WHERE disk_id={}")
            vals = [newNumberOfFiles, newAvailMb, newBytesStored, diskId]
            self.query2(sql, args = vals)
            self.triggerEvents()
        finally:
            self.relGlobalDbSem()


    def diskInDb(self, diskId):
        """
        Check if disk with the given Disk ID is available in the DB.

        diskId:    Disk ID (string).

        Returns:   1 = Disk ID was found, 0 = Disk ID not found (integer).
        """
        T = TRACE()

        sql = "SELECT disk_id FROM ngas_disks WHERE disk_id={}"
        res = self.query2(sql, args = (diskId,))
        if not res:
            return 0
        return 1


    def setLogicalNameForDiskId(self, diskId, logicalName):
        """
        Change the Logical Name of the disk with the given Disk ID.

        diskId:        Disk ID (string).

        logicalName:   New Logical Name (string).

        Returns:       Void.
        """
        T = TRACE()

        sql = "UPDATE ngas_disks SET logical_name={} WHERE disk_id={}"
        self.query2(sql, args = (logicalName, diskId))


    def getLogicalNameFromDiskId(self, diskId):
        """
        Query the Logical Name of a disk from the DB, based on
        the Disk ID of the disk.

        diskId:      Disk ID (string).

        Returns:     Logical Name or None if not found (string | None).
        """
        T = TRACE()

        sql = "SELECT logical_name FROM ngas_disks WHERE disk_id={}"
        res = self.query2(sql, args = (diskId,))
        if not res:
            return None
        return res[0][0]


    def getDiskCompleted(self, diskId):
        """
        Check if a disk is marked in the NGAS DB as completed.

        diskId:    ID of the disk (string).

        Returns:   1 = completed, 0 = not completed. If the disk
                   is not registered None is returned (integer).
        """
        T = TRACE()

        sql = "SELECT completed FROM ngas_disks WHERE disk_id={}"
        res = self.query2(sql, args = (diskId,))
        if not res:
            return None
        return res[0][0]


    def getSlotIdFromDiskId(self, diskId):
        """
        Get the Slot ID for a disk, given by the Disk ID for the disk.

        diskId:    ID of the disk (string).

        Returns:   Slot ID of disk. If disk is not found None is
                   returned (string | None).
        """
        T = TRACE()

        sql = "SELECT slot_id FROM ngas_disks WHERE disk_id={}"
        res = self.query2(sql, args = (diskId,))
        if not res:
            return None
        return res[0][0]


    def getDiskInfoFromDiskId(self, diskId):
        """
        Query the information for one disk (referred to by its ID), and
        return this in raw format.

        The information about the disk is stored in a list with the
        the lay-out following the sequence as listed in the overall man-page
        for the ngamsDbBase class.

        diskId:   ID of the disk (string).

        Returns:  Disk information for the disk or [] if disk was
                  not found (list).
        """
        T = TRACE()

        sql = "SELECT %s FROM ngas_disks nd WHERE disk_id={}" % \
                ngamsDbCore.getNgasDisksCols()
        res = self.query2(sql, args = (diskId,))
        if not res:
            return []
        return res[0]


    def getDiskInfoFromDiskIdList(self, diskIdList):
        """
        Get disk information from a list of Disk IDs given. The result is
        returned in a list containing again lists with the field as described
        in documentation for ngamsDbBase.getDiskInfoFromDiskId().

        diskIdList:   List with Disk IDs (list/string).

        Returns:      List with disk information (list/list).
        """
        T = TRACE()

        params = []
        sql = "SELECT %s FROM ngas_disks nd WHERE disk_id IN (%s)"
        for di in diskIdList:
            params.append('{}')
        sql = sql % (ngamsDbCore.getNgasDisksCols(), ','.join(params))
        res = self.query2(sql, args = diskIdList)
        if not res:
            return []
        return res


    def getMaxDiskNumber(self, cat = None):
        """
        Get the maximum disk index (number) in connection with the
        Logical Disk Names in the DB.

        cat:       'M' for Main, 'R' for Replication (string).

        Returns:   The maximum disk number or None if this could not
                   be generated (integer).
        """
        T = TRACE()

        sql = []
        vals = []
        sql.append("SELECT logical_name FROM ngas_disks")
        if cat:
            sql.append(" WHERE logical_name LIKE {}")
            vals.append('%%%s-%%' % (cat,))
        else:
            sql.append(" WHERE logical_name LIKE {} or logical_name LIKE {}")
            vals.append('%M-%')
            vals.append('%R-%')

        res = self.query2(''.join(sql), args = vals)
        if not res:
            return None

        logNameDic = {}
        for subRes in res:
            tmpName = subRes[0]
            logNameDic[tmpName[(len(tmpName) - 6):]] = 1
        logNames = logNameDic.keys()
        logNames.sort()
        retVal = int(logNames[-1])
        return retVal


    def writeDiskEntry(self,
                       diskId,
                       archive,
                       installationDate,
                       type,
                       manufacturer,
                       logicalName,
                       hostId,
                       slotId,
                       mounted,
                       mountPoint,
                       numberOfFiles,
                       availableMb,
                       bytesStored,
                       completed,
                       completionDate,
                       checksum,
                       totalDiskWriteTime,
                       lastCheck,
                       lastHostId):
        """
        The method writes the information in connection with a disk in the
        NGAS DB. If an entry already exists for that disk, it is updated
        with the information contained in the Disk Info Object. Otherwise,
        a new entry is created.

        diskId
        ...
        lastHostId:      Values for the columns in the ngas_disks
                         table (use values returned from ngamsDiskInfo).

        Returns:         Returns 1 if a new entry was created in the DB
                         and 0 if an existing entry was updated (integer/0|1).
        """
        T = TRACE()

        sql = []
        vals = []

        instDate = self.convertTimeStamp(installationDate)
        bytesStored = int(bytesStored)
        addDiskHistEntry = 0

        if self.diskInDb(diskId):
            sql.append(("UPDATE ngas_disks SET "
                       "archive={},installation_date={},type={},manufacturer={},"
                       "logical_name={},host_id={},slot_id={},mounted={},"
                       "mount_point={},number_of_files={},available_mb={},"
                       "bytes_stored={},completed={},checksum={},"
                       "total_disk_write_time={}"))
            vals = [archive, instDate, type, manufacturer, logicalName, hostId,\
                    slotId, mounted, mountPoint, numberOfFiles, availableMb,\
                    bytesStored, completed, checksum, totalDiskWriteTime]

            if lastCheck is not None:
                lastCheckTmp = self.convertTimeStamp(lastCheck)
                sql.append(",last_check={}")
                vals.append(lastCheckTmp)

            if lastHostId:
                sql.append(",last_host_id={}")
                vals.append(lastHostId)

            sql.append(" WHERE disk_id={}")
            vals.append(diskId)
        else:
            sql.append(("INSERT INTO ngas_disks "
                       "(disk_id, archive, installation_date, "
                       "type, manufacturer, logical_name, "
                       "host_id, slot_id, mounted, mount_point, "
                       "number_of_files, available_mb, bytes_stored, "
                       "completed, checksum, total_disk_write_time, "
                       "last_host_id) VALUES "
                       "({},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{})"))
            vals = [diskId, archive, instDate, type, manufacturer, logicalName,\
                    hostId, slotId, mounted, mountPoint, numberOfFiles,\
                    availableMb, bytesStored, completed, checksum,\
                    totalDiskWriteTime, lastHostId]
            addDiskHistEntry = 1

        res = self.query2(''.join(sql), args = vals)

        if completionDate is not None:
            complDate = self.convertTimeStamp(completionDate)
            sql = "UPDATE ngas_disks SET completion_date={} WHERE disk_id={}"
            self.query2(sql, args = (complDate, diskId))

        self.triggerEvents([diskId, mountPoint])
        return addDiskHistEntry


    def getLogicalNamesMountedDisks(self, host):
        """
        Get the Logical Names for the disks mounted on the given host.
        A list is returned, which contains the Logical Names of the disks
        mounted.

        host:      Name of host where the disk must be mounted (string).

        Returns:   List with Logical Names (list).
        """
        T = TRACE()

        sql = "SELECT logical_name FROM ngas_disks WHERE host_id={} AND mounted=1"
        res = self.query2(sql, args = (host,))
        if not res:
            return []
        logNames = []
        for disk in res:
            logNames.append(disk[0])
        return logNames


    def getMtPtFromDiskId(self, diskId):
        """
        Get the mount point for the disk referred to.

        diskId:      ID of the disk (string).

        Returns:     Mount point of disk or None if not mounted or
                     not found (string|None)
        """
        T = TRACE()

        sql = "SELECT mount_point FROM ngas_disks WHERE disk_id={}"
        res = self.query2(sql, args = (diskId,))
        if not res:
            return None
        return res[0][0]


    def getDiskIdsMtPtsMountedDisks(self, host):
        """
        Get the mount points for the disks mounted on the given host.
        A list is returned, which contains the Logical Names of the disks
        mounted.

        host:      Name of host where the disk must be mounted (string).

        Returns:   List with tuples containing Disk IDs and Mount
                   Points (list/tuple).
        """
        T = TRACE(5)

        sql = ("SELECT disk_id, mount_point FROM ngas_disks "
                "WHERE host_id={} AND mounted=1")
        res = self.query2(sql, args = (host,))
        if not res:
            return []
        return res


    def getSlotIdsMountedDisks(self,
                               host):
        """
        Get the Slot IDs for the disks mounted on the given host. A list is
        returned, which contains the Slot IDs of the disks mounted.

        host:      Name of host where the disk must be mounted (string).

        Returns:   List with Slot IDs (list).
        """
        T = TRACE()

        sql = "SELECT slot_id FROM ngas_disks WHERE host_id={} AND mounted=1"
        res = self.query2(sql, args = (host,))
        if not res:
            return []
        slotIds = []
        for disk in res:
            slotIds.append(disk[0])
        return slotIds


    def getDiskIdsMountedDisks(self, host, mtRootDir):
        """
        Get the Disk IDs for the disks mounted on the given host. A list is
        returned, which contains the Disk IDs of the disks mounted.

        host:        Name of host where the disk must be mounted (string).

        mtRootDir:   Base directory for NG/AMS (string).

        Returns:     List with Disk IDs (list).
        """
        T = TRACE()

        if not mtRootDir.endswith('/'):
            mtRootDir += "/"
        mtRootDir += "%"
        sql = ("SELECT disk_id FROM ngas_disks WHERE host_id={} AND mounted=1 "
                "AND mount_point LIKE {}")
        res = self.query2(sql, args = (host, mtRootDir))
        if not res:
            return []
        diskIds = []
        for disk in res:
            diskIds.append(disk[0])
        return diskIds


    def getDiskIdFromSlotId(self, host, slotId):
        """
        Get a Disk ID for corresponding Slot ID and host name.

        host:     Host name (string).

        slotId:   ID of slot (string).

        Returns:  Disk ID or None if no match found (string).
        """
        T = TRACE()

        sql = "SELECT disk_id FROM ngas_disks WHERE slot_id={} AND host_id={}"
        res = self.query2(sql, args = (slotId, host))
        if not res:
            return None
        return res[0][0]


    def getDiskIds(self):
        """
        Query the Disk IDs contained in the NGAS DB and return these in a list.

        Returns:    List with Disk IDs (list).
        """
        T = TRACE()

        sql = "SELECT disk_id FROM ngas_disks"
        res = self.query2(sql)
        if not res:
            return []
        retList = []
        for diskId in res:
            retList.append(diskId[0])
        return retList


    def getDiskInfoForSlotsAndHost(self, host, slotIdList):
        """
        From a given host and a given list of Slot IDs, the method
        returns a list with the disk info for the disks matching these.

        host:         Host name where the disks considered must be
                      mounted (string).

        slotIdList:   List of Slot IDs for the disk considered (list).

        Returns:      List with Disk Info objects  or [] if no matches
                      were found (list/ngamsDiskInfo).
        """
        T = TRACE()
        vals = []
        params = []
        sql = []

        sql.append("SELECT %s FROM ngas_disks nd WHERE " % ngamsDbCore.getNgasDisksCols())
        if slotIdList:
            for p in slotIdList:
                params.append('{}')
                vals.append(p)
            sql.append(" nd.slot_id IN (%s) AND " % ','.join(params))
        sql.append("nd.host_id={}")
        vals.append(host)
        res = self.query2(''.join(sql), args = vals)
        if not res:
            return []
        return res


    def getBestTargetDisk(self, diskIds, mtRootDir):
        """
        Find the best suitable target disk among a set of disks referred
        to by their Disk IDs. The condition is:

        Get ID for disk that is most full, which is Main Disk, mounted, has
        a Host ID corresponding to this host ID, which is not completed,
        which has the lowest Slot ID and which has the Disk ID among the set
        of disks defined for the given mime-type.

        diskIds:     List with Disk IDs to probe for (list).

        mtRootDir:   Base diretory for the NG/AMS Server (string).

        Returns:     Disk ID or None if no matches were found (string)
        """
        T = TRACE()

        params = []
        vals = []
        for di in diskIds:
            params.append('{}')
            vals.append(di)

        if not mtRootDir.endswith('/'):
            mtRootDir += "/"
        mtRootDir += "%"

        sql = ("SELECT disk_id FROM ngas_disks WHERE "
               "completed=0 AND disk_id IN (%s) "
               "AND mount_point LIKE {} "
               "ORDER BY bytes_stored desc, installation_date asc") % ','.join(params)
        vals.append(mtRootDir)
        res = self.query2(sql, args = vals)
        if not res:
            return None
        return res[0][0]


    def deleteDiskInfo(self, diskId, delFileInfo = 1):
        """
        Delete a record for a certain disk in the NGAS DB.

        CAUTION:  IF THE DB USER WITH WHICH THERE IS LOGGED IN HAS PERMISSION
                  TO EXECUTE DELETE STATEMENTS, THE INFORMATION ABOUT THE
                  DISK IN THE NGAS DB WILL BE DELETED! THIS INFORMATION
                  CANNOT BE RECOVERED!!

        diskId:        ID of disk for which to delete the entry (string).

        delFileInfo:   If set to 1, the file information for the files
                       stored on the disk is deleted as well (integer/0|1).

        Returns:       Reference to object itself.
        """
        T = TRACE()

        fileInfoDbmName = None
        fileInfoDbm = None
        try:

            # Get the information about the files on the disk (before this
            # information is deleted).
            if delFileInfo and self.getCreateDbSnapshot():
                diskInfo = ngamsDiskInfo.ngamsDiskInfo().read(self, diskId)
                fileInfoDbmName = self.genTmpFile('DISK_INFO')
                fileInfoDbm = ngamsDbm.ngamsDbm(fileInfoDbmName, cleanUpOnDestr=0, writePerm = 1)
                fileCount = 0
                for fileInfo in self.getFileInfoList(diskId, fetch_size=1000):
                    fileInfoDbm.add(str(fileCount), fileInfo)
                    fileCount += 1
                fileInfoDbm.sync()

            # Delete the disk info.
            sql = "DELETE FROM ngas_disks WHERE disk_id={}"
            self.query2(sql, args = (diskId,))

            # Delete file info if requested.
            if delFileInfo:
                sql = "DELETE FROM ngas_files WHERE disk_id={}"
                self.query2(sql, args = (diskId,))

                # Create a File Removal Status Document.
                if (self.getCreateDbSnapshot()):
                    op = NGAMS_DB_CH_FILE_DELETE
                    fileInfoDbm.initKeyPtr()
                    key, fileSqlInfo = fileInfoDbm.getNext()
                    while (key):
                        tmpFileObj = ngamsFileInfo.ngamsFileInfo().\
                                     unpackSqlResult(fileSqlInfo)
                        self.createDbRemFileChangeStatusDoc(diskInfo,
                                                            tmpFileObj)

                        key, fileSqlInfo = fileInfoDbm.getNext()

                    self.triggerEvents([diskInfo.getDiskId(),
                                        diskInfo.getMountPoint()])

            return self
        except Exception:
            logger.exception("Error deleting disk info from DB")
            raise
        finally:
            if fileInfoDbm:
                del fileInfoDbm
            if fileInfoDbmName:
                rmFile(fileInfoDbmName)


    def getLastDiskCheck(self, hostId = ""):
        """
        Queries all the Last Check Flags for all disks or for all disks
        currently mounted in a specific host. A Dictionary is returned
        containining the Disk IDs as keys, and the time for the last check
        in seconds. If the value is NULL in the DB, it is set to 0.

        hostId:    If specified, only disks mounted in this system are
                   taken into account (string).

        Returns:   Dictionary with entry for each disk. Key is Disk
                   ID (dictionary).
        """
        sql = []
        vals = []
        sql.append("SELECT disk_id, last_check from ngas_disks")
        if hostId:
            sql.append(" WHERE host_id = {}")
            vals.append(hostId)
        res = self.query2(''.join(sql), args = vals)
        diskDic = {}
        for diskInfo in res:
            if (not diskInfo[1]):
                timeSinceLastCheck = 0
            else:
                timeSinceLastCheck = fromiso8601(diskInfo[1], local=True)
            diskDic[diskInfo[0]] = timeSinceLastCheck
        return diskDic


    def setLastCheckDisk(self, diskId, timeSecs):
        """
        Update the Last Check Flag for a disk.

        diskId:           ID of disk for which to update record (string).

        timeSecs:         Time in seconds to set for the disk (integer).

        Returns:          Reference to object itself.
        """
        dbTime = self.convertTimeStamp(timeSecs)
        sql = "UPDATE ngas_disks SET last_check={} WHERE disk_id={}"
        self.query2(sql, args = (dbTime, diskId))
        self.triggerEvents()
        return self


    def getMinLastDiskCheck(self, hostId):
        """
        Get the timestamp for the disk that was checked longest time ago
        for all disks mounted in a specific NGAS Host.

        hostId:   Host ID of the host to consider (string).

        Returns:  Time since the 'oldest', last check (seconds since epoch)
                  (integer).
        """
        T = TRACE()

        sql = "SELECT min(last_check) FROM ngas_disks WHERE host_id={}"
        res = self.query2(sql, args = (hostId,))
        if not res:
            return None
        val = res[0][0]
        return fromiso8601(val, local=True)

    def getAvailableVolumes(self, hostId):
        """
        Returns a list of rows for all disks that are not marked as completed
        on ``hostId``.
        """
        sql = "SELECT %s FROM ngas_disks nd WHERE completed=0 AND host_id={0}"
        sql = sql % ngamsDbCore.getNgasDisksCols()
        return self.query2(sql, args=(hostId,))

    def updateDiskInfo(self, fileSize, diskId):
        """
        Update the row for the volume ``diskId`` hosting the new file of size
        ``fileSize``.
        """
        sqlQuery = "UPDATE ngas_disks SET " +\
                   "number_of_files=(number_of_files + 1), " +\
                   "bytes_stored=(bytes_stored + {0}) WHERE " +\
                   "disk_id={1}"
        self.query2(sqlQuery, args=(fileSize, diskId))
