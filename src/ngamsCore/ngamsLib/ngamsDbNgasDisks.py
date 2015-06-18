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

import os, types
from pccUt import PccUtTime
from ngamsCore import TRACE, getDiskSpaceAvail, iso8601ToSecs, rmFile, error, getUniqueNo, NGAMS_DB_CH_FILE_DELETE
import ngamsDbm, ngamsDbCore


# TODO: Avoid using these classes in this module (mutual dependency):
import ngamsFileInfo, ngamsDiskInfo


class ngamsDbNgasDisks(ngamsDbCore.ngamsDbCore):
    """
    Contains queries for accessing the NGAS Disks Table.
    """

    def updateDiskFileStatus(self,
                             diskId,
                             fileSize):
        """
        Update the NGAS Disks Table according to a new file archived.

        diskId:       Disk ID (string).
        
        fileSize:     Size of file as stored on disk (integer).

        Returns:      Reference to object itself.
        """
        T = TRACE()

        try:
            self.takeGlobalDbSem()
            sqlQuery = "SELECT number_of_files, available_mb, " +\
                       "bytes_stored, mount_point " +\
                       "FROM ngas_disks WHERE disk_id='" + diskId + "'"
            res = self.query(sqlQuery, ignoreEmptyRes=0)

            if (len(res[0]) != 1):
                format = "Cannot find entry for disk with ID: %s."
                errMsg = format % (diskId)
                raise Exception, errMsg

            # Generate new values.
            numberOfFiles    = res[0][0][0]
            bytesStored      = res[0][0][2]
            mountPoint       = res[0][0][3]
            newNumberOfFiles = (numberOfFiles  + 1)
            newAvailMb       = getDiskSpaceAvail(mountPoint)
            newBytesStored   = (bytesStored + fileSize)
            sqlQuery = "UPDATE ngas_disks SET" +\
                       " number_of_files=" + str(newNumberOfFiles) + "," +\
                       " available_mb=" + str(newAvailMb) + "," +\
                       " bytes_stored=" + str(newBytesStored) +\
                       " WHERE disk_id='" + diskId + "'"
            self.query(sqlQuery)
            self.relGlobalDbSem()
            self.triggerEvents()
        except Exception, e:
            self.relGlobalDbSem()
            raise e


    def diskInDb(self,
                 diskId):
        """
        Check if disk with the given Disk ID is available in the DB.

        diskId:    Disk ID (string).
        
        Returns:   1 = Disk ID was found, 0 = Disk ID not found (integer).
        """
        T = TRACE()

        sqlQuery = "SELECT disk_id FROM ngas_disks WHERE disk_id='"+diskId +"'"
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (res == [[]]):
            return 0
        else:
            if (res[0][0][0] == diskId):
                return 1
            else:
                return 0


    def setLogicalNameForDiskId(self,
                                diskId,
                                logicalName):
        """
        Change the Logical Name of the disk with the given Disk ID.

        diskId:        Disk ID (string).
        
        logicalName:   New Logical Name (string).

        Returns:       Void.
        """
        T = TRACE()
        
        sqlQuery = "UPDATE ngas_disks SET logical_name='" + logicalName +"' "+\
                   "WHERE disk_id='" + diskId + "'"
        self.query(sqlQuery)


    def getLogicalNameFromDiskId(self,
                                 diskId):
        """
        Query the Logical Name of a disk from the DB, based on
        the Disk ID of the disk.
        
        diskId:      Disk ID (string).    
    
        Returns:     Logical Name or None if not found (string | None).
        """
        T = TRACE()

        sqlQuery = "SELECT logical_name FROM ngas_disks WHERE disk_id='" +\
                   diskId + "'"
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (len(res[0]) == 1):
            return res[0][0][0]
        else:
            return None


    def getDiskCompleted(self,
                         diskId):
        """
        Check if a disk is marked in the NGAS DB as completed.

        diskId:    ID of the disk (string).

        Returns:   1 = completed, 0 = not completed. If the disk
                   is not registered None is returned (integer).
        """
        T = TRACE()
        
        sqlQuery = "SELECT completed FROM ngas_disks WHERE " +\
                   "disk_id = '" + diskId + "'"
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (len(res[0]) == 0):
            return None
        else:
            return res[0][0][0]


    def getSlotIdFromDiskId(self,
                            diskId):
        """
        Get the Slot ID for a disk, given by the Disk ID for the disk.

        diskId:    ID of the disk (string).

        Returns:   Slot ID of disk. If disk is not found None is
                   returned (string | None).
        """
        T = TRACE()
        
        sqlQuery = "SELECT slot_id FROM ngas_disks WHERE disk_id='" +\
                   diskId + "'"
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (len(res[0]) == 0):
            return None
        else:
            return res[0][0][0]


    def getDiskInfoFromDiskId(self,
                              diskId):
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
        
        sqlQuery = "SELECT " + ngamsDbCore.getNgasDisksCols() + " " +\
                   "FROM ngas_disks nd WHERE disk_id='" + diskId + "'"
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (res == [[]]):
            return []
        else:
            return res[0][0]


    def getDiskInfoFromDiskIdList(self,
                                  diskIdList):
        """
        Get disk information from a list of Disk IDs given. The result is
        returned in a list containing again lists with the field as described
        in documentation for ngamsDbBase.getDiskInfoFromDiskId().

        diskIdList:   List with Disk IDs (list/string).

        Returns:      List with disk information (list/list).
        """
        T = TRACE()
        
        sqlQuery = "SELECT " + ngamsDbCore.getNgasDisksCols() + " " +\
                   "FROM ngas_disks nd WHERE disk_id IN ("
        if (len(diskIdList)):
            tmpDiskIdDic = {}
            for id in diskIdList:
                tmpDiskIdDic[id] = 0
            for diskId in tmpDiskIdDic.keys():
                sqlQuery += "'" + diskId + "', "
            sqlQuery = sqlQuery[0:-2]
        else:
            return []
        sqlQuery += ")"
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (res == [[]]):
            return []
        else:
            return res[0]


    def getMaxDiskNumber(self,
                         cat = None):
        """
        Get the maximum disk index (number) in connection with the
        Logical Disk Names in the DB.

        cat:       'M' for Main, 'R' for Replication (string).

        Returns:   The maximum disk number or None if this could not
                   be generated (integer).
        """
        T = TRACE()
        
        sqlQuery = "SELECT logical_name FROM ngas_disks"
        if (cat):
            sqlQuery += " WHERE logical_name LIKE '%" + cat + "-%'"
        else:
            sqlQuery += " WHERE logical_name LIKE '%M-%' or " +\
                        "logical_name LIKE '%R-%'"
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (len(res[0]) == 0):
            retVal = None
        else:
            logNameDic = {}
            for subRes in res[0]:
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
        
        try:
            # Check if the entry already exists. If yes update it, otherwise
            # insert a new element.
            try:
                self.takeDbSem()
                instDate = self.convertTimeStamp(installationDate)
                self.relDbSem()
            except Exception, e:
                self.relDbSem()
                raise Exception, e

            #bytesStored = string.split(str(bytesStored), ".")[0]
            bytesStored = "%.0f" % float(bytesStored)
            if (self.diskInDb(diskId)):
                sqlQuery = "UPDATE ngas_disks SET " +\
                           "archive='" + archive + "', " +\
                           "installation_date='" + instDate + "', " +\
                           "type='" + type + "', " +\
                           "manufacturer='" + manufacturer + "', " +\
                           "logical_name='" + logicalName + "', " +\
                           "host_id='" + hostId + "', " +\
                           "slot_id='" + slotId + "', " +\
                           "mounted=" + str(mounted) + ", " +\
                           "mount_point='" + mountPoint + "', " +\
                           "number_of_files=" + str(numberOfFiles) + ", "+\
                           "available_mb=" + str(availableMb) + ", " +\
                           "bytes_stored=" + str(bytesStored) + ", " +\
                           "completed=" + str(completed) + ", " +\
                           "checksum='" + checksum + "', " +\
                           "total_disk_write_time=" + str(totalDiskWriteTime)

                if (lastCheck != ""):
                    try:
                        self.takeDbSem()
                        lastCheckTmp = self.convertTimeStamp(lastCheck)
                        self.relDbSem()
                    except Exception, e:
                        self.relDbSem()
                        raise Exception, e      
                        
                    sqlQuery += ", last_check='" + lastCheckTmp + "'"

                if (lastHostId != None):
                    sqlQuery += ", last_host_id='" + lastHostId + "'"

                sqlQuery += " WHERE disk_id='" + diskId + "'"
                addDiskHistEntry = 0
            else:
                sqlQuery = "INSERT INTO ngas_disks " +\
                           "(disk_id, archive, installation_date, " +\
                           "type, manufacturer, logical_name, " +\
                           "host_id, slot_id, mounted, mount_point, "+\
                           "number_of_files, available_mb, bytes_stored, " +\
                           "completed, checksum, total_disk_write_time, " +\
                           "last_host_id) VALUES " +\
                           "('" + diskId + "', " +\
                           "'" + archive + "', " +\
                           "'" + instDate + "', " +\
                           "'" + type + "', " +\
                           "'" + manufacturer + "', " +\
                           "'" + logicalName + "', " +\
                           "'" + hostId + "', " +\
                           "'" + slotId + "', " +\
                           str(mounted) + ", " +\
                           "'" + mountPoint + "', " +\
                           str(numberOfFiles) + ", " +\
                           str(availableMb) + ", " +\
                           str(bytesStored) + ", " +\
                           str(completed) + ", " +\
                           "'" + checksum + "', " +\
                           str(totalDiskWriteTime) + ", " +\
                           "'" + lastHostId + "')"
                addDiskHistEntry = 1
            res = self.query(sqlQuery)

            if (completionDate != ""):
                try:
                    self.takeDbSem()
                    complDate = self.convertTimeStamp(completionDate)
                    self.relDbSem()
                except Exception, e:
                    self.relDbSem()
                    raise Exception, e                   
                sqlQuery = "UPDATE ngas_disks SET completion_date='" +\
                            complDate + "' " + "WHERE disk_id='" + diskId + "'"
                self.query(sqlQuery)

            self.triggerEvents([diskId, mountPoint])
            return addDiskHistEntry
        except Exception, e:   
            raise e

            
    def getLogicalNamesMountedDisks(self,
                                    host):
        """
        Get the Logical Names for the disks mounted on the given host.
        A list is returned, which contains the Logical Names of the disks
        mounted.

        host:      Name of host where the disk must be mounted (string).

        Returns:   List with Logical Names (list).
        """
        T = TRACE()
        
        sqlQuery = "SELECT logical_name FROM ngas_disks WHERE host_id='" +\
                   host + "' AND mounted=1"
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (len(res[0]) == 0):
            return []
        else:
            logNames = []
            for disk in res[0]:
                logNames.append(disk[0])
            return logNames
  

    def getMtPtFromDiskId(self,
                          diskId):
        """
        Get the mount point for the disk referred to.

        diskId:      ID of the disk (string).

        Returns:     Mount point of disk or None if not mounted or
                     not found (string|None)
        """
        T = TRACE()
        
        sqlQuery = "SELECT mount_point FROM ngas_disks WHERE disk_id='" +\
                   diskId + "'"
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (len(res[0]) == 0):
            return None
        else:
            return res[0][0][0]


    def getDiskIdsMtPtsMountedDisks(self,
                                    host):
        """
        Get the mount points for the disks mounted on the given host.
        A list is returned, which contains the Logical Names of the disks
        mounted.

        host:      Name of host where the disk must be mounted (string).

        Returns:   List with tuples containing Disk IDs and Mount
                   Points (list/tuple).
        """
        T = TRACE(5)
        
        sqlQuery = "SELECT disk_id, mount_point FROM ngas_disks " +\
                   "WHERE host_id='" + host + "' AND mounted=1"
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (len(res[0]) == 0):
            return []
        else:
            return res[0]


    def getSlotIdsMountedDisks(self,
                               host):
        """
        Get the Slot IDs for the disks mounted on the given host. A list is
        returned, which contains the Slot IDs of the disks mounted.

        host:      Name of host where the disk must be mounted (string).

        Returns:   List with Slot IDs (list).
        """
        T = TRACE()
        
        sqlQuery = "SELECT slot_id FROM ngas_disks WHERE host_id='" +\
                   host + "' AND mounted=1"
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (len(res[0]) == 0):
            return []
        else:
            slotIds = []
            for disk in res[0]:
                slotIds.append(disk[0])
            return slotIds


    def getDiskIdsMountedDisks(self,
                               host,
                               mtRootDir):
        """
        Get the Disk IDs for the disks mounted on the given host. A list is
        returned, which contains the Disk IDs of the disks mounted.

        host:        Name of host where the disk must be mounted (string).

        mtRootDir:   Base directory for NG/AMS (string).

        Returns:     List with Disk IDs (list).
        """
        T = TRACE()

        if (mtRootDir[-1] != "/"): mtRootDir += "/"
        mtRootDir += "%"
        sqlQuery = "SELECT disk_id FROM ngas_disks WHERE host_id='" +\
                   host + "' AND mounted=1 AND mount_point LIKE '" +\
                   mtRootDir + "'"
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (res == [[]]):
            return []
        else:
            diskIds = []
            for disk in res[0]:
                diskIds.append(disk[0])
            return diskIds


    def getDiskIdFromSlotId(self,
                            host,
                            slotId):
        """
        Get a Disk ID for corresponding Slot ID and host name.

        host:     Host name (string).

        slotId:   ID of slot (string).

        Returns:  Disk ID or None if no match found (string).
        """
        T = TRACE()
        
        sqlQuery = "SELECT disk_id FROM ngas_disks WHERE slot_id='" +\
                   slotId + "' AND host_id='" + host + "'"
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (res == [[]]):
            return None
        else:
            return res[0][0][0]


    def getDiskIds(self):
        """
        Query the Disk IDs contained in the NGAS DB and return these in a list.

        Returns:    List with Disk IDs (list).
        """
        T = TRACE()
        
        sqlQuery = "SELECT disk_id FROM ngas_disks"
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (res == [[]]):
            return []
        else:
            retList = []
            for diskId in res[0]:
                retList.append(diskId[0])
            return retList


    def getDiskInfoForSlotsAndHost(self,
                                   host,
                                   slotIdList):
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
        
        if (slotIdList):
            slotIds = "("
            for id in slotIdList: slotIds = slotIds + "'" + id + "',"
            slotIds = slotIds[0:-1] + ")"
        sqlQuery = "SELECT " + ngamsDbCore.getNgasDisksCols() +\
                   " FROM ngas_disks nd WHERE "
        if (slotIdList): sqlQuery += " nd.slot_id IN " + slotIds + " AND "
        sqlQuery += "nd.host_id='" + host + "'"
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (res == [[]]):
            return []
        else:
            return res[0]


    def getBestTargetDisk(self,
                          diskIds,
                          mtRootDir):
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
        
        # Create a list of IDs for the SQL statement.
        ids = "("
        for id in diskIds:
            ids = ids + "'" + id + "',"
        ids = ids[0:-1] + ")"

        if (mtRootDir[-1] != "/"): mtRootDir += "/"
        mtRootDir += "%"

        # Get ID for best suitable disk.
        sqlQuery = "SELECT disk_id FROM ngas_disks WHERE " +\
                   "completed=0 AND disk_id IN " + ids + " " +\
                   "AND mount_point LIKE '" + mtRootDir + "' " +\
                   "ORDER BY bytes_stored desc, installation_date asc"
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (res == [[]]):
            return None
        else:
            return res[0][0][0]


    def deleteDiskInfo(self,
                       diskId,
                       delFileInfo = 1):
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
        
        fileInfoDbmName = fileInfoDbm = None
        try:
            if (delFileInfo and self.getCreateDbSnapshot()):
                diskInfo = ngamsDiskInfo.ngamsDiskInfo().read(self, diskId)
            else:
                diskInfo = None
                
            if (delFileInfo):
                # Get the information about the files on the disk (before this
                # information is deleted).
                if (self.getCreateDbSnapshot()):
                    ts = PccUtTime.TimeStamp().getTimeStamp()
                    fileName = ts + "_" + str(getUniqueNo()) + "_DISK_INFO"
                    fileInfoDbmName = os.path.\
                                      normpath(self.getDbTmpDir() + "/" +\
                                               fileName)
                    fileInfoDbmName = self.dumpFileInfoList(diskId,
                                                            fileListDbmName=\
                                                            fileInfoDbmName)
                                                            
            # Delete the disk info.
            sqlQuery = "DELETE FROM ngas_disks WHERE disk_id='" + diskId + "'"
            self.query(sqlQuery)
            
            # Delete file info if requested.
            if (delFileInfo):
                sqlQuery = "DELETE FROM ngas_files WHERE disk_id='"+diskId+"'"
                self.query(sqlQuery)

                # Create a File Removal Status Document.
                if (self.getCreateDbSnapshot()):
                    op = NGAMS_DB_CH_FILE_DELETE
                    fileInfoDbm = ngamsDbm.ngamsDbm(fileInfoDbmName)
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
            del fileInfoDbm
            rmFile(fileInfoDbmName + "*")
            return self
        except Exception, e:
            error("Error deleting disk info from DB: %s" % str(e))
            if (fileInfoDbm): del fileInfoDbm
            if (fileInfoDbmName): rmFile(fileInfoDbmName + "*")
            raise e


    def getLastDiskCheck(self,
                         hostId = ""):
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
        sqlQuery = "SELECT disk_id, last_check from ngas_disks"
        if (hostId != ""): sqlQuery += " WHERE host_id = '" + hostId + "'"
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        diskDic = {}
        for diskInfo in res[0]:
            if (not diskInfo[1]):
                timeSinceLastCheck = 0
            else:
                if (type(diskInfo[1]) == types.IntType):
                    timeSinceLastCheck = int(diskInfo[1])
                elif (type(diskInfo[1]) == types.StringType):
                    # Expects an ISO 8601 timestamp.
                    timeSinceLastCheck = int(iso8601ToSecs(diskInfo[1]) + 0.5)
                else:                   
                    timeSinceLastCheck =\
                                       self.convertTimeStampToMx(diskInfo[1]).\
                                       ticks()
            diskDic[diskInfo[0]] = timeSinceLastCheck
        return diskDic


    def setLastCheckDisk(self,
                         diskId,
                         timeSecs):
        """
        Update the Last Check Flag for a disk.

        diskId:           ID of disk for which to update record (string).
        
        timeSecs:         Time in seconds to set for the disk (integer).

        Returns:          Reference to object itself.
        """
        try:
            try:
                self.takeDbSem()
                dbTime = self.convertTimeStamp(timeSecs)
                self.relDbSem()
            except Exception, e:
                self.relDbSem()
                raise Exception, e
            sqlQuery = "UPDATE ngas_disks SET last_check = '" + dbTime +\
                       "' WHERE disk_id='" + diskId + "'"
            self.query(sqlQuery)
            self.triggerEvents()
            return self
        except Exception, e:   
            raise e


    def getMinLastDiskCheck(self,
                            hostId):
        """
        Get the timestamp for the disk that was checked longest time ago
        for all disks mounted in a specific NGAS Host.

        hostId:   Host ID of the host to consider (string).

        Returns:  Time since the 'oldest', last check (seconds since epoch)
                  (integer).
        """
        T = TRACE()
        
        sqlQuery = "SELECT min(last_check) FROM ngas_disks WHERE " +\
                   "host_id='" + hostId + "'"
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (len(res[0]) == 1):
            if (res[0][0][0]):
                if (type(res[0][0][0]) == types.IntType):
                    return res[0][0][0]
                elif (type(res[0][0][0]) == types.StringType):
                    # Expects an ISO 8601 timestamp.
                    return int(iso8601ToSecs(res[0][0][0]) + 0.5)
                else:
                    dt = self.convertTimeStampToMx(res[0][0][0])
                    return int(dt.ticks() + 0.5)
            else:
                return None
        else:
            return None


# EOF
