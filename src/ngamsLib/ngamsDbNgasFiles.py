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
# "@(#) $Id: ngamsDbNgasFiles.py,v 1.11 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  07/03/2008  Created
#

"""
Contains queries for accessing the NGAS Files Table.

This class is not supposed to be used standalone in the present implementation.
It should be used as part of the ngamsDbBase parent classes.
"""

import cPickle

from   ngams import *
import ngamsLib, ngamsDbm, ngamsDbCore

# TODO: Avoid using these classes in this module (mutual dependency):
import ngamsFileInfo, ngamsDiskInfo, ngamsStatus, ngamsFileList


class ngamsDbNgasFiles(ngamsDbCore.ngamsDbCore):
    """
    Contains queries for accessing the NGAS Files Table.
    """

    def fileInDb(self,
                 diskId,
                 fileId,
                 fileVersion = -1):
        """
        Check if file with the given File ID is registered in NGAS DB
        in connection with the given Disk ID.
        
        diskId:        Disk ID (string)

        fileId:        File ID (string).
        
        fileVersion:   Version of the file. If -1 version is not taken
                       into account (integer).

        Returns:       1 = file found, 0 = file no found (integer).
        """
        T = TRACE()

        if (fileVersion != -1):
            sqlQuery = "SELECT file_id FROM ngas_files WHERE file_id='" +\
                       fileId + "' AND disk_id='" + diskId + "' AND " +\
                       "file_version=" + str(fileVersion)
        else:
            sqlQuery = "SELECT file_id FROM ngas_files WHERE file_id='" +\
                       fileId + "' AND disk_id='" + diskId + "'"
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (len(res[0]) == 1):
            if (res[0][0][0] == fileId):
                return 1
            else:
                return 0
        else:
            return 0
        

    def getFileInfoFromDiskIdFilename(self,
                                      diskId,
                                      filename):
        """
        The method queries the file information for a file referred to by the
        Disk ID for the disk hosting it and the filename as stored in the
        NGAS DB.

        diskId:      ID for disk hosting the file (string).

        filename:    NGAS (relative) filename (string).
                         
        Returns:     Return ngamsFileInfo object with the information for the
                     file if found or None if the file was not found
                     (ngamsFileInfo|None).
        """
        T = TRACE()

        # Query for the file.
        sqlQuery = "SELECT %s FROM ngas_files nf WHERE nf.disk_id='%s' AND " +\
                   "nf.file_name='%s'"
        sqlQuery = sqlQuery % (ngamsDbCore.getNgasFilesCols(), diskId,
                               filename)

        # Execute the query directly and return the result.
        res = self.query(sqlQuery)
        if ((len(res) > 0) and (res != [[]])):
            return ngamsFileInfo.ngamsFileInfo().unpackSqlResult(res[0][0])
        else:
            return None


    def getFileInfoList(self,
                        diskId,
                        fileId = "",
                        fileVersion = -1,
                        ignore = None):
        """
        The function queries a set of files matching the conditions
        specified in the input parameters.

        diskId:        Disk ID of disk hosting the file(s) (string).
        
        fileId:        File ID of files to consider. Wildcards can be
                       used (string).
        
        fileVersion:   Version of file(s) to consider. If set to -1 this
                       is not taken into account (integer).

        ignore:        If set to 0 or 1, this value of ignore will be
                       queried for. If set to None, ignore is not
                       considered (None|0|1).

        Returns:       Cursor object (<NG/AMS DB Cursor Object API>).
        """
        T = TRACE()

        fileId = re.sub("\*", "%", fileId)
        sqlQuery = "SELECT " + ngamsDbCore.getNgasFilesCols() +\
                   " FROM ngas_files nf WHERE"

        if (fileId.find("%") != -1):
            fileIdStatement = " nf.file_id LIKE '" + fileId + "'"
        elif (fileId != ""):
            fileIdStatement = " nf.file_id='" + fileId + "'"
        else:
            fileIdStatement = ""

        if (diskId != ""):
            diskIdStatement = " nf.disk_id='" + diskId + "'"
        else:
            diskIdStatement = ""

        if (fileIdStatement): sqlQuery += fileIdStatement
        if (diskIdStatement and fileIdStatement):
            sqlQuery += " AND" + diskIdStatement
        elif (diskIdStatement):
            sqlQuery += diskIdStatement
        if (fileVersion != -1):
            sqlQuery += " AND nf.file_version=" + str(fileVersion)
        if (ignore != None): sqlQuery += " AND nf.ignore=%d" % int(ignore)

        # Create a cursor and perform the query.
        curObj = self.dbCursor(sqlQuery)
        
        return curObj


    def dumpFileInfoList(self,
                         diskId,
                         fileId = "",
                         fileVersion = -1,
                         ignore = None,
                         fileListDbmName = ""):
        """
        The function queries the same info as getFileInfoList(). However,
        rathen than returning a cursor object, it retrieves the info itself
        and stores this in a DBM. This is done in a 'safe manner', whereby
        it is tried to recover from the situation where less files are returned
        than expected.

        ignore:             If set to 0 or 1, this value of ignore will be
                            queried for. If set to None, ignore is not
                            considered (None|0|1).
                       
        fileListDbmName:    Name of DBM where to store the queried info
                            (string).

        Returns:            Complete name of DBM containing the requested
                            info (string).
        """
        T = TRACE()

        # Retrieve the theoretical number of files the query should produce.
        # Try first to get the expected number of files, which will be returned
        expNoOfFiles = -1
        if (self.getDbVerify()):
            for n in range(1):
                noOfFiles = self.getNumberOfFiles(diskId, fileId, fileVersion,
                                                  ignore)
                if (noOfFiles > expNoOfFiles): expNoOfFiles = noOfFiles
                time.sleep(0.050)

        # Generate final name of DBM + create DBM.
        if (not fileListDbmName):
            fileListDbmName = self.getDbTmpDir() + "/" +\
                              ngamsLib.genUniqueFilename("_FILE_INFO")

        # Retrieve/dump the files (up to 5 times).
        curObj = None
        fileListDbm = None
        for n in range(5):
            try:
                fileListDbm = ngamsDbm.ngamsDbm(fileListDbmName,
                                                cleanUpOnDestr = 0,
                                                writePerm = 1)
                fileListDbmName = fileListDbm.getDbmName()
                dbCur = self.getFileInfoList(diskId, fileId, fileVersion,
                                             ignore)
                fileCount = 0
                while (1):
                    res = dbCur.fetch(100)
                    if (not res): break
                    for fileInfo in res:
                        fileListDbm.add(str(fileCount), fileInfo)
                        fileCount += 1
                    time.sleep(0.010)
                fileListDbm.sync()
            except Exception, e:
                rmFile(fileListDbmName + "*")
                if (curObj): del curObj
                if (fileListDbm): del fileListDbm
                raise Exception, e
                
            if (curObj): del dbCur

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
                if (fileListDbm): del fileListDbm
                rmFile(fileListDbmName + "*")
                if (n < 4):
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
                break
        return fileListDbmName


    def getLatestFileVersion(self,
                             fileId):
        """
        The method queries the latest File Version for the file with the given
        File ID. If a file with the given ID does not exist, -1 is returned.

        fileId:    File ID (string).

        Returns:   Latest File Version (integer).
        """
        T = TRACE()
        
        sqlQuery = "SELECT max(file_version) FROM ngas_files WHERE "+\
                   "file_id='" + fileId + "'"
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (len(res[0])):
            if (res[0][0][0] == None):
                return -1
            else:
                return int(res[0][0][0])
        else:
            return -1
    
        
    def setFileStatus(self,
                      fileId,
                      fileVersion,
                      diskId,
                      status):
        """
        Set the checksum value in the ngas_files table.

        fileId:        ID of file (string).
        
        fileVersion:   Version of file (integer).

        diskId:        Disk ID for disk where file is stored (string).
        
        status:        File Status (8 bytes) (string).

        Returns:       Reference to object itself.
        """
        T = TRACE(5)
        
        try:
            sqlQuery = "UPDATE ngas_files SET file_status='" + status + "' " +\
                       "WHERE file_id='" + fileId + "' AND file_version=" +\
                       str(fileVersion) + " AND disk_id='" + diskId + "'"
            res = self.query(sqlQuery)
            self.triggerEvents()
            return self
        except Exception, e:   
            raise e

                   
    def deleteFileInfo(self,
                       diskId,
                       fileId,
                       fileVersion,
                       genSnapshot = 1):
        """
        Delete one record for a certain file in the NGAS DB.

        CAUTION:  IF THE DB USER WITH WHICH THERE IS LOGGED IN HAS PERMISSION
                  TO EXECUTE DELETE STATEMENTS, THE INFORMATION ABOUT THE
                  FILE(S) IN THE NGAS DB WILL BE DELETED! THIS INFORMATION
                  CANNOT BE RECOVERED!!

        diskId:         ID of disk hosting the file (string).

        fileId:         ID of file to be deleted. No wildcards accepted
                        (string).

        fileVersion:    Version of file to delete (integer)

        genSnapshot:    Generate Db Snapshot (integer/0|1).

        Returns:        Reference to object itself.
        """
        T = TRACE()

        fileInfoDbmName = fileInfoDbm = None
        try:
            # We have to update some fields of the disk hosting the file
            # when we delete a file (number_of_files, available_mb,
            # bytes_stored, also maybe later: checksum.
            dbDiskInfo = self.getDiskInfoFromDiskId(diskId)
            fileInfoDbmName = self.dumpFileInfoList(diskId, fileId,
                                                    fileVersion, None,
                                                    fileInfoDbmName)
            fileInfoDbm = ngamsDbm.ngamsDbm(fileInfoDbmName)
            if (fileInfoDbm.getCount() > 0):
                dbFileInfo = fileInfoDbm.get("0")
            else:
                format = "Cannot remove file. File ID: %s, " +\
                         "File Version: %d, Disk ID: %s"
                errMsg = format % (fileId, fileVersion, diskId)
                raise Exception, errMsg
            sqlQuery = "DELETE FROM ngas_files WHERE disk_id='" +\
                       diskId + "' AND file_id='" + fileId +\
                       "' AND file_version=" + str(fileVersion)
            self.query(sqlQuery)

            # Create a File Removal Status Document.
            if (self.getCreateDbSnapshot() and genSnapshot):
                tmpFileObj = ngamsFileInfo.ngamsFileInfo().\
                             unpackSqlResult(dbFileInfo)
                self.createDbFileChangeStatusDoc(NGAMS_DB_CH_FILE_DELETE,
                                                 [tmpFileObj])

            # Now update the ngas_disks entry for the disk hosting the file.
            if (dbDiskInfo):
                newNumberOfFiles = (dbDiskInfo[ngamsDbCore.\
                                               NGAS_DISKS_NO_OF_FILES] - 1)
                if (newNumberOfFiles < 0): newNumberOfFiles = 0
                newAvailMb = (dbDiskInfo[ngamsDbCore.NGAS_DISKS_AVAIL_MB] +
                              int(float(dbFileInfo[ngamsDbCore.\
                                                   NGAS_FILES_FILE_SIZE])/1e6))
                newBytesStored = (dbDiskInfo[ngamsDbCore.\
                                             NGAS_DISKS_BYTES_STORED] -
                                  dbFileInfo[ngamsDbCore.NGAS_FILES_FILE_SIZE])
                if (newBytesStored < 0): newBytesStored = 0
                sqlQuery = "UPDATE ngas_disks SET" +\
                           " number_of_files=" + str(newNumberOfFiles) + "," +\
                           " available_mb=" + str(newAvailMb) + "," +\
                           " bytes_stored=" + str(newBytesStored) +\
                           " WHERE disk_id='" + diskId + "'"
                self.query(sqlQuery)

            self.triggerEvents()
            if (fileInfoDbmName): rmFile(fileInfoDbmName)
            return self
        except Exception, e:
            if (fileInfoDbmName): rmFile(fileInfoDbmName)
            try:
                del cursorObj
            except:
                pass
            raise e
    

    def getSumBytesStored(self,
                          diskId):
        """
        Get the total sum of the sizes of the data files stored on a disk
        and return this.

        diskId:    Disk ID of disk to get the sum for (string).

        Return:    Total sum of bytes stored on the disk (integer).
        """
        T = TRACE()
        
        sqlQuery = "SELECT sum(file_size) from ngas_files WHERE " +\
                   "disk_id='" + diskId + "'"
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (len(res[0]) == 1):
            if (res[0][0][0]):
                return res[0][0][0]
            else:
                return 0
        else:
            return 0


    def createDbFileChangeStatusDoc(self,
                                    operation,
                                    fileInfoObjList,
                                    diskInfoObjList = []):
        """
        The function creates a pickle document in the '<Disk Mt Pt>/.db/cache'
        directory from the information in the 'fileInfoObj' object.

        operation:        Has to be either ngams.NGAMS_DB_CH_FILE_INSERT or
                          ngams.NGAMS_DB_CH_FILE_DELETE (string). 

        fileInfoObj:      List of instances of NG/AMS File Info Object
                          containing the information about the file
                          (list/ngamsFileInfo).

        diskInfoObjList:  It is possible to give the information about the
                          disk(s) in question via a list of ngamsDiskInfo
                          objects (list/ngamsDiskInfo).
                         
        Returns:          Void.
        """
        T = TRACE()

        # TODO: Implementation concern: This class is suppose to be
        # at a lower level in the hierarchie than the ngamsFileInfo,
        # ngamsFileList, ngamsDiskInfo and ngamsStatus classes and as
        # such these should not be used from within this class. All usage
        # of these classes in the ngamsDbBase class should be analyzed.
        # Probably these classes should be made base classes for this class.

        # TODO: Potential memory bottleneck.

        timeStamp = PccUtTime.TimeStamp().getTimeStamp()

        # Sort the File Info Objects according to disks.
        fileInfoObjDic = {}
        for fileInfo in fileInfoObjList:
            if (not fileInfoObjDic.has_key(fileInfo.getDiskId())):
                fileInfoObjDic[fileInfo.getDiskId()] = []
            fileInfoObjDic[fileInfo.getDiskId()].append(fileInfo)

        # Get the mount points for the various disks concerned.
        mtPtDic = {}
        if (diskInfoObjList == []):
            diskIdMtPtList = self.getDiskIdsMtPtsMountedDisks(getHostId())
            for diskId, mtPt in diskIdMtPtList:
                mtPtDic[diskId] = mtPt
        else:
            for diskInfoObj in diskInfoObjList:
                mtPtDic[diskInfoObj.getDiskId()] = diskInfoObj.getMountPoint()

        # Create on each disk the relevant DB Change Status Document.
        tmpFileList = None
        for diskId in fileInfoObjDic.keys():
            statFilePath = os.path.normpath(mtPtDic[diskId] + "/" +\
                                            NGAMS_DB_CH_CACHE)
            statFilename = os.path.normpath(statFilePath + "/" +\
                                            timeStamp + "_" +\
                                            str(getUniqueNo()) + ".pickle")
            tmpStatFilename = statFilename + ".tmp"

            if ((len(fileInfoObjList) == 1) and (diskInfoObjList == [])):
                tmpStatObj = fileInfoObjList[0].setTag(operation)
            else:
                dbId = self.getDbServer() + "." + self.getDbName()
                tmpFileObjList = fileInfoObjDic[diskId]
                tmpFileList = ngamsFileList.ngamsFileList(dbId, operation)
                for fileObj in tmpFileObjList:
                    tmpFileList.addFileInfoObj(fileObj)        
                tmpStatObj = ngamsStatus.ngamsStatus().\
                             setDate(timeStamp).\
                             setVersion(getNgamsVersion()).\
                             setHostId(getHostId()).\
                             setMessage(dbId).\
                             addFileList(tmpFileList)
            
            info(4,"Creating Temporary DB Snapshot: " + statFilename)
            pickleFo = None
            try:
                pickleFo = open(tmpStatFilename, "w")
                cPickle.dump(tmpStatObj, pickleFo, 1)
                pickleFo.close()
                commands.getstatusoutput("mv " + tmpStatFilename + " " +\
                                         statFilename)
            except Exception, e:
                if (pickleFo): pickleFo.close()
                raise e
            del tmpStatObj
            tmpStatObj = None
            del tmpFileList
            tmpFileList = None
        

    def createDbRemFileChangeStatusDoc(self,
                                       diskInfoObj,
                                       fileInfoObj):
        """
        The function creates a File Removal Status Document with the
        information about a file, which has been removed from the DB and
        which should be removed from the DB Snapshot for the disk concerned.

        diskInfoObj:      Disk Info Object with info for disk concerned
                          (ngamsDiskInfo).

        fileInfoObj:      Instance of NG/AMS File Info Object
                          containing the information about the file
                          (ngamsFileInfo).
                         
        Returns:          Void.
        """
        T = TRACE()

        # TODO: Implementation concern: This class is suppose to be
        # at a lower level in the hierarchie than the ngamsFileInfo,
        # ngamsFileList, ngamsDiskInfo and ngamsStatus classes and as
        # such these should not be used from within this class. All usage
        # of these classes in the ngamsDbBase class should be analyzed.
        # Probably these classes should be made base classes for this class.

        timeStamp = PccUtTime.TimeStamp().getTimeStamp()
        mtPt = diskInfoObj.getMountPoint()
        statFilePath = os.path.normpath("%s/%s" % (mtPt, NGAMS_DB_CH_CACHE))
        statFilename = os.path.normpath("%s/%s_%s.%s" %\
                                        (statFilePath, timeStamp,
                                         str(getUniqueNo()),
                                         NGAMS_PICKLE_FILE_EXT))
        tmpStatFilename = os.path.normpath("%s/%s_%s.%s.%s" %\
                                           (statFilePath, timeStamp,
                                            str(getUniqueNo()),
                                            NGAMS_PICKLE_FILE_EXT, 
                                            NGAMS_TMP_FILE_EXT))
        
        info(4,"Creating Temporary DB Snapshot: %s" % statFilename)
        pickleFo = None
        try:
            fileInfoList = [fileInfoObj.getDiskId()] +\
                           [fileInfoObj.getFileId()] +\
                           [fileInfoObj.getFileVersion()]
            pickleFo = open(tmpStatFilename, "w")
            cPickle.dump(fileInfoList, pickleFo, 1)
            pickleFo.close()
            commands.getstatusoutput("mv %s %s" % (tmpStatFilename,
                                                   statFilename))
        except Exception, e:
            if (pickleFo): pickleFo.close()
            rmFile(tmpStatFilename)
            raise e
        

    def getIngDate(self,
                   diskId,
                   fileId,
                   fileVersion):
        """
        Get the ingestion date for the file.
        
        diskId:         ID of disk hosting the file (string).

        fileId:         ID of file to be deleted. No wildcards accepted
                        (string).

        fileVersion:    Version of file to delete (integer)

        Returns:        Ingestion date for file or None if file not found
                        (string/ISO 8601 | None).
        """
        T = TRACE()
        
        sqlQuery = "SELECT ingestion_date FROM ngas_files WHERE "+\
                   "disk_id = '%s' AND file_id = '%s' AND file_version = %d"
        sqlQuery = sqlQuery % (diskId, fileId, fileVersion)
        res = self.query(sqlQuery, ignoreEmptyRes = 0)
        if (len(res[0])):
            if (res[0][0][0] == None):
                return None
            else:
                return timeRef2Iso8601(res[0][0][0])
        else:
            return None


# EOF
