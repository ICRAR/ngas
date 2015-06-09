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
# "@(#) $Id: ngamsDiskInfo.py,v 1.8 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  12/05/2001  Created
#

"""
Contains tools for handling the disk configuration.
"""

import xml.dom.minidom
import PccUtTime
from   ngams import *
import ngamsLib, ngamsFileInfo


def getStorageSetIdFromDiskId(dbConObj,
                              ngamsCfgObj,
                              diskId):
    """
    Return a Storage Set ID from a Disk ID.

    dbConObj:       DB connection object (ngamsDb).
    
    ngamsCfgObj:    NG/AMS Configuration object (ngamsConfig).
    
    diskId:         Disk ID (string).

    Returns:        Storage Set ID for the disk referred to (string).
    """
    # Disk ID -> Slot ID -> Storage Set ID.
    slotId = dbConObj.getSlotIdFromDiskId(diskId)
    if (slotId == None):
        errMsg = genLog("NGAMS_ER_MISSING_DISK_ID", [diskId])
        error(errMsg)
        raise Exception, errMsg  
    set = ngamsCfgObj.getStorageSetFromSlotId(slotId)
    return set.getStorageSetId()


class ngamsDiskInfo:
    """
    Object to handle the information for an NGAS disk.
    """
    
    def __init__(self):
        """
        Constructor method.
        """
        # DB columns.
        self.__diskId             = ""
        self.__archive            = ""
        self.__installationDate   = ""
        self.__type               = ""
        self.__manufacturer       = ""
        self.__logicalName        = ""        
        self.__hostId             = ""
        self.__slotId             = ""
        self.__mounted            = -1
        self.__mountPoint         = ""
        self.__numberOfFiles      = -1
        self.__availableMb        = -1
        self.__bytesStored        = -1.0
        self.__completed          = 0
        self.__completionDate     = ""
        self.__checksum           = ""
        self.__totalDiskWriteTime = 0.0
        self.__lastCheck          = ""
        self.__lastHostId         = None

        # Additional information for a disk.
        self.__storageSetId  = ""
        self.__fileList = []


    def getObjStatus(self):
        """
        Return a list with the current status of the object. The format
        of the list is:

          [[<xml attribute name>, <value>, ...], ...]

        Returns:    List with object status (list/list).
        """
        return [["DiskId", self.getDiskId()],
                ["Archive", self.getArchive()],
                ["InstallationDate", self.getInstallationDate()],
                ["Type", self.getType()],
                ["Manufacturer", self.getManufacturer()],
                ["LogicalName", self.getLogicalName()],
                ["HostId", self.getHostId()],
                ["SlotId", self.getSlotId()],
                ["Mounted", self.getMounted()],
                ["MountPoint", self.getMountPoint()],
                ["NumberOfFiles", self.getNumberOfFiles()],
                ["AvailableMb", self.getAvailableMb()],
                ["BytesStored", self.getBytesStoredStr()],
                ["Completed", self.getCompleted()],
                ["CompletionDate", self.getCompletionDate()],
                ["Checksum", self.getChecksum()],
                ["TotalDiskWriteTime", self.getTotalDiskWriteTime()],
                ["LastCheck", self.getLastCheck()]]


    def setArchive(self,
                   archive):
        """
        Set the archive name.

        archive:  Name of archive (string).

        Returns:  Reference to object itself.
        
        """
        self.__archive = trim(archive, "\" ")
        return self


    def getArchive(self):
        """
        Return archive name.

        Returns:  Archive name (string).
        """
        return self.__archive


    def setDiskId(self,
                  id):
        """
        Set Disk ID.

        id:        Set Disk ID (string).
 
        Returns:   Reference to object itself.
        """
        self.__diskId = trim(id, "\" ")
        return self


    def getDiskId(self):
        """
        Get Disk ID.

        Returns:  Disk ID (string).
        """
        return self.__diskId


    def setLogicalName(self,
                       name):
        """
        Set Logical Disk Name.

        name:      Logical Disk Name (string).
 
        Returns:   Reference to object itself.
        """
        self.__logicalName = trim(name, "\" ")
        return self


    def getLogicalName(self):
        """
        Get the Logical Disk Name.

        Returns:  Logical Disk Name (string).
        """
        return str(self.__logicalName)


    def setHostId(self,
                  id):
        """
        Set Host ID.

        id:        Host ID (string).
 
        Returns:   Reference to object itself.
        """
        if (id):
            self.__hostId = str(trim(id, "\" "))
        else:
            self.__hostId = ""
        return self


    def getHostId(self):
        """
        Get Host ID.

        Returns:  Host ID (string).
        """
        return self.__hostId


    def setSlotId(self,
                  id):
        """
        Set Slot ID.

        id:        Slot ID (string).
 
        Returns:   Reference to object itself.
        """
        self.__slotId = trim(str(id), "\" ")
        return self


    def getSlotId(self):
        """
        Get Slot ID.

        Returns:  Slot ID (string).
        """
        return self.__slotId


    def setMounted(self,
                   mounted):
        """
        Set Disk Mounted Flag.

        mounted:   1 = mounted, 0 = not mounted (integer).
 
        Returns:   Reference to object itself.
        """
        self.__mounted = int(mounted)
        return self


    def getMounted(self):
        """
        Return the Disk Mounted Flag.

        Returns:   Disk Mounted Flag (integer).
        """
        return self.__mounted


    def setMountPoint(self,
                      mountPoint):
        """
        Set Mount Point.

        mountPoint:  Mount point (string).
 
        Returns:     Reference to object itself.
        """
        if (mountPoint):
            mntPt = trim(mountPoint, "\" ")
            if mntPt[0] != '/':
                mntPt = NGAMS_SRC_DIR + '/' + mntPt
            self.__mountPoint = mntPt
        else:
            self.__mountPoint = ""
        return self


    def getMountPoint(self):
        """
        Get Disk Mount Point.
 
        Returns:   Mount point (string).
        """
        return self.__mountPoint


    def setNumberOfFiles(self,
                         no):
        """
        Set number of files stored on disk.

        no:        Number of files (integer).        
 
        Returns:   Reference to object itself.
        """
        self.__numberOfFiles = int(no)
        return self


    def getNumberOfFiles(self):
        """
        Return number of files stored on disk.

        Returns:    Returns number of files on disk (integer).
        """
        return self.__numberOfFiles


    def setAvailableMb(self,
                       mb):
        """
        Set Available disk space (MB).

        mb:        Available disk space in MB (integer).
 
        Returns:   Reference to object itself.
        """
        self.__availableMb = int(mb)
        return self


    def getAvailableMb(self):
        """
        Return the amount of available disk space (MB).

        Returns:    Available disk space in MB (integer).
        """
        return self.__availableMb 


    def setBytesStored(self,
                       bytes):
        """
        Set the number of bytes stored on the disk.

        bytes:     Bytes stored (string|float).
 
        Returns:   Reference to object itself.
        """
        self.__bytesStored = float(bytes)
        return self


    def getBytesStored(self):
        """
        Get the number of bytes stored.

        Returns:    Number of bytes stored (float).
        """
        return self.__bytesStored


    def getBytesStoredStr(self):
        """
        Get the number of bytes stored as a string.

        Returns:    Number of bytes stored (string).
        """
        #return string.split(str(self.__bytesStored), ".")[0]
        return "%.0f" % float(self.__bytesStored)


    def setCompleted(self,
                     completed):
        """
        Set the Disk Completed Flag.

        completed:   1 = completed, 0 = not completed (integer).
 
        Returns:     Reference to object itself.
        """
        self.__completed = int(completed)
        return self


    def getCompleted(self):
        """
        Return the Disk Completed Flag.

        Returns:     Disk Completed Flag (integer).
        """
        return self.__completed


    def setCompletionDate(self,
                          date):
        """
        Set completion date.
        
        date:      Completion date (string).
 
        Returns:   Reference to object itself.
        """
        self.__completionDate = timeRef2Iso8601(date)
        return self


    def setCompletionDateFromSecs(self,
                                  dateSecs):
        """
        Set the completion date from seconds since epoch.

        dateSecs:  Completion date in seconds since epoch (integer).
 
        Returns:   Reference to object itself.
        """
        self.__completionDate = PccUtTime.TimeStamp().\
                                initFromSecsSinceEpoch(dateSecs).\
                                getTimeStamp()
        return self


    def getCompletionDate(self):
        """
        Return the completion date as ISO 8601 time stamp.

        Returns:    Completion date (string).
        """
        if (not self.getCompleted()):
            return ""
        else:
            return self.__completionDate


    def setType(self,
                type):
        """
        Set Disk Type.
        
        type:      Disk Type (string).
 
        Returns:   Reference to object itself.
        """
        self.__type = trim(type, "\" ")
        return self


    def getType(self):
        """
        Return the Disk Type.

        Returns:   Disk Type (string).
        """
        return self.__type


    def setManufacturer(self,
                        manufacturer):
        """
        Set Disk Manufacturer.
        
        manufacturer:   Disk Manufacturer (string).
 
        Returns:        Reference to object itself.
        """
        if (manufacturer == None): manufacturer = ""
        self.__manufacturer = trim(manufacturer, "\" ")
        return self


    def getManufacturer(self):
        """
        Return the Disk Manufacturer.

        Returns:   Disk Manufacturer (string).
        """
        return self.__manufacturer


    def setInstallationDate(self,
                            date):
        """
        Set installation date.
        
        date:      Installation date (string).
 
        Returns:   Reference to object itself.
        """
        self.__installationDate = timeRef2Iso8601(date)
        return self


    def setInstallationDateFromSecs(self,
                                    dateSecs):
        """
        Set the installation date from seconds since epoch.

        dateSecs:  Installation date in seconds since epoch (integer).
 
        Returns:   Reference to object itself.
        """
        self.__installationDate = PccUtTime.TimeStamp().\
                                  initFromSecsSinceEpoch(dateSecs).\
                                  getTimeStamp()
        return self


    def getInstallationDate(self):
        """
        Return the installation date as ISO 8601 time stamp.

        Returns:    Installation date (string).
        """
        return self.__installationDate


    def setChecksum(self,
                    checksum):
        """
        Set checksum value for disk.

        checksum:  Checksum value (string).
 
        Returns:   Reference to object itself.
        """
        if (not checksum):
            self.__checksum = ""
        else:
            self.__checksum = str(checksum).strip()
        return self


    def getChecksum(self):
        """
        Return checksum value for disk.

        Returns:   Checksum value (string).
        """
        return self.__checksum 


    def setTotalDiskWriteTime(self,
                              time):
        """
        Set Total Disk Write Time.

        time:      Total Disk Write Time (float).
 
        Returns:   Reference to object itself.
        """
        self.__totalDiskWriteTime = time
        return self


    def getTotalDiskWriteTime(self):
        """
        Return the Total Disk Write Time.

        Returns:   Total Disk Write Time in seconds (float).
        """
        return self.__totalDiskWriteTime


    def setLastCheck(self,
                     date):
        """
        Set date for the last check.
        
        date:      Date for last check (string).
 
        Returns:   Reference to object itself.
        """
        self.__lastCheck = timeRef2Iso8601(date)
        return self


    def setLastCheckFromSecs(self,
                             dateSecs):
        """
        Set the last check date from seconds since epoch.

        dateSecs:  Last check date in seconds since epoch (integer).
 
        Returns:   Reference to object itself.
        """
        self.__lastCheck = PccUtTime.TimeStamp().\
                           initFromSecsSinceEpoch(dateSecs).getTimeStamp()
        return self


    def getLastCheck(self):
        """
        Return the last check date as ISO 8601 time stamp.

        Returns:    Last check date (string).
        """
        return self.__lastCheck


    def setLastHostId(self,
                      host):
        """
        Set name of the last host in which the disk was installed.
        
        host:      Name of host (string).
 
        Returns:   Reference to object itself.
        """
        self.__lastHostId = host
        return self


    def getLastHostId(self):
        """
        Return the name of the last host.

        Returns:    Name of the last host (string).
        """
        return self.__lastHostId


    def read(self,
             dbConObj,
             diskId):
        """
        Query information about a specific HDD and set the
        class member variables accordingly.

        dbConObj:  Reference to DB connection object (ngamsDb).

        diskId:    Disk ID (string).

        Returns:   Reference to object itself.
        """
        T = TRACE()

        res = dbConObj.getDiskInfoFromDiskId(diskId)
        if (res == []):
            errMsg = genLog("NGAMS_ER_UNKNOWN_DISK", [diskId])
            raise Exception, errMsg
        else:
            self.unpackSqlResult(res)
        return self


    def unpackSqlResult(self,
                        sqlResult):
        """
        Unpack the result from an SQL query, whereby the columns
        of the ngas_disks table were queried as described in documentation
        for ngamsDb.getDiskInfoFromDiskId().

        sqlResult: Result of the SQL query (tuple).

        Returns:   Reference to object itself.
        """
        T = TRACE(5)
        
        self.\
               setDiskId(sqlResult[0]).\
               setArchive(sqlResult[1]).\
               setLogicalName(sqlResult[2]).\
               setHostId(sqlResult[3]).\
               setSlotId(sqlResult[4]).\
               setMounted(sqlResult[5]).\
               setMountPoint(sqlResult[6]).\
               setNumberOfFiles(sqlResult[7]).\
               setAvailableMb(sqlResult[8]).\
               setBytesStored(sqlResult[9]).\
               setType(sqlResult[10]).\
               setManufacturer(sqlResult[12]).\
               setInstallationDate(sqlResult[13]).\
               setChecksum(sqlResult[14]).\
               setTotalDiskWriteTime(sqlResult[15]).\
               setCompleted(sqlResult[16])
        if (sqlResult[17]): self.setCompletionDate(sqlResult[17])
        if (sqlResult[18]): self.setLastCheck(sqlResult[18])
        if (sqlResult[19]): self.setLastHostId(sqlResult[19])
        return self


    def write(self,
              dbConObj):
        """
        Write the information contained in the object into the
        DB specified by the DB connection object.

        dbConObj:     DB connection object (ngamsDb).

        Returns:      Returns 1 if a new entry was created in the DB
                      and 0 if an existing entry was updated (integer/0|1).
        """
        addedNewEntry = dbConObj.writeDiskEntry(self.getDiskId(),
                                                self.getArchive(),
                                                self.getInstallationDate(),
                                                self.getType(),
                                                self.getManufacturer(),
                                                self.getLogicalName(),
                                                self.getHostId(),
                                                self.getSlotId(),
                                                self.getMounted(),
                                                self.getMountPoint(),
                                                self.getNumberOfFiles(),
                                                self.getAvailableMb(),
                                                self.getBytesStoredStr(),
                                                self.getCompleted(),
                                                self.getCompletionDate(),
                                                self.getChecksum(),
                                                self.getTotalDiskWriteTime(),
                                                self.getLastCheck(),
                                                self.getLastHostId())
        return addedNewEntry


    def dumpBuf(self,
                dumpFileInfo = 0,
                ignoreUndefFields = 0):
        """
        Dump contents of object into a string buffer.

        dumpFileInfo:       Dump also possible file info in connection with the
                            object (integer\0|1).

        ignoreUndefFields:  Don't take fields, which have a length of 0
                            (integer/0|1).                 

        Returns:            String buffer with disk info (string).
        """
        format = prFormat1()
        buf = "DiskStatus:\n"
        objStat = self.getObjStatus()
        for fieldName, val in objStat:
            if (not ignoreValue(ignoreUndefFields, val)):
                buf += format % (fieldName + ":", str(val))
        if (dumpFileInfo):
            for fileInfoObj in self.getFileObjList():
                buf += fileInfoObj.dumpBuf(ignoreUndefFields)
        return buf


    def genXml(self,
               genLimitedInfo = 0,
               genFileStatus = 1,
               ignoreUndefFields = 0):
        """
        Generate an XML DOM Node from the contents of the object.

        genLimitedInfo:     1 = generate only generic info (integer).

        genFileStatus:      Generate file status (integer/0|1).

        ignoreUndefFields:  Don't take fields, which have a length of 0
                            (integer/0|1).
                            
        Returns:            XML Dom Node (Node).
        """
        T = TRACE(5)

        ign = ignoreUndefFields
        diskStatusEl = xml.dom.minidom.Document().createElement("DiskStatus")
        objStat = self.getObjStatus()
        for fieldName, val in objStat:
            if ((fieldName == "HostId") or (fieldName == "SlotId") or
                (fieldName == "Mounted") or (fieldName == "MountPoint") or
                (fieldName == "LastCheck")):
                if ((not genLimitedInfo) and (not ignoreValue(ign, val))):
                    diskStatusEl.setAttribute(fieldName, str(val))
            else:
                if (not ignoreValue(ign, val)):
                    diskStatusEl.setAttribute(fieldName, str(val))
        
        if (genFileStatus):
            for file in self.getFileObjList():
                fileStatusEl = file.genXml(0, ignoreUndefFields)
                diskStatusEl.appendChild(fileStatusEl)

        return diskStatusEl


    def unpackFromDomNode(self,
                          diskNode,
                          ignoreVarDiskPars = 0):
        """
        Unpack the disk information contained in a DOM DiskStatus
        Node and set the members of the object accordingly.

        diskNode:           DOM Disk Node (Node).

        ignoreVarDiskPars:  Ignore the variable part of the disk status:
                            Host ID, Slot ID, Mounted, Mount
                            Point (integer/0|1).

        Returns:            Reference to object itself.
        """
        T = TRACE()
        
        self.setDiskId(getAttribValue(diskNode, "DiskId"))
        self.setArchive(getAttribValue(diskNode, "Archive"))
        self.setInstallationDate(getAttribValue(diskNode, "InstallationDate"))
        self.setType(getAttribValue(diskNode, "Type"))
        self.setManufacturer(getAttribValue(diskNode, "Manufacturer", 1))
        self.setLogicalName(getAttribValue(diskNode, "LogicalName"))

        # These attributes are not contained in certain Status XML
        # Documents, e.g. in the NgasDiskInfo files.
        if (not ignoreVarDiskPars):
            self.setHostId(getAttribValue(diskNode, "HostId"))
            self.setSlotId(getAttribValue(diskNode, "SlotId"))
            self.setMounted(getAttribValue(diskNode, "Mounted"))
            self.setMountPoint(getAttribValue(diskNode, "MountPoint"))

        self.setNumberOfFiles(getAttribValue(diskNode,"NumberOfFiles"))
        self.setAvailableMb(getAttribValue(diskNode, "AvailableMb"))
        self.setBytesStored(getAttribValue(diskNode, "BytesStored"))
        self.setCompleted(getAttribValue(diskNode, "Completed"))
        self.setCompletionDate(getAttribValue(diskNode, "CompletionDate", 1))
        self.setChecksum(getAttribValue(diskNode, "Checksum"))
        self.setTotalDiskWriteTime(getAttribValue(diskNode,
                                                  "TotalDiskWriteTime"))

        # Handle files.
        fileNodes = diskNode.getElementsByTagName("FileStatus")
        for fileNode in fileNodes:
            fileInfo = ngamsFileInfo.ngamsFileInfo().\
                       unpackFromDomNode(fileNode, self.getDiskId())
            self.addFileObj(fileInfo)

        return self


    def unpackXmlDoc(self,
                     xmlDoc,
                     ignoreVarDiskPars = 0):
        """
        Unpack the disk information contained in an NGAS DiskStatus Element
        and set the members of the object accordingly.

        xmlDoc:             XMl document (string).

        ignoreVarDiskPars:  Ignore the variable part of the disk status:
                            Host ID, Slot ID, Mounted, Mount
                            Point (integer/0|1).

        Returns:            Reference to object itself.
        """
        T = TRACE()

        statusNode = xml.dom.minidom.parseString(xmlDoc).\
                     getElementsByTagName("DiskStatus")[0]
        self.unpackFromDomNode(statusNode, ignoreVarDiskPars)
        return self


    def getInfo(self,
                dbConObj,
                ngamsCfgObj,
                diskId,
                mimeType):
        """
        Query the information about the disk and set the members of the
        object accordingly.

        dbConObj:       DB connection object (ngamsDb).
        
        ngamsCfgObj:    Configuration object (ngamsConfig).
        
        diskId:         Disk ID (string).
        
        mimeType:       Mime-type (string).

        Returns:        Reference to object itself.
        """
        self.read(dbConObj, diskId)
        self.setStorageSetId(getStorageSetIdFromDiskId(dbConObj, ngamsCfgObj,
                                                       diskId))
        return self


    def setStorageSetId(self,
                        id):
        """
        Set Storage Set ID.

        id:        Storage Set ID (string).        
 
        Returns:   Reference to object itself.
        """
        self.__storageSetId = id
        return self


    def getStorageSetId(self):
        """
        Return the Storage Set ID.

        Returns:  Storage Set ID (string).
        """
        return self.__storageSetId


    def addFileObj(self,
                   fileInfoObj):
        """
        Add a File Info object to the disk info object.

        fileInfoObj:   File info object (ngamsFileInfo).
 
        Returns:       Reference to object itself.
        """
        self.__fileList.append(fileInfoObj)
        return self


    def getFileObjList(self):
        """
        Get the list of file info objects.

        Returns:    List with file info objects ([ngamsFileInfo, ...]).
        """
        return self.__fileList


    def getFileObj(self,
                   no):
        """
        Get a file info object referred to by its number (first number is 0).

        no:       Number of file info object (integer).

        Returns:  File info object (ngamsFileInfo).
        """
        return self.__fileList[no]


    def getNoOfFileObjs(self):
        """
        Return the number of file info objects contained in this instance.

        Returns:  Number of file info objects (integer).
        """
        return len(self.__fileList)


    def getStagingArea(self):
        """
        Return the Staging Area of the disk.

        Returns:   Staging Area or '' if the disk seem not to be mounted
                   (string).
        """
        if (self.getMountPoint() == ""): return ""
        return os.path.normpath(self.getMountPoint() + "/" + NGAMS_STAGING_DIR)
        

if __name__ == '__main__':
    """
    Main function.
    """
    setLogCond(0, 0, "", 5)

    import ngamsDb
    db = ngamsDb.ngamsDb("TESTSRV", "", "", "")
    diskInfo = ngamsDiskInfo()
    diskInfo.read(db, "DiskId-1-1")
    print diskInfo.dumpBuf()
    diskInfo.setDiskId(diskInfo.getDiskId() + "-")
    diskInfo.write(db)
    

# EOF
