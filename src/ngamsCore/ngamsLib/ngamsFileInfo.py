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
# "@(#) $Id: ngamsFileInfo.py,v 1.11 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  12/05/2001  Created
#

"""
Contains classes to handle the information in connection with
an archived file.
"""

import xml.dom.minidom

from .ngamsCore import ignoreValue, getAttribValue, prFormat1, genLog, fromiso8601, toiso8601


# TODO:
#
# There is a conceptual circular dependency between this class and ngamsDb,
# which inherits from ngamsDbJoin. This class uses ngamsDbJoin instances to
# write and read properties of instances of this class, but the ngamsDbJ*
# classes in general depend on the ngamsFileInfo and similar classes, which in
# theory should be simple domain classes with not much behaviour of them.
#
# In particular, the unpackSqlResult and genSqlResult methods of this class
# should probably live in one of the ngamsDb* classes, which would mean some
# changes in parts of the code that use those methods.


class ngamsFileInfo:
    """
    Class to handle the information in connection with a file from the NGAS DB.
    """

    def __init__(self):
        """
        Constructor method.
        """
        self.__diskId               = ""
        self.__filename             = ""
        self.__fileVersion          = -1
        self.__fileId               = ""
        self.__format               = ""
        self.__fileSize             = -1
        self.__uncompressedFileSize = -1
        self.__compression          = ""
        self.__ingestionDate        = None
        self.__ignore               = -1
        self.__checksum             = ""
        self.__checksumPlugIn       = ""
        self.__fileStatus           = ""
        self.__creationDate         = None

        self.__tag                  = ""
        self.__ioTime               = -1.
        self.__ingestionRate        = -1.
        self.__containerId          = None

        # Specific OS info about file.
        self.__permissions          = ""
        self.__owner                = ""
        self.__group                = ""
        self.__modDate              = None
        self.__accDate              = None


    def getObjStatus(self):
        """
        Return a list with the current status of the object. The format
        of the list is:

          [[<xml attribute name>, <value>, ...], ...]

        Returns:    List with object status (list/list).
        """
        # Fields in XML document/ASCII dump
        ing_date = ''
        creation_date = ''
        mod_date = ''
        acc_date = ''
        if self.__ingestionDate is not None:
            ing_date = toiso8601(self.__ingestionDate)
        if self.__creationDate is not None:
            creation_date = toiso8601(self.__creationDate)
        if self.__modDate is not None:
            mod_date = toiso8601(self.__modDate)
        if self.__accDate is not None:
            acc_date = toiso8601(self.__accDate)

        return [["DiskId", self.getDiskId()],
                ["FileName", self.getFilename()],
                ["FileId", self.getFileId()],
                ["FileVersion", self.getFileVersion()],
                ["Format", self.getFormat()],
                ["FileSize", self.getFileSize()],
                ["UncompressedFileSize", self.getUncompressedFileSize()],
                ["Compression", self.getCompression()],
                ["IngestionDate", ing_date],
                ["Ignore", self.getIgnore()],
                ["Checksum", self.getChecksum()],
                ["ChecksumPlugIn",self.getChecksumPlugIn()],
                ["FileStatus", self.getFileStatus()],
                ["CreationDate", creation_date],
                ["Tag", self.getTag()],
                ["Permissions", self.getPermissions()],
                ["Owner", self.getOwner()],
                ["Group", self.getGroup()],
                ["ModificationDate", mod_date],
                ["AccessDate", acc_date],
                ["TotalIoTime", self.getIoTime()],
                ["IngestionRate", self.getIngestionRate()],
                ["ContainerId", self.getContainerId()]
                ]


    def setDiskId(self,
                  id):
        """
        Set Disk ID.

        id:        Disk ID (string).

        Returns:   Reference to object itself.
        """
        self.__diskId = id.strip("\" ")
        return self


    def getDiskId(self):
        """
        Get Disk ID.

        Returns:   Disk ID (string).
        """
        return self.__diskId


    def setFilename(self,
                    filename):
        """
        Set filename.

        filename:   Filename (string).

        Returns:    Reference to object itself.
        """
        self.__filename = filename.strip("\" ")
        return self


    def getFilename(self):
        """
        Get filename.

        Returns:   Filename (string).
        """
        return self.__filename


    def setFileId(self,
                  id):
        """
        Set File ID.

        id:         File ID (string).

        Returns:    Reference to object itself.
        """
        self.__fileId = id.strip("\" ")
        return self


    def getFileId(self):
        """
        Get File ID.

        Returns:   File ID (string).
        """
        return self.__fileId


    def setFileVersion(self,
                       version):
        """
        Set File Version.

        version:    File Version (integer).

        Returns:    Reference to object itself.
        """
        if (str(version).strip()): self.__fileVersion = int(version)
        return self


    def getFileVersion(self):
        """
        Get File Version.

        Returns:   File Version (string).
        """
        return self.__fileVersion


    def setFormat(self,
                  format):
        """
        Set file format.

        format:     File format (string).

        Returns:    Reference to object itself.
        """
        self.__format = format.strip("\" ")
        return self


    def getFormat(self):
        """
        Get file format.

        Returns:   File format (string).
        """
        return self.__format


    def setFileSize(self,
                    size):
        """
        Set file size.

        size:       File size in bytes (integer).

        Returns:    Reference to object itself.
        """
        self.__fileSize = int(size)
        return self


    def getFileSize(self):
        """
        Get file size.

        Returns:    File size in bytes (integer).
        """
        return self.__fileSize


    def setUncompressedFileSize(self,
                                size):
        """
        Set uncompresssed file size.

        size:       Uncompressed file size (integer).

        Returns:    Reference to object itself.
        """
        self.__uncompressedFileSize = int(size)
        return self


    def getUncompressedFileSize(self):
        """
        Get the uncompressed file size.

        Returns:   Uncompressed file size (integer).
        """
        return self.__uncompressedFileSize


    def setCompression(self,
                       compression):
        """
        Set compression method applied on the file.

        compression: Compresion method applied on the file (string).

        Returns:     Reference to object itself.
        """
        if ((not compression) or (compression == "None")):
            self.__compression = ""
        else:
            self.__compression = compression.strip("\" ")
        return self


    def getCompression(self):
        """
        Get the compression method.

        Returns:   Compression method (string).
        """
        return self.__compression


    def setIngestionDate(self,
                         date):
        """
        Set the ingestion date for the file.

        date:       Ingestion date for file (number).

        Returns:    Reference to object itself.
        """
        self.__ingestionDate = date
        return self


    def getIngestionDate(self):
        """
        Get the ingestion date.

        Returns:   Ingestion date (number).
        """
        return self.__ingestionDate


    def setIgnore(self,
                  ignore):
        """
        Set the Ignore Flag for the file.

        ignore:     1 = ignore (integer).

        Returns:    Reference to object itself.
        """
        if (ignore == None): ignore = 0
        self.__ignore = ignore
        return self


    def getIgnore(self):
        """
        Get the Ignore Flag for the file.

        Returns:   Ignore Flag. 1 = ignore (integer).
        """
        return self.__ignore


    def setChecksum(self,
                    checksum):
        """
        Set the checksum for the file.

        checksum:     Checksum (string).

        Returns:      Reference to object itself.
        """
        self.__checksum = checksum
        return self


    def getChecksum(self):
        """
        Get the checksum for the file.

        Returns:   Checksum (string).
        """
        return self.__checksum


    def setChecksumPlugIn(self,
                          plugIn):
        """
        Set the Checksum Plug-In used to calculate the file checksum.

        plugIn:       Name of Checksum Plug-In (string).

        Returns:      Reference to object itself.
        """
        self.__checksumPlugIn = plugIn
        return self


    def getChecksumPlugIn(self):
        """
        Get the name of the Checksum Plug-In used to calculate the
        checksum for the file.

        Returns:   Name of Checksum Plug-In (string).
        """
        return self.__checksumPlugIn


    def setFileStatus(self,
                      status):
        """
        Set the File Status field.

        status:       File Status (string).

        Returns:      Reference to object itself.
        """
        self.__fileStatus = str(status)
        return self


    def getFileStatus(self):
        """
        Get the File Status.

        Returns:   File Status (string).
        """
        return self.__fileStatus


    def setCreationDate(self,
                        date):
        """
        Set the Creation Date for the file

        date:       Creation Date for file (number).

        Returns:    Reference to object itself.
        """
        self.__creationDate = date
        return self


    def getCreationDate(self):
        """
        Get the Creation Date for the file.

        Returns:   Creation Date (string).
        """
        return self.__creationDate


    def setTag(self,
               tag):
        """
        Set the Tag field.

        tag:          Tag (string).

        Returns:      Reference to object itself.
        """
        self.__tag = str(tag)
        return self


    def getTag(self):
        """
        Get the Tag.

        Returns:   Tag (string).
        """
        return self.__tag


    ########################################################################
    # These methods are used to handle the file system info about the file.
    ########################################################################
    def setPermissions(self,
                       permissions):
        """
        Set the permissions.

        permissions:  Permissions (UNIX style) (string).

        Returns:      Reference to object itself.
        """
        self.__permissions = str(permissions)
        return self


    def getPermissions(self):
        """
        Get the permissions.

        Returns:   Permissions (UNIX style) (string).
        """
        return self.__permissions


    def setOwner(self,
                 owner):
        """
        Set the owner.

        owner:        File owner (user name) (string).

        Returns:      Reference to object itself.
        """
        self.__owner = str(owner)
        return self


    def getOwner(self):
        """
        Get the owner.

        Returns:   Owner (user name) (string).
        """
        return self.__owner


    def setGroup(self,
                 group):
        """
        Set the group.

        group:        Group related to file (string).

        Returns:      Reference to object itself.
        """
        self.__group = str(group)
        return self


    def getGroup(self):
        """
        Get the group.

        Returns:   Group (string).
        """
        return self.__group


    def setModDate(self,
                   date):
        """
        Set the Modification Date for the file.

        date:       Modification Date for file (number).

        Returns:    Reference to object itself.
        """
        self.__modDate = date
        return self


    def getModDate(self):
        """
        Get the Modification Date for the file.

        Returns:   Modification Date (number).
        """
        return self.__modDate


    def setAccDate(self,
                   date):
        """
        Set the Access Date for the file.

        date:       Access Date for file (number).

        Returns:    Reference to object itself.
        """
        self.__accDate = date
        return self


    def getAccDate(self):
        """
        Get the Access Date for the file.

        Returns:   Access Date (number).
        """
        return self.__accDate


    def setIoTime(self,
                   ioTime):
        """
        Set the total I/O time for the file (in seconds).

        ioTime:     float, seconds

        Returns:    Reference to object itself.
        """
        if (not ioTime): return self
        self.__ioTime = float(ioTime)
        return self


    def getIoTime(self):
        """
        Get the I/O time for the file (in seconds).

        Returns:   I/O time (float).
        """
        return self.__ioTime


    def setIngestionRate(self,
                         ingestionRate):
        """
        Set the ingestion rate for the file (in bytes/seconds).

        ingestionRate:  float, bytes/second

        Returns:        Reference to object itself.
        """
        if (not ingestionRate): return self
        self.__ingestionRate = ingestionRate
        return self


    def getIngestionRate(self):
        """
        Get the ingestion rate for the file (in bytes/second).

        Returns:   Ingestion rate (float).
        """
        return self.__ingestionRate


    def setContainerId(self,
                       containerId):
        """
        Set the ID of the container to which this file belongs.

        containerId: string

        Returns:     Reference to object itself.
        """
        if (not containerId): return self
        self.__containerId = containerId
        return self


    def getContainerId(self):
        """
        Get the ID of the container to which this file belongs.

        Returns:   Container ID (string).
        """
        return self.__containerId
    ########################################################################


    def unpackSqlResult(self,
                        sqlQueryRes):
        """
        Sets the members of the class from the query information as
        returned from ngamsDb.getFileInfoFromFileIdHostId().

        sqlQueryRes:   Query information (list).

        Return:        Reference to object itself.
        """
        self.setDiskId(sqlQueryRes[0])
        self.setFilename(sqlQueryRes[1])
        self.setFileId(sqlQueryRes[2])
        self.setFileVersion(sqlQueryRes[3])
        self.setFormat(sqlQueryRes[4])
        self.setFileSize(sqlQueryRes[5])
        uncomprSz = sqlQueryRes[6]
        self.setUncompressedFileSize(uncomprSz)
        self.setCompression(sqlQueryRes[7])
        if sqlQueryRes[8]: self.setIngestionDate(fromiso8601(sqlQueryRes[8], local=True))
        self.setIgnore(sqlQueryRes[9])
        self.setChecksum(sqlQueryRes[10])
        self.setChecksumPlugIn(sqlQueryRes[11])
        self.setFileStatus(sqlQueryRes[12])
        if sqlQueryRes[13]: self.setCreationDate(fromiso8601(sqlQueryRes[13], local=True))
        self.setIoTime(sqlQueryRes[14])
        self.setIngestionRate(sqlQueryRes[15])
        self.setContainerId(sqlQueryRes[16])
        return self


    def genSqlResult(self):
        """
        Generate a list with the information as read from ngas_files and
        defined by the variables ngamsDbCore.NGAS_FILES_DISK_ID, ... .

        Returns:   List with information from one row of ngas_files (list).
        """
        ing_date = ''
        creation_date = ''
        if self.__ingestionDate is not None:
            ing_date = toiso8601(self.__ingestionDate, local=True)
        if self.__creationDate is not None:
            creation_date = toiso8601(self.__creationDate, local=True)

        return [self.getDiskId(), self.getFilename(), self.getFileId(),
                int(self.getFileVersion()), self.getFormat(),
                int(self.getFileSize()), int(self.getUncompressedFileSize()),
                self.getCompression(), ing_date,
                int(self.getIgnore()), self.getChecksum(),
                self.getChecksumPlugIn(), self.getFileStatus(),
                creation_date, self.getIoTime(), self.getIngestionRate(),
                self.getContainerId()]


    def read(self,
             hostId,
             dbConObj,
             fileId,
             fileVersion = 1,
             diskId = None):
        """
        Query information about a specific file and set the class member
        variables accordingly.

        dbConObj:         DB connection object (ngamsDb).

        fileId:           File ID (string).

        fileVersion:      Version of the file to query info for (integer).

        diskId:           Used to refer to a specific disk (string).

        Returns:          Reference to object itself.
        """
        fileInfo = dbConObj.getFileInfoFromFileIdHostId(hostId, fileId,
                                                        fileVersion, diskId)
        if (not fileInfo):
            fileInfo = dbConObj.getFileInfoFromFileId(fileId, fileVersion,
                                                       diskId, dbCursor=False)
            if fileInfo:
                fileInfo = fileInfo[0]
        if (fileInfo == []):
            errMsg = genLog("NGAMS_ER_UNAVAIL_FILE", [fileId])
            raise Exception(errMsg)
        else:
            self.unpackSqlResult(fileInfo)
        return self


    def write(self,
              hostId,
              dbConObj,
              genSnapshot = 1,
              updateDiskInfo = 0,
              prev_disk_id=None):
        """
        Write the contents of the object into the NGAS DB.

        dbConObj:        DB connection object (ngamsDb).

        genSnapshot:     Generate temporay snapshot file (integer/0|1).

        updateDiskInfo:  Update automatically the disk info for the
                         disk hosting this file (integer/0|1).

        Returns:         Reference to object itself.
        """
        dbConObj.writeFileEntry(hostId,
                                self.getDiskId(), self.getFilename(),
                                self.getFileId(), self.getFileVersion(),
                                self.getFormat(), self.getFileSize(),
                                self.getUncompressedFileSize(),
                                self.getCompression(), self.getIngestionDate(),
                                self.getIgnore(), self.getChecksum(),
                                self.getChecksumPlugIn(), self.getFileStatus(),
                                self.getCreationDate(), self.getIoTime(),
                                self.getIngestionRate(),
                                genSnapshot=genSnapshot,
                                updateDiskInfo=updateDiskInfo,
                                prev_disk_id=prev_disk_id)
        return self


    def unpackXmlDoc(self,
                     xmlDoc):
        """
        Unpack the file information contained in an FileStatus Element and set
        the members of the object accordingly.

        xmlDoc:             XML document (string).

        Returns:            Reference to object itself.
        """
        statusNode = xml.dom.minidom.parseString(xmlDoc).\
                     getElementsByTagName("FileStatus")[0]
        self.unpackFromDomNode(statusNode)
        return self


    def unpackFromDomNode(self,
                          fileNode,
                          diskId = None):
        """
        Unpack the file information contained in a DOM FileStatus
        Node and set the members of the object accordingly.

        fileNode:       DOM File Node object (DOM object).

        diskId:         Disk ID for disk where file is stored (string).

        Returns:        Reference to object itself.
        """
        if (diskId != None):
            self.setDiskId(diskId)
        else:
            self.setDiskId(getAttribValue(fileNode, "DiskId", 1))
        uncomprSize = getAttribValue(fileNode, "UncompressedFileSize", 1)
        checksumPi  = getAttribValue(fileNode, "ChecksumPlugIn", 1)
        self.\
               setChecksum(getAttribValue(fileNode,      "Checksum", 1)).\
               setChecksumPlugIn(checksumPi).\
               setCompression(getAttribValue(fileNode,   "Compression", 1)).\
               setFileId(getAttribValue(fileNode,        "FileId", 1)).\
               setFilename(getAttribValue(fileNode,      "FileName", 1)).\
               setUncompressedFileSize(uncomprSize).\
               setFileSize(getAttribValue(fileNode,      "FileSize", 1)).\
               setFileStatus(getAttribValue(fileNode,    "FileStatus", 1)).\
               setFileVersion(getAttribValue(fileNode,   "FileVersion", 1)).\
               setFormat(getAttribValue(fileNode,        "Format", 1)).\
               setGroup(getAttribValue(fileNode,         "Group", 1)).\
               setIgnore(getAttribValue(fileNode,        "Ignore", 1)).\
               setOwner(getAttribValue(fileNode,         "Owner", 1)).\
               setPermissions(getAttribValue(fileNode,   "Permissions", 1)).\
               setTag(getAttribValue(fileNode,           "Tag", 1)).\
               setIoTime(getAttribValue(fileNode,        "TotalIoTime", 1)).\
               setIngestionRate(getAttribValue(fileNode, "IngestionRate", 1)).\
               setContainerId(getAttribValue(fileNode,   "ContainerId", 1))

        creation_date = getAttribValue(fileNode, "CreationDate", 1)
        if creation_date:
            self.setCreationDate(fromiso8601(creation_date))
        ingestion_date = getAttribValue(fileNode, "IngestionDate", 1)
        if ingestion_date:
            self.setIngestionDate(fromiso8601(ingestion_date))
        mod_date = getAttribValue(fileNode, "ModificationDate", 1)
        if mod_date:
            self.setModDate(fromiso8601(mod_date))
        acc_date = getAttribValue(fileNode,       "ModificationDate", 1)
        if acc_date:
            self.setAccDate(fromiso8601(acc_date))

        return self


    def genXml(self,
               storeDiskId = 0,
               ignoreUndefFields = 0):
        """
        Generate an XML DOM Node object from the contents of this
        instance of ngamsFileInfo.

        storeDiskId:        Store Disk ID in XML document (1 = store)
                            (integer).

        ignoreUndefFields:  Don't take fields, which have a length of 0
                            (integer/0|1).

        Returns:            XML DOM Node object (Node).
        """
        ign = ignoreUndefFields
        fileStatusEl = xml.dom.minidom.Document().createElement("FileStatus")
        objStat = self.getObjStatus()
        for fieldName, val in objStat:
            if (fieldName == "DiskId"):
                if (storeDiskId and (not ignoreValue(ign, self.getDiskId()))):
                    fileStatusEl.setAttribute("DiskId", self.getDiskId())
            else:
                if (not ignoreValue(ign, val)):
                    fileStatusEl.setAttribute(fieldName, str(val))
        return fileStatusEl


    def dumpBuf(self,
                ignoreUndefFields = 0):
        """
        Dump contents of object into a string buffer.

        ignoreUndefFields:     Don't take fields, which have a length of 0
                               (integer/0|1).

        Returns:               String buffer with contents of object (string).
        """
        format = prFormat1()
        buf = "FileStatus:\n"
        objStat = self.getObjStatus()
        for fieldName, val in objStat:
            if (not ignoreValue(ignoreUndefFields, val)):
                buf += format % (fieldName + ":", val)
        return buf


    def clone(self):
        """
        Make a copy of the object containing the same data and return this.

        Returns:   Copy of this object (ngamsFileInfo).
        """
        return ngamsFileInfo().\
               setDiskId(self.getDiskId()).\
               setFilename(self.getFilename()).\
               setFormat(self.getFormat()).\
               setFileVersion(self.getFileVersion()).\
               setFileId(self.getFileId()).\
               setFormat(self.getFormat()).\
               setFileSize(self.getFileSize()).\
               setUncompressedFileSize(self.getUncompressedFileSize()).\
               setCompression(self.getCompression()).\
               setIngestionDate(self.getIngestionDate()).\
               setIgnore(self.getIgnore()).\
               setChecksum(self.getChecksum()).\
               setChecksumPlugIn(self.getChecksumPlugIn()).\
               setFileStatus(self.getFileStatus()).\
               setCreationDate(self.getCreationDate()).\
               setTag(self.getTag()).\
               setIoTime(self.getIoTime()).\
               setIngestionRate(self.getIngestionRate()).\
               setContainerId(self.getContainerId())


# EOF
