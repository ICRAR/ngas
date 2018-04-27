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
# "@(#) $Id: ngamsFileSummary1.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  14/03/2008  Created
#

"""
Class to handle the information in connection with one entry in the NGAS Cache.
"""

from .ngamsCore import TRACE, getBoolean
from . import ngamsDbCore


class ngamsFileSummary1:
    """
    Class to handle the information in connection with one entry in the
    NGAS Cache.
    """

    def __init__(self):
        """
        Constructor method.
        """
        self.__slotId         = None
        self.__mountPoint     = None
        self.__filename       = None
        self.__checksum       = None
        self.__checksumPlugIn = None
        self.__fileId         = None
        self.__fileVersion    = None
        self.__fileSize       = None
        self.__fileStatus     = None
        self.__diskId         = None
        self.__ignore         = None
        self.__hostId         = None


    def setSlotId(self,
                  slotId):
        """
        Set the Slot ID member of the class.

        slotId:   ID for the slot, hosting the disk (string).

        Returns:  Reference to object itself.
        """
        self.__slotId = slotId
        return self


    def getSlotId(self):
        """
        Return the value of the Slot ID member.

        Returns:  Value of Slot ID member (string).
        """
        return self.__slotId


    def setMountPoint(self,
                      mountPoint):
        """
        Set the mountpoint member of the class.

        mountPoint:   Mountpoint for the disk (string).

        Returns:      Reference to object itself.
        """
        self.__mountPoint = mountPoint
        return self


    def getMountPoint(self):
        """
        Return the value of the mountpoint member.

        Returns:  Value of mounpoint member (string).
        """
        return self.__mountPoint


    def setFilename(self,
                    filename):
        """
        Set the filename member of the class.

        filename:   Name of the file (string).

        Returns:    Reference to object itself.
        """
        self.__filename = filename
        return self


    def getFilename(self):
        """
        Return the value of the Filename

        Returns:  Value of Filename member (integer).
        """
        return self.__filename


    def setChecksum(self,
                    checksum):
        """
        Set the checksum member of the class.

        checksum:   Checksum for the file (string).

        Returns:    Reference to object itself.
        """
        self.__checksum = checksum
        return self


    def getChecksum(self):
        """
        Return the value of the checksum member.

        Returns:  Value of checksum member (string).
        """
        return self.__checksum


    def setChecksumPlugIn(self,
                          checksumPlugIn):
        """
        Set the checksum plug-in member of the class.

        checksumPlugIn:   Checksum Plug-In for the file (string).

        Returns:          Reference to object itself.
        """
        self.__checksumPlugIn = checksumPlugIn
        return self


    def getChecksumPlugIn(self):
        """
        Return the value of the Checksum Plug-In member.

        Returns:  Value of Checksum Plug-In member (string).
        """
        return self.__checksumPlugIn


    def setFileId(self,
                  fileId):
        """
        Set the File ID member of the class.

        fileId:   ID of the file (string).

        Returns:  Reference to object itself.
        """
        self.__fileId = fileId
        return self


    def getFileId(self):
        """
        Return the value of the File ID member.

        Returns:  Value of File ID member (string).
        """
        return self.__fileId


    def setFileVersion(self,
                       fileVersion):
        """
        Set the File Version member of the class.

        fileVersion:   Version of the file (integer).

        Returns:       Reference to object itself.
        """
        try:
            self.__fileVersion = int(fileVersion)
        except:
            msg = "Wrong format of File Version given: %s" % str(fileVersion)
            raise Exception(msg)
        return self


    def getFileVersion(self):
        """
        Return the value of the File Version member.

        Returns:  Value of File Version member (integer).
        """
        return self.__fileVersion


    def setFileSize(self,
                    fileSize):
        """
        Set the File Size member of the class.

        fileSize:   Size of the file (string).

        Returns:    Reference to object itself.
        """
        self.__fileSize = int(fileSize)
        return self


    def getFileSize(self):
        """
        Return the value of the File Size

        Returns:  Value of File Size member (integer).
        """
        return self.__fileSize


    def setFileStatus(self,
                      status):
        """
        Set the File Status member of the class.

        status:    Status for the file (string).

        Returns:   Reference to object itself.
        """
        self.__fileStatus = status
        return self


    def getFileStatus(self):
        """
        Return the value of the File Status.

        Returns:  Value of File Status member (integer).
        """
        return self.__fileStatus


    def setDiskId(self,
                  diskId):
        """
        Set the Disk ID member of the class.

        diskId:   ID of the disk (string).

        Returns:  Reference to object itself.
        """
        self.__diskId = diskId
        return self


    def getDiskId(self):
        """
        Return the value of the Disk ID member.

        Returns:  Value of Disk ID member (string).
        """
        return self.__diskId


    def setIgnore(self,
                  ignore):
        """
        Set the Ignore Flag member of the class.

        ignore:   Ignore Flag (boolean | string | integer).

        Returns:  Reference to object itself.
        """
        self.__ignore = getBoolean(ignore)
        return self


    def getIgnore(self):
        """
        Return the value of the Ignore Flag member.

        Returns:  Value of the Ignore Flag member (boolean).
        """
        return self.__ignore


    def setHostId(self,
                  hostId):
        """
        Set the Host ID member of the class.

        hostId:   ID of the host, hosting the disk hosting the file (string).

        Returns:  Reference to object itself.
        """
        self.__hostId = hostId
        return self


    def getHostId(self):
        """
        Return the value of the Host ID member.

        Returns:  Value of Host ID member (string).
        """
        return self.__hostId


    def unpackSqlInfo(self,
                      sqlInfo):
        """
        Unpack the SQL information resulting from a File Summary 1 Query.

        sqlInfo:   List with information resulting from a Summary 1 Query
                   (list).

        Returns:   Reference to object itself.
        """
        T = TRACE()

        self.\
               setSlotId(sqlInfo[ngamsDbCore.SUM1_SLOT_ID]).\
               setMountPoint(sqlInfo[ngamsDbCore.SUM1_MT_PT]).\
               setFilename(sqlInfo[ngamsDbCore.SUM1_FILENAME]).\
               setChecksum(sqlInfo[ngamsDbCore.SUM1_CHECKSUM]).\
               setChecksumPlugIn(sqlInfo[ngamsDbCore.SUM1_CHECKSUM_PI]).\
               setFileId(sqlInfo[ngamsDbCore.SUM1_FILE_ID]).\
               setFileVersion(sqlInfo[ngamsDbCore.SUM1_VERSION]).\
               setFileSize(sqlInfo[ngamsDbCore.SUM1_FILE_SIZE]).\
               setFileStatus(sqlInfo[ngamsDbCore.SUM1_FILE_STATUS]).\
               setDiskId(sqlInfo[ngamsDbCore.SUM1_DISK_ID]).\
               setIgnore(sqlInfo[ngamsDbCore.SUM1_FILE_IGNORE]).\
               setHostId(sqlInfo[ngamsDbCore.SUM1_HOST_ID])

        return self

# EOF
