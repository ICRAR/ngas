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
# "@(#) $Id: ngamsDapiStatus.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  08/10/2001  Created
#

"""
Contains class for handling the return status of the Data Archiving Plug-Ins.
"""

class ngamsDapiStatus:
    """
    Object to keep information returned from a DAPI.
    """

    def __init__(self):
        """
        Constructor method.
        """
        self.__status            = ""
        self.__diskId            = ""
        self.__relFilename       = ""
        self.__fileId            = ""
        self.__fileVersion       = -1
        self.__format            = ""
        self.__fileSize          = 0
        self.__uncomprSize       = 0
        self.__compression       = ""
        self.__relPath           = ""
        self.__slotId            = ""
        self.__ioTime            = 0
        self.__fileExists        = -1
        self.__completeFilename  = ""


    def setStatus(self,
                  status):
        """
        Set status of plug-in invocation (SUCCESS|FAILURE)

        Status:    Status (string).

        Returns:   Reference to object itself.
        """
        self.__status = status
        return self


    def getStatus(self):
        """
        Return status of plug-in invocation.

        Returns:   Status (string).
        """
        return self.__status
   

    def setDiskId(self,
                  diskId):
        """
        Set Disk ID.

        diskId:    Disk ID (string).

        Returns:   Reference to object itself.
        """
        self.__diskId = diskId
        return self


    def getDiskId(self):
        """
        Return Disk ID.

        Returns:   Disk ID (string).
        """
        return self.__diskId

        
    def setRelFilename(self,
                       relFilename):
        """
        Set the Relative Filename (name relative to mount point of disk).

        relFilename:  Relative Filename (string).

        Returns:      Reference to object itself.
        """
        self.__relFilename = relFilename
        return self


    def getRelFilename(self):
        """
        Return Relative Filename (name relative to mount point of disk).

        Returns:      Relative Filename (string).
        """
        return self.__relFilename
        

    def setFileId(self,
                  fileId):
        """
        Set File ID.

        fileId:    File ID (string).

        Returns:   Reference to object itself.
        """
        self.__fileId = fileId
        return self


    def getFileId(self):
        """
        Return File ID.

        Returns:    File ID (string).
        """
        return self.__fileId


    def setFileVersion(self,
                       fileVersion):
        """
        Set File Version.

        fileVersion:    File Version (integer).

        Returns:        Reference to object itself.
        """
        self.__fileVersion = int(fileVersion)
        return self


    def getFileVersion(self):
        """
        Return File Version.

        Returns:    File Version (string).
        """
        return self.__fileVersion


    def setFormat(self,
                  format):
        """
        Set file format.

        format:    File format  (string).

        Returns:   Reference to object itself.
        """
        self.__format = format
        return self


    def getFormat(self):
        """
        Return the file format.

        Returns:  File format (string).
        """
        return self.__format


    def setFileSize(self,
                    fileSize):
        """
        Set file size in bytes.

        fileSize:  File size (integer).

        Returns:   Reference to object itself.
        """
        self.__fileSize = int(fileSize)
        return self


    def getFileSize(self):
        """
        Return file size in bytes.

        Returns:   File size in bytes (integer).
        """
        return self.__fileSize


    def setUncomprSize(self,
                       uncomprSize):
        """
        Set uncompressed file size (in bytes).

        uncomprSize:  Uncompressed file size in bytes (integer).

        Returns:      Reference to object itself.
        """
        self.__uncomprSize = int(uncomprSize)
        return self


    def getUncomprSize(self):
        """
        Return uncompressed file size in bytes.

        Returns:     File size in bytes (integer).
        """
        return self.__uncomprSize


    def setCompression(self,
                       compression):
        """
        Set compression used if any.

        compression:   Compression (string).

        Returns:       Reference to object itself.
        """
        self.__compression = compression
        return self


    def getCompression(self):
        """
        Return compression used.

        Returns:    Compresseion (string).
        """
        return self.__compression


    def setRelPath(self,
                   relPath):
        """
        Set Relative Path of file (relative to mount point of disk).

        relPath:   Relative Path of file (string).

        Returns:   Reference to object itself.
        """
        self.__relPath = relPath
        return self


    def getRelPath(self):
        """
        Return Relative Path of file.

        Returns:   Relative Path (string).
        """
        return self.__relPath


    def setSlotId(self,
                  slotId):
        """
        Set Slot ID.

        slotId:    Slot ID (string).

        Returns:   Reference to object itself.
        """
        self.__slotId = slotId
        return self


    def getSlotId(self):
        """
        Return Slot ID.

        Returns:  Slot ID (string).
        """
        return self.__slotId


    def setIoTime(self,
                  ioTime):
        """
        Set IO time used in connection with file handling.

        ioTime:    IO time in seconds (float).

        Returns:   Reference to object itself.
        """
        self.__ioTime = float(ioTime)
        return self


    def getIoTime(self):
        """
        Return IO time in connection with file handling.

        Returns:   IO time in seconds (float).
        """
        return self.__ioTime


    def setFileExists(self,
                      fileExists):
        """
        Set the File-Exists Flag.

        fileExists:   0 = file exists, 1 = file does not exist (integer).

        Returns:      Reference to object itself.
        """
        self.__fileExists = int(fileExists)
        return self


    def getFileExists(self):
        """
        Return the File-Exists Flag.

        Returns:  0 = file exists, 1 = file does not exist (integer).
        """
        return self.__fileExists


    def setCompleteFilename(self,
                            completeFilename):
        """
        Set complete name of file.

        completeFilename:  Complete filename (string).

        Returns:           Reference to object itself.
        """
        self.__completeFilename = str(completeFilename)
        return self


    def getCompleteFilename(self):
        """
        Return complete name of file.

        Returns:   Complete filename (string).
        """
        return self.__completeFilename


    def toString(self):
        """
        Generate an ASCII buffer with the contents of the object.

        Returns:   String buffer with status of object contents (string).
        """
        return "Status: " + self.getStatus() + ", " +\
               "Disk ID: " + self.getDiskId() + ", " +\
               "Relative Filename: " + self.getRelFilename() + ", " +\
               "File ID: " + self.getFileId() + ", " +\
               "File Version: " + str(self.getFileVersion()) + ", " +\
               "Format: " + self.getFormat() + ", " +\
               "File Size: " + str(self.getFileSize()) + ", " +\
               "Uncompressed File Size: " + str(self.getUncomprSize()) + ", "+\
               "Compression: " + self.getCompression() + ", " +\
               "Relative Path: " + self.getRelPath() + ", " +\
               "Slot ID: " + self.getSlotId() + ", " +\
               "IO Time: " + str(self.getIoTime()) + ", " +\
               "File Exists: " + str(self.getFileExists()) + ", " +\
               "Complete Filename: " + self.getCompleteFilename()


# EOF
