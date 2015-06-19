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
# "@(#) $Id: ngamsFileList.py,v 1.5 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  22/03/2002  Created
#

"""
Contains definition of a class to handle a list of ngamsFileInfo objects.
"""

import xml.dom.minidom
from ngams import *
import ngamsFileInfo


class ngamsFileList:
    """
    Class that contains a list of ngamsFileInfo objects.
    """

    def __init__(self,
                 id = "",
                 comment = "",
                 status = ""):
        """
        Constructor method.
        """
        self.__id           = id
        self.__comment      = comment
        self.__status       = status
        self.__fileInfoList = []
        self.__fileListList = []


    def setId(self,
              id):
        """
        Set the ID of the File List.

        id:       ID of File List (string).

        Returns:  Reference to object itself.
        """
        self.__id = id
        return self


    def getId(self):
        """
        Return the ID of the File List.

        Returns:   ID of File List (string).
        """
        return self.__id


    def setComment(self,
                   comment):
        """
        Set the comment for the File List.

        comment:  Comment for File List (string).

        Returns:  Reference to object itself.
        """
        self.__comment = comment
        return self


    def getComment(self):
        """
        Return the comment for the File List.

        Returns:   Comment for the File List (string).
        """
        return self.__comment


    def setStatus(self,
                  status):
        """
        Set the status for the File List.

        status:   Status for File List (string).

        Returns:  Reference to object itself.
        """
        self.__status = status
        return self


    def getStatus(self):
        """
        Return the status for the File List.

        Returns:   Status for the File List (string).
        """
        return self.__status


    def addFileInfoObj(self,
                       fileInfoObj):
        """
        Add a File Info Object to the internal list.

        fileInfoObj:    File Info Object (ngamsFileInfo).

        Returns:        Reference to object itself.
        """
        self.__fileInfoList.append(fileInfoObj)
        return self


    def delFileInfoObj(self,
                       delDileInfoObj):
        """
        Delete the referenced File Info Object from the list if available.
        If not available, the request is ignored.

        delFileInfoObj:  Reference to File Info Object to be deleted
                         (ngamsFileInfo).
 
        Returns:         Reference to object itself.
        """
        for idx in range(len(self.getFileInfoObjList())):
            if (self.__fileInfoList[idx] == delDileInfoObj):
                del self.__fileInfoList[idx]
                break
        return self


    def getFileInfoObjList(self):
        """
        Return reference to File Info Object list.

        Returns:    Reference to File Info Object List (list/ngamsFileInfo).
        """
        return self.__fileInfoList


    def getNoOfFileInfoObjs(self):
        """
        Return number of File Info Objects.

        Returns:   Number of File Info Objects in the instance (integer).
        """
        return len(self.getFileInfoObjList())


    def addFileListObj(self,
                       fileListObj):
        """
        Add a File List Object to the internal list.

        fileListObj:    File List Object (ngamsFileList).

        Returns:        Reference to object itself.
        """
        self.__fileListList.append(fileListObj)
        return self
        

    def getFileListObjList(self):
        """
        Return reference to File List Object list.

        Returns:    Reference to File List Object List (list/ngamsFileInfo).
        """
        return self.__fileListList


    def genXml(self):
        """
        Generate an XML DOM Node object from the contents of this
        instance of the ngamsFileList class.
         
        Returns:     XML DOM Node object (Node).
        """
        T = TRACE(5)
        
        fileListEl = xml.dom.minidom.Document().createElement("FileList")
        fileListEl.setAttribute("Id", self.getId())
        if (self.getComment() != ""):
            fileListEl.setAttribute("Comment", self.getComment())
        if (self.getStatus() != ""):
            fileListEl.setAttribute("Status", self.getStatus())
        for fileInfoObj in self.getFileInfoObjList():
            fileInfoXmlNode = fileInfoObj.genXml(1, 1)
            fileListEl.appendChild(fileInfoXmlNode)
        for fileListObj in self.getFileListObjList():
            fileListXmlNode = fileListObj.genXml()
            fileListEl.appendChild(fileListXmlNode)
        return fileListEl


    def unpackFromDomNode(self,
                          fileListNode):
        """
        Unpack a File List from a DOM File List Node object.

        fileListNode:   DOM File List Node object (Node).

        Returns:        Reference to object itself.
        """
        self.setId(getAttribValue(fileListNode, "Id"))
        self.setComment(getAttribValue(fileListNode, "Comment", 1))
        self.setStatus(getAttribValue(fileListNode, "Status", 1))

        # Unpack the File Status Elements.
        fileStatusNodes = ngamsGetChildNodes(fileListNode, "FileStatus")
        for fileStatusNode in fileStatusNodes:
            fileStatusObj = ngamsFileInfo.ngamsFileInfo()
            self.addFileInfoObj(fileStatusObj)
            fileStatusObj.unpackFromDomNode(fileStatusNode)

        # Unpack File List Elements.
        fileListNodes = ngamsGetChildNodes(fileListNode, "FileList")
        for fileListNode in fileListNodes:
            fileListObj = ngamsFileList()
            self.addFileListObj(fileListObj)
            fileListObj.unpackFromDomNode(fileListNode)

        return self


    def dumpBuf(self,
                ignoreUndefFields = 0):
        """
        Dump contents of the File List in a buffer and return this.

        ignoreUndefFields:  Don't take fields, which have a length of 0
                            (integer/0|1).

        Returns:            Buffer with an ASCII representation of the
                            contents of the object (string).
        """
        format = prFormat1()
        buf = "FileList:\n"
        if (not (ignoreUndefFields and not self.getId().strip())):
            buf += format % ("Id:", self.getId())
        if (not (ignoreUndefFields and not self.getComment().strip())):
            buf += format % ("Comment:", self.getComment())
        if (not (ignoreUndefFields and not self.getStatus().strip())):
            buf += format % ("Status:", self.getStatus())
        for fileInfoObj in self.getFileInfoObjList():
            buf += fileInfoObj.dumpBuf(ignoreUndefFields)
        for fileListObj in self.getFileListObjList():
            buf += fileListObj.dumpBuf(ignoreUndefFields)
        return buf
        

# EOF
