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
# "@(#) $Id: ngamsDppiStatus.py,v 1.5 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  07/01/2002  Created
#

"""
Contains class for handling the return result for the Data Processing Plug-Ins.
"""

from   ngams import *
import ngamsLib


class ngamsDppiResult:
    """
    Class to handle a sub-result produced by a Data Processing Plug-In.
    """
    
    def __init__(self,
                 dataType,
                 mimeType = "",
                 dataRef = "",
                 refFilename = "",
                 procDir = "",
                 dataSize = -1):
        """
        Constructor method.

        dataType:         Type of data referred to by object - NGAMS_PROC_FILE,
                          NGAMS_PROC_DATA or NGAMS_PROC_STREAM (string).
        
        mimeType:         Mime-type of data (string).
        
        dataRef:          Data buffer, the name of the file produced by the
                          processing or File Object from where to read the
                          data (string|integer).

        refFilename:      The reference name of the file. This is e.g.
                          the name being written into the Content-Disposition
                          of the HTTP reply to a Retrieve Request (string).
        
        procDir:          Name of temporary directory in which the
                          processing was carried (if created) (string).

        dataSize:         Size of the data. MUST BE SET FOR A STREAM!
                          (integer).
        
        Returns:          Void.
        """
        T = TRACE()

        self.setObjDataType(dataType)
        self.setMimeType(mimeType)
        self.setDataRef(dataRef)
        self.setRefFilename(refFilename)
        self.setProcDir(procDir)
        self.setDataSize(dataSize)


    def setObjDataType(self,
                       dataType):
        """
        Set the Object Data Type to indicate that data is stored in
        the object.

        dataType:  Type of data object is referring to (NGAMS_PROC_DATA|
                                                        NGAMS_PROC_FILE|
                                                        NGAMS_PROC_STREAM).

        Returns:   Reference to object itself.
        """
        self.__objDataType = dataType
        return self


    def getObjDataType(self):
        """
        Return Object Data Type indicating which kind of data the
        object refers to.

        Returns:   Object Data Type (NGAMS_PROC_DATA|NGAMS_PROC_FILE|
                                     NGAMS_PROC_STREAM).
        """
        return self.__objDataType


    def setMimeType(self,
                    mimeType):
        """
        Set the Mime-Type of the data produced by the processing.

        mimeType:  Mime-type of data (string).

        Returns:   Reference to object itself.
        """
        self.__mimeType = mimeType
        return self


    def getMimeType(self):
        """
        Return the Mime-Type of the data contained in the object.

        Returns:   Mime-type of data produced by processing (string).
        """
        return self.__mimeType


    def setDataRef(self,
                   dataRef):
        """
        Set the data (result) of the processing. This can be either a filename
        referring to a file in which the data is stored, or it can be the data
        directly.

        dataRef:     Data produced by the processing. Can be either a
                     filename or the data directly (string).

        Returns:     Reference to object itself.
        """
        self.__dataRef = dataRef
        return self


    def getDataRef(self):
        """
        Return reference to the data produced by the processing. This can be
        either a filename in which the data is stored, or the data directly.

        Returns:   Reference to data produced by processing
                   (string|socket/integer).
        """
        return self.__dataRef


    def setDataSize(self,
                    size):
        """
        Set the data size (in bytes).

        size:     Size of data in bytes (integer).

        Returns:  Reference to object itself.
        """
        self.__dataSize = int(size)
        return self
    

    def getDataSize(self):
        """
        Return the size of the data produced (in bytes).

        Returns:    Data size in bytes (integer).
        """
        if (self.__dataSize != -1):
            return self.__dataSize
        else:
            if (self.getObjDataType() == NGAMS_PROC_DATA):
                return len(self.getDataRef())
            elif (self.getObjDataType() == NGAMS_PROC_FILE):
                return getFileSize(self.getDataRef())
            else:
                return -1


    def setRefFilename(self,
                       refFilename):
        """
        Set the Reference Filename of the status object. The Reference
        Filename is the name which is set as filename in the
        'Content-disposition' HTTP header in the HTTP response when
        sending back data from a Retrieval Request.

        reFilename:   Reference Filename (string).

        Returns:      Reference to object itself.
        """     
        self.__refFilename = os.path.basename(refFilename)        
        return self

    
    def getRefFilename(self):
        """
        Return the name of the Reference Filename. See also setRefFilename().

        Returns:    Name of Reference Filename (string).
        """
        return self.__refFilename
    

    def setProcDir(self,
                   procDir):
        """
        Set the name of the directory used for the processing.

        procDir:    Name of Processing Directory (string).

        Returns:    Reference to object itself.
        """
        self.__procDir = procDir
        return self


    def getProcDir(self):
        """
        Get the name of the directory used for the processing.

        Returns:   
        """
        return self.__procDir
    

class ngamsDppiStatus:
    """
    Handle the data (results) produced by a Data Processing Plug-In.
    """
    
    def __init__(self):
        """
        Constructor method.
        """
        self.__resultObjList = []


    def addResult(self,
                  dppiResultObj):
        """
        Add an NG/AMS Data Processing Plug-In Result Object in the object.

        dppiResultObj:   Reference to result object (ngamsDppiResult).

        Returns:         Reference to object itself.
        """
        self.__resultObjList.append(dppiResultObj)
        return self


    def getResultList(self):
        """
        Return reference to the processing result list.

        Returns:   Result object list (list with ngamsDppiResult objects).
        """
        return self.__resultObjList

    
    def noOfResultObjs(self):
        """
        Return the number of result object contained in the object.
        
        Returns:       Number of result objects (integer).
        """
        return len(self.__resultObjList)


    def getResultObject(self,
                        no):
        """
        Return a specific result object.

        no:         Number of the requested result object.
                    First object = 0 (integer).
              
        Returns:    Reference to result object number (ngamsDppiResult).
        """
        return self.__resultObjList[no]


# EOF
