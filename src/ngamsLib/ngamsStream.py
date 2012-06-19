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
# "@(#) $Id: ngamsStream.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  12/05/2001  Created
#

"""
Contains the implementation of the Stream Class used to handle the
information in connection with a Stream definition.
"""

from ngams import *

           
class ngamsStream:
    """
    Class to handle the information in connection with a Stream definition.
    """

    def __init__(self,
                 mimeType = "",
                 plugIn = "",
                 plugInPars = "",
                 stoSetList = [],
                 hostIdList = []):
        """
        Constructor method.
        """
        if (mimeType):
            self.__mimeType = mimeType
        else:
            self.__mimeType = ""
        if (plugIn):
            self.__plugIn = plugIn
        else:
            self.__plugIn = ""
        if (plugInPars):
            self.__plugInPars = plugInPars
        else:
            self.__plugInPars = ""
        if (stoSetList):
            self.__storageSetIdList = stoSetList
        else:
            self.__storageSetIdList = []
        if (hostIdList):
            self.__hostIdList = hostIdList
        else:
            self.__hostIdList = []


    def setMimeType(self,
                    mimeType):
        """
        Set the mime-type of the Stream definition.

        mimeType:    Mime-type (string).

        Returns:     Reference to object itself.
        """
        self.__mimeType = str(mimeType)
        return self


    def getMimeType(self):
        """
        Return mime-type of Stream definition.

        Returns:     Mime-type (string).
        """
        return self.__mimeType


    def setPlugIn(self,
                  plugIn):
        """
        Set the name of the Plug-In of the Stream definition.

        plugIn:      Name of plug-in (string).

        Returns:     Reference to object itself.
        """
        self.__plugIn = str(plugIn)
        return self


    def getPlugIn(self):
        """
        Return plug-in of Stream definition.

        Returns:     Plug-in (string).
        """
        return self.__plugIn


    def setPlugInPars(self,
                      plugInPars):
        """
        Set the input parameters of the Plug-In.

        plugIn:      Input parameter (string).

        Returns:     Reference to object itself.
        """
        self.__plugInPars = str(plugInPars)
        return self


    def getPlugInPars(self):
        """
        Return plug-in parameters of Stream definition.

        Returns:     Plug-in parameters (string).
        """
        return self.__plugInPars


    def addStorageSetId(self,
                        id):
        """
        Add reference to Storage Set ID.

        id:          Storage Set ID (string).

        Returns:     Reference to object itself.
        """
        self.__storageSetIdList.append(str(id))
        return self


    def getStorageSetIdList(self):
        """
        Return list of Storage Set ID references.

        Returns:   List of Storage Set IDs (list).
        """
        return self.__storageSetIdList


    def addHostId(self,
                  id):
        """
        Add reference to Archiving Unit ID.

        id:          Host ID (or <Host>:<Port>) (string).

        Returns:     Reference to object itself.
        """
        self.__hostIdList.append(str(id))
        return self


    def getHostIdList(self):
        """
        Return list of Host ID references.

        Returns:   List of Host IDs (list).
        """
        return self.__hostIdList

        
# EOF
