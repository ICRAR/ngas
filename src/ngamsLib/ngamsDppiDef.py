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
# "@(#) $Id: ngamsDppiDef.py,v 1.2 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  12/05/2001  Created
#

"""
Contains the implementation of the DPPI Definition Class, used to handle the
information in connection with one DPPI.
"""

from ngams import *


class ngamsDppiDef:
    """
    Class to contain info about one DPPI definition in the configuration.
    """

    def __init__(self,
                 plugInName = "",
                 plugInPars = ""):
        """
        Constructor method. set the name of the plug-in and possible
        parameters if defined.
        
        plugInName:   Name of plug-in (string).
        
        plugInPars:   Parameters for the plug-in (string).
        """
        self.setPlugInName(plugInName)
        self.setPlugInPars(plugInPars)
        self.__mimeTypeList = []


    def setPlugInName(self,
                      plugInName):
        """
        Set the name of the plug-in.

        plugInName:   Name of plug-in (string).

        Returns:      Reference to object itself.
        """
        self.__plugInName = str(plugInName)
        return self


    def getPlugInName(self):
        """
        Return the name of the plug-in.

        Returns:   Name of plug-in (string).
        """
        return self.__plugInName


    def setPlugInPars(self,
                      plugInPars):
        """
        Set the parameters for the plug-in.

        plugInPars:   Parameters for the plug-in (string).

        Returns:      Reference to object itself.
        """
        self.__plugInPars = str(plugInPars)
        return self


    def getPlugInPars(self):
        """
        Return the parameters for the plug-in.

        Returns:   Parameters for the plug-in (string).
        """
        return self.__plugInPars


    def addMimeType(self,
                    mimeType):
        """
        Add a mime-type definition to the object.

        mimeType:  Mime-type (string).

        Returns:   Reference to object itself.
        """
        self.__mimeTypeList.append(str(mimeType))
        return self


    def getMimeTypeList(self):
        """
        Return reference to list containing the mime-types.

        Returns:   List with mime-types (list).
        """
        return self.__mimeTypeList

        
# EOF
