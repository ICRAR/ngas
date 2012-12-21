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
# "@(#) $Id: ngamsCacheEntry.py,v 1.2 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  14/03/2008  Created
#

"""
Class to handle the information in connection with one entry in the NGAS Cache.
"""

import PccUtTime
from   ngams import *
import ngamsLib, ngamsFileSummary1


class ngamsCacheEntry(ngamsFileSummary1.ngamsFileSummary1):
    """
    Class to handle the information in connection with one entry in the
    NGAS Cache.
    """
    
    def __init__(self):
        """
        Constructor method.
        """
        self.__cacheDelete    = False
        self.__lastCheck      = None
        self.__cacheTime      = None
        self.__cacheEntryPars = {}


    def setCacheDelete(self,
                       delete):
        """
        Set the Cache Delete member of the class.

        delete:    File scheduled for deletion (boolean).

        Returns:   Reference to object itself.
        """
        self.__cacheDelete = delete
        return self


    def getCacheDelete(self):
        """
        Get the value of the Cache Delete member.

        Returns:  Value of the Cache Delete member (boolean).
        """
        return self.__cacheDelete


    def setLastCheck(self,
                     lastCheck):
        """
        Set the Last Check (global) member of the class.

        lastCheck:  Time for the last check in seconds since epoch (float).

        Returns:     Reference to object itself.
        """
        try:
            self.__lastCheck = float(lastCheck)
        except:
            msg = "Wrong format of Last Check given: %s" % str(lastCheck)
            raise Exception, msg   
        return self


    def getLastCheck(self):
        """
        Get the value of the Last Check (global) member.

        Returns:  Value of the Last Check member (float).
        """
        return self.__lastCheck


    def setCacheTime(self,
                     cacheTime):
        """
        Set the Cache Time member of the class.

        cacheTime:  Time for when the file entered the cache in seconds since
                    epoch (float).

        Returns:     Reference to object itself.
        """
        try:
            self.__cacheTime = float(cacheTime)
        except:
            msg = "Wrong format of Cache Time given: %s" % str(cacheTime)
            raise Exception, msg   
        return self


    def getCacheTime(self):
        """
        Get the value of the Cahce Time member.

        Returns:  Value of the Cahce Time member (float).
        """
        return self.__cacheTime


    def addPar(self,
               par,
               value):
        """
        Add a parameter in the set of parameters of the object.

        par:     Parameter name (string).

        value:   Value associated to parameter (<Object>).

        Returns: Reference to object itself.
        """
        T = TRACE(5)
        
        self.__cacheEntryPars[par] = value
        return self


    def getPar(self,
               par):
        """
        Return value of parameter associated to the object.

        par:      Parameter name (string).

        Returns:  Value associated to the parameter name (<Object> | None).
        """
        T = TRACE(5)

        if (self.__cacheEntryPars.has_key(par)):
            return self.__cacheEntryPars[par]
        else:
            return None


if __name__ == '__main__':
    """
    Main function.
    """
    setLogCond(0, 0, "", 5)
        

# EOF
