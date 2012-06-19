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
# "@(#) $Id: ngamsEvent.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  26/05/2003  Created
#

"""
Contains class to do event synchronization between threads.
"""

import threading


class ngamsEvent:
    """
    Class to do event synchronization between threads.
    """
    
    def __init__(self):
        """
        Constructor method.
        """
        self.__event = threading.Event()
        self.__eventInfoList = []


    def addEventInfo(self,
                     eventInfo):
        """
        Set the event info to be transferred from one thread to the other.

        eventInfo:    Event info (free format).

        Returns:      Reference to object itself.
        """
        self.__eventInfoList.append(eventInfo)
        return self


    def getEventInfoList(self):
        """
        Get the event info to be transferred from one thread to the other.
        
        Returns:      Event info (free format).
        """
        return self.__eventInfoList


    def isSet(self):
        """
        Returns 1  if the internal event flag is set.

        Returns:  Value of event flag (integer/0|1).
        """
        return self.__event.isSet()


    def set(self):
        """
        Set the internal event flag.

        Returns:   Reference to object itself.
        """
        self.__event.set()
        return self


    def clear(self):
        """
        Clear the internal event flag.

        Returns:   Reference to object itself.
        """
        self.__event.clear()
        self.__eventInfoList = []
        return self


    def wait(self,
             timeOut = None):
        """
        Wait for event flag to be set, possibly applying a timeout.

        timeOut:    Timeout in seconds (float).

        Returns:   Reference to object itself.
        """
        if (timeOut):
            self.__event.wait(timeOut)
        else:
            self.__event.wait()
        return self


# EOF
