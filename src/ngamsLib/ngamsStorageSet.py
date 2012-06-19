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
# "@(#) $Id: ngamsStorageSet.py,v 1.2 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  12/05/2001  Created
#

"""
Contains the implementation of the NG/AMS Storage Set Class.
"""

from ngams import *


class ngamsStorageSet:
    """
    Class to handle information for one Storage Set.
    """

    def __init__(self,
                 storageSetId = "",
                 diskLabel = "",
                 mainDiskSlotId = "",
                 repDiskSlotId = "",
                 mutex = 0,
                 synchronize = 0):
        """
        Constructor method.
        """
        if (storageSetId):
            self.__storageSetId = storageSetId
        else:
            self.__storageSetId   = ""
        if (diskLabel):
            self.__diskLabel      = diskLabel
        else:
            self.__diskLabel      = ""
        if (mainDiskSlotId):
            self.__mainDiskSlotId = mainDiskSlotId
        else:
            self.__mainDiskSlotId = ""
        if (repDiskSlotId):
            self.__repDiskSlotId  = repDiskSlotId
        else:
            self.__repDiskSlotId  = ""
        if (mutex):
            self.__mutex          = int(mutex)
        else:
            self.__mutex          = 0
        if (synchronize):
            self.__synchronize    = int(synchronize)
        else:
            self.__synchronize    = 0


    def setStorageSetId(self,
                        id):
        """
        Set ID for Storage Set.

        id:       ID for the set (string).

        Returns:  Reference to the object itself.
        """
        if (id == None): return self
        self.__storageSetId = str(id)
        return self


    def getStorageSetId(self):
        """
        Return the Storage Set ID.

        Returns:   Storage Set ID (string).
        """
        return self.__storageSetId


    def setDiskLabel(self,
                     label):
        """
        Set name of Disk Label.

        label:    Disk label prefix (string).

        Returns:  Reference to the object itself.
        """ 
        if (label == None): return self
        self.__diskLabel = str(label)
        return self


    def getDiskLabel(self):
        """
        Return the Disk Label.

        Returns:   Disk Label (string).
        """
        return self.__diskLabel


    def setMainDiskSlotId(self,
                          slotId):
        """
        Set the Slot ID for the Main Disk.

        slotId:        Slot ID for the Main Disk (string).

        Returns:       Reference to the object itself.
        """
        if (slotId == None): return self
        self.__mainDiskSlotId = str(slotId)
        return self


    def getMainDiskSlotId(self):
        """
        Return the Slot ID for the Main Disk.

        Returns:       Slot ID for the Main Disk (string).
        """
        return self.__mainDiskSlotId 


    def setRepDiskSlotId(self,
                         slotId):
        """
        Set the Slot ID for the Replication Disk.

        slotId:        Slot ID for the Replication Disk (string).

        Returns:       Reference to the object itself.
        """
        if (slotId == None): return self
        self.__repDiskSlotId = str(slotId)
        return self


    def getRepDiskSlotId(self):
        """
        Return the Slot ID for the Replication Disk.

        Returns:       Slot ID for the Replication Disk (string).
        """
        return self.__repDiskSlotId


    def setMutex(self,
                 mutex):
        """
        Set the Mutuel Disk Exclusion Flag.

        mutex:      0 or 1 (integer).

        Returns:    Reference to the object itself.
        """
        if (mutex == None): return self
        self.__mutex = getInt("StorageSet.Mutex", mutex)
        return self


    def getMutex(self):
        """
        Return the Mutuel Disk Exclusion Flag.

        Returns:   0 or 1 (integer).
        """
        return self.__mutex


    def setSynchronize(self,
                       sync):
        """
        Set the Synchronize Flag.

        sync:       0 or 1 (integer).

        Returns:    Reference to the object itself.
        """
        if (sync == None): return self
        self.__synchronize = getInt("StorageSet.Synchronize", sync)
        return self


    def getSynchronize(self):
        """
        Return the Synchronize Flag.

        Returns:   0 or 1 (integer).
        """
        return self.__synchronize


    def dumpBuf(self):
        """
        Dump the contents of the object into a string buffer.

        Returns:  String buffer with status of the object (string).
        """
        buf = "ngamsStorageSet Status:\n\n"
        buf += "Storage Set ID:           %s\n" % str(self.__storageSetId)
        buf += "Disk Label:               %s\n" % str(self.__diskLabel)
        buf += "Main Disk Slot ID:        %s\n" % str(self.__mainDiskSlotId)
        buf += "Replication Disk Slot ID: %s\n" % str(self.__repDiskSlotId)
        buf += "Mutex Flag:               %s\n" % str(self.__mutex)
        buf += "Synchronize Flag:         %s"   % str(self.__synchronize)
        return buf
        

# EOF
