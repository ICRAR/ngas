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
# "@(#) $Id: ngamsPhysDiskInfo.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  08/10/2001  Created
#

"""
Contains definition of class to handle the 'physical' information
for a disk installed in an NGAS system.
"""

from ngams import *


class ngamsPhysDiskInfo:
    """
    Object to keep information about one of the disks mounted in the
    NGAS system.
    """
    
    def __init__(self):
        """
        Constructor method.
        """
        self.__portNo       = 0
        self.__slotId       = ""
        self.__mountPoint   = ""
        self.__status       = ""
        self.__capacityGb   = 0
        self.__model        = ""
        self.__serialNo     = ""
        self.__type         = ""
        self.__manufacturer = ""
        self.__diskId       = ""
        self.__deviceName   = ""


    def getObjStatus(self):
        """
        Return a list with the current status of the object. The format
        of the list is:

          [[<xml attribute name>, <value>, ...], ...]

        Returns:    List with object status (list/list).
        """
        # Fields in XML document/ASCII dump
        return [["CapacityGb",   self.getCapacityGb()],
                ["DeviceName",   self.getDeviceName()],
                ["DiskId",       self.getDiskId()],
                ["Manufacturer", self.getManufacturer()],
                ["Model",        self.getModel()],
                ["MountPoint",   self.getMountPoint()],
                ["PortNo",       self.getPortNo()],
                ["SerialNo",     self.getSerialNo()],
                ["SlotId",       self.getSlotId()],
                ["Status",       self.getStatus()],
                ["Type",         self.getType()]]


    def dumpBuf(self,
                ignoreUndefFields = 0):
        """
        Dump contents of object into a string buffer.

        ignoreUndefFields:     Don't take fields, which have a length of 0
                               (integer/0|1).
                            
        Returns:               String buffer with contents of object (string).
        """        
        format = prFormat1()
        buf = "DiskStatus:\n"
        objStat = self.getObjStatus()
        for fieldName, val in objStat:
            if (not ignoreValue(ignoreUndefFields, val)):
                buf += format % (fieldName + ":", val)
        return buf

    
    def setPortNo(self,
                  portNo):
        """
        Set port number.

        portNo:    Port number (integer).

        Returns:   Reference to object itself.
        """
        self.__portNo = int(portNo)
        return self


    def getPortNo(self):
        """
        Return port number.

        Returns:   Port number (integer).
        """
        return self.__portNo


    def setSlotId(self,
                  slotId):
        """
        Set Slot Id.

        slotId:    Slot ID (string).

        Returns:   Reference to object itself.
        """
        self.__slotId = slotId
        return self


    def getSlotId(self):
        """
        Return Slot ID.

        Returns:   Slot ID (string).
        """
        return self.__slotId


    def setMountPoint(self,
                      mountPoint):
        """
        Set mount point.

        mountPoint:   Mount point (string).
 
        Returns:      Reference to object itself.
        """
        self.__mountPoint = mountPoint
        return self


    def getMountPoint(self):
        """
        Return mount point.

        Returns:   Mount point (string).
        """
        return self.__mountPoint
   

    def setStatus(self,
                  status):
        """
        Set status of operation to query physical disk information.

        status:    Status of operation (string).

        Returns:   Reference to object itself.
        """
        self.__status = status
        return self


    def getStatus(self):
        """
        Return status of operation to query physical disk information.

        Returns:  Status of operation  (string).
        """
        return self.__status
   

    def setCapacityGb(self,
                      capacityGb):
        """
        Set capacity in GB.

        capacityGb:  Capacity in GB (integer).

        Returns:     Reference to object itself.
        """
        self.__capacityGb = int(capacityGb)
        return self


    def getCapacityGb(self):
        """
        Return capacity in GB.

        Returns:  Capacity in GB (integer).
        """
        return self.__capacityGb
   

    def setModel(self,
                 model):
        """
        Set model of disk.

        model:     Model (string).

        Returns:   Reference to object itself.
        """
        self.__model = model
        return self


    def getModel(self):
        """
        Return mode of disk.

        Returns:   Model (string).
        """
        return self.__model
   

    def setSerialNo(self,
                    serialNo):
        """
        Set serial number of disk.

        serialNo:  Serial number (string).

        Returns:   Reference to object itself.
        """
        self.__serialNo = serialNo
        return self


    def getSerialNo(self):
        """
        Return serial number of disk.

        Returns:  Serial number (string).
        """
        return self.__serialNo
   

    def setType(self,
                type):
        """
        Set type of disk.

        type:      Type (string).

        Returns:   Reference to object itself.
        """
        self.__type = type
        return self


    def getType(self):
        """
        Return type of disk.

        Returns:   Type (string).
        """
        return self.__type


    def setManufacturer(self,
                        manufacturer):
        """
        Set manufacturer of disk.

        manufacturer:   Manufacturer (string).

        Returns:        Reference to object itself.
        """
        self.__manufacturer = manufacturer
        return self


    def getManufacturer(self):
        """
        Return manufacturer of disk.

        Returns:   Manufacturer (string).
        """
        return self.__manufacturer


    def setDiskId(self,
                  diskId):
        """
        Set Disk ID for disk.

        diskId:    Disk ID (string).

        Returns:   Reference to object itself.
        """
        self.__diskId = diskId
        return self


    def getDiskId(self):
        """
        Return Disk ID for disk.

        Returns:   Disk ID (string).
        """
        return self.__diskId
   

    def setDeviceName(self,
                      deviceName):
        """
        Set device name for disk.

        deviceName:  Device name (string).

        Returns:     Reference to object itself.
        """
        self.__deviceName = deviceName
        return self


    def getDeviceName(self):
        """
        Return device name for disk.

        Returns:    Device name (string).
        """
        return self.__deviceName


# EOF
