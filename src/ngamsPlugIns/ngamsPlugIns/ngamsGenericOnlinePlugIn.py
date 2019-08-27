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
# "@(#) $Id: ngamsGenericOnlinePlugIn.py,v 1.5 2008/08/19 20:46:04 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  26/02/2007  Created.
#
"""
Contains the Generic Online Plug-In.

The main features of this plug-in are:

- The mounting of the Data Volumes is left to the system. Usually these will be
mounted at boot time.

- The Data Volumes should be mounted (or located) under the directories:

  /<NGAS Root Dir>/<Volume Dir> or /<NGAS Root Dir>

- A Data Volume is accepted if it contains a file named '.ngas_volume_id'.

- This file should be created by the tool 'ngasPrepareVolume'.

- Other directories found in the specified directory, not containing the
'.ngas_volume_id' file, are ignored.

"""

import glob
import logging
import os

from ngamsLib import ngamsPhysDiskInfo
from ngamsPlugIns.ngamsGenericPlugInLib import NGAS_VOL_INFO_FILE, \
    loadVolInfoFile, NGAS_VOL_INFO_ID, NGAS_VOL_INFO_IGNORE, NGAS_VOL_INFO_TYPE, \
    NGAS_VOL_INFO_MANUFACT


logger = logging.getLogger(__name__)

def ngamsGenericOnlinePlugIn(srvObj,
                             reqPropsObj = None):
    """
    Scan the specified Root Directory/Volume Directory for NGAS Volumes
    and register these as operational volumes.

    srvObj:        Reference to instance of the NG/AMS Server
                   class (ngamsServer).

    reqPropsObj:   NG/AMS request properties object (ngamsReqProps).

    Returns:       Disk info dictionary (dictionary).
    """
    rootDir = srvObj.getCfg().getRootDirectory()
    volumeDir = srvObj.getCfg().getVolumeDirectory()

    # Build the root directory for the NGAS Volumes and make a glob to
    # get directories under this.
    # Note: the ngamsCfg object already calculates an absolute path, which is
    # probably not nice. We still have to account for that though
    if volumeDir.startswith('/'):
        ngasVolDir = volumeDir
    else:
        ngasVolDir = os.path.normpath(rootDir + os.sep + volumeDir)
    dirList = glob.glob(ngasVolDir + os.sep + "*")
    diskInfoDic = {}
    logger.debug('Will check the following directories for rootDir/volumeDir %s/%s: %r', rootDir, volumeDir, dirList)
    for dir in dirList:
        # Check if a '.ngas_volume_info' is found under the directory.
        volInfoFile = os.path.normpath(os.path.join(dir, NGAS_VOL_INFO_FILE))
        if (os.path.exists(volInfoFile)):
            # - It exists, load it
            volInfoDic = loadVolInfoFile(volInfoFile)
            # Create an ngamsPhysDiskInfo object with the information about
            # the slot.
            diskId     = volInfoDic[NGAS_VOL_INFO_ID]
            devName    = NGAS_VOL_INFO_IGNORE
            portNo     = -1
            slotId     = dir.split(os.sep)[-1]
            mtPt       = dir
            status     = "OK"
            capGb      = -1
            model      = NGAS_VOL_INFO_IGNORE
            serialNo   = NGAS_VOL_INFO_IGNORE
            if NGAS_VOL_INFO_TYPE in volInfoDic:
                diskType = volInfoDic[NGAS_VOL_INFO_TYPE]
            else:
                diskType = NGAS_VOL_INFO_IGNORE
            if NGAS_VOL_INFO_MANUFACT in volInfoDic:
                manufact = volInfoDic[NGAS_VOL_INFO_MANUFACT]
            else:
                manufact = NGAS_VOL_INFO_IGNORE
            msg = "Registering volume with parameters: Disk ID: %s, " +\
                  "Device: %s, Port No: %s, Slot ID: %s, Mount Point: %s, "+\
                  "Status: %s, Capacity (GB): %s, Model: %s, Serial#: %s, " +\
                  "Type: %s, Manufacturer: %s"
            logger.debug(msg, diskId, devName, str(portNo), slotId, mtPt,
                           status, str(capGb), model, serialNo, diskType,
                           manufact)
            diskInfoDic[str(slotId)] = ngamsPhysDiskInfo.\
                                       ngamsPhysDiskInfo().\
                                       setPortNo(portNo).\
                                       setSlotId(slotId).\
                                       setMountPoint(mtPt).\
                                       setStatus(status).\
                                       setCapacityGb(capGb).\
                                       setModel(model).\
                                       setSerialNo(serialNo).\
                                       setType(diskType).\
                                       setManufacturer(manufact).\
                                       setDiskId(diskId).\
                                       setDeviceName(devName)
        else:
            logger.debug("%s does not exist", volInfoFile)

    return diskInfoDic