#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2017
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
import glob
import logging
import os

from ngamsLib.ngamsCore import rmFile


logger = logging.getLogger(__name__)

def ngamsJanitorRotatedLogFilestoRemove(srvObj, stopEvt, jan_to_srv_queue):
    """
	Check if there are expired or rotated Local Log Files to remove.

   srvObj:            Reference to NG/AMS server class object (ngamsServer).

   Returns:           Void.
   """
    logger.debug("Check if there are rotated Local Log Files to remove ...")
    logFile = srvObj.getCfg().getLocalLogFile()
    logPath = os.path.dirname(logFile)

    rotLogFilePat = os.path.normpath(logPath + "/LOG-ROTATE-*.nglog")
    rotLogFileList = glob.glob(rotLogFilePat)
    delLogFiles = (len(rotLogFileList) -\
                   srvObj.getCfg().getLogRotateCache())
    if (delLogFiles > 0):
        rotLogFileList.sort()
        for n in range(delLogFiles):
            logger.debug("Removing Rotated Local Log File: " +\
                 rotLogFileList[n])
            rmFile(rotLogFileList[n])
    logger.debug("Checked for expired, rotated Local Log Files")