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
import os
import shutil

from ngamsLib.ngamsCore import info
import ngamsServer


def ngamsJanitorCheckUnsavedLogFile(srvObj, stopEvt):
    """
	Checks to see if we have an unsaved log file after a shutdown and
    archives them.

   srvObj:            Reference to NG/AMS server class object (ngamsServer).

   Returns:           Void.
   """
    info(4, "Checking if we have unsaved Log File ")
    logFile = srvObj.getCfg().getLocalLogFile()
    logPath = os.path.dirname(logFile)
    if (os.path.exists(srvObj.getCfg().getLocalLogFile())):
        unsavedLogFiles = glob.glob(logPath + '/*.unsaved')
        if (len(unsavedLogFiles) > 0):
            info(3, "Archiving unsaved log-files ...")
            for ulogFile in unsavedLogFiles:
                ologFile = '.'.join(ulogFile.split('.')[:-1])
                shutil.move(ulogFile, ologFile)
                ngamsServer.ngamsArchiveUtils.archiveFromFile(srvObj, ologFile, 0,
                                                  'ngas/nglog', None)