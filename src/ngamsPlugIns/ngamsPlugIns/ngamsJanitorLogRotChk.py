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
import math
import os
import shutil
import time

from ngamsLib.ngamsCore import info, isoTime2Secs, iso8601ToSecs, takeLogSem, relLogSem, getLocation, logFlush
import ngamsServer
from pccLog import PccLog


def ngamsJanitorLogRotChk(srvObj, stopEvt):
    """
	Checks to see if its time to rotate the log file.

   srvObj:            Reference to NG/AMS server class object (ngamsServer).

   Returns:           Void.
   """
    info(4, "Checking if a Local Log File rotate is due ...")
    logFile = srvObj.getCfg().getLocalLogFile()
    logPath = os.path.dirname(logFile)
    hostId = srvObj.getHostId()
    #
    logFo = None
    try:
        takeLogSem()

        # For some reason we cannot use the creation date ...
        # Log file starts off empty - so set null value for line
        line = None
        with open(logFile, "r") as logFo:
            for line in logFo:
                if not line or "[INFO]" in line:
                    break
        if line:  # we have a non null value for line ?
            creTime = line.split(" ")[0].split(".")[0]
            logFileCreTime = iso8601ToSecs(creTime)
            logRotInt = isoTime2Secs(srvObj.getCfg(). \
                                     getLogRotateInt())
            deltaTime = (time.time() - logFileCreTime)
            if (deltaTime >= logRotInt):
                now = time.time()
                ts = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(now))
                ts += ".%03d" % ((now - math.floor(now)) * 1000.)
                # It's time to rotate the current Local Log File.
                rotLogFile = "LOG-ROTATE-%s.nglog" % (ts,)
                rotLogFile = os.path. \
                    normpath(logPath + "/" + rotLogFile)
                PccLog.info(1, "Rotating log file: %s -> %s" % \
                            (logFile, rotLogFile), getLocation())
                logFlush()
                shutil.move(logFile, rotLogFile)
                open(logFile, 'a').close()
                msg = "NG/AMS Local Log File Rotated and archived (%s)"
                PccLog.info(1, msg % hostId, getLocation())
        relLogSem()
        if (line != "" and deltaTime >= logRotInt):
            ngamsServer.ngamsArchiveUtils.archiveFromFile(srvObj, rotLogFile, 0,
                                              'ngas/nglog', None)
    except Exception, e:
        relLogSem()
        if (logFo): logFo.close()
        raise e
    info(4, "Checked for Local Log File rotatation")
