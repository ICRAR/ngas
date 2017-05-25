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
"""Deals with logging rotation files, archiving them, and removing old ones"""

import glob
import logging
import os

from ngamsLib import ngamsNotification
from ngamsLib.ngamsCore import rmFile, mvFile
from ngamsPClient import ngamsPClient


logger = logging.getLogger(__name__)
_ArchDestination = "user@example.com:/some/dir/" #Needs to be changed to a realistic Host for analysis work

def SndArchFileForAnalysis(srvObj, filename):
    try:
        simpleFilenm = filename.split("/")
        os.system("scp " + filename + " " + _ArchDestination + simpleFilenm[-1])
    except Exception, e:
        errMsg = str(e) + ". Attempting to send to another host archive file: " + \
                 filename
        ngamsNotification.notify(srvObj.getHostId(), srvObj.getCfg(), "REMOTE HOST NOT AVAILABLE", errMsg)
        raise Exception, errMsg

def run(srvObj, stopEvt, jan_to_srv_queue):

    logdir = os.path.dirname(srvObj.getCfg().getLocalLogFile())

    # The LOG-ROTATE-<timestamp>.nglog.unsaved pattern is produced by the
    # ngamsServer.NgasRotatingFileHandler class, which is the file handler
    # attached to the root logger in the server process
    logger.debug("Checking if there are unsaved rotated logfiles")
    for unsaved in glob.glob(os.path.join(logdir, 'LOG-ROTATE-*.nglog.unsaved')):

        # Remove the .unsaved bit, leave the rest
        fname = '.'.join(unsaved.split('.')[:-1])
        mvFile(unsaved, fname)

        file_uri = "file://" + fname
        host, port = srvObj.get_endpoint()
        ngamsPClient.ngamsPClient(host, port).archive(file_uri, 'ngas/nglog')
        SndArchFileForAnalysis(srvObj, fname)

    logger.debug("Check if there are old rotated logfiles to remove ...")
    max_rotations = max(min(srvObj.getCfg().getLogRotateCache(), 100), 0)
    logfiles = glob.glob(os.path.join(logdir, 'LOG-ROTATE-*.nglog'))
    logfiles.sort()
    for f in logfiles[max_rotations:]:
        logger.info("Removing rotated logfile %s", f)
        rmFile(f)