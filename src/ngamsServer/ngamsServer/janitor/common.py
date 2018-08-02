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
import time

from ngamsLib.ngamsCore import rmFile


logger = logging.getLogger(__name__)

class StopJanitorThreadException(Exception):
    pass

def checkStopJanitorThread(stopEvt):
    """
    Checks if the Janitor Thread should be stopped, raising an exception if needed
    """
    if stopEvt.is_set():
        logger.info("Exiting Janitor Thread")
        raise StopJanitorThreadException()

def suspend(stopEvt, t):
    """
    Sleeps for at maximum ``t`` seconds, or until the Janitor Thread is signaled
    to stop
    """
    if stopEvt.wait(t):
        logger.info("Exiting Janitor Thread")
        raise StopJanitorThreadException()

def checkCleanDirs(startDir,
                   dirExp,
                   fileExp,
                   useLastAccess):
    """
    Check a tree of directories. Delete all empty directories older than
    the given Directory Expiration. Also files are deleted if the file is
    older than File Expiration given.

    startDir:       Starting directory. The function will move downwards from
                    this starting point (string).

    dirExp:         Expiration time in seconds for directories. Empty
                    directories older than this time are deleted (integer).

    fileExp:        Expiration time in seconds for file. Empty file older than
                    this time are deleted (integer).

    useLastAccess:  Rather than using the creation time as reference, the
                    last modification and access date should be used
                    (integer/0|1).

    Returns:        Void.
    """
    timeNow = time.time()
    # TODO: Potential memory bottleneck. Use 'find > file' as for REGISTER
    #       Command.
    entryList = glob.glob(startDir + "/*")
    # Work down through the directories in a recursive manner. If some
    # directories are not deleted during this run because they have contents,
    # they might be deleted during one of the following runs.
    for entry in entryList:
        stat = os.stat(entry)
        if (not useLastAccess):
            refTime = stat.st_ctime # creation time
        else:
            refTime1 = stat.st_mtime # modification time
            refTime2 = stat.st_atime # access time
            if (refTime1 > refTime2):
                refTime = refTime1
            else:
                refTime = refTime2
        if (os.path.isdir(entry)):
            checkCleanDirs(entry, dirExp, fileExp, useLastAccess)
            tmpGlobRes = glob.glob(entry + "/*")
            if (tmpGlobRes == []):
                if ((timeNow - refTime) > dirExp):
                    logger.debug("Deleting temporary directory: %s", entry)
                    rmFile(entry)
        else:
            if (fileExp):
                if ((timeNow - refTime) > fileExp):
                    logger.debug("Deleting temporary file: %s", entry)
                    rmFile(entry)