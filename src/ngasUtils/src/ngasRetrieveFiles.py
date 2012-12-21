

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
# "@(#) $Id: ngasRetrieveFiles.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  04/02/2003  Created
#

"""
Script that retrieves all files from a given Disk ID.
"""

import time

from ngams import *
import ngamsDb, ngamsPClient


def retrieveFiles(diskIdList):
    """
    Retrieve all files associated to a list of Disk IDs.

    diskIdList:   List of Disk IDs (list).

    Returns:      Void.
    """
    dbCon = ngamsDb.ngamsDb("ESOECF", "ngas", "ngas", "bmdhc19wdw==")
    client = ngamsPClient.ngamsPClient("jewel1", 7777)
    for diskId in diskIdList:
        dbCur = dbCon.getFileInfoList("IBM-DTLA-307075-YSDYSG3X871")
        while (1):
            fileList = dbCur.fetch(100)
            if (not fileList): break
            for fileInfo in fileList:
                fileId = fileInfo[ngamsDb.NGAS_FILES_FILE_ID]
                fileVersion = fileInfo[ngamsDb.NGAS_FILES_FILE_VER]
                print "Retrieving file with File ID/Version: %s/%d" %\
                      (fileId, fileVersion)
                startTime = time.time()
                res = client.retrieve2File(fileId, fileVersion,"/tmp/TestFile")
                deltaTime = (time.time() - startTime)
                if (res.getStatus() == NGAMS_SUCCESS):
                    format = "  - Successfully retrieved file with " +\
                             "File ID/Version: %s/%d/Time: %.3fs"
                    print format % (fileId, fileVersion, deltaTime)
                else:
                    format = "  - ERROR retrieving file with " +\
                             "File ID/Version: %s/%d: %s/Time: %.3fs"
                    print format % (fileId, fileVersion,
                                    str(res.getMessage()).replace("\n", " - "),
                                    deltaTime)
                rmFile("/tmp/TestFile")


if __name__ == '__main__':
    """
    Main function.
    """
    retrieveFiles(["IC35L080AVVA07-0-VNC402A4C86K1A",
                   "IBM-DTLA-307075-YSDYSG6P691",
                   "IC35L080AVVA07-0-VNC403A4GUUJAG",
                   "IBM-DTLA-307075-YSDYSG3P399"])


# EOF
