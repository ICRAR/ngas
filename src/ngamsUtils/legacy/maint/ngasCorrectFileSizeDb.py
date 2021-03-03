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
# "@(#) $Id: ngasCorrectFileSizeDb.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  09/08/2004  Created
#
"""
Checks the size of files on a disk and correct the size given in the DB if
necessary. Only the actual/archived size is corrected, not the uncompressed
size.

It is checked if the NGAS CRC32 checksum is corresponding to the value in
the DB.

Note, the disk to check must be inserted in the host where this script
is executed.
"""

import sys, os

from ngamsLib import ngamsDb, ngamsDiskInfo, ngamsFileInfo
from ngamsLib.ngamsCore import setLogCond, getHostName, getFileSize, NGAMS_FILE_STATUS_OK
from ngasUtils.src import ngasUtilsLib


setLogCond(0, "NGASLog", 0, "", 1)


def checkCorrectFileSizeDb(diskId,
                           correct = 0,
                           notifEmail = None):
    """
    Check the size of the files registered on the given disk. If a file
    has a size different from the one stored in the DB, the size is
    corrected in the DB

    diskId:     ID of disk for which to check the files (string).

    Returns:    Void
    """
    server, db, user, password = ngasUtilsLib.getDbPars()
    db = ngamsDb.ngamsDb(server, db, user, password, createSnapshot=0)
    tmpDiskInfo = ngamsDiskInfo.ngamsDiskInfo().read(getHostName(), db, diskId)
    if (tmpDiskInfo.getHostId() != getHostName()):
        raise Exception, "The disk: %s is not inserted in this system!!" %\
              diskId
    notifMsg = "\nFILE SIZE CHECK/CORRECTION REPORT:\n" +\
               "(only files with an incorrect size in the DB are reported)\n"+\
               "\nDisk ID: " + diskId + "\n" +\
               128 * "="
    sys.stdout.write(notifMsg)
    dbCursor = db.getFileSummary1(getHostName(), [diskId], fileStatus=[])
    while (1):
        res = dbCursor.fetch(128)
        if (not res): break
        for fileInfo in res:
            # IMPL: This works only on calibration frames for the moment.
            #       (these are uncompressed -> file size == uncompr file size)
            filename      = fileInfo[2]
            if (filename.find("/M.") == -1): continue
            mountPoint    = fileInfo[1]
            dbChecksum    = fileInfo[3]
            fileId        = fileInfo[5]
            fileVer       = fileInfo[6]
            dbFileSize    = int(fileInfo[7])
            diskId        = fileInfo[9]
            complFilename = os.path.normpath(mountPoint + "/" + filename)
            diskFileSize = getFileSize(complFilename)
            msg = "\nCheck size of file: %s/%s/%d: " % (diskId,fileId,fileVer)
            if (diskFileSize == dbFileSize):
                msg += "Size OK"
            else:
                msg += "SIZE WRONG!"
                # Check checksum.
                from ngamsPlugIns import ngamsGenCrc32
                checksum = ngamsGenCrc32.ngamsGenCrc32(None, complFilename, 0)
                if (checksum != dbChecksum):
                    msg += " ILLEGAL CHECKSUM!"
                else:
                    msg += " CHECKSUM OK!"
                    if (correct):
                        tmpFileInfo = ngamsFileInfo.\
                                      ngamsFileInfo().read(db, fileId, fileVer,
                                                           diskId)
                        tmpFileInfo.setFileSize(diskFileSize)
                        tmpFileInfo.setUncompressedFileSize(diskFileSize)
                        tmpFileInfo.setFileStatus(NGAMS_FILE_STATUS_OK)
                        if (not tmpFileInfo.getCreationDate()):
                            tmpFileInfo.setCreationDate(tmpFileInfo.\
                                                        getIngestionDate())
                        tmpFileInfo.write(getHostName(), db, 0, 1)
                        msg += " FILE SIZE CORRECTED!"
                notifMsg += msg
            sys.stdout.write(msg)
            sys.stdout.flush()
    notifMsg += "\n%s\n" % (128 * "=")
    print "\n" + 128 * "="
    if (notifEmail):
        ngasUtilsLib.sendEmail("FILE SIZE CHECK/CORRECTION REPORT",
                               notifEmail, notifMsg, "text/plain",
                               "FILE-SIZE-CHECK-REP-%s" % diskId)


def correctUsage():
    """
    Generate buffer with man-page.

    Returns:   Buffer with man-page (string).
    """
    buf = "\nCorrect usage is:\n\n"
    buf += "> python ngasCorrectFileSizeDb.py -diskId <Disk ID> [-correct] " +\
           "[-accessCode <Code>] [-notifEmail <Email List>]\n"
    return buf


if __name__ == '__main__':
    """
    Main function to invoke the tool.
    """
    # Parse input parameters.
    diskId       = None
    correct      = 0
    accessCode   = None
    notifEmail   = None
    idx          = 1
    while idx < len(sys.argv):
        par = sys.argv[idx].upper()
        try:
            if (par == "-ACCESSCODE"):
                idx += 1
                accessCode = sys.argv[idx]
            elif (par == "-CORRECT"):
                correct = 1
            elif (par == "-DISKID"):
                idx += 1
                diskId = sys.argv[idx]
            elif (par == "-NOTIFEMAIL"):
                idx += 1
                notifEmail = sys.argv[idx]
            else:
                sys.exit(1)
            idx += 1
        except Exception, e:
            print "\nProblem initializing tool: %s\n" %  str(e)
            print correctUsage()
            sys.exit(1)
    if (not notifEmail):
        notifEmail = ngasUtilsLib.\
                     getParNgasRcFile(ngasUtilsLib.NGAS_RC_PAR_NOTIF_EMAIL)
    if (not accessCode): accessCode = ngasUtilsLib.input("Enter Access Code:")
    try:
        if (not diskId):
            print correctUsage()
            raise Exception, "Incorrect command line parameter(s) given!"
        ngasUtilsLib.checkAccessCode(accessCode)
        checkCorrectFileSizeDb(diskId, correct, notifEmail)
    except Exception, e:
        print "\nProblem encountered:\n\n" + str(e) + " -- bailing out\n"


# EOF
