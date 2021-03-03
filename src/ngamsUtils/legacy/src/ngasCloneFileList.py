

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
# "@(#) $Id: ngasCloneFileList.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  19/02/2003  Created
#

"""
Clones a list of files as given in a REMDISK Status Error Report or
given in a file list.

A REMDISK Error Report has the following contents:


REMDISK STATUS ERROR REPORT - MISSING FILE COPIES:

==Summary:

Date:                       2003-12-04T13:29:14.961
NGAS Host:                  ngahu
Disk ID:                    IC35L080AVVA07-0-VNC400A4C1G8RA
Files Detected:             30

==File List:

File ID                          Version      Copies
-------------------------------- -------      ------
MIDI.2003-10-07T04:50:08.496     1            2
MIDI.2003-10-05T22:15:15.000     1            2
MIDI.2003-10-05T23:17:51.000     1            2
MIDI.2003-10-06T07:09:49.662     1            2
...


A File List has the following contents:

<Disk ID> <File ID> <File Version>
<Disk ID> <File ID> <File Version>
...

"""
import sys, os, time

from ngams import *
import ngamsDb, ngamsPClient
import ngamsLib
import ngasUtils, ngasUtilsLib, ngasVerifyCloning


def loadFileList(fileListName):
    """
    Load the File List file or the

    fileListName:   Name of File List with references to files to
                    clone (string).

    Returns:        List with information about files to clone of the format:

                      [[<Disk ID>, <File ID>, <File Version>], ...]
    """
    fo = open(fileListName)
    fileBuf = fo.read()
    fo.close()
    fileLines = fileBuf.split("\n")

    if (fileBuf.find("REMDISK STATUS ERROR REPORT") != -1):
        print "ERROR: Re-implement this part"
        return []

        fileRefList = []
        idx = 0
        noOfLines = len(fileLines)
        while (idx < noOfLines):
            if (fileLines[idx].find("File ID") == -1): break
            idx += 1
        idx += 1
        while (idx < noOfLines):
            line = fileLines[idx]
            print "IMPL"
    else:
        fileRefList = ngasUtilsLib.parseFileList(fileListName)
    return fileRefList


def cloneFileList(host,
                  port,
                  fileListName,
                  notifEmail):
    """
    Clone the files given in the file list, which is a Notification Email
    Message from the NG/AMS Server.

    host:            Name of NGAS host to contact (string).

    port:            Port number used by NG/AMS Server (integer).

    fileListName:    File containing the Email Notification Message from
                     the NG/AMS Server about missing file copies (string).

    notifEmail:      Comma separated list of email recipients that should be
                     informed about the actions carried out, or the actions
                     that would be carried out if executing the command
                     (integer/0|1).

    Returns:         Void.
    """
    fileRefList = loadFileList(fileListName)
    ngamsClient = ngamsPClient.ngamsPClient(host, port)
    server, db, user, password = ngasUtilsLib.getDbPars()
    dbCon = ngamsDb.ngamsDb(server, db, user, password, 0)
    report = "FILE CLONING REPORT:\n\n"
    for fileInfo in fileRefList:
        diskId  = fileInfo[0]
        fileId  = fileInfo[1]
        fileVer = fileInfo[2]
        msg = "Cloning file: %s/%s/%s" % (str(diskId),str(fileId),str(fileVer))
        sys.stdout.write(msg)
        tmpMsg = " - Status: "
        sys.stdout.write(tmpMsg)
        msg += tmpMsg
        res = ngamsClient.clone(fileId, diskId, fileVer, wait=1)
        if (res.getStatus() == NGAMS_FAILURE):
            status = NGAMS_FAILURE
        else:
            # Wait for the command to finish.
            #status = ngasVerifyCloning.waitCloneCmd(res.getRequestId(),
            #                                        ngamsClient, 60)
            # Get the Disk ID of a disk in that system, different than the
            # source disk, where the file is stored.
            curObj = dbCon.getFileSummary1(host, [], [fileId])
            tmpFileList = curObj.fetch(1000)
            tmpDiskId = ""
            for fileInfo in tmpFileList:
                tmpDiskId  = fileInfo[ngamsDb.SUM1_DISK_ID]
                tmpFileVer = fileInfo[ngamsDb.SUM1_VERSION]
                if (tmpDiskId != diskId): break
            del curObj
            if (tmpDiskId != diskId):
                # Check that the file has been cloned.
                res = ngamsClient.sendCmdGen(host, port, NGAMS_CHECKFILE_CMD,1,
                                             "", [["host_id", host],
                                                  ["disk_id", tmpDiskId],
                                                  ["file_id", fileId],
                                                  ["file_version", fileVer]])
                if (res.getMessage().find("NGAMS_INFO_FILE_OK") != -1):
                    status = NGAMS_SUCCESS
                else:
                    status = NGAMS_FAILURE
            else:
                status = "CHECK!!!"
        sys.stdout.write(status + "\n")
        msg += status + "\n"
        report += msg

    report += 100 * "-" + "\n"
    print 100 * "-" + "\n"
    if (notifEmail):
        ngasUtilsLib.sendEmail("ngasCloneFileList: FILE CLONE REPORT",
                               notifEmail, report)



def correctUsage():
    """
    Generate buffer with man-page.

    Returns:   Buffer with man-page (string).
    """
    buf = "\nCorrect usage is:\n\n"
    buf += "> python ngasCloneFileList.py -fileList <File List> " +\
           "[-host <Host ID>] [-port <Port>]\n" +\
           "[-accessCode <Code>] [-notifEmail <Email List>]\n"
    return buf


if __name__ == '__main__':
    """
    Main function to invoke the tool.
    """
    # Parse input parameters.
    host         = ""
    port         = 0
    accessCode   = ""
    fileListName = ""
    notifEmail   = ""
    idx          = 1
    while idx < len(sys.argv):
        par = sys.argv[idx].upper()
        try:
            if (par == "-ACCESSCODE"):
                idx += 1
                accessCode = sys.argv[idx]
            elif (par == "-FILELIST"):
                idx += 1
                fileListName = sys.argv[idx]
            elif (par == "-HOST"):
                idx += 1
                host = sys.argv[idx]
            elif (par == "-PORT"):
                idx += 1
                port = int(sys.argv[idx])
            elif (par == "-NOTIFEMAIL"):
                idx += 1
                notifEmail = sys.argv[idx]
            else:
                sys.exit(1)
            idx += 1
        except Exception, e:
            print "\nProblem initializing Clone Tool: %s\n" %  str(e)
            print correctUsage()
            sys.exit(1)

    if (not notifEmail):
        notifEmail = ngasUtilsLib.\
                     getParNgasRcFile(ngasUtilsLib.NGAS_RC_PAR_NOTIF_EMAIL)
    if (host == ""):
        host = ngasUtilsLib.getParNgasRcFile(ngasUtilsLib.NGAS_RC_PAR_HOST)
    if (port == 0):
        port = int(ngasUtilsLib.\
                   getParNgasRcFile(ngasUtilsLib.NGAS_RC_PAR_PORT))
    try:
        if (not fileListName):
            print correctUsage()
            raise Exception, "Incorrect command line parameter(s) given!"
        if (not accessCode):
            accessCode = ngasUtilsLib.input("Enter Access Code:")
        ngasUtilsLib.checkAccessCode(accessCode)
        cloneFileList(host, port, fileListName, notifEmail)
    except Exception, e:
        print "\nProblem encountered:\n\n" + str(e) + " -- bailing out\n"


# EOF
