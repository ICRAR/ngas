

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
# "@(#) $Id: ngasArchiveFileList.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  30/01/2004  Created
#

"""
The ngasArchiveFileList Tool is used to archive a set of file listed
in a File List:

[<Path>/]<Filename>
[<Path>/]<Filename>
...
"""

import sys, os, time

from ngams import *
import ngamsDb, ngamsLib, ngamsFileInfo, ngamsDiskInfo, ngamsRemUtils
import ngamsStatus, ngamsReqProps, ngamsPClient
import ngasUtils, ngasUtilsLib


def archiveFileList(host,
                    port,
                    fileListFile,
                    noVersioning,
                    notifEmail,
                    mimeType):
    """
    Archive a set of files from a file list.

    host:          Name of NGAS host to contact (string).

    port:          Port number used by NG/AMS Server (port).

    fileListFile:  File containing list of files to be archived (string).

    noVersioning:  Apply no versioning (integer/0|1).

    notifEmail:    Comma separated list of email recipients that should be
                   informed about the actions carried out, or the actions
                   that would be carried out if executing the command
                   (integer/0|1).

    mimeType:      Client defined mime-type (string).

    Returns:       Void.
    """
    archFileList = ngasUtilsLib.parseFileList(fileListFile)
    client = ngamsPClient.ngamsPClient(host, port)
    fileArchStatList = []
    for file in archFileList:
        info(0, "Archiving file: %s ... " % file[0])
        try:
            status = client.archive(file[0], mimeType, 1, noVersioning)
            fileArchStatList.append((file[0], status))
        except Exception, e:
            status = ngamsStatus.ngamsStatus().\
                     setStatus(NGAMS_FAILURE).setMessage(str(e))
            fileArchStatList.append((file[0], status))

    # Generate report.
    repFormat = "%-40s %-8s %-60s\n"
    report = "FILE ARCHIVING STATUS:\n\n"
    report += repFormat % ("Filename:", "Status", "Message")
    report += 140 * "-" + "\n"
    for stat in fileArchStatList:
        report += repFormat % (stat[0], stat[1].getStatus(),
                               stat[1].getMessage())
    report += 140 * "-" + "\n"
    print report
    if (notifEmail):
        ngasUtilsLib.sendEmail("ngasArchiveFileList: FILE ARCHIVING REPORT",
                               notifEmail, report)


def correctUsage():
    """
    Return the usage/online documentation in a string buffer.

    Returns:  Man-page (string).
    """
    buf = "\nCorrect usage is:\n\n" +\
          "> ngasArchiveFileList -fileList <File List> [-host <host>]\n" +\
          "                      [-port <Port>] [-accessCode <Code>]\n" +\
          "                      [-notifEmail <Email List>]\n" +\
          "                      [-noVersioning] [-mimeType <Mime-type>]" +\
          "\n\n"
    return buf

  
if __name__ == '__main__':
    """
    Main function to execute the tool.
    """
    setLogCond(0, "", 0, "", 0)

    # Parse input parameters.
    accessCode   = ""
    fileList     = ""
    host         = ""
    port         = 0
    noVersioning = 0
    notifEmail   = None
    mimeType     = ""
    idx = 1
    while idx < len(sys.argv):
        par = sys.argv[idx].upper()
        try:
            if (par == "-ACCESSCODE"):
                idx += 1
                accessCode = sys.argv[idx]
            elif (par == "-FILELIST"):
                idx += 1
                fileList = sys.argv[idx]
            elif (par == "-HOST"):
                idx += 1
                host = sys.argv[idx]
            elif (par == "-MIMETYPE"):
                idx += 1
                mimeType = sys.argv[idx]
            elif (par == "-NOVERSIONING"):
                noVersioning = 1
            elif (par == "-NOTIFEMAIL"):
                idx += 1
                notifEmail = sys.argv[idx]
            elif (par == "-PORT"):
                idx += 1
                port = int(sys.argv[idx])
            else:
                sys.exit(1)
            idx += 1
        except Exception, e:
            print "\nProblem executing the tool: %s\n" % str(e)
            print correctUsage()  
            sys.exit(1)
    if (not fileList):
        print correctUsage()
        sys.exit(1)
    if (notifEmail == None):
        notifEmail = ngasUtilsLib.\
                     getParNgasRcFile(ngasUtilsLib.NGAS_RC_PAR_NOTIF_EMAIL)
    if (host == ""):
        host = ngasUtilsLib.getParNgasRcFile(ngasUtilsLib.NGAS_RC_PAR_HOST)
    if (port == 0):
        port = int(ngasUtilsLib.\
                   getParNgasRcFile(ngasUtilsLib.NGAS_RC_PAR_PORT))
    try:
        if (not accessCode):
            accessCode = ngasUtilsLib.input("Enter Access Code:")
        ngasUtilsLib.checkAccessCode(accessCode)
        archiveFileList(host, port, fileList, noVersioning, notifEmail,
                        mimeType)
    except Exception, e:
        print "\nProblem encountered:\n\n" + str(e) + " -- bailing out\n"


# EOF
