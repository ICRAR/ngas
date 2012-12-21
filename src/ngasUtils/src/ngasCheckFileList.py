

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
# "@(#) $Id: ngasCheckFileList.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  19/01/2004  Created
#

"""
Utility to check the consistency of a list of files.

A File List has the following contents:

  <Disk ID> <File ID> <File Version>
  <Disk ID> <File ID> <File Version>
  ...


It is also possible to use a DCC Report as input file list. The tool will
then generate a file list internally from this.
"""

import sys

from ngams import *
import ngamsPClient
import ngasUtils, ngasUtilsLib

setLogCond(0, "NGASLog", 0, "", 0)

     
def checkFileList(host,
                  port,
                  fileListFile,
                  notifEmail,
                  ignoreDiskId):
    """
    Check if each file in the list is acessible by sending a CHECKFILE
    Command to the specified NG/AMS Server.

    host:             Host name of remote NG/AMS Server (string).
    
    port:             Port number used by remote NG/AMS Server (integer).
    
    fileListFile:     File containing list of Files IDs for files
                      to check (string).

    notifEmail:       Comma separated list of email recipients that should be
                      informed about the actions carried out, or the actions
                      that would be carried out if executing the command
                      (integer/0|1).
                      
    ignoreDiskId:     Ignore the Disk ID of the file, just check that
                      one such file is available (integer/0|1).

    Returns:          Void.
    """
    fileRefList = ngasUtilsLib.parseFileList(fileListFile)
    client = ngamsPClient.ngamsPClient(host, port)
    fo = open(fileListFile)
    report = 100 * "=" + "\n"
    report += "FILE CHECK REPORT:\n"
    report += 100 * "-" + "\n"
    sys.stdout.write("\n" + report)
    for fileInfo in fileRefList:
        diskId  = fileInfo[0]
        fileId  = fileInfo[1]
        fileVer = fileInfo[2]
        if (not ignoreDiskId):
            msg = "Checking file: %s/%s/%s - Status: " %\
                  (str(diskId), str(fileId), str(fileVer))
        else:
            msg = "Checking file: %s/%s - Status: " %\
                  (str(fileId), str(fileVer))
        sys.stdout.write(msg)
        pars = [["file_id", fileId], ["file_version", fileVer]]
        if (not ignoreDiskId): pars.append(["disk_id", diskId])
        res = client.sendCmdGen(host, port, NGAMS_CHECKFILE_CMD, 1, "", pars)
        if (res.getMessage().find("FILE_OK") != -1):
            status = NGAMS_SUCCESS + "/OK"
        else:
            status = NGAMS_FAILURE + "/" + res.getMessage()
        sys.stdout.write(status + "\n")
        msg += status
        report += msg + "\n"
    report += 100 * "-" + "\n"
    print 100 * "-" + "\n"    
    if (notifEmail):
        ngasUtilsLib.sendEmail("ngasCheckFileList: FILE CHECK REPORT",
                               notifEmail, report)
       

def correctUsage():
    """
    Generate buffer with man-page.

    Returns:   Buffer with man-page (string).
    """
    buf = "\nCorrect usage is:\n\n"
    buf += "> ngasCheckFileList.py [-host <Host>] [-port <Port>] " +\
           "-fileList <FileList File>|<DCC Report (ASCII)> " +\
           "[-notifEmail <Email List>]\n" +\
           "[-ignoreDiskId]\n"
    return buf


if __name__ == '__main__':
    """
    Main function to invoked the tool.
    """
    fileListFile = ""
    host         = ""
    port         = 0
    notifEmail   = ""
    ignoreDiskId = 0
    idx          = 1
    while idx < len(sys.argv):
        par = sys.argv[idx].upper()
        if (par == "-FILELIST"):
            idx += 1
            fileListFile = sys.argv[idx]
        elif (par == "-HOST"):
            idx += 1
            host = sys.argv[idx]
        elif (par == "-PORT"):
            idx += 1
            port = int(sys.argv[idx])
        elif (par == "-NOTIFEMAIL"):
            idx += 1
            notifEmail = sys.argv[idx]
        elif (par == "-IGNOREDISKID"):
            ignoreDiskId = 1
        else:
            correctUsage()  
            sys.exit(1)
        idx += 1

    if (not notifEmail):
        notifEmail = ngasUtilsLib.\
                     getParNgasRcFile(ngasUtilsLib.NGAS_RC_PAR_NOTIF_EMAIL)
    if (host == ""):
        host = ngasUtilsLib.getParNgasRcFile(ngasUtilsLib.NGAS_RC_PAR_HOST)
    if (port == 0):
        port = int(ngasUtilsLib.\
                   getParNgasRcFile(ngasUtilsLib.NGAS_RC_PAR_PORT))

    try:
        if (not fileListFile):
            print correctUsage()  
            raise Exception, "Incorrect command line parameter(s) given!"
        checkFileList(host, port, fileListFile, notifEmail, ignoreDiskId)
    except Exception, e:
        print "\nProblem encountered:\n\n" + str(e) + " -- bailing out\n"


# EOF
