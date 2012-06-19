

#
#    ALMA - Atacama Large Millimiter Array
#    (c) European Southern Observatory, 2002
#    Copyright by ESO (in the framework of the ALMA collaboration),
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
# "@(#) $Id: ngasDiscardFiles.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  21/01/2004  Created
#


"""
Tool to remove files from the NGAS DB + Disk.

The disk(s) hosting he files to be removed/discarded from the NGAS Archive
must inserted in the NGAS System on which this command is executed.

The format of the file list is:

<Disk ID> <File ID> <File Version>
<Disk ID> <File ID> <File Version>
...


It is also possible to give a list of files, referred to by the complete
path, e.g.:

/NGAS/data7/saf/2001-06-11/41/WFI.2001-06-11T22:07:05.290.fits.Z
/NGAS/data7/saf/2001-06-11/36/WFI.2001-06-11T22:07:05.290.fits.Z
/NGAS/data7/saf/2001-06-11/93/WFI.2001-06-11T22:07:05.290.fits.Z
/NGAS/data7/saf/2001-06-11/86/WFI.2001-06-11T22:07:05.290.fits.Z
...


Note, in this case the tool will not attempt to remove the information
about the files from the DB. This should only be used to remove files
stored on disk, but not registered in the DB.


                          *** CAUTION ***
                          
THIS IS A VERY DANGEROUS TOOL TO USE, SINCE IT ALLOWS TO REMOVE ARCHIVED
FILES FROM AN NGAS ARCHIVE ALSO IF THESE ARE AVAILABLE IN LESS THAN 3
COPIES. SHOULD BE USED WITH GREAT CAUTION!!!
"""

import sys, os, time

from ngams import *
import ngamsDb, ngamsLib, ngamsFileInfo, ngamsDiskInfo
import ngasUtils, ngasUtilsLib, ngamsPClient


def _unpackFileInfo(fileListLine):
    """
    Unpack the information in the line read from the File List File.

    fileListLine:  Line as read from the file (string).

    Returns:       Tuple with file info:

                   (<Disk ID>, <File ID>, <File Version>)  (tuple)
    """
    fileInfoList = []
    lineEls = fileListLine.split(" ")
    for el in lineEls:
        if ((el.strip() != "") and (el.strip() != "\t")):
            fileInfoList.append(el.strip())
            if (len(fileInfoList) == 3): break
    if (len(fileInfoList) == 3):
        return (fileInfoList[0], fileInfoList[1], fileInfoList[2])
    else:
        raise Exception, "Illegal line found in File List File: " +\
              fileListLine


def discardFiles(fileListFile,
                 execute,
                 notifEmail):
    """
    Remove files from the NGAS DB + from from the disk. If files are given by
    their full path, only the file is removed from disk, but the DB information
    remains.

    fileListFile:   Name of file containing list with references to
                    files to remove (string).
    
    execute:        Actual remove the files (integer/0|1).
    
    notifEmail:     List of email addresses to inform about the
                    execution of the discation procedure (string).

    Returns:        Void.
    """
    host = ngasUtilsLib.getParNgasRcFile(ngasUtilsLib.NGAS_RC_PAR_HOST)
    port = int(ngasUtilsLib.getParNgasRcFile(ngasUtilsLib.NGAS_RC_PAR_PORT))
    client = ngamsPClient.ngamsPClient()

    # Try to issue a DISCARD Command to the associated NG/AMS Server for
    # each file listed in the File List.
    fo = open(fileListFile)
    fileListBuf = fo.readlines()
    fo.close()
    successStatList = []
    failedStatList = []
    for line in fileListBuf:
        line = line.strip()
        if ((line == "") or (line[0] == "#")): continue
        if (line[0] != "/"):
            # It's a "<Disk ID> <File ID> <File Version>" line.
            diskId, fileId, fileVersion = _unpackFileInfo(line)
            params = [["disk_id", diskId], ["file_id", fileId],
                      ["file_version", fileVersion], ["execute", execute]]
        else:
            # The file is referred to by its complete path.
            params = [["path", line], ["execute", execute]]
        status = client.sendCmdGen(host, port, NGAMS_DISCARD_CMD, pars=params)
        if (status.getStatus() == NGAMS_SUCCESS):
            successStatList.append((line, status))
        else:
            failedStatList.append((line, status))

    # Generate report (Email Notification).
    report = "FILE DISCARD REPORT:\n"
    report += "Host: %s" % getHostName()
    if (len(failedStatList)):
        if (execute):
            report += "\n\n=Failed File Discards:\n\n"
        else:
            report += "\n\n=Rejected File Discard Requests:\n\n"
        for statInfo in failedStatList:
            report += "%s: %s\n" % (statInfo[0], statInfo[1].getMessage())
    if (len(successStatList)):
        if (execute):
            report += "\n\n=Discarded Files:\n\n"
        else:
            report += "\n\n=Accepted Discard File Requests:\n\n"
        for statInfo in successStatList:
            report += "%s: %s\n" % (statInfo[0], statInfo[1].getMessage())
    report += "\n# EOF\n"
    print "\n" + report
    if (notifEmail):
        ngasUtilsLib.sendEmail("ngasDiscardFiles: FILE DISCARD REPORT",
                               notifEmail, report)


def correctUsage():
    """
    Return the usage/online documentation in a string buffer.

    Returns:  Man-page (string).
    """
    buf = "\nCorrect usage is:\n\n" +\
          "> python ngasDicardFiles.py [-accessCode <Code>] \n" +\
          "         -fileList <File List> | -dccMsg <DCC Msg File>\n" +\
          "         [-execute] [-notifEmail <Email List>]\n\n" +\
          __doc__ + "\n"
    return buf

  
if __name__ == '__main__':
    """
    Main function to execute the tool.
    """

    # Parse input parameters.
    accessCode   = ""
    fileListFile = ""
    dccMsgFile   = ""
    execute      = 0
    notifEmail   = None
    idx = 1
    while idx < len(sys.argv):
        par = sys.argv[idx].upper()
        try:
            if (par == "-ACCESSCODE"):
                idx += 1
                accessCode = sys.argv[idx]
            elif (par == "-FILELIST"):
                idx += 1
                fileListFile = sys.argv[idx]
            elif (par == "-DCCMSG"):
                idx += 1
                dccMsgFile = sys.argv[idx]
            elif (par == "-EXECUTE"):
                execute = 1
            elif (par == "-NOTIFEMAIL"):
                idx += 1
                notifEmail = sys.argv[idx]
            else:
                sys.exit(1)
            idx += 1
        except Exception, e:
            print "\nProblem executing the File Discard Tool: %s\n" % str(e)
            print correctUsage()  
            sys.exit(1)
    if (notifEmail == None):
        notifEmail = ngasUtilsLib.\
                     getParNgasRcFile(ngasUtilsLib.NGAS_RC_PAR_NOTIF_EMAIL)
    if (dccMsgFile and (not fileListFile)):
        fileListFile = "/tmp/ngasDiscardFiles.tmp"
        rmFile(fileListFile)
        ngasUtilsLib.dccMsg2FileList(dccMsgFile, fileListFile)
    try:
        if (not fileListFile):
            print correctUsage()  
            raise Exception, "Incorrect command line parameter(s) given!"
        if (not accessCode):
            accessCode = ngasUtilsLib.input("Enter Access Code:")
        ngasUtilsLib.checkAccessCode(accessCode)
        discardFiles(fileListFile, execute, notifEmail)
    except Exception, e:
        print "\nProblem encountered:\n\n" + str(e) + " -- bailing out\n"


# EOF
