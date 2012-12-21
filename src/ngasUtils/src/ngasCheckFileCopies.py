

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
# "@(#) $Id: ngasCheckFileCopies.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  30/01/2004  Created
#

"""
The ngasCheckCopies Tool, checks how many copies of each file is found in the
system, which are registered on the referenced disk.

For each file found on the disk, the following output is generated:

<File ID> <File Version> <Total> <Good> 
...

- whereby, Good Copies refer to copies marked as being OK in the DB.
"""

import sys, os, time

from ngams import *
import ngamsDb, ngamsLib, ngamsFileInfo, ngamsDiskInfo
import ngasUtils, ngasUtilsLib


def checkCopies(diskId,
                notifEmail):
    """
    Check the total number of copies of the files found on the referred disk
    in the NGAS DB. Generate a report indicating the number of copies found.

    diskId:           ID of disk for which to check files (string).
        
    notifEmail:       Comma separated list of recipients of the report
                      generated (string).

    Returns:          Void.
    """
    # Open DB connection.
    server, db, user, password = ngasUtilsLib.getDbPars()
    dbCon = ngamsDb.ngamsDb(server, db, user, password, 0)

    # Get the information about the files on the referenced disk.
    info(0,"Retrieving info about files on disk: %s ..." % diskId)
    dbCur = dbCon.getFileSummary1(hostId=None, diskIds=[diskId], fileIds=[],
                                  ignore=0)
    diskFileList = []
    while (1):
        tmpList = dbCur.fetch(500)
        if (tmpList == []): break
        diskFileList += tmpList
        time.sleep(0.100)
        if ((len(diskFileList) % 100) == 0):
            sys.stdout.write(".")
            sys.stdout.flush()
    del dbCur
    print ""
    info(0,"Retrieved info about files on disk: %s" % diskId)

    # Get all files found in the system with the given File ID/File Version.
    globFileList = []
    queryFileIds = []
    count = 0
    noOfFiles = len(diskFileList)
    info(0,"Retrieving info about all File IDs/Versions in the system " +\
         "on disk: %s ..." % diskId)
    for fileInfo in diskFileList:
        count += 1
        queryFileIds.append(fileInfo[ngamsDb.SUM1_FILE_ID])
        if (((len(queryFileIds) % 20) == 0) or (count == noOfFiles)):
            dbCur = dbCon.getFileSummary1(None, [], queryFileIds, ignore=0)
            while (1):
                tmpList = dbCur.fetch(500)
                if (tmpList == []): break
                globFileList += tmpList
            del dbCur
            queryFileIds = []            
            time.sleep(0.010)
            if ((len(globFileList) % 100) == 0):
                sys.stdout.write(".")
                sys.stdout.flush()
    print ""
    info(0,"Retrieved info about all File IDs/Versions in the system " +\
         "on disk: %s" % diskId)

    # Now, go through this list, and generate a dictionary with
    # File ID/File Version as keys.
    globFileDic = {}
    for fileInfo in globFileList:
        fileKey = ngamsLib.genFileKey(None, fileInfo[ngamsDb.SUM1_FILE_ID],
                                      fileInfo[ngamsDb.SUM1_VERSION])
        if (not globFileDic.has_key(fileKey)): globFileDic[fileKey] = []
        globFileDic[fileKey].append(fileInfo)

    # Order the list according to 1) Number of copies, 2) Alphabetically.
    fileKeys = globFileDic.keys()
    fileKeys.sort()
    sortFileDic = {}
    for fileKey in fileKeys:
        fileInfoList = globFileDic[fileKey]
        noOfCopies = len(fileInfoList)
        if (not sortFileDic.has_key(noOfCopies)):
            sortFileDic[noOfCopies] = {}
        sortFileDic[noOfCopies][fileKey] = fileInfoList

    # Go through the global file dictionary and check file for each
    # File ID/File Version the requested information.
    report = "FILE COPIES CHECK REPORT:\n\n"
    report += "Disk ID: " + diskId + "\n\n"
    format = "%-40s %-7s %-7s %-7s\n"
    report += format % ("File ID", "Version", "Total", "Good")
    report += 80 * "-" + "\n"
    noKeys = sortFileDic.keys()
    noKeys.sort()
    for noKey in noKeys:
        noKeyDic = sortFileDic[noKey]
        fileKeys = noKeyDic.keys()
        fileKeys.sort()
        for fileKey in fileKeys:
            totCopies     = 0
            goodCopies    = 0
            for fileInfo in noKeyDic[fileKey]:
                totCopies += 1
                if ((fileInfo[ngamsDb.SUM1_FILE_STATUS][0] == "0") and
                    (fileInfo[ngamsDb.SUM1_FILE_IGNORE] == 0)):
                    goodCopies += 1
            fileId  = fileInfo[ngamsDb.SUM1_FILE_ID]
            fileVer = fileInfo[ngamsDb.SUM1_VERSION]
            report += format % (fileId, str(fileVer), str(totCopies),
                                str(goodCopies))

    if (len(noKeys)):
        report += 80 * "-" + "\n\n"
    else:
        report += "No files found on the given disk!\n\n"
    print "\n" + report

    if (notifEmail):
        report 
        ngasUtilsLib.sendEmail("ngasCheckFileCopies: " +\
                               "FILE COPIES CHECK REPORT (%s)" % diskId,
                               notifEmail, report)
        

def correctUsage():
    """
    Return the usage/online documentation in a string buffer.

    Returns:  Man-page (string).
    """
    buf = "\nCorrect usage is:\n\n" +\
          "> ngasCheckFileCopies.py -diskId <Disk ID> " +\
          "[-notifEmail <Email List>]\n\n"
    return buf

  
if __name__ == '__main__':
    """
    Main function to execute the tool.
    """
    setLogCond(0, "", 0, "", 1)

    # Parse input parameters.
    diskId       = ""
    notifEmail   = None
    idx = 1
    while idx < len(sys.argv):
        par = sys.argv[idx].upper()
        try:
            if (par == "-DISKID"):
                idx += 1
                diskId = sys.argv[idx]
            elif (par == "-NOTIFEMAIL"):
                idx += 1
                notifEmail = sys.argv[idx]
            elif (par == "-VERBOSE"):
                idx += 1
                setLogCond(0, "", 0, "", int(sys.argv[idx]))
            else:
                sys.exit(1)
            idx += 1
        except Exception, e:
            print "\nProblem executing the tool: %s\n" % str(e)
            print correctUsage()  
            sys.exit(1)
    if (notifEmail == None):
        notifEmail = ngasUtilsLib.\
                     getParNgasRcFile(ngasUtilsLib.NGAS_RC_PAR_NOTIF_EMAIL)
    try:
        if (not diskId):
            print correctUsage()  
            raise Exception, "Incorrect command line parameter(s) given!"
        checkCopies(diskId, notifEmail)
    except Exception, e:
        print "\nProblem encountered:\n\n" + str(e) + " -- bailing out\n"


# EOF
