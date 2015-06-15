

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
# "@(#) $Id: ngasRegisterExtFiles.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  30/01/2004  Created
#

"""
The ngasRegisterExtFiles tool is used to register files in the NGAS DB as
EXTERNAL Files. This means, that they have a normal entry in the NGAS DB
(ngas_files) but the actual storage media is not handled by NGAS. This is
reflected by setting the Mount Point of the ngas_disks table to 'EXTERNAL'.

This is used when files are ingested into NGAS so that NGAS maintains
the main copy. The replica however, is stored on another media, e.g.
a tape or a DVD.

The tool is invoked with a file containing a list of Disk IDs (unique
identifier for the external media) and the File ID/Version. E.g.:

# Example File List:

<Disk ID> <File ID> [<File Version>]
<Disk ID> <File ID> [<File Version>]
...


Comment lines initiated with '#' are ignored. So are blank lines. If a
File Version is not given, a version number equal to 1 is chosen.

If files have already been registered, they are not registered again. I.e.,
there is no danger in running the tool with the same file references as
input.
"""

import sys, os, time

from ngams import *
import ngamsDb, ngamsFileInfo, ngamsDiskInfo
import ngamsLib
import ngasUtils, ngasUtilsLib


# IMPL: Use ngasUtilsLib.parseFileList()
def parseFileList(fileListFile):
    """
    Function that parses the the given file list file and returns the
    entries in a list:

      [[<Disk ID>, <File ID>, <File Version>], ...]

    fileListFile:    Name of file in which the file entries are
                     contained (string).

    Returns:         List containing the info for the files to
                     register (list).
    """
    fileRefList = []
    fo = open(fileListFile)
    fileLines = fo.readlines()
    fo.close()
    lineNo = 0
    for line in fileLines:
        lineNo += 1
        line = line.strip()
        if (not line): continue
        if (line[0] != "#"):
            fields = []
            lineEls = line.split(" ")
            for el in lineEls:
                if (el.strip()): fields.append(el.strip())
            if (len(fields) < 2):
                raise Exception, "Syntax error in file list: %s in line: %d" %\
                      (fileListFile, lineNo)
            if (len(fields) == 2): fields.append(1)
            fields[2] = int(fields[2])
            fileRefList.append(fields)

    return fileRefList


def genReport(regFileList):
    """
    Generate a report from the list of files. The list is structured as:

      ((<Disk ID>, <File ID>, <File Version>), ...)

    regFileList:    List of files (list).
    
    Returns:        Report (string).
    """
    # If execute is specified loop over the files and remove them one by one.
    fileInfoFormat = "%-32s %-32s %-3d\n"
    # Generate report.
    report = "EXTERNAL FILE REGISTRATION REPORT:"
    report += "\n\nRegistered Files:\n\n"
    report += "%-32s %-32s %s\n" % ("Disk ID", "File ID", "File Version")
    report += 78 * "-" + "\n"
    for fileInfo in regFileList:
        report += fileInfoFormat % (fileInfo[0], fileInfo[1], fileInfo[2])
    report += "\n"
    return report

 
def ingestExtFiles(fileListFile,
                   notifEmail):
    """
    Ingest the list of files given in the file referred to.

    The files are listed as:

    <Disk ID> <File ID> [<File Version>]
    ...


    fileListFile:   Filename of file containing the file list (string).

    notifEmail:     List of email addresses to inform about the
                    execution of the discation procedure (string).

    Returns:        Void.
    """
    fileInfoList = parseFileList(fileListFile)

    # Open DB connection.
    server, db, user, password = ngasUtilsLib.getDbPars()
    dbCon = ngamsDb.ngamsDb(server, db, user, password, 0)

    # Find distinct Disk IDs.
    diskIdDic = {}
    for fileInfo in fileInfoList: diskIdDic[fileInfo[0]] = None
    info(1,"Disk IDs: " + str(diskIdDic.keys()))

    # Register disks referred to as external disks in the DB.
    dbDiskDic = {}
    for diskId in diskIdDic.keys():
        if (not dbDiskDic.has_key(diskId)):
            diskInfo = dbCon.getDiskInfoFromDiskId(diskId)
            if (diskInfo != []):
                diskInfoObj = ngamsDiskInfo.ngamsDiskInfo().\
                              unpackSqlResult(diskInfo)
            else:
                # Create a new entry for that disk in the DB.
                info(1,"Creating new entry for external disk with ID: " +\
                     str(diskId))
                diskInfoObj = ngamsDiskInfo.ngamsDiskInfo().\
                              setArchive("EXTERNAL").\
                              setDiskId(diskId).\
                              setLogicalName(diskId).\
                              setHostId("").\
                              setSlotId("").\
                              setMounted(0).\
                              setMountPoint("EXTERNAL").\
                              setNumberOfFiles(0).\
                              setAvailableMb(0).\
                              setBytesStored(0).\
                              setCompleted(1).\
                              setCompletionDateFromSecs(0).\
                              setType("EXTERNAL").\
                              setManufacturer("UNKNOWN").\
                              setInstallationDateFromSecs(time.time()).\
                              setChecksum(0).\
                              setTotalDiskWriteTime(0).\
                              setLastCheckFromSecs(0).\
                              setLastHostId("").\
                              setStorageSetId(diskId)
                diskInfoObj.write(dbCon)
            diskIdDic[diskId] = diskInfoObj

    # Loop over the files and register them in the DB.
    fileRegList = []
    sys.stdout.write("Registering files ...")
    fileCount = 0
    for fileInfo in fileInfoList:
        diskId  = fileInfo[0]
        fileId  = fileInfo[1]
        fileVer = fileInfo[2]
        fileInfoObj = ngamsFileInfo.ngamsFileInfo().\
                      setDiskId(diskId).\
                      setFilename(fileId).\
                      setFileId(fileId).\
                      setFileVersion(fileVer).\
                      setFormat("").\
                      setFileSize(0).\
                      setUncompressedFileSize(0).\
                      setCompression("").\
                      setIngestionDateFromSecs(0).\
                      setIgnore(0).\
                      setChecksum("").\
                      setChecksumPlugIn("").\
                      setFileStatus(NGAMS_FILE_STATUS_OK).\
                      setCreationDateFromSecs(0).\
                      setTag("EXTERNAL")
        fileInfoObj.write(dbCon, 0, 1)
        fileRegList.append((diskId, fileId, fileVer))
        time.sleep(0.050)
        fileCount += 1
        if ((fileCount % 10) == 0): sys.stdout.write(".")
    sys.stdout.write("\n")
    info(1,"Registered %d files" % fileCount)

    report = genReport(fileRegList)
    if (notifEmail):
        ngasUtilsLib.sendEmail("ngasRegisterExtFiles: FILE REGISTRATION " +\
                               "REPORT", notifEmail, report)


def correctUsage():
    """
    Return the usage/online documentation in a string buffer.

    Returns:  Man-page (string).
    """
    buf = "\nCorrect usage is:\n\n" +\
          "> python ngasRegisterExtFiles.py [-accessCode <Code>] \n" +\
          "         -fileList <File List>\n"
    return buf

  
if __name__ == '__main__':
    """
    Main function to execute the tool.
    """
    setLogCond(0, "", 0, "", 1)

    # Parse input parameters.
    accessCode   = ""
    fileListFile = ""
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
    try:
        if (not fileListFile):
            print correctUsage()  
            raise Exception, "Incorrect command line parameter given!"
        if (not accessCode):
            accessCode = ngasUtilsLib.input("Enter Access Code:")
        ngasUtilsLib.checkAccessCode(accessCode)
        ingestExtFiles(fileListFile, notifEmail)
    except Exception, e:
        print "\nProblem encountered:\n\n" + str(e) + " -- bailing out\n"


# EOF
