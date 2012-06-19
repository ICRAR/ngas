

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
# "@(#) $Id: ngasArchiveTool.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  13/10/2005  Created
#

_doc =\
"""
The ngasArchiveTool utility is used to archive a given set of files found
by scanning a directory structure referenced by the command line parameters.
It is possible to request that the tool scans the directories recursively
starting from the directory given on the command line.

Each time a file has been archived, the information about this is updated
in an internally managed DB. If the --cleanup parameter is specified, the
sources of the files, which have been successfully archived, will be deleted.
Prior to deleting these, the tool check the remote NGAS Server(s) if the files
have been properly archived (via the CHECKFILE Command).

When the tool has finished archiving the set of file selected by the criterias
given, an email report will be sent to a specified set of recipients.


The defined input parameters to the tool are:

%s

"""

import sys, os, time, getpass

import pcc, PccUtTime

from ngams import *
import ngamsDbm, ngamsLib, ngamsFileInfo
import ngamsPClient
import ngasUtils
from ngasUtilsLib import *

# Constants.
NGAS_ARCH_TOOL_WD = "NGAS_ARCHIVE_TOOL"

# Definition of predefined command line parameters.
_options = [\
    ["_startTime", [], time.time(), NGAS_OPT_INT, "",
     "Internal Parameter: Start time for running the tool."],
    ["id", [], None, NGAS_OPT_MAN, "=<ID>",
     "Unique ID for this instance running. The same ID can be " +\
     "re-used when launching the tool, but it is only allowed to " +\
     "have one instance with a given ID running at any time."],
    ["servers", [], None, NGAS_OPT_MAN, "=<Server List>",
     "Comma separated list of servers to contact (<Host>:<Port>,...)"],
    ["srcDir", [], None, NGAS_OPT_MAN, "=<Src Dir>",
     "Directory where the tool will scan for files to archive. " +\
     "If the 'recursive' option is specified, sub-directories will " +\
     "also be scanned."],
    ["extensions", [], None, NGAS_OPT_MAN, "=<Ext List>", 
     "File extensions to look for. For a FITS file this should be " +\
     "'fits'. For a compress'ed FITS file 'fits.Z'."],
    ["recursive", [], 0, NGAS_OPT_OPT, "",
     "Work recursively down through sub-directories of the " +\
     "specified 'srcDir'."],
    ["force", [], 0, NGAS_OPT_OPT, "",
     "Force execution of tool."],
    ["list", [], 0, NGAS_OPT_OPT, "", 
     "List the files selected to be archived."],
    ["archive", [], 0, NGAS_OPT_OPT, "",
     "Archive the files."],
    ["check", [], 0, NGAS_OPT_OPT, "",
     "Check if the files that have been archived are properly " +\
     "archived into NGAS. This is done by sending a CHECKFILE " +\
     "Command per file to the remote NGAS System."],
    ["cleanUp", [], 0, NGAS_OPT_OPT, "",
     "Remove the files that have been successfully archived from the "+\
     "source location."],
    ["workDir", [], "/tmp", NGAS_OPT_OPT, "=<Work Dir>", 
     "Working directory of the tool. The tool will create a " +\
     "directory '<Work Dir>/NGAS_ARCHIVE_TOOL' directory, where it will " +\
     "store its house-keeping files."],
    ["noVersioning", [], 0, NGAS_OPT_OPT, "",
     "If specified, the no_versioning parameter (value 1) will be " +\
     "submitted to NGAS."],
    ["mimeType", [], "", NGAS_OPT_OPT, "=<Mime-type>",
     "If provided, the specified mime-type will be submitted to NGAS " +\
     "(parameter mime_type)."]]
_optDic, _optDoc = genOptDicAndDoc(_options)
__doc__ = _doc % _optDoc


def getOptDic():
    """
    Return reference to command line options dictionary.

    Returns:  Reference to dictionary containing the command line options
              (dictionary).
    """
    return _optDic


def correctUsage():
    """
    Return the usage/online documentation in a string buffer.

    Returns:  Man-page (string).
    """
    return __doc__


def checkCreateWorkDir(optDic):
    """
    Check if the working directory is existing, if not, create it.

    optDic:   Dictionary with options (dictionary).

    Returns:  Name of working directory (string).
    """
    info(4,"Entering checkCreateWorkDir() ...")
    wd = os.path.normpath("%s/%s" % (optDic["workDir"][NGAS_OPT_VAL],
                                     NGAS_ARCH_TOOL_WD))
    checkCreatePath(wd)
    info(4,"Leaving checkCreateWorkDir()")
    return wd


def getLockFile(optDic):
    """
    Generate and return the lock file for this session of the tool.

    optDic:   Dictionary with options (dictionary):

    Returns:  Lock filename (string).
    """
    info(4,"Entering getLockFile() ...")
    lockFile = os.path.normpath("%s/%s/%s.lock" %\
                                (optDic["workDir"][NGAS_OPT_VAL],
                                 NGAS_ARCH_TOOL_WD,
                                 optDic["id"][NGAS_OPT_VAL]))
    info(4,"Leaving getLockFile()")
    return lockFile


def getDbmName(optDic):
    """
    Generate and return the DBM file for this session of the tool.

    optDic:   Dictionary with options (dictionary):

    Returns:  DBM filename (string).
    """
    info(4,"Entering getDbmName() ...")
    dbmName = os.path.normpath("%s/%s/%s.bsddb" %\
                               (optDic["workDir"][NGAS_OPT_VAL],
                                NGAS_ARCH_TOOL_WD, 
                                optDic["id"][NGAS_OPT_VAL]))
    info(4,"Leaving getDbmName()")
    return dbmName


def _scanDir(srcDir,
             recursive,
             fileExtList):
    """
    Scan the given directory file for files. If diretories are found in the
    directory, these are returned in a list.

    srcDir:         Directory to scan for matching files (string).

    recursive:      Traverse the directory tree recursively (integer/0|1).
    
    fileExtList:    List with extensions to take into account (list).

    Returns:        List with ngamsFileInfo objects for matching files (list).
    """
    info(4,"Entering _scanDir() ...")
    srcDir = os.path.expanduser(srcDir)
    info(2,"Scanning directory: %s ..." % srcDir)
    fileInfoList = []
    fileList = glob.glob(os.path.normpath("%s/*" % srcDir))
    for file in fileList:
        file = file.strip()
        if (os.path.isdir(file) and recursive):
            fileInfoList += _scanDir(file, recursive, fileExtList)
        else:
            # Extension matching?
            for ext in fileExtList:
                if ((file.find("." + ext) + len("." + ext)) == len(file)):
                    # The extension matches. Get the info for the file.
                    statInfo = os.stat(file)
                    tmpFileObj = ngamsFileInfo.ngamsFileInfo().\
                                 setFilename(file).\
                                 setAccDateFromSecs(statInfo[7]).\
                                 setModDateFromSecs(statInfo[8]).\
                                 setCreationDate(statInfo[9]).\
                                 setFileSize(statInfo[6])
                    fileInfoList.append(tmpFileObj)
                    break
    info(4,"Leaving _scanDir()")
    return fileInfoList

    
def scanDirs(srcDir,
             recursive,
             fileExts):
    """
    Scan the source directory for candidate files. Select only files with
    an extension given in the file extension list. If requested the tool
    will traverse the directories recursively.

    srcDir:     Source directory (starting point for the scan) (string).
    
    recursive:  Traverse the directories recursively (integer/0|1).
    
    fileExts:   Comma separated list of file extensions to consider (string).

    Returns:    List with ngamsFileInfo objects containing info about the
                candidate files (list/ngamsFileInfo).
    """
    info(4,"Entering scanDirs() ...")
    fileExtList = []
    for ext in fileExts.split(","): fileExtList.append(ext.strip())
    fileExtList = _scanDir(srcDir, recursive, fileExtList)
    info(4,"Leaving scanDirs()")
    return fileExtList
        

def listFiles(optDic,
              fileInfoList):
    """
    List info about matching files.

    optDic:        Dictionary containing the options (dictionary).

    fileInfoList:  List with ngamsFileInfo object (list).

    Returns:       Void.
    """
    info(4,"Entering listFiles() ...")
    if (optDic["list"][NGAS_OPT_VAL]):
        format = "%-100s  %-23s  %-23s  %-23s  %-8s"
        buf = 190 * "-" + "\n"
        if (fileInfoList != []):
            buf += "CANDIDATE FILES FOUND:\n\n" +\
                   format % ("Filename", "Access Time", "Modification Time",
                             "Creation Time", "File Size (B)\n")
            buf += 190 * "-" + "\n"
            for fileInfo in fileInfoList:
                fileEntry = format % (fileInfo.getFilename(),
                                      fileInfo.getAccDate(),
                                      fileInfo.getModDate(),
                                      fileInfo.getCreationDate(),
                                      str(fileInfo.getFileSize()))
                buf += fileEntry + "\n"
        else:
            buf += "NO MATCHING FILES FOUND!\n"
        buf += 190 * "-" + "\n"
        print buf
    info(4,"Leaving listFiles()")


def checkFile(dbmObj,
              clientObj,
              fileInfoObj):
    """
    Check if a file is propery archived into the NGAS Archive. In case yes, 1
    is returned, otherwise 0 is returned.

    dbmObj:          DBM containing info about the processing (ngamsDbm).
    
    clientObj:       Instance of ngamsPClient used to communicate with the
                     remote server (ngamsPClient).
    
    fileInfoObj:     Instance of ngamsFileInfo containing the status of the
                     source file (ngamsFileInfo).

    Returns:         Void.
    """
    info(4,"Entering checkFile() ...")
    filename = fileInfoObj.getFilename()
    dbmEntry = dbmObj.get(filename)
    if (dbmEntry[1] == None):
        notice("File: %s appears not to be archived!" % filename)
        dbmEntry[2] = None
        dbmObj.add(filename, dbmEntry)
    elif (dbmEntry[1].getDiskStatusList() == []):
        notice("File: %s appears not to be archived!" % filename)
        dbmEntry[2] = None
        dbmObj.add(filename, dbmEntry)
    else:
        statObj = dbmEntry[1]
        diskId = statObj.getDiskStatusList()[0].getDiskId()
        fileId = statObj.getDiskStatusList()[0].getFileObj(0).\
                 getFileId()
        fileVer = statObj.getDiskStatusList()[0].getFileObj(0).\
                  getFileVersion()
        msg = "Probing if target file: %s/%s/%d (source: %s) is " +\
              "properly archived in the remote NGAS Archive ..."
        info(1, msg % (diskId, fileId, fileVer, filename))
        reqPars = [["disk_id", diskId],
                   ["file_id", fileId],
                   ["file_version", str(fileVer)]]
        checkStatObj = clientObj.sendCmdGen("", 0, NGAMS_CHECKFILE_CMD,
                                            pars=reqPars)
        if (checkStatObj.getMessage().find("NGAMS_INFO_FILE_OK") != -1):
            msg = "Target file: %s/%s/%d (source: %s) seems to " +\
                  "properly archived in the remote NGAS Archive ..."
            info(1,msg % (diskId, fileId, fileVer, filename))
        else:
            msg = "Target file: %s/%s/%d (source: %s) seems NOT to be " +\
                  "properly archived in the remote NGAS Archive ..."
            notice(1,msg % (diskId, fileId, fileVer, filename))
        dbmEntry[2] = checkStatObj
        dbmObj.add(filename, dbmEntry)
        
    info(4,"Leaving checkFile()")


def genReport(dbmObj):
    """
    Generate report from contents of the tool internal DBM.

    dbmObj:   DBM Object (ngamsDbm).

    Returns:  Void.
    """
    info(4,"Entering genReport() ...")
    filenames = dbmObj.keys()
    report = "NGAS ARCHIVE UTILITY - STATUS REPORT:\n\n"
    format = "%-100s %-32s %-7s %-11s %-11s\n"
    report += format % ("Source File","File ID","Version","Checked","Removed")
    report += 165 * "-" + "\n"
    for filename in filenames:
        dbmEntry = dbmObj.get(filename)
        info(2,"Reporting status of source file: %s ..." % filename)
        archStatObj = dbmEntry[1]
        repStatus = 0
        if (archStatObj):
            if (archStatObj.getDiskStatusList() != []):
                repStatus = 1
        if (repStatus):
            fileId = archStatObj.getDiskStatusList()[0].getFileObj(0).\
                     getFileId()
            fileVer = archStatObj.getDiskStatusList()[0].getFileObj(0).\
                      getFileVersion()
        else:
            fileId = "NOT ARCHIVED"
            fileVer = "------"
        checkStatObj = dbmEntry[2]
        checked = "-------"
        if (checkStatObj != None):
            if (checkStatObj.getMessage().find("NGAMS_INFO_FILE_OK")!= -1):
                checked = "CHECKED/OK"
            else:
                checked = "CHECKED/NOK"

        if (dbmEntry[3]):
            removed = "REMOVED"
        else:
            removed = "NOT REMOVED"
        report += format % (filename, fileId, str(fileVer), checked, removed)
    report += 165 * "-" + "\n"
           
    info(4,"Leaving genReport()")
    return report
 
 
def execute(optDic):
    """
    Carry out the tool execution.

    optDic:    Dictionary containing the options (dictionary).

    Returns:   Void.
    """
    info(4,"Entering execute() ...")
    if (optDic["help"][NGAS_OPT_VAL]):
        print correctUsage()
        sys.exit(0)
    if ((not optDic["force"][NGAS_OPT_VAL]) and
        (os.path.exists(getLockFile(optDic)))):
        msg = "An instance of this tool with ID: %s appears to " +\
              "be running - bailing out!"
        raise Exception, msg % optDic["id"][NGAS_OPT_VAL]
    else:
        commands.getstatusoutput("touch %s" % getLockFile(optDic))
    getCheckAccessCode(optDic)

    # Open the DBM.
    dbm = ngamsDbm.ngamsDbm(getDbmName(optDic), writePerm=1, autoSync=1)
    
    # Scan source directory.
    fileInfoList = scanDirs(optDic["srcDir"][NGAS_OPT_VAL],
                            optDic["recursive"][NGAS_OPT_VAL],
                            optDic["extensions"][NGAS_OPT_VAL])

    # Ensure there is an entry for each file in the DBM.
    for fileObj in fileInfoList:
        if (not dbm.hasKey(fileObj.getFilename())):
            # The contents of the info for each file is:
            # [<Src File Info (ngamsFileInfo)>,
            #  <Archive Status (ngamsStatus)>,
            #  <Check File Status (ngamsStatus)>,
            #  <Src Removed (0|1|-1)>]
            dbm.add(fileObj.getFilename(), [fileObj, None, None, 0])

    # List candidate files found?
    listFiles(optDic, fileInfoList)

    # Create instance of ngamsPClient to communicate with NGAS (if relevant).
    if ((optDic["archive"][NGAS_OPT_VAL]) or
        (optDic["check"][NGAS_OPT_VAL]) or
        (optDic["cleanUp"][NGAS_OPT_VAL])):
        client = ngamsPClient.ngamsPClient().\
                 parseSrvList(optDic["servers"][NGAS_OPT_VAL])
    
    # Archive files?
    if (optDic["archive"][NGAS_OPT_VAL]):
        info(1,"Archiving the files found ...")
        mt = optDic["mimeType"][NGAS_OPT_VAL]
        nv = optDic["noVersioning"][NGAS_OPT_VAL]
        for fileInfoObj in fileInfoList:
            filename = fileInfoObj.getFilename()
            dbmEntry = dbm.get(filename)
            if (dbmEntry[1] == None):
                info(1,"Archiving file: %s ..." % filename)
                archiveFile = 1
            elif (dbmEntry[1].getStatus() != NGAMS_SUCCESS):
                info(1,"Retrying to archive file: %s ..." % filename)
                archiveFile = 1
            else:
                info(1,"File: %s already archived" % filename)
                archiveFile = 0
            if (archiveFile):
                statObj = client.archive(filename, mimeType=mt,noVersioning=nv)
                info(1,"Result of archiving file: %s: %s/%s" %\
                     (filename, statObj.getStatus(),
                      statObj.getMessage().replace("\n", "   ")))
                dbmEntry[1] = statObj
                dbm.add(filename, dbmEntry)

    # Carry out check?
    if (optDic["check"][NGAS_OPT_VAL] or (optDic["cleanUp"][NGAS_OPT_VAL])):
        info(1,"Checking the files registered as archived ...")
        for fileInfoObj in fileInfoList: checkFile(dbm, client, fileInfoObj)

    # Clean up?
    if (optDic["cleanUp"][NGAS_OPT_VAL]):
        info(1,"Removing source file if properly archived ...")
        # Go through the check status and remove source files properly
        # archived. In this case we check the entire DBM (also older entries).
        # IMPL: Use maybe a second DBM to avoid occupying lots of memory.
        filenames = dbm.keys()
        for filename in filenames:
            dbmEntry = dbm.get(filename)
            info(1,"Checking if source file: %s can be removed ..." % filename)
            checkStatObj = dbmEntry[2]
            if (checkStatObj == None):
                notice("Source file: %s has not be successfully checked" %\
                       filename)
            elif (checkStatObj.getMessage().find("NGAMS_INFO_FILE_OK") != -1):
                info(1,"Source file: %s has been checked - removing ..." %\
                     filename)
                rmFile(filename)
                if (not os.path.exists(filename)):
                    info(1,"Source file: %s removed" % filename)
                    dbmEntry[3] = 1
                else:
                    warning("Source file: %s could not be removed" % filename)
                    dbmEntry[3] = -1
                dbm.add(filename, dbmEntry)

    # Generate report?
    if (optDic["notifEmail"][NGAS_OPT_VAL]):
        info(1,"Generating report about actions carried out ...")
        report = genReport(dbm)
        sendEmail("NGAS ARCHIVE TOOL: STATUS REPORT - ID: %s" %\
                  optDic["id"][NGAS_OPT_VAL],
                  optDic["notifEmail"][NGAS_OPT_VAL],
                  report, "text/plain", "ngamsArchiveTool_%s_report.txt" %\
                  optDic["id"][NGAS_OPT_VAL])

    # Clean up DBM if the file was successfully archived, checked and removed.
    filenames = dbm.keys()
    for filename in filenames:
        dbmEntry = dbm.get(filename)
        if ((dbmEntry[2] != None) and (dbmEntry[3] != 0)):
            if (dbmEntry[2].getMessage().find("NGAMS_INFO_FILE_OK") != -1):
                info(3,"Removing DBM entry for source file: %s ..." % filename)
                dbm.rem(filename)
                info(3,"DBM entry for source file: %s removed" % filename)

    info(1,"Requested instructions executed")
    info(4,"Leaving execute()")


if __name__ == '__main__':
    """
    Main function to execute the tool.
    """
    try:
        optDic = parseCmdLine(sys.argv, getOptDic())
        checkCreateWorkDir(optDic)
    except Exception, e:
        print "\nProblem executing the tool:\n\n%s\n" % str(e)
        sys.exit(1) 
    if (getDebug()):
        execute(optDic)
        rmFile(getLockFile(optDic))
    else:
        try:
            execute(optDic)
            rmFile(getLockFile(optDic))
        except Exception, e:
            rmFile(getLockFile(optDic))
            if (str(e) == "0"): sys.exit(0)
            print "\nProblem executing the tool:\n\n%s\n" % str(e)
            sys.exit(1)
                  

# EOF
