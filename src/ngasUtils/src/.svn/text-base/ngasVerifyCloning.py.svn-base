

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
# "@(#) $Id: ngasVerifyCloning.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  19/02/2003  Created
#

"""
Contains function to verify that a cloning carried has in fact cloned
all files so that they are available on the target disks/host.

NOTE: TO BE EXECUTED ON THE HOST ON WHICH THE FILES WHERE CLONED (TARGET
      HOST, PRESENCE OF FILES CHECKED).
"""
import sys, os, time

from ngams import *
import ngamsDb, ngamsLib, ngamsGenCrc32, ngamsPClient, ngamsFileInfo
import ngasUtils, ngasUtilsLib

# IBM-DTLA-307075-YSDYSG6L675
# python ngasVerifyCloning.py -srcDiskId IBM-DTLA-307075-YSDYSG6L675


def _getTargHost():
    """
    Get DB name for the target host where the Auto Clone was executed.

    Returns:   DB reference for target host (string).
    """
    if (os.environ.has_key("_NGAS_VERIFY_CLONING_TARGET_HOST_")):
        trgHost = os.environ["_NGAS_VERIFY_CLONING_TARGET_HOST_"]
    else:
        trgHost = getHostName()
    return trgHost


def _getFileInfo1(ngamsDbObj,
                  diskId):
    """
    Retrieve information about files supposedly cloned.

    ngamsDbObj:    NG/AMS DB object (ngamsDb).
    
    diskId:        ID of disk to check (string).

    Returns:       NG/AMS Cursor Object (ngamsDbCursor).
    """
    info(5,"Entering _getFileInfo1() ...")
    
    sqlQueryFormat = "SELECT nd.slot_id, nd.mount_point, nf.file_name, " +\
                     "nf.checksum, nf.checksum_plugin, nf.file_id, " +\
                     "nf.file_version, nf.file_size, nf.file_status, " +\
                     "nd.disk_id, nf.ignore, nd.host_id " +\
                     "FROM ngas_disks nd noholdlock, " +\
                     "ngas_files nf noholdlock " +\
                     "WHERE nf.disk_id='%s' AND nd.disk_id='%s'"
    sqlQuery = sqlQueryFormat % (diskId, diskId)
    
    # Create a cursor and perform the query.
    ngamsDbObj._startTimer()
    server, db, user, password = ngasUtilsLib.getDbPars()
    curObj = ngamsDb.ngamsDb(server, db, user, password, 0).dbCursor(sqlQuery)
    ngamsDbObj._storeRdTime()

    info(5, "Leaving _getFileInfo1(). DB Time: " + ngamsDbObj.getDbTimeStr())
    return curObj


def _getFileInfo2(ngamsDbObj,
                  hostId,
                  srcDisk):
    """
    Get information about all files stored on the given host.

    ngamsDbObj:    NG/AMS DB Object (ngamsDb).
    
    hostId:        ID of host for which to query files (string).

    srcDisk:       Source disk, i.e., disk that was cloned (string).

    Returns:       NG/AMS Cursor Object (ngamsDbCursor).
    """
    info(5,"Entering _getFileInfo2() ...")
    
    sqlQueryFormat = "SELECT nd.slot_id, nd.mount_point, nf.file_name, " +\
                     "nf.checksum, nf.checksum_plugin, nf.file_id, " +\
                     "nf.file_version, nf.file_size, nf.file_status, " +\
                     "nd.disk_id, nf.ignore, nd.host_id " +\
                     "FROM ngas_disks nd noholdlock, " +\
                     "ngas_files nf noholdlock " +\
                     "WHERE nd.host_id='%s' " +\
                     "AND nf.disk_id=nd.disk_id AND nf.disk_id!='%s'"
    sqlQuery = sqlQueryFormat % (hostId, srcDisk) 

    # Create a cursor and perform the query.
    ngamsDbObj._startTimer()
    server, db, user, password = ngasUtilsLib.getDbPars()
    curObj = ngamsDb.ngamsDb(server, db, user, password, 0).dbCursor(sqlQuery)
    ngamsDbObj._storeRdTime()

    info(5, "Leaving_getFileInfo2 (). DB Time: " + ngamsDbObj.getDbTimeStr())
    return curObj


def cloneCheckFile(diskId,
                   fileId,
                   fileVersion,
                   dbConObj,
                   pClientObj,
                   checkRep):
    """
    Clone a file a check afterwards if it was successfully cloned.

    diskId:       ID of disk cloned (string).
    
    fileId:       ID of file cloned (string).
    
    fileVersion:  Version of file to check (integer).
    
    dbConObj:     DB Connection Object (ngamsDb).

    pClientObj:   Initiated instance of NG/AMS P-Client Object (ngamsPClient).
    
    checkRep:     Check report (string).

    Returns:      Updated Check Report (string).
    """
    msg = "\n-> Attempting to clone file: %s/%s/%s" %\
          (diskId, fileId, fileVersion)
    print msg
    msg += " - status: "
    res = pClientObj.clone(fileId, diskId, fileVersion, wait=1)
    if (res.getStatus() == "FAILURE"):
        status = "FAILURE: " + str(res.getMessage()) + "\n"
    else:
        # Check if file was really cloned.
        res = dbConObj.getFileInfoFromFileIdHostId(_getTargHost(), fileId,
                                                   fileVersion)
        if (res == []):
            status = "FAILURE: File not cloned!\n"
        else:
            fileInfo = ngamsFileInfo.ngamsFileInfo().\
                       unpackSqlResult(res)
            tmpPars = [["disk_id", fileInfo.getDiskId()],
                       ["file_id", fileId],
                       ["file_version", fileVersion]]
            res = pClientObj.sendCmdGen(getHostName(),
                                        pClientObj.getPort(),
                                        NGAMS_CHECKFILE_CMD,
                                        wait=1, pars=tmpPars)
            status = res.getMessage() + "\n"
    return msg + status
    

def checkCloning(srcDiskId,
                 autoClone):
    """
    Function to check if all files registered for a given disk, have
    been cloned onto disks in the system where this tool is executed.

    srcDiskId:     ID of the cloned disk (string).
    
    autoClone:     If 1 files not cloned will be automatically cloned
                   by the tool (integer/0|1).

    Returns:       Status message (string).
    """
    if (autoClone):
        portNo = ngasUtilsLib.getParNgasRcFile("NgasPort")
        ngamsClient = ngamsPClient.ngamsPClient(getHostName(), portNo)
    else:
        ngamsClient = None

    checkRepMain =  79 * "=" + "\n"
    checkRepMain += "Clone Verification Report - Disk: " + srcDiskId + "\n\n"
    checkRep     = ""    
    server, db, user, password = ngasUtilsLib.getDbPars()
    dbCon = ngamsDb.ngamsDb(server, db, user, password, 0)
    test = 0
    if (not test):
        # Get information about files on the Source Disk.
        diskInfo = dbCon.getDiskInfoFromDiskId(srcDiskId)
        if (not diskInfo):
            raise Exception, "Specified Disk ID: " + srcDiskId +\
                  " is unknown. Aborting."
        if (diskInfo[ngamsDb.NGAS_DISKS_HOST_ID].strip() == getHostName()):
            raise Exception, "Source disk specified Disk ID: %s" % srcDiskId+\
                  " is inserted in this node: %s" % getHostName()
        srcFiles = []
        if (diskInfo[ngamsDb.NGAS_DISKS_HOST_ID].strip() != ""):
            srcHost = diskInfo[ngamsDb.NGAS_DISKS_HOST_ID].strip()
        elif (diskInfo[ngamsDb.NGAS_DISKS_LAST_HOST_ID].strip() != ""):
            srcHost = diskInfo[ngamsDb.NGAS_DISKS_LAST_HOST_ID].strip()
        else:
            srcHost = None
        dbCur = _getFileInfo1(dbCon, srcDiskId)
        while (1):
            fileList = dbCur.fetch(200)
            if (fileList == []): break
            srcFiles += fileList
        del dbCur
        
        # Get information about files on the Target Host (= this host).
        trgFiles = []
        # NGAS Utils Functional Tests: Use special target host if requested.
        dbCur = _getFileInfo2(dbCon, _getTargHost(), srcDiskId)
        while (1):
            fileList = dbCur.fetch(200)
            if (fileList == []): break
            trgFiles += fileList
        del dbCur
    else:
        # We load this information from a file where we have dumped
        # it previously to avoid too long execution time.
        import ngasVerifyCloningFileInfo
        srcFiles = ngasVerifyCloningFileInfo.getSrcFiles()
        trgFiles = ngasVerifyCloningFileInfo.getTrgFiles()

    # Build a dictionary with the target files with
    # (<File ID>, <File Version>) as keys.
    trgFileDic = {}
    for fileInfo in trgFiles:
        trgFileDic[(fileInfo[ngamsDb.SUM1_FILE_ID],
                    fileInfo[ngamsDb.SUM1_VERSION])] = fileInfo
    
    # Now go through each source file and check if it is registered
    # in the DB among the target files.
    for fileInfo in srcFiles:
        srcFileId  = fileInfo[ngamsDb.SUM1_FILE_ID]
        srcFileVer = fileInfo[ngamsDb.SUM1_VERSION]
        key = (srcFileId, srcFileVer)
        
        # Check if target file is present in the DB.
        if (not trgFileDic.has_key(key)):
            checkRep += "*** Missing target file in DB: " + str(fileInfo)+ "\n"
            if (autoClone):
                checkRep += cloneCheckFile(fileInfo[9], fileInfo[5],
                                           fileInfo[6], dbCon, ngamsClient,
                                           checkRep) + "\n"
            continue

        trgFileInfo = trgFileDic[key]
        mtPt = trgFileInfo[ngamsDb.SUM1_MT_PT]
        filename = trgFileInfo[ngamsDb.SUM1_FILENAME]
        complTrgFilename = os.path.normpath(mtPt + "/" + filename)
        msg = "*** Checking file: " + complTrgFilename
        if (autoClone): msg = "\n" + msg
        print msg

        # 1. Check that the target file is physically present on the
        #    target disk.
        if (not os.path.exists(complTrgFilename)):
            checkRep += "Missing target file on disk: " + str(fileInfo) + "\n"
            if (autoClone):
                checkRep += cloneCheckFile(fileInfo[9], fileInfo[5],
                                           fileInfo[6], dbCon, ngamsClient,
                                           checkRep) + "\n"
            continue
        
        # 2. Check that the size is correct.
        srcFileSize  = fileInfo[ngamsDb.SUM1_FILE_SIZE]
        trgFileSize = ngamsLib.getFileSize(complTrgFilename)
        if (srcFileSize != trgFileSize):
            checkRep += "Wrong size of target file: " + str(fileInfo) + "\n"
            checkRep += " - Check file manually!" + "\n"

    if (checkRep):
        checkRepMain += checkRep
    else:
        checkRepMain += "No descrepancies found\n"
    
    return checkRepMain


def correctUsage():
    """
    Print correct usage of tool to stdout.
    
    Returns:   Void.
    """
    print "\nCorrect usage is:\n"
    print "> python ngasVerifyCloning.py -diskId \"<Disk ID>[,<Disk ID>]\" " +\
          "[-autoClone] [-notifEmail <Email Rec List>]"
    print "\n"

  
if __name__ == '__main__':
    """
    Main function.
    """
    if (len(sys.argv) < 2):
        correctUsage()
        sys.exit(1)

    # Parse input parameters.
    diskId    = ""
    autoClone = 0
    notifEmail = None
    idx = 1
    while idx < len(sys.argv):
        par = sys.argv[idx].upper()
        try:
            if (par == "-DISKID"):
                idx += 1
                diskId = sys.argv[idx]
            elif (par == "-AUTOCLONE"):
                autoClone = 1
            elif (par == "-NOTIFEMAIL"):
                idx += 1
                notifEmail = sys.argv[idx]
            else:
                sys.exit(1)
            idx += 1
        except Exception, e:
            print "\nProblem initializing Clone Verification Tool: %s\n" %\
                  str(e)
            correctUsage()  
            sys.exit(1)

    if (not diskId):
         correctUsage()  
         sys.exit(1)

    try:
        if (notifEmail == None):
            notifEmail = ngasUtilsLib.\
                         getParNgasRcFile(ngasUtilsLib.NGAS_RC_PAR_NOTIF_EMAIL)
 
        # Execute the cloning.
        diskIdList = diskId.split(",")
        print 79 * "="
        for srcDiskId in diskIdList:
            rep = checkCloning(srcDiskId, autoClone)
            if (autoClone):
                rep += "\n\nNOTE: Files might have been cloned - please " +\
                       "re-run the tool without the -autoClone option\n\n"
            print rep
            print 79 * "="

        # Send the email notification report.
        if (1):
            ngasUtilsLib.sendEmail("Cloning Verification Report for " +\
                                   "disk: " + srcDiskId, notifEmail, rep,
                                   "text/plain","CLONE_VER_REP_"+srcDiskId)
    except Exception, e:
        print "ERROR occurred executing the Clone Verification " +\
              "Tool: \n\n" + str(e) + "\n"
        sys.exit(1)
                
#
# EOF
