

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
# "@(#) $Id: ngasRetireDisk.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  30/01/2004  Created
#

"""
The ngasRetireDisk Tool is used to retire disks, i.e., to mark disks as
'RETIRED' in the NGAS DB, remove the entries for these disks after having
created a back-up in the ngas_files_retired table.

It is only possible to retire disks that are not mounted. In addition, it
is not possible to retire a disk where less than 3 independent copies are
available in the system. This however, can be enforced by specifying the
'force' option.
"""

import sys, os, time

from ngams import *
import ngamsDb, ngamsLib, ngamsFileInfo, ngamsDiskInfo, ngamsRemUtils
import ngamsReqProps, ngamsServer
import ngasUtils, ngasUtilsLib


def retireDisk(diskId,
               force,
               execute,
               notifEmail):
    """
    Remove file entries from the ngas_files DB, which are stored on disks
    having the mount point column set to 'RETIRED'.

    diskId:        Disk ID of disk to retire (string).

    execute:       Execute the command. If set to 0, the command is executed
                   otherwise it is only checked if the command can be
                   executed (integer/0|1).

    force:         Even though there are less than 3 copies of each file on
                   the disk in the system or even if the disk is mounted
                   retire the disk (integer/0|1).

    notifEmail:    Comma separated list of email recipients that should be
                   informed about the actions carried out, or the actions
                   that would be carried out if executing the command
                   (integer/0|1).

    Returns:       Void.
    """
    info(0,"Executing Retire Disk Procedure on disk with ID: %s" % diskId)

    # Open DB connection.
    info(3,"Open DB connection")
    server, db, user, password = ngasUtilsLib.getDbPars()
    dbCon = ngamsDb.ngamsDb(server, db, user, password, 0)
    # Dummy server object, needed by one of the methods used ...
    srvObj = ngamsServer.ngamsServer().setDb(dbCon)

    # Get information about the disk.
    info(3,"Get info about disk in question")
    sqlDiskInfo = dbCon.getDiskInfoFromDiskId(diskId)
    if (sqlDiskInfo == []):
        raise Exception, "Disk ID given: %s not found in the NGAS DB!" % diskId
    diskInfoObj = ngamsDiskInfo.ngamsDiskInfo().unpackSqlResult(sqlDiskInfo)
    info(0,"Disk with ID: %s has Logical Name: %s" %\
         (diskInfoObj.getDiskId(), diskInfoObj.getLogicalName()))

    # Check if the disk is marked as mounted, in case yes, reject it.
    if (diskInfoObj.getMounted()):
        errMsg = "Disk with ID: %s is marked as mounted in the DB! "+\
                 "Rejecting request."
        raise Exception, errMsg % diskId

    # Check if there are at least three copies of each file.
    if (not force):
        basePath = "/tmp/.ngasRetireDisk"
        checkCreatePath(basePath)
        ngasUtilsLib.checkDelTmpDirs(basePath + "/*")
        tmpPath = "/tmp/.ngasRetireDisk/" + str(time.time())
        checkCreatePath(tmpPath)
        dbFilePat = tmpPath + "/DBM"
        lessCopyDbm, notRegDbm, allFilesDbm =\
                     ngamsRemUtils.checkFileCopiesAndReg(srvObj, 3, dbFilePat,
                                                         None, diskId, 1)
        srvObj.getCfg().storeVal("NgamsCfg.Notification[1].SmtpHost",
                                 "smtphost.hq.eso.org")
        reqPropsObj = ngamsReqProps.ngamsReqProps().\
                      setCmd("ngasRetireDisk").\
                      addHttpPar("notif_email", notifEmail).\
                      addHttpPar("disk_id", diskId)
        status = ngamsRemUtils._remStatErrReport(srvObj, reqPropsObj,
                                                 dbFilePat, lessCopyDbm,
                                                 notRegDbm, allFilesDbm,
                                                 diskId)
        commands.getstatusoutput("rm -rf " + dbFilePat + "*")
        if (status):
            errMsg = "Cannot retire disk with ID: %s - one or more files " +\
                     "are not available in at least 3 copies!"
            raise Exception, errMsg % diskId

    # First query the files in question.
    info(0,"Get list of files stored on disk: %s ..." %\
         diskInfoObj.getLogicalName())
    query = "SELECT nf.disk_id, file_id, file_version " +\
            "FROM ngas_files nf, ngas_disks nd WHERE " +\
            "nd.disk_id='%s' AND nf.disk_id='%s'"
    query = query % (diskId, diskId)
    statFileList = dbCon.query(query, 1)
    info(0,"Got list of files stored on disk: %s" %\
         diskInfoObj.getLogicalName())

    # Back-up the files on the retired disk to the ngas_files_retired table.
    if (execute):
        info(0,"Insert files on Retired Disk into ngas_files_retired")
        query = "INSERT INTO ngas_files_retired SELECT disk_id, " +\
                "file_name, file_id, file_version, format, file_size, " +\
                "uncompressed_file_size, compression, ingestion_date, " +\
                "file_ignore, checksum, checksum_plugin, file_status, " +\
                "creation_date FROM ngas_files WHERE disk_id='%s'"
        query = query % diskId
        dbCon.query(query, 1)
        info(0,"Inserted files on Retired Disk into ngas_files_retired")

    # Remove the entries from the DB.
    if (execute):
        info(0,"Remove entry from disk from DB ...")
        query = "DELETE FROM ngas_files WHERE disk_id='%s'" % diskId
        dbCon.query(query, 1)
        info(0,"Removed entry from disk from DB")

    # Set the mount point of the referenced disk to 'RETIRED'.
    if (execute):
        info(0,"Setting mount point of Retired Disk to 'RETIRED' ...")
        query = "UPDATE ngas_disks SET mount_point='RETIRED' WHERE " +\
                "disk_id='%s'"
        query = query % diskId
        dbCon.query(query, 1)
        info(0,"Set mount point of Retired Disk to 'RETIRED'")

    # Generate report.
    if (notifEmail):
        info(0,"Generate DISK RETIREMENT STATUS REPORT ...")
        report = "DISK RETIREMENT STATUS REPORT - %s:\n\n" % diskId
        if (not execute):
            report += "Information for disk/files concerned if command " +\
                      "is executed.\n\n"
        else:
            report += "Information for disk/files concerned.\n\n"
        report += "Date:       %s\n" % PccUtTime.TimeStamp().getTimeStamp()
        report += "NGAS Host:  %s\n" % getHostName()
        report += "Disk ID:    %s\n\n" % diskId
        tmpFormat = "%-32s %-32s %-12s\n"
        report += tmpFormat % ("Disk ID", "File ID", "File Version")
        report += tmpFormat % (32 * "-", 32 * "-", 12 * "-")
        if (len(statFileList[0])):
            for fileInfo in statFileList[0]:
                report += tmpFormat % (fileInfo[0], fileInfo[1], fileInfo[2])
        else:
            report += "No files found on disk!\n"
        report += 78 * "-" + "\n"
        subject = "ngasRetireDisk: DISK RETIREMENT REPORT - %s" % diskId
        ngasUtilsLib.sendEmail(subject, notifEmail, report)
        info(0,"Generated DISK RETIREMENT STATUS REPORT")
    info(0,"Finished Disk Retirement Procedure for disk: %s/%s" %\
         (diskInfoObj.getDiskId(), diskInfoObj.getLogicalName()))


def correctUsage():
    """
    Return the usage/online documentation in a string buffer.

    Returns:  Man-page (string).
    """
    buf = "\nCorrect usage is:\n\n" +\
          "> ngasRemoveRetiredFiles.py -diskId <Disk ID> [-execute] " +\
          "[-force] [-accessCode <Code>] [-notifEmail <Email List>]\n\n"
    return buf


if __name__ == '__main__':
    """
    Main function to execute the tool.
    """
    setLogCond(0, "", 0, "", 1)

    # Parse input parameters.
    accessCode   = ""
    diskId       = ""
    execute      = 0
    force        = 0
    notifEmail   = None
    idx = 1
    while idx < len(sys.argv):
        par = sys.argv[idx].upper()
        try:
            if (par == "-ACCESSCODE"):
                idx += 1
                accessCode = sys.argv[idx]
            elif (par == "-DISKID"):
                idx += 1
                diskId = sys.argv[idx]
            elif (par == "-EXECUTE"):
                execute = 1
            elif (par == "-FORCE"):
                force = 1
            elif (par == "-NOTIFEMAIL"):
                idx += 1
                notifEmail = sys.argv[idx]
            else:
                sys.exit(1)
            idx += 1
        except Exception, e:
            print "\nProblem executing the tool: %s\n" % str(e)
            print correctUsage()
            sys.exit(1)
    if (not diskId):
        print correctUsage()
        sys.exit(1)
    if (notifEmail == None):
        notifEmail = ngasUtilsLib.\
                     getParNgasRcFile(ngasUtilsLib.NGAS_RC_PAR_NOTIF_EMAIL)
    try:
        if (not accessCode):
            accessCode = ngasUtilsLib.input("Enter Access Code:")
        ngasUtilsLib.checkAccessCode(accessCode)
        retireDisk(diskId, force, execute, notifEmail)
    except Exception, e:
        print "\nProblem encountered:\n\n" + str(e) + " -- bailing out\n"


# EOF
