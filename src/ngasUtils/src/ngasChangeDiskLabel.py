

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
# "@(#) $Id: ngasChangeDiskLabel.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  30/01/2004  Created
#

"""
The ngasChangeDiskLabel tool is used to change the label (Logical Name)
of a disk in the NGAS DB. The disk to change the label is referred to
by its Disk ID.
"""

import sys, os, time

from ngams import *
import ngamsDb, ngamsFileInfo, ngamsDiskInfo
import ngamsLib
import ngasUtils, ngasUtilsLib


def changeDiskLabel(diskId,
                    newLabel = None,
                    execute = 0):
    """
    Change the Disk Label (ngas_disks.logical_name) of the disk referred
    to by its Disk ID.

    diskId:     Disk ID of the disk (string).

    newLabel:   New label to allocate to the disk (string).

    execute:    Execute the change (integer/0|1).

    Returns:    Void.
    """
    # Open DB connection.
    server, db, user, password = ngasUtilsLib.getDbPars()
    dbCon = ngamsDb.ngamsDb(server, db, user, password, 0)

    # Now, loop and ask the user to enter the new label and to confirm when OK.
    while (1):
        sqlDiskInfo = dbCon.getDiskInfoFromDiskId(diskId)
        if (not sqlDiskInfo):
            msg = "Disk with ID: %s appears not to be known to this system!"
            raise Exception, msg % diskId
        diskInfoObj = ngamsDiskInfo.ngamsDiskInfo().\
                      unpackSqlResult(sqlDiskInfo)
        orgDiskLabel = diskInfoObj.getLogicalName()
        print "Present Disk Label: %s" % orgDiskLabel
        if (newLabel):
            newDiskLabel = newLabel
        else:
            newDiskLabel = ngasUtilsLib.input("Enter new Disk Label:")
        if (sqlDiskInfo == []):
            raise Exception, "Disk ID given: %s not found in the NGAS DB!" %\
                  diskId
        print "\nChanging Disk Label for Disk with:"
        print " - Disk ID:        " + diskId
        print " - Disk Label:     " + orgDiskLabel
        print " - New Disk Label: " + newDiskLabel + "\n"
        if (execute):
            break
        else:
            choice = ngasUtilsLib.input("Is this correct (Y/N) [N]?").upper()
            if (choice == "Y"): break
            
    print "Changing Disk Label from %s to %s ..." %\
          (orgDiskLabel, newDiskLabel)

    # Check if this disk label already has been allocated.
    logNames = dbCon.getLogicalNamesMountedDisks(getHostName())
    for logName in logNames:
        if (logName == newDiskLabel):
            raise Exception, "Disk Label: %s is already in use!" % newDiskLabel

    diskInfoObj.setLogicalName(newDiskLabel).write(dbCon)
    sqlDiskInfo = dbCon.getDiskInfoFromDiskId(diskId)
    diskInfoObj = ngamsDiskInfo.ngamsDiskInfo().unpackSqlResult(sqlDiskInfo)
    report = "DISK LABEL CHANGE REPORT:\n\n"
    msg = "Changed Disk Label from %s to %s - new disk status:" %\
          (orgDiskLabel, newDiskLabel)
    report += msg + "\n\n"
    print msg
    msg = diskInfoObj.dumpBuf()
    report += msg + "\n\n"
    print msg

    print("\nPut the given disk Online/reboot the host in " +\
          "which the disk is inserted."
          "\n\nCheck the disk info in the NGAS WEB Interfaces!\n")
    
    notifEmail = ngasUtilsLib.\
                 getParNgasRcFile(ngasUtilsLib.NGAS_RC_PAR_NOTIF_EMAIL)
    if (notifEmail):
        ngasUtilsLib.sendEmail("ngasChangeDiskLabel: DISK LABEL CHANGE REPORT",
                               notifEmail, report)


def correctUsage():
    """
    Return the usage/online documentation in a string buffer.

    Returns:  Man-page (string).
    """
    buf = "\nCorrect usage is:\n\n" +\
          "$ ngasChangeDiskLabel -diskId <Disk ID> " +\
          "[-accessCode <Code>] [-newLabel <Label>] [-execute]\n\n"
    return buf

  
if __name__ == '__main__':
    """
    Main function to execute the tool.
    """
    setLogCond(0, "", 0, "", 1)

    # Parse input parameters.
    accessCode   = None
    diskId       = None
    notifEmail   = None
    newLabel     = None
    execute      = 0
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
            elif (par == "-NEWLABEL"):
                idx += 1
                newLabel = sys.argv[idx]
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
    if (notifEmail == None):
        notifEmail = ngasUtilsLib.\
                     getParNgasRcFile(ngasUtilsLib.NGAS_RC_PAR_NOTIF_EMAIL)
    try:
        if (not diskId):
            print correctUsage()  
            raise Exception, "Incorrect command line parameter(s) given!"
        if (not accessCode):
            accessCode = ngasUtilsLib.input("Enter Access Code:")
        ngasUtilsLib.checkAccessCode(accessCode)
        changeDiskLabel(diskId, newLabel, execute)
    except Exception, e:
        print "\nProblem encountered:\n\n" + str(e) + " -- bailing out\n"


# EOF
