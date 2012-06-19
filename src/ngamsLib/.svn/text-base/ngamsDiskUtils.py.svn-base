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
# "@(#) $Id: ngamsDiskUtils.py,v 1.8 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  13/05/2001  Created
#

"""
Contains utilities for handling the disk configuration.
"""

import  sys, os, re, string, threading, glob, commands
import  xml.dom.minidom
import  PccUtTime
from    ngams import *
import  ngamsLib, ngamsHighLevelLib, ngamsNotification
import  ngamsDiskInfo, ngamsStatus, ngamsPhysDiskInfo


# Semaphore to protect critical operations done on the ngas_disk table.
_ngamsDisksSem = threading.Semaphore(1)


def prepNgasDiskInfoFile(diskInfoObj,
                         yankNewlines = 0,
                         yankElSpaces = 0):
    """
    Create and prepare an NGAS Disk Info Status Object and set the
    attributes in the ngamsStatus object accordingly. The NgasDiskInfo
    XML document is returned.

    diskInfoObj:     NGAS Disk Info Object containing information about the
                     disk for which to generate the NgasDiskInfo file
                     (ngasDiskInfo).
    
    yankNewlines:    If set to 1 newline characters will be removed from
                     the resulting XML document (integer/0|1).

    yankElSpaces:    Yank spaces between elements (integer/0|1).

    Returns:         NgasDiskInfo XML document (string/xml).
    """
    status = ngamsStatus.ngamsStatus()
    status.\
             setDate(PccUtTime.TimeStamp().getTimeStamp()).\
             setVersion(getNgamsVersion()).setHostId(getHostId()).\
             setMessage("Disk status file").addDiskStatus(diskInfoObj)
    xmlDoc = status.genXml(0, 1, 0, 0, 1).toprettyxml()
    if (yankNewlines): xmlDoc = re.sub("\n", "", str(xmlDoc)).strip()
    if (yankElSpaces):
        xmlDoc = re.sub("> *<", "><", xmlDoc)
        xmlDoc = re.sub(">\t*<", "><", xmlDoc)
    return xmlDoc


def getDiskCompleted(dbConObj,
                     diskId):
    """
    Return 1 if a disk is marked as completed in the DB, otherwise
    0 is returned.

    dbConObj:   NGAS DB connection object (ngamsDb).
    
    diskId:     Disk ID for the disk (string).

    Returns:    1 if disk is marked as completed, otherwise 0 (integer).
    """
    info(4,"Checking if disk with ID: " + diskId+" is marked as completed ...")
    completed = dbConObj.getDiskCompleted(diskId)
    if (completed == None):
        info(4,"Disk with ID: " + diskId +\
             " is not marked as completed (is not registered)")
        return 0
    else:
        if (completed):
            info(4,"Disk with ID: " + diskId + " is completed")
        else:
            info(4,"Disk with ID: " + diskId + " is not completed")
        return completed


def isMainDisk(slotId,
               ngamsCfg):
    """
    Based on the mount point, return 1 if the disk is a Main Disk.
    
    slotId:       Slot ID for the disk (string).
    
    ngamsCfg:     Instance of NG/AMS Configuration Class (ngamsConfig).

    Returns:      1 if disk is a Main Disk, otherwise 0 (integer).
    """
    set = ngamsCfg.getStorageSetFromSlotId(slotId)
    if (set.getMainDiskSlotId() == slotId):
        return 1
    else:
        return 0


def checkDisks(dbConObj,
               ngamsCfgObj,
               diskDic):
    """
    First it is checked if all HDDs marked in the DB as mounted in this
    NGAS system are actually mounted.
    
    Check the HDDs currently installed and for each HDD it is checked
    if there is an entry for this. If not a new entry for this HDD is
    created.

    Subsequently it is checked if each of the disks now marked as mounted
    and thus available in this system, are really available. If not,
    an exception is thrown.

    Finally it is checked that for each Stream defined in the NG/AMS
    Configuration, that the disks of at least one Storage Set are
    available.

    dbConObj:       DB connection object (ngamsDb).
    
    ngamsCfgObj:    Configuration object (ngamsConfig).
    
    diskDic:        Dictionary containing ngamsPhysDiskInfo objects with the
                    information about the disk configuration (dictionary).
    
    Returns:        Void.
    """
    T = TRACE()
    
    diskList = getDiskInfoForMountedDisks(dbConObj, getHostId(),
                                          ngamsCfgObj.getRootDirectory())
    # The "mtDiskDic" dictionary contains information (ngasDiskInfo objects)
    # for each disk mounted on this system.
    if (diskList != []):
        info(1,"All System Types: Checking if disks registered in the " +\
             "DB as mounted on this system really are mounted physically ...")
    mtDiskDic = {}
    for disk in diskList:
        info(1,"Checking for availability of disk with ID: " +\
             disk.getDiskId() + " ...")
        found = 0
        for slotId in diskDic.keys():
            if (diskDic[slotId].getDiskId() == disk.getDiskId()):
                found = 1
                mtDiskDic[slotId] = disk
                break
        if (found == 0):
            info(1,"Disk with ID: " + disk.getDiskId() +\
                 " not available anymore - modifying entry in DB.")
            disk.setHostId("")
            disk.setSlotId("")
            disk.setMountPoint("")
            disk.setMounted(0)
            disk.write(dbConObj)
        else:
            info(1,"Disk with ID: " + disk.getDiskId() + " available.")

    # Ensure we have the information available for each disk, either from the
    # DB or from the NGAS Disk Info files (if one of these are available).
    #
    # Note: If both information about the disk is available in the DB and
    #       in the NgasDiskInfo file, the information for the disk is taken
    #       from the one, which has the most recent InstallationDate defined.
    slotIdList = []
    for slotId in diskDic.keys():
        slotIdList.append(slotId)
    slotIdList.sort()
    for slotId in slotIdList:
        slotId = str(slotId)
        # Get from DB if not already read + if disk in DB.
        if (not mtDiskDic.has_key(slotId)):
            if (dbConObj.diskInDb(diskDic[slotId].getDiskId())):
                dbDiskInfo = ngamsDiskInfo.ngamsDiskInfo()
                dbDiskInfo.read(dbConObj, diskDic[slotId].getDiskId())
                mtDiskDic[slotId] = dbDiskInfo
        else:
            dbDiskInfo = mtDiskDic[slotId]

        # Get info from NgasDiskInfo file (if available).
        ngasDiskInfoFile = getNgasDiskInfoFile(diskDic, slotId)

        # Decide which information to use.
        if (not mtDiskDic.has_key(slotId)):
            mtDiskDic[slotId] = ngasDiskInfoFile
        elif (mtDiskDic.has_key(slotId) and ngasDiskInfoFile):
            # Take the info in the NgasDiskInfo file if this is the most
            # recent one.
            if (ngasDiskInfoFile.getInstallationDate() >
                dbDiskInfo.getInstallationDate()):
                info(1,"Disk information in NgasDiskInfo File for disk with "+\
                     "ID: " + ngasDiskInfoFile.getDiskId() + "/Slot ID: " +\
                     slotId + ", is more recent than the one found in the " +\
                     "NGAS DB for that disk. Taking information for disk " +\
                     "from NgasDiskInfo File.")
                mtDiskDic[slotId] = ngasDiskInfoFile
        else:
            mtDiskDic[slotId] = None

    if (ngamsCfgObj.getAllowArchiveReq() and (diskDic != {})):
        info(1,"Archiving System: Checking that all disks defined in the " +\
             "configuration and which are mounted and not completed, can be "+\
             "accessed. Check that installed disks, have entries in the " +\
             "NGAS DB ...")
        mtSlotIds  = diskDic.keys()
        cfgSlotIds = []
        for cfgSlotId in ngamsCfgObj.getSlotIds():
            cfgSlotIds.append(cfgSlotId)
        cfgSlotIds.sort()
        
        notifInfo = []
        # Disks, which should not be updated/inserted in the DB are contained
        # in the "rmDiskDic" dictionary.
        rmDiskDic = {}
        # Disks, which should be updated/inserted in the DB are contained
        # in the "dbUpdateDic" dictionary.
        dbUpdateDic = {}
        for slotId in cfgSlotIds:
            slotId = str(slotId)
            if (ngamsLib.elInList(mtSlotIds, slotId)):
                diskInfo = diskDic[slotId]
                assocSlotId = ngamsCfgObj.getAssocSlotId(slotId)

                # If a disk is marked as 'completed' in the DB or in the
                # NGAS Disk Info File, we simply update its entry in the DB.
                if (mtDiskDic[slotId]):
                    if (mtDiskDic[slotId].getCompleted()):
                        info(2,"Disk in Slot with ID: " + slotId + " " +\
                             "is marked as 'completed' - just updating " +\
                             "info in DB")
                        dbUpdateDic[slotId] = ["UPDATE", mtDiskDic[slotId]]
                        continue

                # Check for disk accessibility
                diskRef = "Slot ID: " + str(diskInfo.getSlotId()) +\
                          " - Disk ID: " + diskInfo.getDiskId()
                info(1,"Checking for accessibility of disk: " + diskRef+" ...")
                if (checkDiskAccessibility(diskInfo.getMountPoint()) == -1):
                    errMsg = genLog("NGAMS_ER_DISK_INACCESSIBLE", [diskRef])
                    error(errMsg)
                    rmDiskDic[slotId] = slotId
                    notifInfo.append(["DISK INACCESSIBLE", errMsg])
                    if (dbUpdateDic.has_key(assocSlotId)):
                        dbUpdateDic[assocSlotId] = None
                        del dbUpdateDic[assocSlotId]
                    continue
                else:
                    info(1,"Disk: " + diskRef + " is accessible")

                info(2,"Check if the disk with ID: " + diskInfo.getDiskId() +\
                     " already has an entry in the DB ...")
                tmpDiskInfo = mtDiskDic[slotId]

                # Check the disk information or simply mark the disk
                # for update in the DB.
                if (tmpDiskInfo == None):
                    # If the associated disk is not already found to be
                    # wrong, we add an ADD entry in the Update DB Dictionary.
                    if (not rmDiskDic.has_key(assocSlotId)):
                        dbUpdateDic[slotId] = ["ADD"]
                else:
                    info(2,"Entry found in DB for disk with ID: " +\
                         diskInfo.getDiskId())

                    info(2,"Check that this disk already registered " +\
                         "is used correctly as Main or Replication Disk ...")
                    # From the Logical Name of the disk, we can identify
                    # if it was previously used as a Main or as a Replication
                    # Disk (e.g.: "LS-FitsStorage3-M-000003"). From the Slot
                    # ID we check via the NG/AMS Configuration if the disk is
                    # installed in a Main or Replication Slot.
                    try:
                        tmpType = tmpDiskInfo.getLogicalName().split("-")[-2]
                    except Exception, e:
                        raise Exception, "Illegal Logical Name specified: " +\
                              str(tmpDiskInfo.getLogicalName()) + " for " +\
                              "disk with ID: " + tmpDiskInfo.getDiskId()
                    if (tmpType == "M"):
                        prevMainDisk = 1
                    else:
                        prevMainDisk = 0
                    mainDisk = isMainDisk(slotId, ngamsCfgObj)
                        
                    # Check: Disk was previously installed as a Main Disk,
                    # but is now installed in a Replication Slot.
                    if (prevMainDisk and not mainDisk):
                        errMsg = genLog("NGAMS_ER_MAIN_DISK_WRONGLY_USED",
                                        [str(slotId),
                                         tmpDiskInfo.getLogicalName()])
                        error(errMsg)
                        notifInfo.append(["MAIN DISK USED AS " +\
                                          "REPLICATION DISK", errMsg])
                        rmDiskDic[slotId] = slotId
                        continue

                    # Check: Disk was previously installed as a Replication
                    # Disk, but is now installed in a Main Slot.
                    if (not prevMainDisk and mainDisk):
                        errMsg = genLog("NGAMS_ER_REP_DISK_WRONGLY_USED",
                                        [str(slotId),
                                         tmpDiskInfo.getLogicalName()])
                        error(errMsg)
                        notifInfo.append(["REPLICATION DISK USED AS " +\
                                          "MAIN DISK", errMsg])
                        rmDiskDic[slotId] = slotId
                        continue
                                     
                    # Everything OK, update existing entry in the DB.
                    # If the associated disk is not already found to be
                    # wrong, we add an entry in the Update DB Dictionary.
                    if (not rmDiskDic.has_key(assocSlotId)):
                        dbUpdateDic[slotId] = ["UPDATE", tmpDiskInfo]

                # Create some standard directories on the disk if not
                # already there.
                if (ngamsCfgObj.getAllowArchiveReq() or
                    ngamsCfgObj.getAllowRemoveReq()):
                    diskDbDir = os.path.normpath(diskInfo.getMountPoint() +\
                                                 "/" + NGAMS_DB_DIR)
                    checkCreatePath(diskDbDir)
                    diskDbCacheDir = os.path.\
                                     normpath(diskInfo.getMountPoint() +\
                                              "/" + NGAMS_DB_CH_CACHE)
                    checkCreatePath(diskDbCacheDir)

        # Check that each disk accepted so far has an associated disk if this
        # is specified like this in the configuration.
        info(2,"Check if each disk has an associated disk if the " +\
             "configuration specifies this ...")
        for slotId in dbUpdateDic.keys():
            # We do not consider disks that are already completed.
            if (mtDiskDic[slotId]):
                if (mtDiskDic[slotId].getCompleted()): continue
            assocSlotId = ngamsCfgObj.getAssocSlotId(slotId)
            if (assocSlotId):
                if (not dbUpdateDic.has_key(assocSlotId)):
                    msg = "Disk in slot: %s has no associated " +\
                          "disk in slot: %s - rejecting disk in slot: %s"
                    info(2,msg % (slotId, assocSlotId, slotId))
                    del dbUpdateDic[slotId]
                    rmDiskDic[slotId] = slotId

        # Remove entries from the Disk Dictionary, which are invalid.
        for slId in rmDiskDic.keys():
            for id in [slId, ngamsCfgObj.getAssocSlotId(slId)]:
                if (diskDic.has_key(id)):
                    info(1,"Removing invalid disk info object from Disk " +\
                         "Dictionary -" +\
                         " Port No: " + str(diskDic[id].getPortNo()) +\
                         " Mount Point: " +\
                         str(diskDic[id].getMountPoint()) +\
                         " Disk ID: " + diskDic[id].getDiskId())
                    diskDic[id] = None
                    del diskDic[id]
                    if (dbUpdateDic.has_key(id)):
                        dbUpdateDic[id] = None
                        del dbUpdateDic[id]
                        
        # Generate a Notification Message with the possible errors
        # accumulated so far and send out Error Notification Messages.
        if (len(notifInfo) > 0):
            errMsg = "FOLLOWING PROBLEMS WERE ENCOUNTERED WHILE CHECKING " +\
                     "THE NGAS DISK CONFIGURATION:\n"
            for err in notifInfo:
                errMsg = errMsg + "\n" + err[0] + ":\n" + err[1] + "\n"
            ngamsNotification.notify(ngamsCfgObj, NGAMS_NOTIF_ERROR,
                                     "DISK CONFIGURATION INCONSISTENCIES/" +\
                                     "PROBLEMS ENCOUNTERED", errMsg)

        # Write information in the NGAS DB about the disks available.
        slotIds = []
        for slotId in dbUpdateDic.keys():
            slotIds.append(slotId)

        # Sort the slots such that the Main Slot is always listed before the
        # Replication Slot + sort the slots 'logically' according to the name.
        slotIdDic = {}
        for slotId in slotIds:
            storageSet = ngamsCfgObj.getStorageSetFromSlotId(slotId)
            slotIdDic[storageSet.getMainDiskSlotId()] = storageSet
        tmpSortedSlotIds = ngamsLib.logicalStrListSort(slotIdDic.keys())
        sortedSlotIds = []
        for slotId in tmpSortedSlotIds:
            slotId = slotId.strip()
            storageSet = slotIdDic[slotId]
            sortedSlotIds.append(storageSet.getMainDiskSlotId())
            if (storageSet.getRepDiskSlotId() != ""):
                sortedSlotIds.append(storageSet.getRepDiskSlotId())

        # Add or update the information for the Storage Sets in the DB
        for slId in sortedSlotIds:
            if (dbUpdateDic[slId][0] == "ADD"):
                addDiskInDb(dbConObj, ngamsCfgObj, slId, diskDic)
            else:
                updateDiskInDb(dbConObj, ngamsCfgObj, slId, diskDic,
                               dbUpdateDic[slId][1])
  
        info(2,"Archiving System: Check that there is at least one " +\
             "Storage Set available for each Stream defined ...")
        probMimeTypeList = []
        for stream in ngamsCfgObj.getStreamList():
            info(1,"Checking for target disks availability for mime-type: " +\
                 stream.getMimeType())
            if (checkStorageSetAvailability(dbConObj, ngamsCfgObj,
                                            stream.getMimeType()) !=
                NGAMS_SUCCESS):
                probMimeTypeList.append(stream.getMimeType())
        if ((len(probMimeTypeList) > 0) and
            (not ngamsCfgObj.getArchiveUnits())):
            errMsg = ""
            for mimeType in probMimeTypeList:
                errMsg = errMsg + " " + mimeType
            errMsg = genLog("NGAMS_WA_NO_TARG_DISKS", [errMsg])
            warning(errMsg)
            ngamsNotification.notify(ngamsCfgObj, NGAMS_NOTIF_NO_DISKS,
                                     "DISK SPACE INAVAILABILITY", errMsg)
    else:
        # It is not an Archiving System: Check if the disks in the Disk Dic
        # (the disks mounted, are registered in the DB. If yes, the info
        # for the disk is updated, if no, a new entry is added.
        info(2,"Non Archiving System: Check if the disks mounted are " +\
             "registered in the DB ...")
        slotIds = diskDic.keys()
        slotIds.sort()
        for slotId in slotIds:
            diskId = diskDic[slotId].getDiskId()
            diskInfo = ngamsDiskInfo.ngamsDiskInfo()
            try:
                diskInfo.read(dbConObj, diskId)
                info(2,"Marking disk with ID: " + diskId +\
                     " and mount point: " + diskDic[slotId].getMountPoint() +\
                     " as mounted in the DB.")
                updateDiskInDb(dbConObj, ngamsCfgObj, slotId, diskDic,diskInfo)
            except:
                info(2,"Creating new entry for disk with ID: " + diskId +\
                     " and mount point: " +  diskDic[slotId].getMountPoint() +\
                     " in the DB.")
                hasDiskInfo = 0
                if (mtDiskDic.has_key(slotId)):
                    if (mtDiskDic[slotId]): hasDiskInfo = 1
                if (hasDiskInfo):
                    updateDiskInDb(dbConObj, ngamsCfgObj, slotId, diskDic,
                                   mtDiskDic[slotId])
                else:
                    addDiskInDb(dbConObj, ngamsCfgObj, slotId, diskDic)
            

def checkStorageSetAvailability(dbConObj,
                                ngamsCfgObj,
                                mimeType):
    """
    Check if for a given Stream (defined by its mime-type) the disks of
    a Storage Set are available.

    dbConObj:       DB connection object (ngamsDb).
    
    ngamsCfgObj:    Instance of NG/AMS Configuration Class (ngamsConfig).
    
    mimeType:       Mime-type to check for (string).

    Returns:        NGAMS_SUCCESS/NGAMS_FAILURE (string).
    """
    if (ngamsLib.trueArchiveProxySrv(ngamsCfgObj)): return NGAMS_SUCCESS
    try:
        findTargetDisk(dbConObj, ngamsCfgObj, mimeType, 0)
        return NGAMS_SUCCESS
    except Exception, e:
        warning("Error encountered checking for storage set availability: " +
                str(e))
        return NGAMS_FAILURE


def checkDiskAccessibility(mountPoint):
    """
    Check if a disk is accessible, i.e., if it is possible to
    write on the given mount point.

    mountPoint:    Mount point of disk (string).

    Returns:       0: OK, -1: Failure accessing disk (integer)
    """
    testFile = os.path.normpath(mountPoint + "/NgamsTestFile")
    info(5,"Testing disk accessibility - creating test file: " +\
         testFile + " ...")
    res = commands.getstatusoutput("touch " + testFile)
    commands.getstatusoutput("rm -f " + testFile)
    if (int(res[0]) != 0):
        info(5,"Problem accessing disk with mount point: " + mountPoint)
        return -1
    else:
        info(5,"Successfully accessed disk with mount point: " + mountPoint)
        return 0
      
        
def genLogicalName(dbConObj,
                   ngamsCfgObj,
                   diskId,
                   slotId):
    """
    Builds a human readable name for the disk, of the form:

      <Disk Label>-M|R-<number>

    dbConObj:         DB connection object (ngamsDb).
    
    ngamsCfgObj:      NG/AMS Configuration Object (ngamsConfig).

    diskId:           Disk ID (string).
       
    slotId:           Slot ID (string).
    
    Returns:          Logical name for disk (string).
    """
    info(4,"Generating Logical Name for disk with ID: " + diskId +\
         " - in slot: " + str(slotId))
    set = ngamsCfgObj.getStorageSetFromSlotId(slotId)
    if (set.getMainDiskSlotId() == slotId):
        cat = "M"
    else:
        cat = "R"
    diskLabel = set.getDiskLabel()
    if (diskLabel != ""):
        idPrefix = diskLabel + "-" + cat
    else:
        idPrefix = cat
    # Get the disk number.
    if (cat == "M"):
        # Get the highest number currently found in the DB.
        maxDiskNumber = dbConObj.getMaxDiskNumber()
    else:
        # For a Replication Disk - simply get the index of the Main Disk.
        mainDiskId = dbConObj.getDiskIdFromSlotId(getHostId(),
                                                  set.getMainDiskSlotId())
        mainLogName = dbConObj.getLogicalNameFromDiskId(mainDiskId)
        maxDiskNumber = (int(mainLogName.split("-")[-1]) - 1)
    if (maxDiskNumber == None):
        number = "000001"
    else:
        # The logical name could e.g. be: "Fits-M-000001"
        number = padString(str(maxDiskNumber + 1), 6, "0")

    logName = idPrefix + "-" + number
    info(4,"Generated Logical Name for disk with ID: " + diskId +\
         " - in slot: " + str(slotId) + ": " + logName)
    return logName


def getNgasDiskInfoFile(diskDic,
                        slotId):
    """
    Check if the disk has an NGAS Disk Info file. In case yes,
    return this. Otherwise return None.

    diskDic:     Dictionary containing ngamsPhysDiskInfo objects
                 with the information about the disk configuration
                 (dictionary).
                 
    slotId:      Slot ID (string).

    Returns:     A Disk Info Object (ngamsDiskInfo) or None if no
                 NGAS Disk Info found for the disk.
    """    
    T = TRACE()

    # Check if there is an NgasDiskInfo file for the disk. In case yes,
    # load this and use these values for adding the entry in the DB.
    diskInfoFile = os.path.normpath(diskDic[slotId].getMountPoint() + "/" +\
                                    NGAMS_DISK_INFO)
    info(4,"Checking if NGAS Disk Info file available: " + diskInfoFile)
    retVal = None
    if (os.path.exists(diskInfoFile)):
        info(2,"Found Disk Info File for disk in slot: " + slotId)
        statusObj = ngamsStatus.ngamsStatus().load(diskInfoFile, 1)
        retVal = statusObj.getDiskStatusList()[0]
    return retVal


def addDiskInDb(dbConObj,
                ngamsCfgObj,
                slotId,
                diskDic):
    """
    Add a disk in the NGAS DB.
    
    dbConObj:       DB connection object (ngamsDb).
    
    ngamsCfgObj:    NG/AMS Configuration Object (ngamsConfig).
    
    slotId:         Slot ID (string).
    
    diskDic:        Dictionary containing ngamsPhysDiskInfo objects with the
                    information about the disk configuration (dictionary).

    Returns:        Void.
    """
    T = TRACE()

    diskId = diskDic[slotId].getDiskId()
   
    info(1,"Adding disk with ID: " + diskId + " - Slot ID: " + slotId +\
         " not registered in the NGAS DB ...")

    info(2,"Creating a new entry in the NGAS DB for disk with ID: " +\
         diskId + " ...")
    # In the following the assumption is made that if a disk doesn't have an
    # entry in the NGAS DB, this means that the disk is 'clean', i.e. doesn't
    # contain files already, or if, then these are not taken into account.
    installationDate   = PccUtTime.TimeStamp().initFromNow().getTimeStamp()
    logicalName        = genLogicalName(dbConObj, ngamsCfgObj, diskId, slotId)
    mountPoint         = diskDic[slotId].getMountPoint()

    # Create the disk info element.
    diskEntry = ngamsDiskInfo.ngamsDiskInfo().\
                setArchive(ngamsCfgObj.getArchiveName()).\
                setInstallationDate(installationDate).\
                setLogicalName(logicalName).\
                setHostId(getHostId()).\
                setSlotId(slotId).\
                setMounted(1).\
                setMountPoint(mountPoint).\
                setNumberOfFiles(0).\
                setAvailableMb(getDiskSpaceAvail(mountPoint)).\
                setBytesStored(0).\
                setCompleted(0).\
                setChecksum("").\
                setTotalDiskWriteTime(0.0).\
                setLastHostId(getHostId())

    # Ensure that the values extracted from the disk BIOS are actually taken
    # from the disk BIOS.
    diskEntry.setDiskId(diskId).setType(diskDic[slotId].getType())
    diskEntry.setManufacturer(diskDic[slotId].getManufacturer())

    # Write the information in the DB.
    addedNewEntry = diskEntry.write(dbConObj)

    # Add an entry in the NGAS Disks History Table.
    if (addedNewEntry):
        dbConObj.addDiskHistEntry(diskId, "Disk Registered", "text/xml",
                                  prepNgasDiskInfoFile(diskEntry, 1, 1))

    info(1,"Added disk with ID: " + diskId + " in the NGAS DB")


def updateDiskInDb(dbConObj,
                   ngamsCfgObj,
                   slotId,
                   diskDic,
                   diskInfoObj):
    """
    Update information for a disk in the NGAS DB.

    dbConObj:       DB connection object (ngamsDb).
    
    ngamsCfgObj:    NG/AMS Configuration Object (ngamsConfig).
    
    slotId:         Slot ID (string).
    
    diskDic:        Dictionary containing ngamsPhysDiskInfo objects
                    with the information about the disk configuration
                    (dictionary).

    diskInfoObj:    DB Disk Info Object containing the information for
                    the disk (ngamsDiskInfo).

    Returns:        Void.
    """
    T = TRACE()

    diskId = diskDic[slotId].getDiskId()
    info(4,"Updating information for disk with ID: " + diskId +\
         " in the NGAS DB ...")
    archive            = ngamsCfgObj.getArchiveName()
    mountPoint         = diskDic[slotId].getMountPoint()
    mounted            = 1
    bytesStored        = dbConObj.getSumBytesStored(diskId)
    numberOfFiles      = dbConObj.getNumberOfFiles(diskId, ignore=0)
    availableMb        = getDiskSpaceAvail(mountPoint)
    diskInfoObj.\
                  setDiskId(diskId).\
                  setArchive(archive).setHostId(getHostId()).\
                  setSlotId(slotId).setMounted(mounted).\
                  setMountPoint(mountPoint).setLastHostId(getHostId()).\
                  setBytesStored(bytesStored).setNumberOfFiles(numberOfFiles).\
                  setAvailableMb(availableMb)
    addedNewEntry = diskInfoObj.write(dbConObj)
    
    # Add an entry in the NGAS Disks History Table.
    if (addedNewEntry):
        dbConObj.addDiskHistEntry(diskId, "Disk Registered", "text/xml",
                                  prepNgasDiskInfoFile(diskInfoObj, 1, 1))
    
    info(4,"Updated information for disk with ID: " + diskId +\
         " in the NGAS DB")
    

def markDisksAsUnmountedInDb(dbConObj,
                             ngamsCfgObj):
    """
    Mark all disks which are marked as mounted in the NGAS DB as unmounted.

    dbConObj:       DB connection object (ngamsDb).

    ngamsCfgObj:    NG/AMS Configuration Object (ngamsConfig).
  
    Returns:        Void.
    """
    diskList = getDiskInfoForMountedDisks(dbConObj, getHostId(),
                                          ngamsCfgObj.getRootDirectory())
    for disk in diskList:
        markDiskAsUmountedInDb(dbConObj, disk.getDiskId())


def markDiskAsUmountedInDb(dbConObj,
                           diskId):
    """
    Mark a disk as unmounted in the NGAS DB.

    dbConObj:    DB connection object (ngamsDb).
    
    diskId:      Disk ID (string).

    Returns:     Void.
    """
    info(1,"Marking disk with ID: " + diskId +\
         " as unmounted in the NGAS DB ...")
    diskInfoObj = ngamsDiskInfo.ngamsDiskInfo()
    diskInfoObj.read(dbConObj, diskId).\
                               setHostId("").setSlotId("").setMounted(0).\
                               setMountPoint("").setLastHostId(getHostId()).\
                               write(dbConObj)
    info(1,"Marked disk with ID: " + diskId+" as unmounted in the NGAS DB ...")


def getAssociatedDiskId(diskId,
                        ngamsCfgObj,
                        diskDic):
    """
    Get Disk ID for the disk _currently_ associated with the disk with the
    given ID (if any).

    diskId:         Disk ID (string).
    
    ngamsCfgObj:    Instance of NG/AMS Configuration Class (ngamsConfig).
    
    diskDic:        Dictionary containing ngamsPhysDiskInfo objects
                    with the information about the disk configuration
                    (dictionary).

    Returns:        ID of disk currently associated to the disk with the
                    given ID or '' (string).
    """
    # Find the Slot ID for the given Disk ID.
    slotId = ""
    for id in diskDic.keys():
        if (diskDic[id].getDiskId() == diskId):
            slotId = id
            break

    # Find and return the Disk ID of the associated disk.
    assocSlotId = ngamsCfgObj.getAssocSlotId(slotId)
    for id in diskDic.keys():
        if (id == assocSlotId): return diskDic[id].getDiskId()
        
    return ""


def updateDiskStatusDb(dbConObj,
                       piStat):
    """
    Updates the information in connection with a disk when it comes
    to the number of files stored, the available space, bytes stored and the
    total I/O time.

    Checksum not yet supported.

    dbConObj:       DB connection object (ngamsDb).
    
    piStat:         Status as returned by the DAPIs (ngamsDapiStatus).

    Returns:        Disk Info Object (ngamsDiskInfo).
    """
    info(4,"Updating disk status for disk with ID: "+piStat.getDiskId()+" ...")

    global _ngamsDisksSem
    _ngamsDisksSem.acquire()

    diskInfo = ngamsDiskInfo.ngamsDiskInfo()
    diskInfo.read(dbConObj, piStat.getDiskId())
    # Increment only the number of files (stored on the disk), if a
    # new file (not already existing in this environment) was stored.
    if (not piStat.getFileExists()):
        diskInfo.setNumberOfFiles(diskInfo.getNumberOfFiles() + 1)
    diskInfo.setAvailableMb(getDiskSpaceAvail(diskInfo.getMountPoint()))
    diskInfo.setBytesStored(diskInfo.getBytesStored() + piStat.getFileSize())
    diskInfo.setTotalDiskWriteTime(diskInfo.getTotalDiskWriteTime() +\
                                   piStat.getIoTime())
    diskInfo.write(dbConObj)

    _ngamsDisksSem.release()
    info(4,"Updated disk status for disk with ID: " + piStat.getDiskId())

    return diskInfo


def getDiskInfoForMountedDisks(dbConObj,
                               host,
                               mtRootDir):
    """
    Retrieve a list of disk entries from the ngas_disks table
    which appear to be mounted for this system.

    dbConObj:     DB connection object (ngamsDb).
    
    host:         Host name (string).

    mtRootDir:    Base mount directory for NG/AMS (string).
    
    Returns:      List containing the ngamsDiskInfo elements for the
                  disks registered as mounted in this system (list).
    """
    T = TRACE()

    host = string.split(host, ".")[0]
    diskIds = dbConObj.getDiskIdsMountedDisks(host, mtRootDir)
    diskList = []
    for diskId in diskIds:
        diskInfo = ngamsDiskInfo.ngamsDiskInfo()
        diskInfo.read(dbConObj, diskId)
        diskList.append(diskInfo)
    return diskList


def getDiskInfoObjsFromMimeType(dbConObj,
                                ngamsCfgObj,
                                mimeType,
                                sendNotification = 1):
    """
    Get the Disk Info for the disks allocated related to a certain
    mime-type/stream.

    dbConObj:           DB connection object (ngamsDb).
    
    ngamsCfgObj:        NG/AMS Configuration Object (ngamsConfig).
    
    mimeType:           Mime-type (string).

    sendNotification:   1 = send email notification message in case
                        no disks are found (integer).

    Returns:            List of Disk Info Objects (list/ngamsDiskInfo).
    """
    T = TRACE()

    stream = ngamsCfgObj.getStreamFromMimeType(mimeType)
    if (stream == None):
        errMsg = genLog("NGAMS_AL_NO_STO_SETS", [mimeType])
        raise Exception, errMsg
    slotIds = []
    for id in stream.getStorageSetIdList():
        set = ngamsCfgObj.getStorageSetFromId(id)
        slotIds.append(set.getMainDiskSlotId())
        if (set.getRepDiskSlotId() != ""):
            slotIds.append(set.getRepDiskSlotId())
    diskInfo = dbConObj.getDiskInfoForSlotsAndHost(getHostId(), slotIds)
    if (diskInfo == []):
        errMsg = genLog("NGAMS_AL_NO_STO_SETS", [mimeType])
        alert(errMsg)
        if (sendNotification):
            ngamsNotification.notify(ngamsCfgObj, NGAMS_NOTIF_NO_DISKS,
                                     "NO STORAGE SET (DISKS) AVAILABLE",errMsg)
        raise Exception, errMsg  

    # Unpack the disk information into ngamsDiskInfo objects.
    diskInfoObjs = []
    for diRaw in diskInfo:
        tmpDiskInfo = ngamsDiskInfo.ngamsDiskInfo().unpackSqlResult(diRaw)
        diskInfoObjs.append(tmpDiskInfo)

    return diskInfoObjs


def dumpDiskInfoAllDisks(dbConObj,
                         ngamsCfgObj):
    """
    Dump disk status into the NgasDiskInfo files on each disk.

    dbConObj:       DB connection object (ngamsDb).
    
    ngamsCfgObj:    NG/AMS Configuration Object (ngamsConfig).

    Returns:        Void.
    """
    diskList = getDiskInfoForMountedDisks(dbConObj, getHostId(),
                                          ngamsCfgObj.getRootDirectory())
    for disk in diskList:
        dumpDiskInfo(dbConObj, ngamsCfgObj, disk.getDiskId(),
                     disk.getMountPoint())


def dumpDiskInfo(dbConObj,
                 ngamsCfgObj,
                 diskId,
                 mountPoint):
    """
    Dump the disk status information for one disk.

    dbConObj:      DB connection object (ngamsDb).

    ngamsCfgObj:   NG/AMS Configuration Object (ngamsConfig).

    diskId:        Disk ID (string).

    mountPoint:    Mount point (string).
    
    Returns:       NgasDiskInfo XML document (string/xml).
    """
    # Get disk info from DB
    diskInfo = ngamsDiskInfo.ngamsDiskInfo()
    try:
        diskInfo.read(dbConObj, diskId)
    except:
        errMsg = genLog("NGAMS_ER_DISK_STATUS", [diskId])
        error(errMsg)
        ngamsNotification.notify(ngamsCfgObj, NGAMS_NOTIF_ERROR,
                                 "MISSING DISK IN DB", errMsg)
        return
    
    # Create status object and save the XML in the status file
    ngasDiskInfo = prepNgasDiskInfoFile(diskInfo)
    filename = os.path.normpath(mountPoint + "/" + NGAMS_DISK_INFO)

    # Check if it is possible to write in the file if it exists.
    if (os.path.exists(filename) and (not ngamsLib.fileWritable(filename))):
        return
    
    info(4,"Writing NGAS Disk Status for disk with ID: " + diskId +\
         " - into file: " + filename)
    info(5,"Contents of NgasDiskInfo: %s is: |%s|" %\
         (filename, str(ngasDiskInfo).replace("\n", "   ")))
    fd = open(filename, "w")
    fd.write(ngasDiskInfo)
    fd.close()
    return ngasDiskInfo


# Cache for the findTargetDisk() to make it work more efficiently.
_diskInfoObjsDic   = {}
_bestTargetDiskDic = {}
_diskInfoDic       = {}

def findTargetDiskResetCache():
    """
    Reset the cache used by the findTargetDisk() function.

    Returns:   Void.
    """
    global _diskInfoObjsDic, _bestTargetDiskDic, _diskInfoDic
    _diskInfoObjsDic   = {}
    _bestTargetDiskDic = {}
    _diskInfoDic       = {}


def findTargetDisk(dbConObj,
                   ngamsCfgObj,
                   mimeType,
                   sendNotification = 1,
                   diskExemptList = [],
                   caching = 0,
                   reqSpace = None):
    """
    Find a target disk for a file being received.

    dbConObj:          DB connection object (ngamsDb).
    
    ngamsCfgObj:       Instance of NG/AMS Configuration Class (ngamsConfig).
    
    mimeType:          Mime-type of file (string).

    sendNotification:  1 = send Email Notification Message (integer).

    diskExemptList:    List with Disk IDs of disks, which it is not 
                       desirable to consider (list/string).

    caching:           Used to increase the speed of this function
                       if it is invoked many times sequentially. SHOULD
                       BE USED WITH GREAT CARE! (integer/0|1).

    reqSpace:          The required space needed in bytes (integer).

    Returns:           ngamsDiskInfo object containing the necessary
                       information (ngamsDiskInfo).
    """
    T = TRACE()

    startTime = time.time()

    # Get Disk IDs matching the mime-type.
    info(3, "Finding target disks - mime-type is: " + mimeType)
    global _diskInfoObjsDic
    if ((not caching) or (not _diskInfoObjsDic.has_key(mimeType))):
        diskInfoObjs = getDiskInfoObjsFromMimeType(dbConObj, ngamsCfgObj,
                                                   mimeType, sendNotification)
        if (caching): _diskInfoObjsDic[mimeType] = diskInfoObjs
    else:
        diskInfoObjs = _diskInfoObjsDic[mimeType]

    # Analyze which of the dynamic Disks Sets are available to be used
    # as Target Disk Set, i.e., only Disk Sets where both Main Disk and
    # Replication Disk (if available) are not completed.
    diskIdDic = {}
    slotIdDic = {}
    for diskInfoObj in diskInfoObjs:
        diskIdDic[diskInfoObj.getDiskId()] = diskInfoObj
        slotIdDic[diskInfoObj.getSlotId()] = diskInfoObj
 
    # If the disk is a Main Disk, and if it is not completed, and if there is
    # an associated Replication Disk and it is also not completed, we accept
    # the disk as a possible candidate disk.
    diskIds = []
    for diskInfoObj in diskInfoObjs:
        # We disregard disks listed in the exemptlist.
        if (ngamsLib.elInList(diskExemptList, diskInfoObj.getDiskId())):
            continue

        # Only a Main Disk (not completed) can be considered.
        if (isMainDisk(diskInfoObj.getSlotId(), ngamsCfgObj) and
            (not diskInfoObj.getCompleted())):

            # Check if the Main Disk has enough space on it to store the file.
            # Even though a disk is not completed, it may be that there is not
            # enough space to store a given file.
            if (reqSpace):
                if (diskInfoObj.getAvailableMb() < (reqSpace / 1048576.)):
                    continue

            # If replication is on, check the associated Replication Disk
            # (if any).
            repSlotId = ngamsCfgObj.getAssocSlotId(diskInfoObj.getSlotId())
            if (ngamsCfgObj.getReplication() and (repSlotId != "") and
                (slotIdDic.has_key(repSlotId))):
                repDiskObj = slotIdDic[repSlotId]
                if (not repDiskObj.getCompleted()):
                    # Check if the Replication Disk has enough space.
                    if (reqSpace):
                        if (repDiskObj.getAvailableMb() <
                            (reqSpace / 1048576.)):
                            continue                    
                    diskIds.append(diskInfoObj.getDiskId())
            else:
                diskIds.append(diskInfoObj.getDiskId())

    # If no storage sets found, generate a log message.
    if (diskIds == []):
        errMsg = genLog("NGAMS_AL_NO_STO_SETS", [mimeType])
        alert(errMsg)
        if (sendNotification):
            ngamsNotification.notify(ngamsCfgObj, NGAMS_NOTIF_NO_DISKS,
                                     "NO DISKS AVAILABLE", errMsg)
        raise Exception, errMsg

    # Find the best target disk.
    global _bestTargetDiskDic
    key = str(diskIds)[1:-1].replace("'", "_").replace(", ", "")
    if ((not caching) or (not _bestTargetDiskDic.has_key(key))):
        diskId = dbConObj.getBestTargetDisk(diskIds,
                                            ngamsCfgObj.getRootDirectory())
        if (caching): _bestTargetDiskDic[key] = diskId
    else:
        diskId = _bestTargetDiskDic[key]

    if (diskId == None):
        errMsg = genLog("NGAMS_AL_NO_STO_SETS", [mimeType])
        alert(errMsg)
        if (sendNotification):
            ngamsNotification.notify(ngamsCfgObj, NGAMS_NOTIF_NO_DISKS,
                                     "NO DISKS AVAILABLE", errMsg)
        raise Exception, errMsg
    else:
        global _diskInfoDic
        key = diskId + "_" + mimeType
        if ((not caching) or (not _diskInfoDic.has_key(key))):
            diskInfo = ngamsDiskInfo.ngamsDiskInfo()
            diskInfo.getInfo(dbConObj, ngamsCfgObj, diskId, mimeType)
            if (caching): _diskInfoDic[key] = diskInfo
        else:
            diskInfo = _diskInfoDic[key]

        return diskInfo


if __name__ == '__main__':
    """
    Main function.
    """
    print "Available (MB)="+ str(getDiskSpaceAvail("/export/diskb"))
    sys.exit(0)
    
    import ngamsDb, ngamsConfig

    setLogCond(0, 0, "", 5)
    cfg = ngamsConfig.ngamsConfig().\
          load("/home/jknudstr/ngams/ngamsTest/NgamsCfg.xml")
    db = ngamsDb.ngamsDb("TESTSRV", "ngas_dev", "ngas_dbo", "ngas_dbo_pw")
    checkDisks(db, cfg)
    

# EOF
