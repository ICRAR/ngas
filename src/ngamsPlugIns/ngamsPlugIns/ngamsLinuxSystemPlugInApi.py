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
# "@(#) $Id: ngamsLinuxSystemPlugInApi.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# awicenec  15/06/2001  Created.
# jknudstr  10/05/2001  Introduced in ngams project.
#
"""
This module contains tools for the System Check/Online/Offline Plug-Ins
for Linux.
"""
# Constants.
#MTAB_DEFAULT    = "/etc/mtab"
#FSTAB_DEFAULT   = "/etc/fstab"
#FSTAB_ENTRY_TPL = ['<dev>', '<mntPt>', '<fstype>', 'noauto,user', '0', '0']
##_MODULE = '3w-xxxx'
##_MNT_PREFIX = '/NGAS'
##_URI = "http://localhost:1080/technical.html"
# def getMntDict(mtabFile = MTAB_DEFAULT,
#                short = 1):
#     """
#     Return a dictionary containing all the entries in a mtab file (default
#     is /etc/mtab). The keys in the dictionary are the mount points. The rest
#     of the mtab records are returned in a list, where the first element is
#     the device name. If the short parameter is set to 1 (default) only the
#     device name is returned as key values.
#     Synopsis:   getMntDict([mtabFile=<mtabFile>][,short = 0|1])
#     mtabFile:   Mount tab file (string).
#     short:      Short output (int)
#     Returns:    Dictionary with entries in mtab file (dictionary).
#     """
#     try:
#         f = open(mtabFile,'r')
#         mtab = f.readlines()
#         f.close()
#     except Exception, e:
#         error(str(e))
#         errMsg = "Problems opening mtab file (" + str(e) + ") "
#         raise Exception, errMsg
#     mntDict = {}
#     for l in mtab:
#         record = l[:-1].split()
#         if short == 0:
#             rest = []
#             rest.append(record[0])
#             rest.extend(record[2:])
#         else:
#             rest = record[0]
#         mntDict.update({record[1]:rest})
#     return mntDict
# def getMountedDev(mntPt):
#     """
#     Return the device name for a mount point <mntPT> or an empty string.
#     Synopsis:   getMountedDev(mntPt)
#     mntPt:      Mount point (string).
#     Returns:    Mount point or "" if device name not found (string).
#     """
#     mntDict = getMntDict()            # all existing mounts
#     for k in mntDict.keys():
#         if k == mntPt: return mntDict[k]
#     return ""

import glob
import logging
import os
import subprocess

from ngamsLib import ngamsHostInfo
from ngamsLib.ngamsCore import getHostName, execCmd


logger = logging.getLogger(__name__)

def mountToMountpoint(devName,
                      mntPt,
                      readOnly,
                      fstype="reiserfs"):
    """
    Method mounts a device <devName> to a mount point <mntPt>. The optional
    parameter fstype can be used to specify a filesystem type different than
    reiserfs. To mount a device which is in the /etc/fstab use the
    following command:

    mountToMountpoint("", <mntPt>, fstype="")

    Synopsis:   mountToMountpoint(devName, mntPt[, fstype = <fstype>])

    devName:    Device name (string).

    mntPt:      Mount point (string).

    readOnly:   0 if not read-only, 1 if read-only (integer).

    fstype:     File system type (string).

    Returns:    Void.
    """
    command = ["sudo", "mount"]
    if (readOnly):
        command.append('-r')
    command += [devName, mntPt]
    logger.debug("Command to mount disk: %s", subprocess.list2cmdline(command))

#     if (getMountedDev(mntPt)):   # already mounted
#         warnMsg = "Device " + getMountedDev(mntPt) + ' already mounted ' + \
#                   'to specified mountpoint: ' + mntPt
#         info(1,warnMsg)
#     else:
#         if not os.path.exists(mntPt):
#             try:
#                 posix.mkdir(mntPt)
#             except exceptions.OSError,e:
#                 errMsg = "Failed creating mountpoint " + mntPt + ":" + str(e)
#                 error(errMsg)
#                 raise Exception, errMsg
#         stat, out = commands.getstatusoutput(command)
#         if (stat != 0):
#             errMsg = "Failed mounting device " + getMountedDev(mntPt) + \
#                      ':' + out
#             raise Exception, errMsg

    if not os.path.exists(mntPt):
        try:
            os.mkdir(mntPt)
        except OSError as e:
            errMsg = "Failed creating mountpoint " + mntPt + ":" + str(e)
            logger.exception(errMsg)
            raise

    stat, out, _ = execCmd(command)
    if stat != 0 or b'already mounted' in out:
        errMsg = "Failed mounting device. Device/mount point: %s/%s" %\
                 (devName, mntPt)
        raise Exception(errMsg)


def umountMountpoint(mntPt):
    """
    Method unmounts a device mounted on <mntPt>.

    Synopsis:  unmountMountpoint(mntPt)

    mntPoint:  Mount point (string).

    Returns:   Void.
    """
    command = ["sudo", "umount", mntPt]
    stat, out, _ = execCmd(command)
    if stat != 0 or b'not mounted' in out:
        errMsg = "Failed unmounting. Mount point: %s" % mntPt
        raise Exception(errMsg)


# TODO: Needed?
def checkModule(module):
    """
    Check whether module <module> exists and is loaded. Method returns
    a tuple, <mtup>, where mtab[0] reports the existence of a module, and
    mtab[1] reports the load status of a module.

    NOTE: This is Linux specific and won't work on any other system

    Synopsis:   checkModule(<module>)

    module:     Module name (string).

    Returns:    2-element tuple (tuple).
    """
    mex,ml = (-1,-1)     # default return code
    if type(module) != type(""):
        errMsg = "Parameter module passed to " + __name__ + "." + \
                 "checkModule has invalid type (should be string)"
        raise Exception(errMsg)

    # command = "/sbin/modprobe -l | /bin/grep \"/" + module + ".o$\""
    command = "find /lib/modules -name " + module + ".*"
    stat, out, _ = execCmd(command)

    if stat > 0 or not out.strip():
        mex,ml = (0,0)    # there is no module like this
    else:
        mex = 1       # module exists
        stat, out, _ = execCmd("/sbin/lsmod | /bin/grep " + \
                                              "\"^" + module + "\"")
        if out:
            ml = 1   # module is loaded
        else:
            ml = 0  # module not loaded

    mtup = (mex,ml)
    return mtup


# TODO: Needed?
def insMod(module):
    """
    Wrapper around the insmod program.

    NOTE: This is Linux specific and won't work on any other system

    Synopsis:   insMod(<module>)

    module:     Module name (string).

    Returns:    0 upon success or if module is already loaded (int).
    """
    try:
        redHatVersionFile = "/etc/redhat-release"
        fo = open(redHatVersionFile, "r")
        buf = fo.read()
        fo.close()
        ver = buf.split(" ")[4]
        if (ver == "9"): return 0
    except:
        pass

    stat = checkModule(module)
    istat = 0
    if stat == (1,0):
        # From V2.3 the 3ware module is always installed.
        #(istat,out) = commands.getstatusoutput("/sbin/insmod " + module)
        #(istat,out) = commands.getstatusoutput("/sbin/modprobe " + module)
        pass
    elif stat == (0,0):
        errMsg = "Module " + module + " does not exists"
        raise Exception(errMsg)
    elif stat == (1,1):
        logger.info("Module %s already loaded", module)
        return 0
    if istat > 0:
        errMsg = "Problem while inserting module " + module + ":" + str(stat)
        raise Exception(errMsg)

    return 0


# TODO: Needed?
def rmMod(module):
    """
    Wrapper around the rmmod program.

    NOTE: This is Linux specific and won't work on any other system.

    Synopsis:   rmMod(<module>)

    module:     Module name (string).

    Returns:    0 upon success or if module is not loaded (int).
    """
    stat = checkModule(module)
    if (stat[1]):
        execCmd("/sbin/rmmod " + module)
    else:
        return 0

    return 0


# def checkFstab(mntPt):
#     """
#     Check FSTAB configuration. Return tuple with FS tab entries.

#     mntPt:    Mount point (string).

#     Returns:  Tuple with fstab entries (tuple).
#     """
#     try:
#         fil = open(FSTAB_DEFAULT,'r')
#         fstab = fil.readlines()
#         fil.close()
#     except Exception,e:
#         errMsg = "Problem opening " + FSTAB_DEFAULT + ":" + str(e)
#         error(errMsg)
#         raise Exception, errMsg

#     for l in fstab:
#         arr = l.split()
#         if len(arr) > 1 and arr[1] == mntPt:
#             return arr   #match! return entry

#     return []       # no match! return empty list


# def createFstabEntry(fsList):
#     """
#     Generates an entry at the end of the fstab file which is a
#     tab-join of a list <fsList> like the one returned by checkFstab.

#     CAUTION: no check about the existence of another entry for the
#              same mountpoint or the same device.

#     fsList:   List with fstab entries (tuple).

#     Returns:  Void.
#     """
#     from string import join

#     try:
#         fil = open(FSTAB_DEFAULT,'a')
#     except Exception,e:
#         errMsg = "Problem opening " + FSTAB_DEFAULT + ":" + str(e)
#         error(errMsg)
#         raise Exception, errMsg

#     fil.write(join(fsList,'\t') + '\n')
#     fil.close()


# def removeFstabEntries(diskDic):
#     """
#     Remove all fstab entries.

#     diskDic:   Disk info dictionary (dictionary).

#     Returns:   Void.
#     """
#     for key in diskDic.keys():
#         mtPt = diskDic[key].getMountPoint()
#         removeFstabEntry(mtPt)


# def removeFstabEntry(mntPt):
#     """
#     Removes an entry for the mountpoint <mntPt> from the fstab file.

#     CAUTION: It rewrites the fstab file on the spot (no temporary file)

#     Synopsis:    nl = removeFstabEntry(<mntPt>)

#     mntPt:       Mount point (string).

#     Returns:     Number of lines removed (int).
#     """
#     nl = 0
#     try:
#         fil = open(FSTAB_DEFAULT,'r+')
#         fstab = fil.readlines()
#         fil.seek(0,0)
#     except Exception,e:
#         errMsg = "Problem opening " + FSTAB_DEFAULT + ":" + str(e)
#         error(errMsg)
#         raise Exception, errMsg

#     for l in fstab:
#         arr = l.split()
#         if len(arr) > 1 and arr[1] == mntPt:
#             info(3,"Removing fstab entry for mount point: " + mntPt)
#             nl = nl + 1
#             pass                # don't write this line
#         else:
#             fil.write(l)        # rewrite this line

#     fil.truncate()              # close the file here, the rest is junk
#     fil.close()
#     return nl


def ngamsMount(srvObj,
               diskDic,
               slotIds,
               complReadOnly = 0):
    """
    Function mounts all disks registered in the diskDic dictionary.

    NOTE: This is NGAMS specific and relies on the structure of the
          NGAMS Disk Dictionary.

    Synopsis:      ngamsMount(<diskDic>)

    srvObj:        Instance of the NG/AMS Server class (ngamsServer).

    diskDic:       Disk info dictionary (dictionary).

    slotIds:       List of Slot IDs for the disk to consider (list/string).

    complReadOnly: Mount completed volumes read-only (integer/0|1).

    Returns:       Void.
    """
    # Get information about this host.
    hostInfo = srvObj.getDb().getHostInfoFromHostIds([getHostName()])
    if (len(hostInfo) != 1):
        errMsg = "Problem querying information about host: " + getHostName() +\
                 " from the NGAS DB."
        raise Exception(errMsg)
    else:
        hostInfoObj = ngamsHostInfo.ngamsHostInfo().\
                      unpackFromSqlQuery(hostInfo[0])
    slotIds.sort()
    for slotId in slotIds:
        if (not diskDic.has_key(slotId)): continue
        #####entry = [diskDic[slotId].getDeviceName(),
        #####         diskDic[slotId].getMountPoint(),
        #####         "auto", "noauto,user", "0", "0"]
        #####createFstabEntry(entry)
        logger.debug("Mounting: %s", diskDic[slotId].getMountPoint())
        if (srvObj.getCfg().getAllowRemoveReq() or
            srvObj.getCfg().getAllowArchiveReq()):
            readOnly = 0
        else:
            readOnly = 1
        if (complReadOnly):
            if (srvObj.getDb().getDiskCompleted(diskDic[slotId].getDiskId())):
                readOnly = 1
        try:
            mountToMountpoint(diskDic[slotId].getDeviceName(),
                              diskDic[slotId].getMountPoint(),
                              readOnly, fstype = "")
        except Exception:
            del diskDic[slotId]
            logger.exception("Error while mounting")
            continue



def ngamsUmount(diskDic,
                slotIds):
    """
    Function unmounts all disks registered in the diskDic dictionary.

    NOTE: This is NGAMS specific and relies on the structure of the
          NGAMS Disk Dictionary.

    Synopsis:   ngamsMount(<diskDic>)

    diskDic:    NGAMS specific disk info (dictionary).

    slotIds:    List of Slot IDs for the disk to consider (list/string).

    Returns:    Void.
    """
    slotIds.sort()
    for slotId in slotIds:
        if (not diskDic.has_key(slotId)):
            continue
        else:
            logger.info("Unmounting disk with mount point: %s",
                 diskDic[slotId].getMountPoint())
            umountMountpoint(diskDic[slotId].getMountPoint())
            #####removeFstabEntry(diskDic[slotId].getMountPoint())


def umount(mtRootPt):
    """
    Function that tries to unmount all mount points of the type:

       /NGAS/data* and /NGAS/volume*

    Possible errors are ignored.

    mtRootPt:    NGAS mount root point (string).

    Returns:     Void.
    """
    if (mtRootPt.strip() == ""):
        raise Exception("Error in ngamsLinuxSystemPlugInApi.umount(): " +\
              "Mount Root Point cannot be \"\"")
    mtPtList = glob.glob("%s/data*" % mtRootPt)
    mtPtList += glob.glob("%s/volume*" % mtRootPt)
    for mtPt in mtPtList:
        try:
            umountMountpoint(mtPt)
            #####removeFstabEntry(mtPt)
        except:
            pass

# EOF
