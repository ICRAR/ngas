

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
# "@(#) $Id: ngasDiskFormat.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# A.Wicenec 20/06/2002  Created.
# jknudstr  03/03/2004  Changed to ext3fs + using 3ware command line util.
#

"""
This module contains tools for preparing disks for usage in NGAS
"""

import sys, os, re, time, commands
from commands import getstatusoutput
from popen2 import popen4

from ngams import *
import ngamsEscaladeUtils

from ngasUtils import *
import ngasUtilsLib

setLogCond(sysLog=0, sysLogPrefix="", locLogLevel=0, locLogFile="",
           verboseLevel=0)


if (0):
    _uri = "http://localhost:1080/technical.html"
else:
    _uri = "/usr/local/sbin/tw_cli"
_tmpMount = "/NGAS/tmp_mount"


def usage():
    """
    This module contains tools for preparing disks for usage in NGAS

    Returns:   Void.
    """
    pass


def getKnownDevices(url,
                    devStartIdx = "a"):
    """
    Function returns the devices and slot numbers known to an Escalade
    controller in a dictionary.

    Synopsis:  result = getKnownDevices(<url>)

    Input:     url:     string, default http://localhost:1080/technical
    Output:    result:  dictionary of the form {'/dev/sda':'1','/dev/sdb':'2'}
    """
    commands.getstatusoutput("sudo /usr/local/sbin/tw_cli maint rescan")
    result = {}
    if (_uri.find("http") != -1):
        res = ngamsEscaladeUtils.parseHtmlInfo(url, "/dummy",
                                               ["1", "2", "3", "4", "5", "6",
                                                "7", "8"])
    else:
        res = ngamsEscaladeUtils.parseCmdLineInfo("/dummy",
                                                  controllers = None,
                                                  oldFormat = 0,
                                                  slotIds = ["*"],
                                                  devStartIdx = devStartIdx)
    rkeys = res.keys()
    rkeys.sort()
    for k in rkeys:
        result.update({res[k]._ngamsPhysDiskInfo__deviceName[:-1]:k})
    return result


def checkPartitions(dev):
    """
    Function checks the device for existing partitions

    Synopsis: result = checkPartitions(<dev>)

    Input:    dev:      string, device name like '/dev/sda'
    Output:   result:   boolean, ==1 if there are partitions on that device

    NOTE: There are two possiblities of non existing partitions. The first one
          is a completely new disk which never had a partition created on it.
          The second one is a disk which had partitions created, but deleted
          afterwards.
    """
    command = "/sbin/sfdisk -l %s" % dev
    (status,result) = getstatusoutput(command)
    result = result.split('\n')
    
    if status == 0:
        if result[5] == 'No partitions found':
            return 0
        else:
            n = 0
            for l in result[5:]:
                n = n + 1
                l = l.split()
                if l[0] == ('%s%d' % (dev,n)) and \
                   l[1:] == ['0', '-', '0', '0', '0', 'Empty']:
                    return 0
                else:
                    return 1
    else:
        print result
        return status


def pollLogFile(logFile,
                triggerStr,
                timeOut):
    """
    Poll a log file for a specific log entry. When this has been entered,
    the function returns 1.

    If the specified log entry is not logged within the given timeout,
    0 is returned.

    logFile:     Log file to poll (string).
    
    triggerStr:  String (event) to poll for (string).
    
    timeOut:     Timeout to wait for the event to occur (integer).
    
    Returns:     1 if the event was not logged, otherwise 0 (integr/0|1).
    """
    stat = 1
    startTime = time.time()
    while ((time.time() - startTime) < timeOut):
        if (os.path.exists(logFile)):
            break
        else:
            time.sleep(1)
    fo = open(logFile)
    buf = ""
    while ((time.time() - startTime) < timeOut):
        buf += fo.read()
        if (buf.find(triggerStr) != -1):
            stat = 0
            break
        else:
            time.sleep(1)
    if ((time.time() - startTime) > timeOut):
        print "WARNING: Timeout waiting for command termination!!"
    fo.close()
    return stat
            

def createPartition(dev,
                    attempts = 10,
                    timeOut = 1000):
    """
    Create a single partition on <dev> spanning the whole device.
    """
    count = 1
    stat = 1
    while (count < attempts):
        logFile = "/tmp/ngasDiskFormat.log"
        os.system("rm -f %s" % logFile)
        command = "/sbin/sfdisk %s > %s" % (dev, logFile)
        print "Command to create partition: " + command
        startTime = time.time()
        sfdisk = popen4(command)
        # This creates a partition spanning the whole disk
        sfdisk[1].write("\n;\n;\n;\n;\n")
        startTime = time.time()
        sfdisk[0].close()
        sfdisk[1].close()
        stat = pollLogFile(logFile, "Re-reading the partition table ...",
                           timeOut)
        os.system("rm -f %s" % logFile)
        deltaTime = int(time.time() - startTime + 0.5)
        if (stat):
            msg = "Formatting disk failed (timed out) after %ds/attempt: %d"
            print msg % (deltaTime, count)
            timeOut += (timeOut * .1)
        else:
            print "Formatting disk succeeded after %ds/attempt: %d" %\
                  (deltaTime, count)
            break
        count += 1
    return stat


def createFileSystem(partition,
                     fsType = "ext3fs",
                     attempts = 10,
                     timeOut = 1000):
    """
    Create a reiserfs file system on <partition>.
    """
    count = 1
    logFile = "/tmp/ngasDiskFormat.log"
    while (count <= attempts):
        os.system("rm -f %s" % logFile)
        if (fsType == "reiserfs"):
            command = "/sbin/mkreiserfs %s > %s" % (partition, logFile)
        else:
            command = "/sbin/mke2fs -j %s -m 1 > %s" % (partition, logFile)
        print "Command to create file system: " + command
        startTime = time.time()
        mkfs = popen4(command)
        mkfs[1].write("y\n")
        mkfs[0].close()
        mkfs[1].close()
        if (count == 1):
            tmpTimeOut = 10
        else:
            tmpTimeOut = timeOut
        stat = pollLogFile(logFile, "Writing superblocks and filesystem " +\
                           "accounting information: done", tmpTimeOut)
        deltaTime = int(time.time() - startTime + 0.5)
        if (stat):
            msg = "File system creation failed (timed out) after " +\
                  "%ds/attempt: %d"
            print msg % (deltaTime, count)
            timeOut += (timeOut * .1)
        else:
            print "File system creation succeeded after %ds/attempt: %d" %\
                  (deltaTime, count)
            break
        os.system("rm -f %s" % logFile)
        count += 1
    return stat


def tmpMountPartition(partition,
                      tmpDir,
                      fsType = "ext3"):
    """
    Mount <partition> to <tmpDir>. If <tmpDir> does not exist it is created.
    """
    stat = 8192
    if (os.path.exists(tmpDir) == 0): os.mkdir(tmpDir)
    command = '/bin/mount -t %s %s %s' % (fsType, partition,tmpDir)
    print "Mount command: " + command
    (stat,res) = getstatusoutput(command)
    if stat != 0:
        print res
        return stat
    else:
        return 0
    
    
def umountPartition(tmpDir):
    """
    Unmount <tmpDir> and remove the directory is unmount was successful.
    """
    command = "/bin/umount %s" % tmpDir
    (stat,res) = getstatusoutput(command)
    if (stat != 0):
        return 1
    else:
        os.rmdir(tmpDir)
        return 0


def changeOwnerGroupPerm(path,
                         user,
                         group):
    """
    Change ownership user and group of <path> to the values given in the
    global variables user and group. The user and group names have to be
    available in /etc/passwd and /etc/group, respectively.
    """
    stat, out = commands.getstatusoutput("chown %s:%s %s" %\
                                         (user, group, path))
    if (stat): return stat
    stat, out = commands.getstatusoutput("chmod go+rx %s" % path)
    return stat


def countDown(sec):
    """
    Counts seconds <sec> backwards.
    """
    for ii in range(sec,-1,-1):
        print "%5d%s" % (ii,chr(13)),
        sys.stdout.flush()
        if ii > 0: time.sleep(1)
    print "\n"


def usage():
    """
    """
    print "\nCorrect usage is:" +\
          "$ sudo <...>ngasDiskFormat [-all] [-device <Device>] " +\
          "[-user <User>] [-group <Group>] [-force] [-help]\n"
    


if __name__ == '__main__':
    """
    Main function.
    """
    # Check if NG/AMS Server is running.
    port = 7777
    if (ngasUtilsLib.checkSrvRunning(getHostName(),
                                     port) != NGAMS_NOT_RUN_STATE):
        msg = "NG/AMS Server %s:%d is running - please stop before " +\
              "invoking this tool"
        print msg % (getHostName(), port)
        sys.exit(1)

    # Unmount NGAS disks if mounted.
    commands.getstatusoutput("umount /NGAS/data*")

    # Parse command line parameters.
    all     = 0
    dev     = ""
    user    = "ngas"
    group   = "ngas"
    force   = 0
    to      = 1000
    idx     = 1
    while (idx < len(sys.argv)):
        arg = sys.argv[idx].upper()
        if (arg == "-ALL"):
            all = 1
        elif (arg == "-DEVICE"):
            idx += 1
            dev = sys.argv[idx]
        elif (arg == "-USER"):
            idx += 1
            user = sys.argv[idx]
        elif (arg == "-GROUP"):
            idx += 1
            group = sys.argv[idx]
        elif ((arg == "-HELP") or (arg == "-H")):
            usage()
            sys.exit(0)
        elif (arg == "-FORCE"):
            force = 1
        elif (arg == "-TIMEOUT"):
            idx += 1
            to = int(sys.argv[idx])
        else:
            usage()
            sys.exit(1)
        idx += 1
    
    print "\n\nWARNING: THIS TOOL MAY DELETE DATA ON DATA VOLUMES!!"
    print "ARE YOU SURE YOU WANT TO CONTINUE (y/n)?"
    answer = sys.stdin.readline()[0:-1].upper()
    if (answer != "Y"): sys.exit(0)
    
    print "\nThe media will be formatted with ext3fs - is this desirable " +\
          "(y/n) [y]?"
    answer = sys.stdin.readline()[0:-1].upper()
    if (answer == "N"): sys.exit(0)

    if (not dev):
        print "\nEnter the start index for the 3ware disk devices (a|b|c ...)"
        devStartIdx = sys.stdin.readline()[0:-1]
        try:
            testIdx = ord(devStartIdx)
        except:
            testIdx = -1
        if ((testIdx < 97) or (testIdx > 122)):
            raise Exception, "Illegal starting index for the 3ware " +\
                  "disk device!"

        devDic = getKnownDevices(_uri, devStartIdx)
        devKeys = devDic.keys()
        devKeys.sort()
        print "Known Devices:"
        for tmpDev in devKeys:
            print "Device name %s: Slot number %s" % (tmpDev, devDic[tmpDev])

    if (dev):
        devs = [dev]
    elif (not all):
        if (devKeys == []):
            raise Exception, "No appropriate devices found on this system!"
        print "Enter device name (e.g. %s):" % devKeys[0]
        w = sys.stdin.readline()[0:-1]
        devs = w.split(",")
    else:
        devs = []
        for dev in devKeys:
            devs.append(dev)
    for dev in devs:
        print "\nHandling disk corresponding to device: %s" % str(dev)
        status = checkPartitions(dev)
        if (status == 256):
            print "%s: Permission denied" % dev
        elif ((status == 0) or force):
            if (status == 0): print "No partitions found on that disk"
            print "Creating partition on device: %s..." % dev
            stat = createPartition(dev, timeOut=to)
            if (stat != 0):
                print "Return status %s unkown!" % str(stat)
                if (len(devs) == 1):
                    print "Bailing out!!"
                    sys.exit()
                continue
            (stat,res) = getstatusoutput('sync;sync;sync')
            if (stat != 0):
                print "Return status %d of sync unkown!" % stat
                print "Bailing out!!"
                sys.exit()

            print "Creating file system on device: %s ..." % dev
            partition = dev + "1"
            stat = createFileSystem(partition, timeOut=to)
            if (stat != 0):
                print "Return status %d of createFileSystem unkown!" % stat
                print "Bailing out!!"
                sys.exit()
            (stat, res) = getstatusoutput("sync;sync;sync")
            if (stat != 0):
                print "Return status %d of sync unkown!" % stat
                print "Bailing out!!"
                sys.exit()

            print "Mounting new partition ..."
            stat = tmpMountPartition(partition, _tmpMount)
            if (stat != 0):
                print "Return status of mount %d unkown!" % stat
                print "Bailing out!!"
                sys.exit()
            print "Changing group and owner ..."
            stat = changeOwnerGroupPerm(_tmpMount, user, group)
            if (stat != 0):
                print "Return status of changeOwnerGroupPerm %d unknown!"%stat
                print "Bailing out!!"
                sys.exit()
            print "Changing permissions ..."
            os.chmod(_tmpMount, 0775)
            stat = umountPartition(_tmpMount)
            if (stat != 0):
                print "Return status of umountPartition %d unkown!" % stat
                print "Bailing out!!"
                sys.exit()
        elif (status == 1):
            print "There are partitions on this disk already!!!"
        else:
            print "Return status %d unkown!" % status
    sys.exit()


# EOF
