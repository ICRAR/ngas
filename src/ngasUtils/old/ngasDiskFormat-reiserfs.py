

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
# "@(#) $Id: ngasDiskFormat-reiserfs.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# A.Wicenec 20/06/2002  Created.
#

"""
This module contains tools for preparing disks for usage in NGAS
"""
import sys,os,re,time
from commands import getstatusoutput
from popen2 import popen4

from ngams import *
import ngamsEscaladeUtils

_dev   = None      # no default for device
_user  = 'ngas'    # default user
_group = 'ngas'    # default group
_url   = 'http://localhost:1080/technical.html'  # how to access 3dmd
_tmp_mount = '/NGAS/tmp_mount'

def usage():
    """
    This module contains tools for preparing disks for usage in NGAS

    Synopsis: ngamsDiskFormat [-d <dev> -u <user> -g <group>]
    """
    import pydoc
    print pydoc.help('ngamsDiskFormat.usage')
    sys.exit()
    

def getKnownDevices(url):
    """
    Function returns the devices and slot numbers known to an Escalade
    controller in a dictionary.

    Synopsis:  result = getKnownDevices(<url>)

    Input:     url:     string, default http://localhost:1080/technical
    Output:    result:  dictionary of the form {'/dev/sda':'1','/dev/sdb':'2'}
    """
    result = {}
    res = ngamsEscaladeUtils.parseHtmlInfo(url,'/dummy',
                                               ["1", "2", "3", "4", "5", "6",
                                                "7", "8"])
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

    command = '/sbin/sfdisk -l %s' % dev
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


def createPartition(dev):
    """
    Create a single partition on <dev> spanning the whole device.
    """
    command = '/sbin/sfdisk %s > /dev/null 2>&1' % dev
    sfdisk = popen4(command)
    sfdisk[1].write('\n;\n;\n;\n;\n')   # this creates a partition spanning the
                                        # whole disk
    sfdisk[0].close()
    sfdisk[1].close()
    return 0


def createFileSystem(partition):
    """
    Create a reiserfs file system on <partition>.
    """
    command = '/sbin/mkreiserfs %s  > /dev/null 2>&1' % partition
    mkfs = popen4(command)
    mkfs[1].write('y\n')
    mkfs[0].close()
    mkfs[1].close()
    return 0



def tmpMountPartition(partition,tmpDir):
    """
    Mount <partition> to <tmpDir>. If <tmpDir> does not exist it is created.
    """
    stat = 8192
    if (os.path.exists(tmpDir) == 0):
        os.mkdir(tmpDir)
    command = '/bin/mount -t reiserfs %s %s' % (partition,tmpDir)
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
    command = '/bin/umount %s' % tmpDir
    (stat,res) = getstatusoutput(command)
    if stat != 0:
        return 1
    else:
        os.rmdir(tmpDir)
        return 0


def changeOwnerGroup(path):
    """
    Change ownership user and group of <path> to the values given in the
    global variables _user and _group. The user and group names have to be
    available in /etc/passwd and /etc/group, respectively.

    NOTE: The lambda mechanism needs global variables in order to work.
    """

    # get the passwd and the group files
    f = open('/etc/passwd')
    passwd = f.readlines()
    f.close()
    f = open('/etc/group')
    groups = f.readlines()
    f.close()

    # get the entries for input user and group
    p = filter(lambda x:x[0:len(_user)+1]== _user+':',passwd)
    g = filter(lambda x:x[0:len(_group)+1]== _group+':',groups)
    
    if len(p) != 1 or len(g) != 1:
        return 1
    else:
        p = p[0]
        g = g[0]

    # extract the uid and gid
    uid = p.split(':')[2]
    gid = g.split(':')[2]

    os.chown(path,int(uid),int(gid))

    return 0


def countDown(sec):
    """
    Counts seconds <sec> backwards.
    """
    for ii in range(sec,-1,-1):
        print "%5d%s" % (ii,chr(13)),
        sys.stdout.flush()
        if ii > 0: time.sleep(1)
    print "\n"



if __name__ == '__main__':
    print "\n\nWARNING: THIS TOOL MAY DELETE DATA ON DATA VOLUMES!!"
    print "ARE YOU SURE YOU WANT TO CONTINUE (y/n)?"
    answer = sys.stdin.readline()[0:-1].upper()
    if (answer != "Y"): sys.exit(0)
    

    import getopt

    opts,args = getopt.getopt(sys.argv[1:],"d:u:g:h",\
               ["device","user","group","help"])

    for o,v in opts:
        if o in ("-d","--device"):
            _dev = v
        if o in ("-u","--user"):
            _user = v
        if o in ("-g","--group"):
            _group = v
        if o in ("-h","--help"):
            usage()

if not _dev:
    res = getKnownDevices(_url)
    rkeys = res.keys()
    rkeys.sort()
    print "Known devices: "
    for k in rkeys:
        print 'Device name %s: Slot number %s' % (k,res[k])

    print "Enter device name (e.g. /dev/sda):"
    w = sys.stdin.readline()[0:-1]
    while (re.match('[^q]',w)):
        test = filter(lambda x: x==w,res.keys())
        if len(test) > 0:

            # here follows the real work

            _dev = test[0]
            status = checkPartitions(_dev)
            if status == 256:
                print "%s: Permission denied" % _dev
            elif status == 0:
                print "OK. No partitions found on that disk"
                print "Creating partition..."
                stat = createPartition(_dev)
                if stat != 0:
                    print "Return status %d unkown!" % stat
                    print "Bailing out!!"
                    sys.exit()
                (stat,res) = getstatusoutput('sync;sync;sync')
                if stat != 0:
                    print "Return status %d of sync unkown!" % stat
                    print "Bailing out!!"
                    sys.exit()

                countDown(10)   # necessary to have everything finished!!

                print "Creating file system..."
                partition = _dev + '1'
                stat = createFileSystem(partition)
                if stat != 0:
                    print "Return status %d of createFileSystem unkown!" % stat
                    print "Bailing out!!"
                    sys.exit()
                (stat,res) = getstatusoutput('sync;sync;sync')
                if stat != 0:
                    print "Return status %d of sync unkown!" % stat
                    print "Bailing out!!"
                    sys.exit()

                countDown(10)   # necessary to have everything finished!!

                print "Mounting new partition..."
                stat = tmpMountPartition(partition,_tmp_mount)
                if stat != 0:
                    print "Return status of mount %d unkown!" % stat
                    print "Bailing out!!"
                    sys.exit()
                print "Changing group and owner..."
                stat = changeOwnerGroup(_tmp_mount)
                if stat != 0:
                    print "Return status of changeOwnerGroup %d unkown!" % stat
                    print "Bailing out!!"
                    sys.exit()
                print "Changing permissions..."
                os.chmod(_tmp_mount,0775)
                (stat,res) = getstatusoutput('ls -ld '+_tmp_mount)
                print "\nCheck this:"
                print res
                stat = umountPartition(_tmp_mount)
                if stat != 0:
                    print "Return status of umountPartition %d unkown!" % stat
                    print "Bailing out!!"
                    sys.exit()
                sys.exit()
            elif status == 1:
                print "There are partitions on this disk already!!!"
            else:
                print "Return status %d unkown!" % status
            sys.exit()
        else:
            print "Illegal device name entered: ",w
            print "Bailing out!!"
            sys.exit()


# EOF
