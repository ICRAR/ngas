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
#********************************************************************
# Who                  When        What
# chen.wu@icrar.org    2014-11-11  Created

"""
Client API to move an observation from NGAS host A to NGAS host B
The server side (in a separate script) is deployed to each NGAS server
To run this script, please ensure all NGAS servers in the cluster are up running
"""

from psycopg2.pool import ThreadedConnectionPool
import sys, urllib2

g_db_pool = ThreadedConnectionPool(1, 4, database = None, user = None,
                            password = ''.decode('base64'),
                            host = None)
MOVE_SUCCESS = 'MOVEALLOK'
UNKNOWN_ERROR = 9999
DEBUG = 0 # If set to 1, no changes (data movement or database updates) only print out information

def _moveFile(srcHost,
              filePath,
              fileId,
              diskId,
              fileVersion,
              checkSum,
              tgtHost):
    """
    fileVersion:    (string)
    Returns:    a two-element tuple: (code, info)
    """
    u = "http://%s/MWA_MIT_MOVE?file_path=%s&file_id=%s&file_version=%d&disk_id=%s" % (srcHost, filePath, fileId, fileVersion, diskId) +\
        "&target_host=%s&crc_db=%s&debug=%d" % (tgtHost, checkSum, DEBUG)
    if (DEBUG):
        print u
    try:
        resp = urllib2.urlopen(u)
        msg = 'Moved %s from %s to %s' % (fileId, srcHost.split(':')[0], tgtHost)
        print msg
        return (0, str(resp.readlines()))
    except Exception, exp:
        if (type(exp) is urllib2.URLError):
            errMsg = 'NGAS server %s is not up running' % srcHost
            print errMsg
            return (-1, errMsg)
        else:
            if (type(exp) is urllib2.HTTPError):
                msg = str(exp.readlines())
                errCode = exp.code
            else:
                msg = str(exp)
                errCode = UNKNOWN_ERROR

            errMsg = '%s' % (msg)
            print errMsg
            return (errCode, errMsg)


def moveObs(obsId, tgtHost):
    """
    tgtHost:    string, e.g. eor-01
    """
    if (len(obsId) != 10):
        print 'Invalid MWA obsId %s' % obsId
        return 1

    # exclude files already on the target host
    query = "SELECT b.host_id, b.mount_point || '/' || a.file_name, a.file_id, a.disk_id, a.file_version, a.checksum" +\
            " FROM ngas_files a, ngas_disks b WHERE a.disk_id = b.disk_id AND substring(a.file_id, 1,10) = '%s'" % obsId +\
            " AND b.host_id <> '%s:7777'" % tgtHost +\
            " ORDER BY file_id"

    lfs = None
    conn = g_db_pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute(query)
        lfs = cur.fetchall()
    finally:
        if (cur):
            del cur
        g_db_pool.putconn(conn)

    if (not lfs or (len(lfs) == 0)):
        print 'No files found for obs %s that could be moved' % obsId
        return 2

    lastFileId = None
    lastSucceeded = False
    done_dict = {} # key - fileId, val - result
    print "Moving %d files to %s" % (len(lfs), tgtHost)
    for ff in lfs:
        srcHost = ff[0]
        filePath = ff[1]
        fileId = ff[2]
        diskId = ff[3]
        fileVersion = ff[4]
        checkSum = ff[5]

        if (fileId == lastFileId and lastSucceeded):
            continue

        # do it
        retcode, info = _moveFile(srcHost, filePath,
                                  fileId, diskId, fileVersion,
                                  checkSum, tgtHost)
        lastFileId = fileId
        lastSucceeded = (retcode == 0)
        done_dict[fileId] = (retcode, info, srcHost.split(':')[0]) # eor-11:7777 -> eor-11

    # produce summary
    gt = 0
    wt = 0
    bt = 0
    glist = []
    wlist = []
    blist = []
    for fid, ret in done_dict.items():
        msg = '%s: %s: %s --> %s' % (ret[1], fid, ret[2], tgtHost)
        if (0 == ret[0]):
            if (ret[1].find(MOVE_SUCCESS) > -1):
                glist.append(msg)
                gt += 1
            else:
                wlist.append(msg)
                wt += 1
        else:
            blist.append(msg)
            bt += 1

    # report it
    title = "Summary - moving observation %s to %s" % (obsId, tgtHost)
    print "=" * len(title)
    print title
    print "=" * len(title)

    print "%d completely successful:" % gt
    for el in glist:
        print "\t" + el

    print "%d succeeded with warnings:" % wt
    for el in wlist:
        print "\t" + el

    print "%d failed:" % bt
    for el in blist:
        print "\t" + el

    print "-" * len(title)

def usage():
    print 'python MoveMWAObs.py <obsId> <targetHost>'
    print 'e.g. python MoveMWAObs.py 1064769208 eor-13'

if __name__ == '__main__':
    if (len(sys.argv) != 3):
        usage()
        sys.exit(1)
    moveObs(sys.argv[1], sys.argv[2])
