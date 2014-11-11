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

#********************************************************************
# Who                  When        What
# chen.wu@icrar.org    2014-11-11  Created
#

"""
Client API to move an observation from NGAS host A to NGAS host B
"""

from psycopg2.pool import ThreadedConnectionPool

g_db_pool = ThreadedConnectionPool(1, 4, database = 'ngas', user = 'ngas_ro', 
                            password = 'bmdhcyRybw==\n'.decode('base64'), 
                            host = 'ngas.mit.edu')

def _moveFile(srcHost,  
              filePath, 
              fileId, 
              diskId, 
              fileVersion,
              checkSum,
              tgtHost):
    """
    fileVersion:    (int)
    Returns:    a two element tuple: (code, info)
    """
    pass

def moveObs(obsId, tgtHost):
    """
    tgtHost:    string, e.g. eor-01
    """
    if (len(obsId) != 10):
        print 'Invalid obsId %s' % obsId
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
        print 'No files found for obs %s' % obsId
        return 2
    
    lastFileId = None
    lastSucceeded = False
    done_dict = {} # key - fileId, val - result
    
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
                                  checkSum)
        lastFileId = fileId
        lastSucceeded = (retcode == 0)
        done_dict[fileId] = (retcode, info, srcHost)
    
    # track summary
    gt = 0
    wt = 0
    bt = 0
    glist = []
    wlist = []
    blist = []
    for fid, ret in done_dict.items():
        msg = '%s: %s: %s --> %s' % (ret[1], fid, ret[2], tgtHost)
        if (0 == ret[0]):
            if ('OK' == ret[1]):
                glist.append(msg)
                gt += 1
            else:
                wlist.append(msg)
                wt += 1
        else:
            blist.append(msg)
            bt += 1
    
    # report summary
    title = "Summary - moving observation %s to %s" % (obsId, tgtHost)
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
    print 'python MoveMWAObs.py <obsId>'
    print 'e.g. python MoveMWAObs.py xxxx'

if __name__ == '__main__':
    pass