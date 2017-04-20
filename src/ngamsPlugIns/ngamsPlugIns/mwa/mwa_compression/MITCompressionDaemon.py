#    ICRAR - International Centre for Radio Astronomy Research
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
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu      2014-09-01  Created
#

import datetime, logging, os, commands, threading, sys
import urllib2
import psycopg2
import sqlite3 as dbdrv

logger = logging.getLogger(__name__)

MIT_processing_root = '/home/ngas/processing'

pickupStatus_FAIL_ONLY = 1
pickupStatus_NEW_ONLY = 2
pickupStatus_FAIL_NEW = 3

eor_host = ['eor-02:7777', 'eor-03:7777', 'eor-04:7777', 'eor-05:7777', 'eor-06:7777', 'eor-07:7777', 'eor-08:7777',
            'eor-10:7777', 'eor-11:7777', 'eor-12:7777', 'eor-13:7777', 'eor-14:7777']

def execCmd(cmd, failonerror = False, okErr = []):
    re = commands.getstatusoutput(cmd)
    if (re[0] != 0 and not (re[0] in okErr)):
        errMsg = 'Fail to execute command: "%s". Exception: %s' % (cmd, re[1])
        if (failonerror):
            raise Exception(errMsg)

    return re

def getMITDBConn():
    """
    TODO - use connection pool soon!
    """
    logger.info('Connecting to database')
    try:
        l_db_conn = psycopg2.connect(database = 'ngas', user= 'ngas',
                            password = 'bmdhcyRkYmE=\n'.decode('base64'),
                            host = 'ngas.mit.edu')
        return l_db_conn
    except Exception, e:
        errStr = 'Cannot create LTA DB Connection: %s' % str(e)
        raise Exception, errStr

def executeQuery(conn, sqlQuery):
    try:
        cur = conn.cursor()
        cur.execute(sqlQuery)
        return cur.fetchall()
    finally:
        if (cur):
            del cur

def rotateLogFiles(logfile):
    """
    """
    dt = datetime.datetime.now()
    timestr = dt.strftime('%Y-%m-%dT%H-%M-%S')
    if (os.path.exists(logfile)):
        #move it to another file name with timestamp
        rlognm = MIT_processing_root + '/compress' + timestr + '.log'
        execCmd('mv %s %s' % (logfile, rlognm))

def _executeCompression(fileList, sqlite_file, thdname):
    """
    run the compression command remotely
    and update the sqlite local database, produce logs
    """
    #file_id, disk_id, file_version, obs_id, host_id, file_path
    thdname += ' - '
    logger.info(thdname + 'Thread started')

    for fi in fileList:
        host = fi[4]
        u = 'http://%s/MWACOMPRESS?file_path=%s&file_id=%s&file_version=%d&disk_id=%s' % (host, fi[5], fi[0], fi[2], fi[1])
        logger.info(thdname + u)
        query = ''
        try:
            resp = urllib2.urlopen(u)
            #update the status and compression_date for this file
            dt = datetime.datetime.now()
            compress_date = dt.strftime('%Y-%m-%dT%H-%M-%S')
            query = "update mitfile set status = 0, comment = 'OK', compression_date = '%s' where host_id = '%s' and file_path = '%s'" % (compress_date, host, fi[5])
            logger.info(thdname + 'Compression %s on %s is OK, updating database...' % (fi[0], host))
        except Exception, exp:
            comment = ''
            status = 1
            warn_msg = ''
            if (type(exp) is urllib2.HTTPError):
                errors = exp.readlines()
                for ee in errors:
                    comment += ee
                warn_msg = 'Compression %s on %s failed due to: %s, updating database...' % (fi[0], host, comment)
            elif (type(exp) is urllib2.URLError):
                comment = str(exp)
                warn_msg = 'NGAS server is not running: %s!' % host
                status = 2
            else:
                comment = str(exp)
                warn_msg = 'Unexpected error: Compression %s on %s failed due to: %s, updating database...' % (fi[0], host, comment)
                status = 3

            if (len(comment) > 256):
                comment = comment[0:255]
            dt = datetime.datetime.now()
            compress_date = dt.strftime('%Y-%m-%dT%H-%M-%S') # last attempted date for connecting to the server
            query = "update mitfile set status = %d, comment = '%s', compression_date = '%s' where host_id = '%s' and file_path = '%s'" % (status, comment, compress_date, host, fi[5])
            logger.warning(thdname + warn_msg)
        finally:
            logger.info(thdname + query)
        if (query):
            try:
                dbconn = dbdrv.connect(sqlite_file)
            except Exception, eee:
                logger.error("Cannot link to sqlite db %s" % sqlite_file)
                sys.exit(1)
            #logger.debug("Getting the cursor for the UPDATE query")
            cur = dbconn.cursor()
            try:
                #logger.debug("Executing the UPDATE query")
                cur.execute(query)
                #logger.debug("Committing the UPDATE query")
                dbconn.commit()
                #logger.debug("Commit done")
            except Exception, ex:
                logger.error(thdname + "Fail to execute SQL query: %s" % str(ex))
            finally:
                """
                if (cur != None):
                    cur.close()
                """
                #pass
                if (dbconn != None):
                    dbconn.close()

    logger.info(thdname + 'Thread stopped')


def compressObs(fileList, sqlite_file):
    """
    Compress all files of a single observation
    for each host, a separate thread will be launched
    """
    leng = len(fileList)
    if (leng  < 1):
        logger.error('No files in the list')
        return

    obsId = fileList[0][3]
    logger.info("Processing observation started %s with %d files" % (obsId, leng))

    curList = []
    curKey = ''
    thdList = []
    timeout_perfile = 200

    for fi in fileList:
        k = "%s" % (fi[4]) #host
        if (not curKey):
            curKey = k
            curList.append(fi)
            continue
        if (k != curKey): # new host
            #start a new thread
            thdname = 'CompressThrd_' + k
            logger.info("Staring a new thread: %s" % thdname)
            args = (curList, sqlite_file, thdname)
            tr = threading.Thread(None, _executeCompression, thdname, args)
            thdList.append((tr, timeout_perfile * len(curList)))
            tr.setDaemon(0)
            tr.start()

            del curList
            curList = [fi]
            curKey = k
        else:
            curList.append(fi)

    if (curList != None and len(curList) > 0):
        k = curList[0][4]
        thdname = 'CompressThrd_' + k
        logger.info("Staring a new thread: %s" % thdname)
        args = (curList, sqlite_file, thdname)
        tr = threading.Thread(None, _executeCompression, thdname, args)
        thdList.append((tr, timeout_perfile * len(curList)))
        tr.setDaemon(0)
        tr.start()

    logger.debug("Get ready for joining...")
    #join all threads or until timed out
    for thd in thdList:
        logger.info("Joining thread %s" % thd[0].name)
        thd[0].join(float(thd[1]))
        if (thd[0].isAlive()): # this is a time-out
            logger.warning('Host Thread timed out: %s' % thd[0].name)
        else:
            logger.info("Host Thread completed: %s" % thd[0].name)

    # check if all files of that observation have been compressed,
    logger.debug("Checking NGAS DB...")
    query = "select count(file_id) from mitfile where obs_id = '%s' and status <> 0" % obsId
    cc = None
    try:
        dbconn = dbdrv.connect(sqlite_file)
    except Exception, eee:
        logger.error("Cannot link to sqlite db %s" % sqlite_file)
        sys.exit(1)
    cur = dbconn.cursor()
    try:
        cur.execute(query)
        cc = cur.fetchall() # this is ok for now, but not after we have millions of files
    except Exception, exx:
        cc = None
        logger.error("Fail to query sqlite db for counting: %s" % str(exx))
    finally:
        if (cur != None):
            cur.close()
        logger.info(query)

    if ((cc != None) and (cc[0][0] == 0)):
        # if so, update the NGAS database (postgresql)
        dt = datetime.datetime.now()
        compress_date = dt.strftime('%Y-%m-%dT%H-%M-%S')
        ngasconn = getMITDBConn()
        query = "insert into apps_obs_compression values('%s', '%s')" % (obsId, compress_date)
        cur = ngasconn.cursor()
        try:
            cur.execute(query)
            ngasconn.commit()
        except Exception, ex:
            logger.error(thdname + "Fail to insert into NGAS DB SQL query: %s" % str(ex))
        finally:
            if (cur != None):
                cur.close()
            if (ngasconn != None):
                ngasconn.close()

            logger.info(query)

    if (dbconn != None):
        dbconn.close()

    logger.info("Processing observation completed %s" % obsId)

def doIt(db_dir, pickupStatus = pickupStatus_NEW_ONLY):
    """
    loop thru the database, find the ones that have not yet been done
    """
    sqlite_file = '%s/compress.sqlite' % db_dir
    if (not os.path.exists(sqlite_file)):
        logger.error("Cannot locate local sqlite %s" % sqlite_file)
        raise Exception('Cannot find compress.sqlite in %s' % db_dir)


    dbconn = dbdrv.connect(sqlite_file)
    query = "select file_id, disk_id, file_version, obs_id, host_id, file_path from mitfile where status "
    if (pickupStatus == pickupStatus_FAIL_NEW):
        query += " <> 0 "
    elif (pickupStatus == pickupStatus_NEW_ONLY):
        query += " = -1 "
    else: # failed only
        query += " > 0 "
    query += "order by obs_id, host_id"

    logger.info(query)

    cur = dbconn.cursor()
    cur.execute(query)
    allfiles = cur.fetchall() # this is ok for now, but not after we have millions of files
    cur.close()

    curList = []
    curKey = ''
    for fi in allfiles:
        k = "%s" % (fi[3]) # obs_id
        if (not curKey):
            curKey = k
            curList.append(fi)
            continue

        if (k != curKey): # new obs
            # compress all files belong to the last obs_id
            #if (len(curList) > 2):
            compressObs(curList, sqlite_file)
            # setup the new chunk
            del curList
            curList = [fi]
            curKey = k
        else:
            curList.append(fi)

    if (curList != None and len(curList) > 0):
        compressObs(curList, sqlite_file)

    if (dbconn != None):
        dbconn.close()

def pingHost(url, timeout = 5):
    """
    To check if a host is successfully running

    Return:
    0        Success
    1        Failure
    """
    cmd = 'curl --connect-timeout %d %s' % (timeout, url)
    try:
        return execCmd(cmd)[0]
    except Exception, err:
        return 1

if __name__ == '__main__':

    logfile = MIT_processing_root + '/compress.log'
    rotateLogFiles(logfile)

    # setup run-time log file
    FORMAT = "%(asctime)-15s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(filename = logfile, level=logging.DEBUG, format = FORMAT)
    logger.info('MIT Compression Started.......')

    # check if all servers are up running?
    logger.info("Checking if all NGAS servers are up running...")
    for ho in eor_host:
        ret = pingHost(ho + '/STATUS')
        if (ret != 0):
            logger.error("NGAS is not up running: %s, quit now" % ho)
            sys.exit(1)

    doIt(MIT_processing_root)
