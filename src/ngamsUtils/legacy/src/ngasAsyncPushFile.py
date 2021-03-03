
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
# "@(#) $Id: ngasArchiveTool.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu       12/06/2013  Created
#

import os, sys, urllib2, psycopg2
from ngamsMWAAsyncProtocol import *
import cPickle as pickle
from urlparse import urlparse
from optparse import OptionParser

cortex_url = 'http://cortex.ivec.org:7779/ASYNCLISTRETRIEVE'
nz_url = 'http://kirk.research.vuw.ac.nz:7777/QARCHIVE'

g_db_conn = None # MWA metadata database connection

def getMWADBConn():
    global g_db_conn
    if (g_db_conn and (not g_db_conn.closed)):
        return g_db_conn

    """
    config = ngamsJobMAN.getConfig()
    confSec = 'MWA DB'
    db_name = config.get(confSec, 'db')
    db_user = config.get(confSec, 'user')
    db_passwd = config.get(confSec, 'password')
    db_host = config.get(confSec, 'host')
    """
    try:
        """
        g_db_conn = psycopg2.connect(database = db_name, user = db_user,
                            password = db_passwd.decode('base64'),
                            host = db_host)
        """
        g_db_conn = psycopg2.connect(database = None, user = None,
                            password = ''.decode('base64'),
                            host = None)
        return g_db_conn
    except Exception, e:
        errStr = 'Cannot create MWA DB Connection: %s' % str(e)
        raise Exception, errStr

def executeQuery(conn, sqlQuery):
    try:
        cur = conn.cursor()
        cur.execute(sqlQuery)
        return cur.fetchall()
    finally:
        if (cur):
            del cur

def getFileIdsByObsNum(obs_num):
    """
    Query the mwa database to get a list of files
    associated with this observation number

    obs_num:        observation number (string)
    num_subband:    number of sub-bands, used to check if the num_corr is the same

    Return:     A dictionary, key - correlator id (starting from 1, int), value - a list of file ids belong to that correlator
    """
    sqlQuery = "SELECT filename FROM data_files WHERE observation_num = '%s' ORDER BY SUBSTRING(filename, 27);" % str(obs_num)
    conn = getMWADBConn()
    res = executeQuery(conn, sqlQuery)
    ret = []
    for re in res:
        ret.append(re[0])

    return ret

def isValidURL(url):
    try:
        o = urlparse(url)
        if (not o):
            return False
    except Exception, err:
          return False

    return True

def readObsFrmFile(fpath):
    obsList = []
    with open(fpath) as f:
        lines = f.readlines()
        for line in lines:
            obses = line.split()
            for obs in obses:
                obsList.append(obs)

    return obsList
    #print len(obsList[1:].split(','))


def pushFile(src_url, dest_url, obs_list, file_list, fname):
    """
    """
    fileList = []
    obsNumList = []
    if (obs_list and len(obs_list) > 0):
        obsNumList += obs_list.split(',')
    if (fname):
        obsNumList += readObsFrmFile(fpath)
    for obsNum in obsNumList:
        flist = getFileIdsByObsNum(obsNum)
        if (len(flist)):
            fileList += flist
    if (file_list and len(file_list) > 0):
        fileList += file_list.split(',')

    if (len(fileList)):
        myReq = AsyncListRetrieveRequest(fileList, dest_url)
        strReq = pickle.dumps(myReq)
        try:
            strRes = urllib2.urlopen(src_url, data = strReq, timeout = 60).read() #HTTP Post
            print "Got result from src_url: '%s'" % strRes
        except urllib2.URLError, urlerr:
            print 'Fail to contact src_url: %s' % str(urlerr)

def main():
    parser = OptionParser()
    parser.add_option("-s", "--src", dest = "src_url", help = "%s -s %s -d %s -o %s" % (sys.argv[0], cortex_url, nz_url, '1051551136'))
    parser.add_option("-d", "--dest", dest = "dest_url", help = "destination url")
    parser.add_option("-o", "--obs", dest = "obs_list", help = "observation list separated by comma")
    parser.add_option("-f", "--files", dest = "file_list", help = "file list separated by comma")
    parser.add_option("-n", "--fname", dest = "file_name", help = "path to a file containing obsIds separated by whitespace")
    (options, args) = parser.parse_args()
    if (not options.src_url or
        (not options.dest_url)):
        parser.print_help()
        exit(1)
    if (not isValidURL(options.src_url) or
        not isValidURL(options.dest_url)):
        print 'Please specify valid URLs'
        exit(1)
    if (not options.obs_list and
        (not options.file_list) and
        (not options.file_name)):
        parser.print_help()
        exit(1)
    if (options.file_name):
        if (not os.path.exists(options.file_name)):
            print 'File %s does not exist' % options.file_name
            return
    pushFile(options.src_url, options.dest_url, options.obs_list, options.file_list, options.file_name)

if __name__ == "__main__":
    main()
    #readObsFrmFile('luke_list')