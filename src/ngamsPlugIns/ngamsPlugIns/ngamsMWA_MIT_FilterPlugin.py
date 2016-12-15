#
#    (c) University of Western Australia
#    International Centre of Radio Astronomy Research
#    M468/35 Stirling Hwy
#    Perth WA 6009
#    Australia
#
#    Copyright by UWA,
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
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu      01/10/2012  Created
"""
NGAS now supports two types of filters

1. attribute filter, which operates at the database level
2. match filter, which operates at each individual file level

NGAS does NOT yet support hybrid filter type with both levels
so if the plugin has the function "getAttrFilterSql", NGAS will
consider it as an attribute filter. Otherwise, NGAS will treat
it as a (file-level) match filter

e.g.

plugIn = 'ngamsMWA_MIT_FilterPlugin'
bb = eval("hasattr(" + plugIn + ", 'getAttrFilterSql')")


"""
"""
Contains a Filter Plug-In used to filter out those files that
(1) have already been delivered to the remote destination
(2) belong to Solar observations with project_id 'c105' or 'c106'
"""
# maximum connection = 5

import logging
import os

from psycopg2.pool import ThreadedConnectionPool
import pyfits

from ngamsLib import ngamsPlugInApi
from ngamsLib.ngamsCore import info, genLog, loadPlugInEntryPoint


logger = logging.getLogger(__name__)

g_db_pool = ThreadedConnectionPool(1, 5, database = 'mwa', user = 'mwa',
                            password = 'Qm93VGll\n'.decode('base64'),
                            host = 'ngas01.ivec.org')

g_db_conn = None # MWA metadata database connection

#eor_list = ["'G0001'", "'G0004'", "'G0009'", "'G0008'", "'G0010'"] # EOR scientists are only interested in these projects
eor_list = [] # this has become a parameter of the plug-in
proj_separator = '___'


def getAttrFilterSql(tb, an_col, av_col):
    """
    Return attribute filter in the form of SQL WHERE statement (String)

    tb:        name of the attribute_table (String)
    an_col:    name of the attribute_name column in tb (String)
    av_col:    name of the attribute_value column in tb (String)

    """

    s = "%s.%s = '%s' " % (tb, an_col, 'project_id')
    s += "AND (%s.%s = '%s' " % (tb, av_col, 'G0009')
    s += "OR %s.%s = '%s' )" % (tb, av_col, 'G0010')

    return s


def getMWADBConn():
    if (g_db_pool):
        return g_db_pool.getconn()
    else:
        raise Exception('connection pool is None when get conn')
    """
    global g_db_conn
    if (g_db_conn and (not g_db_conn.closed)):
        return g_db_conn
    try:
        g_db_conn = psycopg2.connect(database = 'mwa', user = 'mwa',
                            password = 'Qm93VGll\n'.decode('base64'),
                            host = 'ngas01.ivec.org')
        return g_db_conn
    except Exception, e:
        errStr = 'Cannot create MWA DB Connection: %s' % str(e)
        raise Exception, errStr
    """
def putMWADBConn(conn):
    if (g_db_pool):
        g_db_pool.putconn(conn)
    else:
        raise Exception('connection pool is None when put conn')

def executeQuery(conn, sqlQuery):
    try:
        cur = conn.cursor()
        cur.execute(sqlQuery)
        return cur.fetchall()
    finally:
        if (cur):
            del cur
        putMWADBConn(conn)

def getProjectIdFromMWADB(fileId):
    conn = getMWADBConn()
    sqlQuery = "SELECT projectid FROM mwa_setting WHERE starttime = %s" % (fileId.split('_')[0])
    res = executeQuery(conn, sqlQuery)

    for re in res:
        return re[0]

    return None

def ngamsMWA_MIT_FilterPlugin(srvObj,
                          plugInPars,
                          filename,
                          fileId,
                          fileVersion = -1,
                          reqPropsObj = None):

    """
    srvObj:        Reference to NG/AMS Server Object (ngamsServer).

    plugInPars:    Parameters to take into account for the plug-in
                   execution (string).

    fileId:        File ID for file to test (string).

    filename:      Filename of (complete) (string).

    fileVersion:   Version of file to test (integer).

    reqPropsObj:   NG/AMS request properties object (ngamsReqProps).

    Returns:       0 if the file does not match, 1 if it matches the
                   conditions (integer/0|1).
    """
    pars = ""
    if ((plugInPars != "") and (plugInPars != None)):
        pars = plugInPars
    elif (reqPropsObj != None):
        if (reqPropsObj.hasHttpPar("plug_in_pars")):
            pars = reqPropsObj.getHttpPar("plug_in_pars")
    parDic = ngamsPlugInApi.parseRawPlugInPars(pars)
    if (not parDic.has_key("remote_host") or
        not parDic.has_key("remote_port") or
        not parDic.has_key("project_id")):
        errMsg = "ngamsMWACheckRemoteFilterPlugin: Missing Plug-In Parameter: " +\
                 "remote_host / remote_port / project_id"
        logger.error(errMsg)
        return 0

    proj_ids = parDic["project_id"]

    if (not (proj_ids and len(proj_ids))):
        return 0

    for proj_id in proj_ids.split(proj_separator):
        eor_list.append("'%s'" % proj_id)

    fspi = srvObj.getCfg().getFileStagingPlugIn()
    if (not fspi):
        offline = -1
    else:
        info(2,"Invoking FSPI.isFileOffline: " + fspi + " to check file: " + filename)
        isFileOffline = loadPlugInEntryPoint(fspi, 'isFileOffline')
        offline = isFileOffline(filename)

    try:
        if (offline == 1 or offline == -1):
            # if the file is on Tape or query error, query db instead, otherwise implicit tape staging will block all other threads!!
            info(3, 'File %s appears on Tape, connect to MWA DB to check' % filename)
            projId = getProjectIdFromMWADB(fileId)
            if (not projId or projId == ''):
                logger.error('Cannot get project id from MWA DB for file %s', fileId)
                return 0
            projectId = "'%s'" % projId # add single quote to be consistent with FITS header keywords
        else:
            projectId = pyfits.getval(filename, 'PROJID')
    except:
        err = "Did not find keyword PROJID in FITS file or PROJID illegal"
        errMsg = genLog("NGAMS_ER_DAPI_BAD_FILE", [os.path.basename(filename),
                                                   "ngamsMWA_MIT_FilterPlugIn", err])
        return 0

    if (projectId in eor_list):
        info(3, 'File %s added' % fileId)
        return 1
    else:
        return 0

# EOF
