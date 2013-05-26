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
# Who                   When             What
# -----------------   ----------      ------------
# chen.wu@icrar.org  26/May/2013        Created

"""
This module provides MWA_RTS MRTask with functions for
metadata query, data movement, and HTTP-based communication
during job task execution and scheduling
"""

import psycopg2

db_name = 'mwa'
db_user = 'mwa'
db_passwd = 'Qm93VGll\n'
db_host = 'ngas01.ivec.org'
db_conn = None

def getMWADBConn():
    if (db_conn and (not db_conn.closed)):
        return db_conn
    try:
        db_conn = psycopg2.connect(database = db_name, user= db_user, 
                            password = db_passwd.decode('base64'), 
                            host = db_host)
        return db_conn
    except Exception, e:
        errStr = 'Cannot create MWA DB Connection: %s' % str(e)
        raise Exception, errStr

def getFileIdsByObsNum(obs_num):
    """
    Query the mwa database to get a list of files
    associated with this observation number
    
    obs_num:    observation number (string)
    """
    sqlQuery = "select filename from data_files where observation_num = '%s' order by substring(filename, 27)" % str(obs_num)
    conn = getMWADBConn()
    
    
