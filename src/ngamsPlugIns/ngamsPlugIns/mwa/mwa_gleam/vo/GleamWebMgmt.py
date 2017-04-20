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
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu      19/Feb/2014  Created

from bottle import route, run, request, get, static_file, template, redirect, auth_basic
import psycopg2, csv
from psycopg2.pool import ThreadedConnectionPool

allowd_keys = ['img_rms', 'cat_sepn', 'psf_distortion']

# maximum connection = 5
g_db_pool = None
g_gleam_obj_dict = None

def initConnectionPool():
    global g_db_pool
    g_db_pool = ThreadedConnectionPool(1, 5, database = 'gavo', user = 'zhl',
                            password = 'zhlgly',
                            host = 'mwa-web.icrar.org')

def load_gleam_obj_dict(csv_fn):
    global g_gleam_obj_dict
    with open(csv_fn, mode='r') as infile:
        reader = csv.reader(infile)
        g_gleam_obj_dict = {rows[0].split()[1]:"{0},{1}".format(rows[1], rows[2]) for rows in reader}

@get('/resolve_gleam_obj')
def resolve_gleam_obj():
    obj_name = request.query.get('obj_name')
    if (g_gleam_obj_dict.has_key(obj_name)):
        return g_gleam_obj_dict[obj_name]
    else:
        return "None"

def getGavoDBConn():
    if (g_db_pool):
        return g_db_pool.getconn()
    else:
        raise Exception('connection pool is None when get conn')

def putGavoDBConn(conn):
    if (g_db_pool):
        g_db_pool.putconn(conn)
    else:
        raise Exception('connection pool is None when put conn')

def executeAtomicQuery(conn, sqlQuery):
    try:
        cur = conn.cursor()
        cur.execute(sqlQuery)
        conn.commit()
        return cur.rowcount
    finally:
        if (cur):
            del cur
        putGavoDBConn(conn)

def invalidParam(param):
    if (None == param or len(str(param)) == 0):
        return 1
    else:
        return 0

def check(user, passwd):
 if user == 'gleamer' and passwd == 'BowTie':
     return True
 else:
     return False

@route('/')
@auth_basic(check)
def index():
    print 'auth', request.auth
    print 'remote_addr', request.remote_addr
    return "Apparently it works"

@route('/img/metadata/update')
@auth_basic(check)
def update_metadata():
    fileId = request.query.get('file_id')
    k = request.query.get('key')
    v = request.query.get('value')

    params = [fileId, k, v]
    for para in params:
        if (invalidParam(para)):
            raise HTTPResponse(body="Parameter %s is invalid.\n" % (para), status=400, headers=None)

    if not (k in allowd_keys):
        #abort(401, "Key %s cannot be updated." % (k))
        raise HTTPResponse(body="Key %s cannot be updated.\n" % (k), status=403, headers=None)

    sqlQuery = "UPDATE mwa.gleam SET %s = %s WHERE filename = '%s'" % (k, v, fileId)
    rc = 0
    try:
        conn = getGavoDBConn()
        rc = executeAtomicQuery(conn, sqlQuery)
    except Exception, ex:
        raise HTTPResponse(body="DB Error %s\n" % (str(ex)), status=500, headers=None)

    if (rc == 1):
        return "Key %s successfully updated to value %s for image %s\n" % (k, v, fileId)
    elif (rc > 1):
        mgs = "Warning - more than one images updated. Double check your file_id '%s'\n" % fileId
        raise HTTPResponse(body=msg, status=400, headers=None)
    else:
        msg = "Update failure, appears that file_id '%s' cannot be found\n" % (fileId)
        raise HTTPResponse(body=msg, status=404, headers=None)
    #print 'File_id = %s, key = %s, value = %s' % (fileId, k, v)

if __name__ == "__main__":
    initConnectionPool()
    load_gleam_obj_dict('/home/chen/gleamvo/data/IDR_min_col_noh.csv')
    run(host = '0.0.0.0', #host = '180.149.251.152',
        server = 'paste',
        port = 7778,
        debug = True)