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
import os
import logging
import threading
import psycopg2
import psycopg2.pool
from ngamsLib import ngamsPlugInApi

logger = logging.getLogger(__name__)


class DBPool(object):
    db_lock = threading.Lock()
    db_pool = None


def split_file(filename):
    try:
        # Example: 1096202392_20141001123939_gpubox13_00.fits
        filename = os.path.basename(filename)
        if '.fits' not in filename:
            raise Exception('fits extension not found')

        part = filename.split('_')
        if 'gpubox' not in part[2]:
            raise Exception('gpubox not found in 3rd part')

        return int(part[0]), int(part[1]), part[2]

    except Exception:
        raise Exception('invalid correlator data filename %s' % filename)


def ngamsMWAPawseyFitsPlugIn(srvObj, reqPropsObj):

    logger.info("ngamsMWAPawseyFitsPlugIn handling data for file with URI: %s",
                os.path.basename(reqPropsObj.getFileUri()))

    mime = reqPropsObj.getMimeType()
    parDic = ngamsPlugInApi.parseDapiPlugInPars(srvObj.getCfg(), mime)

    do_update = parDic.get('doupdate', False)

    if do_update is True:
        with DBPool.db_lock:
            if DBPool.db_pool is None:
                try:
                    dbhost = parDic['dbhost']
                    dbname = parDic['dbname']
                    dbuser = parDic['dbuser']
                    dbpass = parDic['dbpassword']
                except Exception as e:
                    raise Exception('Must specify parameters dbhost, dbname, dbuser and dbpassword')

                maxconn = parDic.get('dbmaxconn', 4)

                logger.info("ngamsMWAPawseyFitsPlugIn creating database pool")

                DBPool.db_pool = psycopg2.pool.ThreadedConnectionPool(minconn=1,
                                                                      maxconn=maxconn,
                                                                      host=dbhost,
                                                                      user=dbuser,
                                                                      database=dbname,
                                                                      password=dbpass,
                                                                      port=5432)

    diskInfo = reqPropsObj.getTargDiskInfo()
    stageFilename = reqPropsObj.getStagingFilename()
    uncomprSize = ngamsPlugInApi.getFileSize(stageFilename)
    fileName = os.path.basename(reqPropsObj.getFileUri())
    dpId = fileName

    obsid, obstime, box = split_file(fileName)

    fileVersion, relPath, relFilename, complFilename, fileExists = \
        ngamsPlugInApi.genFileInfo(srvObj.getDb(),
                                   srvObj.getCfg(),
                                   reqPropsObj,
                                   diskInfo,
                                   reqPropsObj.getStagingFilename(),
                                   dpId,
                                   dpId,
                                   [],
                                   [])

    logger.info("ngamsMWAPawseyFitsPlugIn version: %s path: %s relfilename: %s" % (fileVersion, relPath, relFilename))

    if do_update is True:
        sql = 'UPDATE data_files SET remote_archived = True WHERE filename = %s'

        logger.info('Updating: %s', sql % fileName)

        conn = None
        try:
            conn = DBPool.db_pool.getconn()

            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)

            with conn.cursor() as cur:
                try:
                    cur.execute(sql, [fileName])
                except Exception as e:
                    conn.rollback()
                    logger.error('Update error: %s Message: %s', sql % fileName, str(e))
                else:
                    conn.commit()
        except Exception:
            logger.exception('Update error: %s', sql % fileName)
        finally:
            if conn:
                DBPool.db_pool.putconn(conn=conn)

    return ngamsPlugInApi.genDapiSuccessStat(diskInfo.getDiskId(),
                                             relFilename,
                                             dpId,
                                             fileVersion,
                                             mime,
                                             uncomprSize,
                                             uncomprSize,
                                             '',
                                             relPath,
                                             diskInfo.getSlotId(),
                                             fileExists,
                                             complFilename)
