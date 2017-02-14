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
import psycopg2

from ngamsLib import ngamsPlugInApi

logger = logging.getLogger(__name__)

def split_file(filename):
    try:
        # Example: 1096202392_20141001123939_gpubox13_00.fits
        filename = os.path.basename(filename)
        if '.fits' not in filename:
            raise Exception('fits extension not found')

        part = filename.split('_')
        if 'gpubox' not in part[2]:
            raise Exception('gpubox not found in 3rd part')

        return (int(part[0]), int(part[1]), part[2])

    except Exception:
        raise Exception('invalid correlator data filename %s' % filename)


def ngamsMWAFitsPlugIn(srvObj, reqPropsObj):

    logger.info("ngamsMWAFitsPlugIn handling data for file with URI: %s",
                os.path.basename(reqPropsObj.getFileUri()))

    mime = reqPropsObj.getMimeType()
    parDic = ngamsPlugInApi.parseDapiPlugInPars(srvObj.getCfg(), mime)

    try:
        dbhost = parDic['dbhost']
        dbname = parDic['dbname']
        dbuser = parDic['dbuser']
        dbpass = parDic['dbpassword']
    except Exception as e:
        raise Exception('Must specify parameters dbhost, dbname, dbuser and dbpassword')

    diskInfo = reqPropsObj.getTargDiskInfo()
    stageFilename = reqPropsObj.getStagingFilename()
    uncomprSize = ngamsPlugInApi.getFileSize(stageFilename)
    fileName = os.path.basename(reqPropsObj.getFileUri())
    dpId = os.path.splitext(fileName)[0]

    obsid, obstime, box = split_file(fileName)

    fileVersion, relPath, relFilename, complFilename, fileExists = \
                    ngamsPlugInApi.genFileInfo(srvObj.getDb(),
                                                srvObj.getCfg(),
                                                reqPropsObj,
                                                diskInfo,
                                                reqPropsObj.getStagingFilename(),
                                                dpId,
                                                dpId, 
                                                [str(obsid)],
                                                [])

    sql = ('INSERT INTO data_files (observation_num, filetype, size, filename, site_path, host)'
           ' VALUES (%s, %s, %s, %s, %s, %s)')

    uri = 'http://mwangas/RETRIEVE?file_id=%s' % (fileName)

    logger.info('Inserting: %s', sql % (obsid, 8, uncomprSize, fileName, uri, box))

    try:
        with psycopg2.connect(host = dbhost,
                              dbname = dbname,
                              user = dbuser,
                              password = dbpass) as conn:

            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)

            with conn.cursor() as cur:
                try:
                    cur.execute(sql, [obsid, 8, uncomprSize, fileName, uri, box])
                except Exception as e:
                    ''' Do not want to raise an exception here because the sky data is more important than the metadata
                        i.e. we want to keep the sky data even if there is a database problem.
                        If there is an issue with an INSERT then go through and add it manually later (very unlikely to happen)'''
                    conn.rollback()
                    logger.error('Insert error: %s Message: %s', sql % (obsid, 8, uncomprSize, fileName, uri, box), str(e))
                else:
                    conn.commit()
    except Exception:
        logger.exception('Insert error: %s', sql % (obsid, 8, uncomprSize, fileName, uri, box))

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
