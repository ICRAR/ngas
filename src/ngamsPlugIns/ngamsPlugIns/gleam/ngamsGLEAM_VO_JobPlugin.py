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
# cwu      23/Sep/2014  Created
"""
Decompression job plugin that will be called
by the SubscriptionThread._deliveryThread
"""
# used to connect to MWA M&C database

import datetime
import logging
import os

import ephem_utils
from psycopg2.pool import ThreadedConnectionPool
import astropy.io.fits as pyfits

from ngamsLib import ngamsPlugInApi
from ngamsLib.ngamsCore import getHostName


logger = logging.getLogger(__name__)

mime = "images/fits"
myhost = getHostName()

# maximum connection = 3
g_db_pool = ThreadedConnectionPool(1, 3, database = None, user = None,
                            password = ''.decode('base64'),
                            host = None)

mc_db_pool = ThreadedConnectionPool(1, 3, database = None, user = None,
                            password = ''.decode('base64'),
                            host = None)


# decoded job uri:
#     ngasjob://ngamsGLEAM_Decompress_JobPlugin?redo_on_fail=0

# originally encoded joburi (during subscribe command)
#     url=ngasjob://ngamsGLEAM_Decompress_JobPlugin%3Fredo_on_fail%3D0


# fast lookup table
dict_dec = {'2013-08-03':-55.0,'2013-08-05':-26.7,'2013-08-06':-13.0,'2013-08-07':-40.0,
'2013-08-08':1.6,'2013-08-09':-55.0,'2013-08-10':-26.7,'2013-08-12':18.6,
'2013-08-13':-72.0,'2013-08-17':18.6,'2013-08-18':-72.0,'2013-08-22':-13.0,
'2013-08-25':-40.0,'2013-11-04':-27.0,'2013-11-05':-13.0,'2013-11-06':-40.0,
'2013-11-07':1.6,'2013-11-08':-55.0,'2013-11-11':-18.0,'2013-11-12':-72.0,
'2013-11-25':-27.0,'2014-03-03':-27.0,'2014-03-04':-13.0,'2014-03-05':-40.0,
'2014-03-06':1.6,'2014-03-07':-55.0,'2014-03-08':18.0,'2014-03-09':-72.0,
'2014-03-16':-40.0,'2014-03-17':-55.0,'2014-06-09':-27.0,'2014-06-10':-40.0,
'2014-06-11':1.6,'2014-06-12':-55.0,'2014-06-13':-13.0,'2014-06-14':-72.0,
'2014-06-15':18.0,'2014-06-16':-13.0,'2014-06-18':-55.0,'2014-08-04':-27.0,
'2014-08-05':-40.0,'2014-08-06':-55.0,'2014-08-07':-72.0,'2014-08-08':-13.0,
'2014-08-09':1.6,'2014-08-10':18.0,'2014-09-15':-27.0,'2014-09-16':-40.0,
'2014-09-17':-55.0,'2014-09-18':-72.0,'2014-09-19':-13.0,'2014-09-20':1.6,
'2014-09-21':18.0,'2014-10-27':-27.0,'2014-10-28':-40.0,'2014-10-29':-55.0,
'2014-10-30':-72.0,'2014-10-31':-13.0,'2014-11-01':1.6,'2014-11-02':18.0}

def getVODBSchema():
    """
    -- Table: mwa.gleam

    -- DROP TABLE mwa.gleam;

    CREATE TABLE mwa.gleam
    (
      centeralpha real,
      centerdelta real,
      coverage scircle,
      center_freq integer,
      band_width numeric,
      date_obs date,
      stokes integer,
      filename text,
      accref text,
      "owner" text,
      embargo date,
      mime text,
      accsize integer,
      img_rms real,
      cat_sepn real,
      psf_distortion real,
      gleam_phase integer DEFAULT 1,
      robustness integer
    )
    WITH (
      OIDS=FALSE
    );
    ALTER TABLE mwa.gleam OWNER TO gavoadmin;
    GRANT ALL ON TABLE mwa.gleam TO gavoadmin;
    GRANT SELECT ON TABLE mwa.gleam TO untrusted;
    GRANT SELECT ON TABLE mwa.gleam TO gavo;

    -- Index: mwa.gleam_pgspos

    -- DROP INDEX mwa.gleam_pgspos;

    CREATE INDEX gleam_pgspos
      ON mwa.gleam
      USING gist
      (coverage);


    -- Rule: cleanupproducts ON mwa.gleam

    -- DROP RULE cleanupproducts ON mwa.gleam;

    CREATE OR REPLACE RULE cleanupproducts AS
        ON DELETE TO mwa.gleam DO  DELETE FROM dc.products
      WHERE products.accref = old.accref;


    """
    pass

def getVODBConn():
    if (g_db_pool):
        return g_db_pool.getconn()
    else:
        raise Exception('VO connection pool is None when get conn')

def getMCDBConn():
    if (mc_db_pool):
        return mc_db_pool.getconn()
    else:
        raise Exception('MC connection pool is None when get conn')

def putVODBConn(conn):
    if (g_db_pool):
        g_db_pool.putconn(conn)
    else:
        raise Exception('VO connection pool is None when put conn')

def putMCDBConn(conn):
    if (mc_db_pool):
        mc_db_pool.putconn(conn)
    else:
        raise Exception('MC connection pool is None when put conn')

def executeQuery(conn, sqlQuery):
    try:
        cur = conn.cursor()
        cur.execute(sqlQuery)
        return cur.fetchall()
    finally:
        if (cur):
            del cur
        putVODBConn(conn)

def execCmd(cmd, timeout):
    logger.debug('Executing command: %s', cmd)
    try:
        ret = ngamsPlugInApi.execCmd(cmd, timeout)
    except Exception as ex:
        if (str(ex).find('timed out') != -1):
            return (-1, 'Timed out (%d seconds): %s' % (timeout, cmd))
        else:
            return (-1, str(ex))
    if (ret):
        return ret
    else:
        return (-1, 'Unknown error')

def ngamsGLEAM_VO_JobPlugin(srvObj,
                          plugInPars,
                          filename,
                          fileId,
                          fileVersion,
                          diskId):
    """
    srvObj:        Reference to NG/AMS Server Object (ngamsServer).

    plugInPars:    Parameters to take into account for the plug-in
                   execution (string).(e.g. scale_factor=4,threshold=1E-5)

    fileId:        File ID for file to test (string).

    filename:      Filename of (complete) (string).

    fileVersion:   Version of file to test (integer).

    Returns:       the return code of the compression plugin (integer).
    """

    hdrs = pyfits.getheader(filename)
    ra = float(hdrs['CRVAL1'])
    dec = float(hdrs['CRVAL2'])
    date_obs = hdrs['DATE-OBS'].split('T')[0]
    center_freq = int(float(hdrs['CRVAL3'])) / 1000000
    band_width = round(float(hdrs['CDELT3']) / 1000000, 2)
    stokes = int(float(hdrs['CRVAL4']))
    accsize = os.path.getsize(filename)
    embargo = datetime.date.today() - datetime.timedelta(days = 1)
    owner="MRO"
    accref_file =  "gleam/%s" % fileId
    file_url =  "http://%s:7777/RETRIEVE?file_id=%s" % (myhost, fileId)

    gleam_phase = 1
    getf_frmfn = 0
    if 'ORIGIN' in hdrs:
        fits_origin = hdrs['ORIGIN']
        if (fits_origin.find('WSClean') > -1):
            gleam_phase = 2
    else:
        getf_frmfn = 1

    if (getf_frmfn == 1 and fileId.split('_v')[1].split('.')[0] == '2'): # filename pattern is brittle, only use it if no fits header key: ORIGIN
        gleam_phase = 2

    #1068228616_095-103MHz_YY_r0.0_v2.0.fits
    """
    select cast((string_to_array((string_to_array((string_to_array(filename, '_r'))[2], '_'))[1], '.'))[1] as int) from mwa.gleam limit 10;

    # update the robustness value for those already in the database (thus did not get to run this job plugin)
    update mwa.gleam set robustness = cast((string_to_array((string_to_array((string_to_array(filename, '_r'))[2], '_'))[1], '.'))[1] as int)
    where position('escale' in filename) < 1
    and robustness is NULL;
    """
    #TODO - add robustness scheme (reading from the header and filename)
    robustness = 0
    getr_frmfn = 0
    if (gleam_phase == 1):
        if 'ROBUST' in hdrs:
            robustness = int(float(hdrs['ROBUST']))
        else:
            getr_frmfn = 1
    elif (gleam_phase == 2):
        if 'WSCWEIGH' in hdrs:
            wscweigh = hdrs['WSCWEIGH']
            if wscweigh.startswith("Briggs"):
                # value is, for example, "Briggs'(0.5)"
                robustness = int(float(wscweigh[8:-1]))
            else:
                getr_frmfn = 1
        else:
            getr_frmfn = 1
    if (getr_frmfn == 1):
        robustness = int(float(fileId.split('_r')[1].split('_')[0]))

    # get the correct DEC for phase 2
    if (2 == gleam_phase):
        if 'DEC_PNT' in hdrs:
            dec = float(hdrs['DEC_PNT'])
        elif (dict_dec.has_key(date_obs)): # see if we have cached
            dec = dict_dec[date_obs]
        else: # have to query MC database
            obsId = fileId.split('_')[0]
            conn_mc = getMCDBConn()
            cur_mc = conn_mc.cursor()
            res_mc = None
            try:
                cur_mc.execute("SELECT azimuth, elevation FROM rf_stream WHERE starttime = %s" % obsId)
                res_mc = cur_mc.fetchall()
            except Exception:
                logger.exception("Fail to query DEC from DB")
            finally:
                if (cur_mc != None):
                    del cur_mc
                putMCDBConn(conn_mc)

            if (not res_mc or len(res_mc) == 0):
                logger.error("fail to obtain DEC from MC db")
                #raise Exception(errMsg)
            else:
                az = res_mc[0][0]
                elv = res_mc[0][1]
                ra1, dec = ephem_utils.azel2radec(az, elv, int(obsId))


    conn = getVODBConn()
    cur = conn.cursor()
    sqlStr = ''
    try:
        sqlStr = "SELECT scircle '< (%10fd, %10fd), 20d>'" % (ra, dec)
        cur.execute(sqlStr)
        res = cur.fetchall()
        if (not res or len(res) == 0):
            errMsg = "fail to calculate scircle"
            raise Exception(errMsg)
        coverage = res[0][0]

        sqlStr = """INSERT INTO mwa.gleam(embargo,owner,centeralpha,centerdelta,accref,coverage,center_freq,band_width, mime,accsize,date_obs,stokes,filename, gleam_phase, robustness) VALUES('%s', '%s','%s', '%s', '%s','%s', '%s', '%s','%s','%s', '%s', '%s','%s', %d, %d)""" % (embargo,owner,str(ra), str(dec), accref_file, coverage, str(center_freq), str(band_width), mime, str(accsize), str(date_obs),str(stokes),fileId, gleam_phase, robustness)
        #info(3, sqlStr)
        cur.execute(sqlStr)

        sqlStr = """INSERT INTO dc.products(embargo,owner,accref, mime,accesspath,sourcetable) VALUES('%s', '%s', '%s', '%s', '%s', '%s')""" % (embargo,owner,accref_file, mime, file_url, 'mwa.gleam')
        #info(3, sqlStr)
        cur.execute(sqlStr)
        conn.commit()
        logger.debug('File %s added to VO database.', fileId)
    except Exception as exp:
        logger.exception("Unable to execute %s", sqlStr)
        return (1, str(exp))
    finally:
        if (cur):
            del cur
        putVODBConn(conn)

    return (0, 'Done')


