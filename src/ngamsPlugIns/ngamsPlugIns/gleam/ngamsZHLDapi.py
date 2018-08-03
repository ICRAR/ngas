#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2013
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
# "@(#) $Id: ngamsZHLDapi.py,v 1.0 2013/10/15 12:48:12 jagonzal Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  30/03/2006  Created
# ZHANGHAILONG  10/15/2013  Add functions to fill the GAVO table for MWA-WEB.ICRAR.ORG
#
"""
This is a generic Data Archiving Plug-In to archive any kind of data file.

It accepts the following parameters:

mime_type:          Indicates the type of data (mandatory).

target_mime_type:   Mime-type, which will be written in the DB. If not given
                    the DAPI will 'guess' the target mime-type (optional).

file_id:            ID of the file in the NGAS archive. If not given, the
                    basename of the URI indicated in the Archive Request is
                    used as File ID (optional).

versioning:         If 1 versioning is on, default is 0 (0|1 [1]).

checksum:           Checksum of the file (optional (*)).

checksum_cmd:       Command to calculate the checksum (optional (*)).

compression:        Command used to compress the file. If not given the file is
                    not compressed (optional (**)).

compression_ext:    Extension resulting from applying the specified compression
                    tool on the file (optional (**)).

*/**: These parameters must be given in pairs, it is not possible only to
      specify one of them.
"""
# Parameters.

import logging
import os
import time

from ngamsLib import ngamsPlugInApi
from ngamsLib.ngamsCore import genLog, toiso8601, FMT_DATE_ONLY
import psycopg2
import astropy.io.fits as pyfits


logger = logging.getLogger(__name__)

TARG_MIME_TYPE  = "target_mime_type"
FILE_ID         = "file_id"
VERSIONING      = "versioning"
COMPRESSION     = "compression"
COMPRESSION_EXT = "compression_ext"

# Constants.
NO_COMPRESSION  = "NONE"


def handlePars(reqPropsObj,
               parDic):
    """
    Parse/handle the HTTP parameters.

    reqPropsObj:  NG/AMS request properties object (ngamsReqProps).

    parDic:       Dictionary with the parameters (dictionary).

    Returns:      Void.
    """
    # Get parameters.
    logger.debug("Get request parameters")
    parDic[TARG_MIME_TYPE]  = None
    parDic[FILE_ID]         = None
    parDic[VERSIONING]      = 1
    parDic[COMPRESSION]     = None
    parDic[COMPRESSION_EXT] = None

    if (reqPropsObj.hasHttpPar(TARG_MIME_TYPE)):
        parDic[TARG_MIME_TYPE] = reqPropsObj.getHttpPar(TARG_MIME_TYPE)

    # If the file_id is not given, we derive it from the name of the URI.
    if (reqPropsObj.hasHttpPar(FILE_ID)):
        parDic[FILE_ID] = reqPropsObj.getHttpPar(FILE_ID)
    if (not parDic[FILE_ID]):
        if (reqPropsObj.getFileUri().find("file_id=") > 0):
            file_id = reqPropsObj.getFileUri().split("file_id=")[1]
            parDic[FILE_ID] = file_id
            logger.info("No file_id given, but found one in the URI: %s", parDic[FILE_ID])
        else:
            parDic[FILE_ID] = os.path.basename(reqPropsObj.getFileUri())
            logger.info("No file_id given, using basename of URI: %s", parDic[FILE_ID])

    if (reqPropsObj.hasHttpPar(VERSIONING)):
        parDic[VERSIONING] = int(reqPropsObj.getHttpPar(VERSIONING))
    # Set also the no_versioning parameter for backwards compatibility reasons
    if (parDic[VERSIONING]):
        reqPropsObj.addHttpPar("no_versioning", "0")
    else:
        reqPropsObj.addHttpPar("no_versioning", "1")

    if (reqPropsObj.hasHttpPar(COMPRESSION)):
        parDic[COMPRESSION] = reqPropsObj.getHttpPar(COMPRESSION)
    if (reqPropsObj.hasHttpPar(COMPRESSION_EXT)):
        parDic[COMPRESSION_EXT] = reqPropsObj.getHttpPar(COMPRESSION_EXT)
    if ((parDic[COMPRESSION] and (parDic[COMPRESSION_EXT] == None)) or
        (not parDic[COMPRESSION] and (parDic[COMPRESSION_EXT] != None))):
        raise Exception(genLog("NGAMS_ER_DAPI",
                                ["Parameters compression and compression_ext"
                                 "must be given together."]))


def compressFile(srvObj,
                 reqPropsObj,
                 parDic):
    """
    Compress the file if required.

    srvObj:       Reference to NG/AMS Server Object (ngamsServer).

    reqPropsObj:  NG/AMS request properties object (ngamsReqProps).

    parDic:       Dictionary with parameters for the DAPI. This is generated
                  with ngamsPlugInApi.parseDapiPlugInPars() (Dictionary).

    Returns:      Tupe containing uncompressed filesize, archived filesize
                  and the format (mime-type) of the resulting data file and
                  the compression method (NONE if the file is not compressed),
                  finally, the extension added by the compression if any
                  (tuple).
    """
    stFn = reqPropsObj.getStagingFilename()

    # If a compression application is specified, apply this.
    uncomprSize = ngamsPlugInApi.getFileSize(stFn)
    comprExt = ""
    if (parDic[COMPRESSION]):
        logger.debug("Compressing file using: %s ...", parDic[COMPRESSION])
        compCmd = "%s %s" % (parDic[COMPRESSION], stFn)
        compress_start = time.time()
        logger.debug("Compressing file with command: %s", compCmd)
        exitCode, stdOut = ngamsPlugInApi.execCmd(compCmd)
        #if (exitCode != 0):
        #    msg ="Problems during archiving! Compressing the file failed. " +\
        #          "Error: %s" % str(stdOut).replace("/n", "   ")
        #    raise Exception, msg
        # If the compression fails, assume that it is because the file is not
        # compressible (although it could also be due to lack of disk space).
        if (exitCode == 0):
            if (parDic[COMPRESSION_EXT]):
                stFn = stFn + "." + parDic[COMPRESSION_EXT]
                comprExt = parDic[COMPRESSION_EXT]
            # Remember to update Staging Filename in the Request Properties
            # Object.
            reqPropsObj.setStagingFilename(stFn)

            # Handle mime-type
            if (parDic[TARG_MIME_TYPE]):
                format = parDic[TARG_MIME_TYPE]
            else:
                format = ngamsPlugInApi.determineMimeType(srvObj.getCfg(),
                                                          stFn)
            compression = parDic[COMPRESSION]

            logger.debug("File compressed. Time: %.3fs", time.time() - compress_start)
        else:
            # Carry on with the original file. We take the original mime-type
            # as the target mime-type.
            format = reqPropsObj.getMimeType()
            compression = NO_COMPRESSION
    else:
        # Handle mime-type
        if (parDic[TARG_MIME_TYPE]):
            format = parDic[TARG_MIME_TYPE]
        else:
            format = reqPropsObj.getMimeType()
        compression = NO_COMPRESSION

    archFileSize = ngamsPlugInApi.getFileSize(reqPropsObj.getStagingFilename())
    return uncomprSize, archFileSize, format, compression, comprExt


def checkForDblExt(complFilename,
                   relFilename):
    """
    If if the File ID is derived from the URI, it might be that there is a
    double extension due to the way the ngamsPlugInApi.genFileInfo() generates
    the filename. This function checks double extensions and remove one of them
    in case there are two.

    complFilename:    Complete filename (string).

    relFilename:      Relative filename (string).

    Returns:          Tuple with complete filename and relative filename
                      (tuple/string).
    """
    filename2, ext1 = os.path.splitext(complFilename)
    filename3, ext2 = os.path.splitext(filename2)
    if ((ext1 != "") and (ext1 == ext2)):
        # Remove one of the extensions.
        complFilename = complFilename[0:-len(ext1)]
        relFilename = relFilename[0:-len(ext1)]

    return (complFilename, relFilename)


def ngamsZHLDapi(srvObj,
                 reqPropsObj):
    """
    Generic Data Archiving Plug-In to handle archiving of any file.

    srvObj:       Reference to NG/AMS Server Object (ngamsServer).

    reqPropsObj:  NG/AMS request properties object (ngamsReqProps).

    Returns:      Standard NG/AMS Data Archiving Plug-In Status
                  as generated by: ngamsPlugInApi.genDapiSuccessStat()
                  (ngamsDapiStatus).
    """
    # For now the exception handling is pretty basic:
    # If something goes wrong during the handling it is tried to
    # move the temporary file to the Bad Files Area of the disk.
    logger.debug("Plug-In handling data for file: %s",
         os.path.basename(reqPropsObj.getFileUri()))
    try:
        parDic = {}
        handlePars(reqPropsObj, parDic)
        diskInfo = reqPropsObj.getTargDiskInfo()
        stgFile = reqPropsObj.getStagingFilename()
        ext = os.path.splitext(stgFile)[1][1:]

        # Generate file information.
        logger.debug("Generate file information")
        dateDir = toiso8601(fmt=FMT_DATE_ONLY)
        fileVersion, relPath, relFilename,\
                     complFilename, fileExists =\
                     ngamsPlugInApi.genFileInfo(srvObj.getDb(),
                                                srvObj.getCfg(),
                                                reqPropsObj, diskInfo,
                                                reqPropsObj.\
                                                getStagingFilename(),
                                                parDic[FILE_ID],
                                                parDic[FILE_ID], [dateDir])
        complFilename, relFilename = checkForDblExt(complFilename,
                                                    relFilename)

        # Compress the file if requested.
        uncomprSize, archFileSize, format, compression, comprExt =\
                     compressFile(srvObj, reqPropsObj, parDic)
        if (comprExt != ""):
            complFilename += ".%s" % comprExt
            relFilename += ".%s" % comprExt

        logger.debug("DAPI finished processing file - returning to host application")
        insertFitsRecords(srvObj, reqPropsObj, stgFile)
        return ngamsPlugInApi.genDapiSuccessStat(diskInfo.getDiskId(),
                                                 relFilename,
                                                 parDic[FILE_ID],
                                                 fileVersion, format,
                                                 archFileSize, uncomprSize,
                                                 compression, relPath,
                                                 diskInfo.getSlotId(),
                                                 fileExists, complFilename)
    except Exception:
        logger.exception("Error occurred in DAPI")
        raise


def insertFitsRecords(srvObj,reqPropsObj, complFileUri):
    extension=os.path.splitext(reqPropsObj.getFileUri())[1].lower()

    if (cmp(extension,'.fit') == 0 or cmp(extension,'.fits') == 0):
        # get the file address?
        file_url =  "http://%s/RETRIEVE?file_id=%s" % ('store02.icrar.org:7777', reqPropsObj.getFileUri())
        # use pyfits open fits file
        accref_file =  "gleam/%s" % reqPropsObj.getFileUri()
        hdu = pyfits.open(complFileUri)
        # get the value of ra, dec
        ra = hdu[0].header['CRVAL1']
        dec = hdu[0].header['CRVAL2']
        date_obs = hdu[0].header['DATE-OBS']
        center_freq = int(hdu[0].header['CRVAL3']/1000000)
        band_width = round(float(hdu[0].header['CDELT3']/1000000), 2)
        stokes = int(hdu[0].header['CRVAL4'])
        mime = "images/fits"
        from os.path import getsize
        accsize = getsize(complFileUri)
        import datetime
        embargo = datetime.date.today() - datetime.timedelta(days=1)
        owner="MRO"
        filename=reqPropsObj.getFileUri()

        try:
            conn = psycopg2.connect("dbname='' user='' host='' password=''")
        except:
            errMsg = "Unable to connect to the GLEAM VO database at mwa-web.icrar.org"
            logger.exception(errMsg)
            raise
        # Create a Cursor object and call its execute() method to perform SQL commands:
        cur = conn.cursor()
        sql = "SELECT scircle '< (%10fd, %10fd), 20d>'" % (ra, dec)
        cur.execute(sql)
        res = cur.fetchall()
        if (not res or len(res) == 0):
            errMsg = "fail to calculate scircle"
            raise Exception(errMsg)
        coverage = res[0][0]
        try:
            sqlStr = """INSERT INTO mwa.gleam(embargo,owner,centeralpha,centerdelta,accref,coverage,center_freq,band_width, mime,accsize,date_obs,stokes,filename ) VALUES('%s', '%s','%s', '%s', '%s','%s', '%s', '%s','%s','%s', '%s', '%s','%s' )""" % (embargo,owner,str(ra), str(dec), accref_file, coverage, str(center_freq), str(band_width), mime, str(accsize), str(date_obs),str(stokes),filename )
            logger.debug(sqlStr)
            cur.execute(sqlStr)
        except:
            logger.exception(" Unable to insert the table mwa.gleam...")
        try:
            sqlStr = """INSERT INTO dc.products(embargo,owner,accref, mime,accesspath,sourcetable) VALUES('%s', '%s', '%s', '%s', '%s', '%s')""" % (embargo,owner,accref_file, mime, file_url, 'mwa.gleam')
            logger.debug(sqlStr)
            cur.execute(sqlStr)
        except:
            logger.exception(" I am unable to insert the table dc.product...")

        # Make the changes to the database persistent
        conn.commit()

        # Close communication with the database
        cur.close()
        conn.close()

