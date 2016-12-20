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
# "@(#) $Id: ngamsFitsPlugIn.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  10/05/2001  Created
#
"""
This Data Archiving Plug-In is used to handle reception and processing
of FITS files.

Note, that the plug-in is implemented for the usage at ESO. If used in other
contexts, a dedicated plug-in matching the individual context should be
implemented and NG/AMS configured to use it.
"""

from collections import defaultdict
import logging
import os
import subprocess
import time

from ngamsLib import ngamsPlugInApi
from ngamsLib.ngamsCore import TRACE, genLog, fromiso8601, tomjd, frommjd,\
    toiso8601, FMT_DATE_ONLY


logger = logging.getLogger(__name__)

def getFitsKeys(fitsFile,
                keyList):
    """
    Get a FITS keyword from a FITS file. A dictionary is returned whereby
    the keys in the keyword list are the dictionary keys and the value
    the elements that these refer to.

    fitsFile:   Filename of FITS file (string).

    keyList:    Tuple of keys for which to extract values (tuple).

    Returns:    Dictionary with the values extracted of the format:

                  {<key 1>: [<val hdr 0>, <val hdr 1> ...], <key 2>: ...}

                (dictionary).
    """
    T = TRACE()

    import pyfits
    keyDic = defaultdict(list)
    try:
        for key in keyList:
            vals = pyfits.getval(fitsFile, key)
            if isinstance(vals, basestring):
                vals = [vals]
            keyDic[key] = list(vals)
        return keyDic
    except Exception, e:
        msg = ". Error: %s" % str(e)
        errMsg = genLog("NGAMS_ER_RETRIEVE_KEYS", [str(keyList),
                                                   fitsFile + msg])
        logger.exception(errMsg)
        raise


def getDpIdInfo(filename):
    """
    Generate the File ID (here DP ID) for the file.

    filename:   Name of FITS file (string).

    Returns:    Tuple containing the value of ARCFILE, the DP ID
                of the file, and the JD date. The two latter deducted from
                the ARCFILE keyword (tuple).
    """
    try:
        keyDic  = getFitsKeys(filename, ["ARCFILE"])
        arcFile = keyDic["ARCFILE"][0]
        els     = arcFile.split(".")
        dpId    = els[0] + "." + els[1] + "." + els[2]
        date    = els[1].split("T")[0]
        # Make sure that the files are stored according to JD
        # (one night is 12am -> 12am).
        isoTime = '.'.join(els[1:])
        ts1 = fromiso8601(isoTime)
        ts2 = tomjd(ts1) - 0.5
        dateDirName = toiso8601(frommjd(ts2), fmt=FMT_DATE_ONLY)

        return [arcFile, dpId, dateDirName]
    except:
        err = "Did not find keyword ARCFILE in FITS file or ARCFILE illegal"
        errMsg = genLog("NGAMS_ER_DAPI_BAD_FILE", [os.path.basename(filename),
                                                   "ngamsFitsPlugIn", err])
        logger.exception(errMsg)
        raise


def checkFitsFileSize(filename):
    """
    Check if the size of the FITS file is a multiple of 2880. If this
    is not the case, we through an exception.

    filename:   FITS file to check (string).

    Returns:    Void.
    """
    if filename.lower().endswith('.fits'):
        size = ngamsPlugInApi.getFileSize(filename)
        if size % 2880 != 0:
            errMsg = ("The size of the FITS file issued "
                     "is not a multiple of 2880 (size: %d)! Rejecting file!")
            errMsg = genLog("NGAMS_ER_DAPI_BAD_FILE",
                            [os.path.basename(filename),
                             "ngamsFitsPlugIn", errMsg % size])
            raise Exception, errMsg


def checkFitsChecksum(reqPropsObj,
                      stgFile = None):
    """
    Carry out a check of the DATASUM and CHECKSUM for each HDU in the file.

    reqPropsObj:    NG/AMS request properties object (ngamsReqProps).

    stgFile:        If specified this is taken rather than from the Request
                    Properties Object (ngamsReqProps).

    Returns:        Void.
    """
    if (not stgFile): stgFile = reqPropsObj.getStagingFilename()
    cmd = "chksumVerFitsChksum %s" % stgFile
    stat, out = ngamsPlugInApi.execCmd(cmd)
    if (out.find("Status: OK") == -1):
        if (out.find("Status: NOT OK") != -1):
            errMsg = genLog("NGAMS_ER_DAPI_BAD_FILE",
                            [stgFile, "checkFitsChecksum",
                             "Illegal CHECKSUM/DATASUM"])
        else:
            errMsg = "Problem found while checking file: %s. " %\
                     out.replace("\n", "   ")

        # Backwards compatibility with old ESO FITS CHECKSUM scheme: If the
        # file pass the previous scheme, we consider it as OK and do not
        # return the failure.
        cmd = "chksumGenChecksum %s" % stgFile
        stat, out = ngamsPlugInApi.execCmd(cmd)
        if (out.strip().find("0/0000000000000000") == -1):
            raise Exception, errMsg
        chksumUtil = "chksumGenChecksum"
    else:
        chksumUtil = "chksumVerFitsChksum"
    logger.debug("File: %s checked with: %s. Result: OK", stgFile, chksumUtil)


def prepFile(reqPropsObj,
             parDic):
    """
    Prepare the file. If it is compressed, decompress it into a temporary
    filename.

    reqPropsObj:  NG/AMS request properties object (ngamsReqProps).

    parDic:       Dictionary with parameters for the DAPI. This is generated
                  with ngamsPlugInApi.parseDapiPlugInPars() (Dictionary).

    Returns:      Tuple containing:

                    (<DP ID>, <Date Obs. Night>, <Compr. Ext.>)   (tuple).
    """
    T = TRACE()

    # If the file is already compressed, we have to decompress it.
    comprExt = ''
    compression = parDic["compression"]
    if compression and 'gzip' in compression:
        comprExt = 'gz'

    tmpFn = reqPropsObj.getStagingFilename()
    if tmpFn.lower().endswith('.gz'):
        newFn = os.path.splitext(tmpFn)[0]
        logger.debug("Decompressing file using gzip: %s", tmpFn)
        subprocess.check_call(['gunzip', '-f', tmpFn], shell = False)
        logger.debug("Decompression success: %s", newFn)
        reqPropsObj.setStagingFilename(newFn)

    checkFitsFileSize(reqPropsObj.getStagingFilename())
    dpIdInfo = getDpIdInfo(reqPropsObj.getStagingFilename())

    return dpIdInfo[1], dpIdInfo[2], comprExt


def compress(reqPropsObj,
             parDic):
    """
    Compress the file if required.

    reqPropsObj:  NG/AMS request properties object (ngamsReqProps).

    parDic:       Dictionary with parameters for the DAPI. This is generated
                  with ngamsPlugInApi.parseDapiPlugInPars() (Dictionary).

    Returns:      Tupe containing uncompressed filesize, archived filesize
                  and the format (mime-type) of the resulting data file
                  (tuple).
    """
    stFn = reqPropsObj.getStagingFilename()
    uncomprSize = ngamsPlugInApi.getFileSize(stFn)
    mime = reqPropsObj.getMimeType()
    compression = parDic["compression"]

    if compression and 'gzip' in compression:
        logger.debug("Compressing file: %s using: %s", stFn, compression)
        compress_start = time.time()
        gzip_name = '%s.gz' % stFn
        subprocess.check_call(['gzip', '--no-name', stFn], shell = False)
        reqPropsObj.setStagingFilename(gzip_name)
        mime = 'application/x-gfits'
        compression = 'gzip --no-name'
        logger.debug("File compressed: %s Time: %.3fs", gzip_name, time.time() - compress_start)
    else:
        compression = ''

    archFileSize = ngamsPlugInApi.getFileSize(reqPropsObj.getStagingFilename())

    return uncomprSize, archFileSize, mime, compression


# DAPI function.
def ngamsFitsPlugIn(srvObj,
                    reqPropsObj):
    """
    Data Archiving Plug-In to handle archiving of FITS files.

    srvObj:       Reference to NG/AMS Server Object (ngamsServer).

    reqPropsObj:  NG/AMS request properties object (ngamsReqProps).

    Returns:      Standard NG/AMS Data Archiving Plug-In Status as generated
                  by: ngamsPlugInApi.genDapiSuccessStat() (ngamsDapiStatus).
    """
    logger.info("Plug-In handling data for file with URI: %s",
            os.path.basename(reqPropsObj.getFileUri()))
    diskInfo = reqPropsObj.getTargDiskInfo()
    parDic = ngamsPlugInApi.parseDapiPlugInPars(srvObj.getCfg(),
                                                reqPropsObj.getMimeType())

    # Check file (size + checksum) + extract information.
    dpId, dateDirName, comprExt = prepFile(reqPropsObj, parDic)

    # Get various information about the file being handled.
    dpIdInfo = getDpIdInfo(reqPropsObj.getStagingFilename())
    dpId = dpIdInfo[1]
    dateDirName = dpIdInfo[2]
    fileVersion, relPath, relFilename,\
                 complFilename, fileExists =\
                 ngamsPlugInApi.genFileInfo(srvObj.getDb(), srvObj.getCfg(),
                                            reqPropsObj, diskInfo,
                                            reqPropsObj.getStagingFilename(),
                                            dpId, dpId, [dateDirName],
                                            [comprExt])

    # If a compression application is specified, apply this.
    uncomprSize, archFileSize, mime, compression = compress(reqPropsObj, parDic)

    # Generate status + return.
    logger.debug("DAPI finished processing of file - returning to main application")
    return ngamsPlugInApi.genDapiSuccessStat(diskInfo.getDiskId(), relFilename,
                                             dpId, fileVersion, mime,
                                             archFileSize, uncomprSize,
                                             compression, relPath,
                                             diskInfo.getSlotId(), fileExists,
                                             complFilename)
