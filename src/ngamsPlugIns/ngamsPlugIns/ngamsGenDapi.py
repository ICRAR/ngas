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
# ******************************************************************************
#
# "@(#) $Id: ngamsGenDapi.py,v 1.3 2010/05/21 12:28:12 jagonzal Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  30/03/2006  Created
# jknudstr  12/09/2006  Gave overhaul
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

compression:        Command used to compress the file. If not given the file is
                    not compressed (optional (**)).

compression_ext:    Extension resulting from applying the specified compression
                    tool on the file (optional (**)).

uncompressed_file_size: If the file is already compressed, this parameter can be used
                        to specify the file size before compression so NGAS can save this
                        information in the DB (optional (**)).

*/**: These parameters must be given in pairs, it is not possible only to
      specify one of them.
"""
# Parameters.

import logging
import os
import subprocess
import time

from ngamsLib import ngamsPlugInApi, ngamsLib
from ngamsLib.ngamsCore import genLog, toiso8601, FMT_DATE_ONLY

logger = logging.getLogger(__name__)

TARG_MIME_TYPE = "target_mime_type"
FILE_ID = "file_id"
COMPRESSION = "compression"
COMPRESSION_EXT = "compression_ext"
UNCOMPRESSED_FILE_SIZE = "uncompressed_file_size"

# Constants.
NO_COMPRESSION = "NONE"


def get_req_param(reqPropsObj, param, default):
    return reqPropsObj[param] if param in reqPropsObj else default


def is_compression_defined(compression):
    return compression is not None and compression.upper() != NO_COMPRESSION


def extract_compression_params(reqPropsObj, plugin_pars):
    plugin_pars[COMPRESSION] = get_req_param(reqPropsObj, COMPRESSION, None)
    plugin_pars[COMPRESSION_EXT] = get_req_param(reqPropsObj, COMPRESSION_EXT, None)
    uncomprSize = get_req_param(reqPropsObj, UNCOMPRESSED_FILE_SIZE, None)
    plugin_pars[UNCOMPRESSED_FILE_SIZE] = int(uncomprSize) if uncomprSize else None

    if is_compression_defined(plugin_pars[COMPRESSION]) and not (plugin_pars[COMPRESSION_EXT] or uncomprSize):
        raise Exception(genLog("NGAMS_ER_DAPI", ["Parameter 'compression' requires 'compression_ext' "
                                                 "or 'uncompressed_file_size'"]))


def handlePars(reqPropsObj, parDic):
    """
    Parse/handle the HTTP parameters.

    reqPropsObj:  NG/AMS request properties object (ngamsReqProps).

    parDic:       Dictionary with the parameters (dictionary).

    Returns:      Void.
    """
    # Get parameters.
    logger.debug("Get request parameters")
    parDic[TARG_MIME_TYPE] = None
    parDic[FILE_ID] = None

    if reqPropsObj.hasHttpPar(TARG_MIME_TYPE):
        parDic[TARG_MIME_TYPE] = reqPropsObj.getHttpPar(TARG_MIME_TYPE)

    # If the file_id is not given, we derive it from the name of the URI.
    if reqPropsObj.hasHttpPar(FILE_ID):
        parDic[FILE_ID] = reqPropsObj.getHttpPar(FILE_ID)
    if not parDic[FILE_ID]:
        if reqPropsObj.getFileUri().find("file_id=") > 0:
            file_id = reqPropsObj.getFileUri().split("file_id=")[1]
            parDic[FILE_ID] = os.path.basename(file_id)
            logger.info("No file_id given, but found one in the URI: %s", parDic[FILE_ID])
        else:
            parDic[FILE_ID] = os.path.basename(reqPropsObj.getFileUri())
            logger.info("No file_id given, using basename of fileUri: %s",
                        parDic[FILE_ID])

    extract_compression_params(reqPropsObj, parDic)


def _compress_data(plugin_pars):
    return is_compression_defined(plugin_pars[COMPRESSION]) and plugin_pars[COMPRESSION_EXT]


def _already_compressed_data(plugin_pars):
    return is_compression_defined(plugin_pars[COMPRESSION]) and plugin_pars[UNCOMPRESSED_FILE_SIZE]


def compress_file(srvObj,
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
    compression = NO_COMPRESSION
    comprExt = ""
    uncomprSize = ngamsPlugInApi.getFileSize(stFn)
    if parDic[TARG_MIME_TYPE]:
        mime_type = parDic[TARG_MIME_TYPE]
    else:
        mime_type = reqPropsObj.getMimeType()
    if _compress_data(parDic):
        logger.debug("Compressing file using: %s ...", parDic[COMPRESSION])
        compCmd = "%s %s" % (parDic[COMPRESSION], stFn)
        compress_start = time.time()
        logger.debug("Compressing file with command: %s", compCmd)
        with open(os.devnull, 'w') as f:
            exitCode = subprocess.call([parDic[COMPRESSION], stFn], stdout=f, stderr=f)
        # If the compression fails, assume that it is because the file is not
        # compressible (although it could also be due to lack of disk space).
        if exitCode == 0:
            if parDic[COMPRESSION_EXT]:
                stFn = stFn + "." + parDic[COMPRESSION_EXT]
                comprExt = parDic[COMPRESSION_EXT]
            # Update Staging Filename in the Request Properties Object
            reqPropsObj.setStagingFilename(stFn)

            # Handle mime-type
            if parDic[TARG_MIME_TYPE]:
                mime_type = parDic[TARG_MIME_TYPE]
            else:
                mime_type = ngamsPlugInApi.determineMimeType(srvObj.getCfg(), stFn)
            compression = parDic[COMPRESSION]

            logger.debug("File compressed. Time: %.3fs", time.time() - compress_start)
        else:
            # Carry on with the original file. We take the original mime-type
            # as the target mime-type.
            mime_type = reqPropsObj.getMimeType()
            compression = NO_COMPRESSION
    elif _already_compressed_data(parDic):
        compression = parDic[COMPRESSION]
        uncomprSize = parDic[UNCOMPRESSED_FILE_SIZE]
        logger.debug("Already compressed file: %s '%s' %s '%d'", COMPRESSION, compression,
                     UNCOMPRESSED_FILE_SIZE, uncomprSize)

    archFileSize = ngamsPlugInApi.getFileSize(reqPropsObj.getStagingFilename())
    return uncomprSize, archFileSize, mime_type, compression, comprExt


def checkForDblExt(complFilename, relFilename):
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
    complFilename = ngamsLib.remove_duplicated_extension(complFilename)
    relFilename = ngamsLib.remove_duplicated_extension(relFilename)
    return complFilename, relFilename


# Signals the server whether this plug-in modifies its incoming contents (or not)
def modifies_content(srvObj, reqPropsObj):
    plugin_pars = {}
    extract_compression_params(reqPropsObj, plugin_pars)
    return _compress_data(plugin_pars)


def ngamsGenDapi(srvObj, reqPropsObj):
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
    baseFilename = os.path.basename(reqPropsObj.getFileUri())
    logger.debug("Plug-In handling data for file: %s", baseFilename)
    try:
        parDic = {}
        handlePars(reqPropsObj, parDic)
        diskInfo = reqPropsObj.getTargDiskInfo()

        # Generate file information.
        logger.debug("Generate file information")
        dateDir = toiso8601(fmt=FMT_DATE_ONLY)
        fileVersion, relPath, relFilename, complFilename, fileExists = \
            ngamsPlugInApi.genFileInfo(srvObj.getDb(),
                                       srvObj.getCfg(),
                                       reqPropsObj, diskInfo,
                                       reqPropsObj.getStagingFilename(),
                                       parDic[FILE_ID],
                                       baseFilename, [dateDir])
        complFilename, relFilename = checkForDblExt(complFilename,
                                                    relFilename)

        # Compress the file if requested.
        uncomprSize, archFileSize, mime_type, compression, comprExt = \
            compress_file(srvObj, reqPropsObj, parDic)
        if comprExt != "":
            complFilename += ".%s" % comprExt
            relFilename += ".%s" % comprExt

        logger.debug("DAPI finished processing file - returning to host application")
        return ngamsPlugInApi.genDapiSuccessStat(diskInfo.getDiskId(),
                                                 relFilename,
                                                 parDic[FILE_ID],
                                                 fileVersion, mime_type,
                                                 archFileSize, uncomprSize,
                                                 compression, relPath,
                                                 diskInfo.getSlotId(),
                                                 fileExists, complFilename)
    except Exception as e:
        msg = "Error occurred in DAPI: %s" % str(e)
        logger.error(msg)
        raise Exception(genLog("NGAMS_ER_DAPI_RM", [msg]))

# EOF
