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
import subprocess
import time

from ngamsLib import ngamsPlugInApi
from ngamsLib.ngamsCore import TRACE, genLog, toiso8601, FMT_DATE_ONLY


logger = logging.getLogger(__name__)

TARG_MIME_TYPE  = "target_mime_type"
FILE_ID         = "file_id"
VERSIONING      = "versioning"
COMPRESSION     = "compression"
COMPRESSION_EXT = "compression_ext"

# Constants.
NO_COMPRESSION  = "NONE"


def extract_compression_params(reqPropsObj, plugin_pars):

    plugin_pars[COMPRESSION]     = None
    plugin_pars[COMPRESSION_EXT] = None

    if COMPRESSION in reqPropsObj:
        plugin_pars[COMPRESSION] = reqPropsObj[COMPRESSION]
    if COMPRESSION_EXT in reqPropsObj:
        plugin_pars[COMPRESSION_EXT] = reqPropsObj[COMPRESSION_EXT]
    if ((plugin_pars[COMPRESSION] and plugin_pars[COMPRESSION_EXT] is None) or
        (not plugin_pars[COMPRESSION] and plugin_pars[COMPRESSION_EXT] is not None)):
        raise Exception, genLog("NGAMS_ER_DAPI",
                                ["Parameters compression and compression_ext"
                                 "must be given together."])

def handlePars(reqPropsObj,
               parDic):
    """
    Parse/handle the HTTP parameters.

    reqPropsObj:  NG/AMS request properties object (ngamsReqProps).

    parDic:       Dictionary with the parameters (dictionary).

    Returns:      Void.
    """
    T = TRACE()

    # Get parameters.
    logger.debug("Get request parameters")
    parDic[TARG_MIME_TYPE]  = None
    parDic[FILE_ID]         = None
    parDic[VERSIONING]      = 1

    if (reqPropsObj.hasHttpPar(TARG_MIME_TYPE)):
        parDic[TARG_MIME_TYPE] = reqPropsObj.getHttpPar(TARG_MIME_TYPE)

    # If the file_id is not given, we derive it from the name of the URI.
    if (reqPropsObj.hasHttpPar(FILE_ID)):
        parDic[FILE_ID] = reqPropsObj.getHttpPar(FILE_ID)
    if (not parDic[FILE_ID]):
        if (reqPropsObj.getFileUri().find("file_id=") > 0):
            file_id = reqPropsObj.getFileUri().split("file_id=")[1]
            parDic[FILE_ID] = os.path.basename(file_id)
            logger.info("No file_id given, but found one in the URI: %s", parDic[FILE_ID])
        else:
            parDic[FILE_ID] = os.path.basename(reqPropsObj.getFileUri())
            logger.info("No file_id given, using basename of fileUri: %s", 
                 parDic[FILE_ID])

    # TODO: It would seem like 'versioning' is not used anywhere,
    #       but still gets more priority than 'no_versioning'.
    #       which is used everywhere
    if VERSIONING in reqPropsObj:
        parDic[VERSIONING] = int(reqPropsObj[VERSIONING])
    elif 'no_versioning' in reqPropsObj:
        parDic[VERSIONING] = 0 if int(reqPropsObj['no_versioning']) else 1

    # Set also the no_versioning parameter for backwards compatibility reasons
    if (parDic[VERSIONING]):
        reqPropsObj.addHttpPar("no_versioning", "0")
    else:
        reqPropsObj.addHttpPar("no_versioning", "1")

    extract_compression_params(reqPropsObj, parDic)

def _compress_data(plugin_pars):
    return plugin_pars[COMPRESSION]

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
    T = TRACE()

    stFn = reqPropsObj.getStagingFilename()

    # If a compression application is specified, apply this.
    uncomprSize = ngamsPlugInApi.getFileSize(stFn)
    comprExt = ""
    if _compress_data(parDic):
        logger.debug("Compressing file using: %s ...", parDic[COMPRESSION])
        compCmd = "%s %s" % (parDic[COMPRESSION], stFn)
        compress_start = time.time()
        logger.debug("Compressing file with command: %s", compCmd)
        with open(os.devnull, 'w') as f:
            exitCode = subprocess.call([parDic[COMPRESSION], stFn], stdout = f, stderr = f)
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
    T = TRACE()

    filename2, ext1 = os.path.splitext(complFilename)
    filename3, ext2 = os.path.splitext(filename2)
    if ((ext1 != "") and (ext1 == ext2)):
        # Remove one of the extensions.
        complFilename = complFilename[0:-len(ext1)]
        relFilename = relFilename[0:-len(ext1)]

    return (complFilename, relFilename)

# Signals the server whether this plug-in modifies its incoming contents (or not)
def modifies_content(srvObj, reqPropsObj):
    plugin_pars = {}
    extract_compression_params(reqPropsObj, plugin_pars)
    return _compress_data(plugin_pars)


def ngamsGenDapi(srvObj,
                 reqPropsObj):
    """
    Generic Data Archiving Plug-In to handle archiving of any file.

    srvObj:       Reference to NG/AMS Server Object (ngamsServer).

    reqPropsObj:  NG/AMS request properties object (ngamsReqProps).

    Returns:      Standard NG/AMS Data Archiving Plug-In Status
                  as generated by: ngamsPlugInApi.genDapiSuccessStat()
                  (ngamsDapiStatus).
    """
    T = TRACE()

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
        return ngamsPlugInApi.genDapiSuccessStat(diskInfo.getDiskId(),
                                                 relFilename,
                                                 parDic[FILE_ID],
                                                 fileVersion, format,
                                                 archFileSize, uncomprSize,
                                                 compression, relPath,
                                                 diskInfo.getSlotId(),
                                                 fileExists, complFilename)
    except Exception, e:
        msg = "Error occurred in DAPI: %s" % str(e)
        logger.error(msg)
        raise Exception(genLog("NGAMS_ER_DAPI_RM", [msg]))

# EOF
