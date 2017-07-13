#
#    ALMA - Atacama Large Millimiter Array
#    (c) European Southern Observatory, 2002
#    Copyright by ESO (in the framework of the ALMA collaboration),
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
# "@(#) $Id: ngamsDAPIMirroring.py,v 1.5 2012/01/30 19:06:15 amanning Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# awicenec  2008/04/10  Created
#
"""
This Data Archiving Plug-In is used to handle reception and processing
of SDM multipart related message files containing Content-Location UIDs.

Note, that the plug-in is implemented for the usage for ALMA. If used in other
contexts, a dedicated plug-in matching the individual context should be
implemented and NG/AMS configured to use it.
"""

import logging
import os

from ngamsLib import ngamsPlugInApi
from ngamsLib.ngamsCore import genLog, toiso8601, FMT_DATE_ONLY


logger = logging.getLogger(__name__)

_PLUGIN_ID = __name__

def specificTreatment(fo):
    """
    Method contains the specific treatment of the file passed from NG/AMS.

    fo:         File object

    Returns:    (file_id, finalFileName, type);
                The finalFileName is a string containing the name of the final
                file without extension. type is the mime-type from the header.
    """
    import rfc822, cgi, re
    _EXT = '.msg'

    filename = fo.name

    uidTempl = re.compile("^[uU][iI][dD]://[aAbBcCzZxX][0-9,a-z,A-Z]+(/[xX][0-9,a-z,A-Z]+){2}(#\w{1,}|/\w{0,}){0,}$")

    try:
        message = rfc822.Message(fo)
        type, tparams = cgi.parse_header(message["Content-Type"])
    except Exception, e:
        err = "Parsing of mime message failed: " + str(e)
        errMsg = genLog("NGAMS_ER_DAPI_BAD_FILE", [os.path.basename(filename),_PLUGIN_ID, err])
        raise Exception, errMsg
    try:
        almaUid = message["alma-uid"]
    except:
        try:
            almaUid = message["Content-Location"]
        except:
            err = "Mandatory alma-uid or Content-Location parameter not found in mime header!"
            errMsg = genLog("NGAMS_ER_DAPI_BAD_FILE", [os.path.basename(filename),_PLUGIN_ID, err])
            raise Exception, errMsg

    if not uidTempl.match(almaUid):
        err = "Invalid alma-uid found in Content-Location: " + almaUid
        errMsg = genLog("NGAMS_ER_DAPI_BAD_FILE", [os.path.basename(filename),_PLUGIN_ID, err])
        raise Exception, errMsg

    try:
        almaUid = almaUid.split('//',2)[1].split('#')[0]
        if almaUid[-1] == '/': almaUid = almaUid[:-1]

        fileId = almaUid
        finalFileName = almaUid.replace('/',':')

    except Exception, e:
        err = "Problem constructing final file name: " + str(e)
        errMsg = genLog("NGAMS_ER_DAPI_BAD_FILE", [os.path.basename(filename),_PLUGIN_ID, err])
        raise Exception, errMsg


    return (fileId, finalFileName, type)


def genFileInfo(dbConObj,
                ngamsCfgObj,
                reqPropsObj,
                trgDiskInfoObj,
                stagingFilename,
                fileId,
                fileVersion,
                baseFilename,
                subDirs = [],
                addExts = []):

    relPath = ngamsCfgObj.getPathPrefix()
    for subDir in subDirs:
        if (relPath != ""): relPath += "/" + subDir
    relPath = relPath + "/" + str(fileVersion)
    relPath = relPath.strip("/")
    complPath = os.path.normpath(trgDiskInfoObj.getMountPoint()+"/"+relPath)
    ext = os.path.basename(stagingFilename).split(".")[-1]
    newFilename = baseFilename
    if not baseFilename.endswith(ext):
        newFilename = newFilename + "." + ext
    for addExt in addExts:
        if (addExt.strip() != ""): newFilename += "." + addExt
    complFilename = os.path.normpath(complPath + "/" + newFilename)
    relFilename = os.path.normpath(relPath + "/" + newFilename)
    logger.debug("Target name for file is: %s", complFilename)

    # We already know that the file does not exist
    fileExists = 0

    return [relPath, relFilename, complFilename, fileExists]

def ngamsGeneric(srvObj,reqPropsObj):
    """
    Data Archiving Plug-In to handle archiving of SDM multipart related
    message files containing ALMA UIDs in the Content-Location mime parameter
    or any other kind of file

    srvObj:       Reference to NG/AMS Server Object (ngamsServer).

    reqPropsObj:  NG/AMS request properties object (ngamsReqProps).

    Returns:      Standard NG/AMS Data Archiving Plug-In Status
                  as generated by: ngamsPlugInApi.genDapiSuccessStat()
                  (ngamsDapiStatus).
    """

    logger.debug("Mirroring plug-in handling data for file: %s", os.path.basename(reqPropsObj.getFileUri()))

    # Create the file
    diskInfo = reqPropsObj.getTargDiskInfo()
    stagingFilename = reqPropsObj.getStagingFilename()
    ext = os.path.splitext(stagingFilename)[1][1:]

    # reqPropsObj format: /MIRRARCHIVE?mime_type=application/x-tar&filename=...
    if (reqPropsObj.getMimeType()):
        format = reqPropsObj.getMimeType()
    else:
        errMsg = "mime_type not specified in MIRRARCHIVE request"
        raise Exception, errMsg

    # File Uri format: http://ngasbe03.aiv.alma.cl:7777/RETRIEVE?disk_id=59622720f79296473f6106c15e5c2240&host_id=ngasbe03:7777&quick_location=1&file_version=1&file_id=backup.2011-02-02T22:01:59.tar

    # Get file id
    fileVersion = reqPropsObj.fileinfo['fileVersion']
    fileId = reqPropsObj.fileinfo['fileId']

    # Specific treatment depending on the mime type
    if ((format.find("multipart") >= 0) or (format.find("multialma") >= 0)):
        logger.debug("applying plug-in specific treatment")
        fo = open(stagingFilename, "r")
        try:
            (fileId, finalName, format) = specificTreatment(fo)
        finally:
            fo.close()
    else:
        finalName = fileId

    logger.debug("File with URI %s is being handled by ngamsGeneric: format=%s file_id=%s file_version=%s finalName=%s",
                 reqPropsObj.getFileUri(), format, fileId, fileVersion, finalName)

    try:
        # Compression parameters
        uncomprSize = ngamsPlugInApi.getFileSize(stagingFilename)
        compression = ""

        # File name and paths
        date = toiso8601(fmt=FMT_DATE_ONLY)
        relPath,relFilename,complFilename,fileExists = genFileInfo(srvObj.getDb(),
                                                                   srvObj.getCfg(),
                                                                   reqPropsObj, diskInfo,
                                                                   stagingFilename, fileId, fileVersion,
                                                                   finalName, [date])
        # Make sure the format is defined
        if not format:
            format = ngamsPlugInApi.determineMimeType(srvObj.getCfg(),stagingFilename)

        # FileSize
        fileSize = ngamsPlugInApi.getFileSize(stagingFilename)

        # Return resDapi object
        return ngamsPlugInApi.genDapiSuccessStat(diskInfo.getDiskId(),
                                                 relFilename,
                                                 fileId, fileVersion, format,
                                                 fileSize, uncomprSize,
                                                 compression, relPath,
                                                 diskInfo.getSlotId(),
                                                 fileExists, complFilename)
    except Exception, e:
        err = "Problem processing file in stagging area: " + str(e)
        errMsg = genLog("NGAMS_ER_DAPI_BAD_FILE", [stagingFilename,_PLUGIN_ID, err])
        raise Exception, errMsg
