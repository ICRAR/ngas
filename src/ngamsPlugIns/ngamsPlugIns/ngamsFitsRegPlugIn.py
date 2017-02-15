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
# "@(#) $Id: ngamsFitsRegPlugIn.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  10/05/2001  Created
#
"""
This Data Register Plug-In is used to handle the registration of FITS files
already stored on an 'NGAS disk', which just need to be registered in the DB.

Note, that the plug-in is implemented for the usage at ESO. If used in other
contexts, a dedicated plug-in matching the individual context should be
implemented and NG/AMS configured to use it.
"""
# Data Registration Function.

import logging

import ngamsFitsPlugIn
from ngamsLib import ngamsPlugInApi
from ngamsLib.ngamsCore import rmFile


logger = logging.getLogger(__name__)

def ngamsFitsRegPlugIn(srvObj,
                       reqPropsObj):
    """
    Data Registration Plug-In to handle registration of FITS files.

    srvObj:       Reference to NG/AMS Server Object (ngamsServer).

    reqPropsObj:  NG/AMS request properties object (ngamsReqProps).

    Returns:      Standard NG/AMS Data Archiving Plug-In Status as generated
                  by: ngamsPlugInApi.genDapiSuccessStat() (ngamsDapiStatus).
    """
    logger.info("Plug-In registering file with URI: %s", reqPropsObj.getFileUri())
    diskInfo = reqPropsObj.getTargDiskInfo()
    parDic = ngamsPlugInApi.parseRegPlugInPars(srvObj.getCfg(),
                                               reqPropsObj.getMimeType())
    stageFile = reqPropsObj.getStagingFilename()

    # If the file is already compressed, we have to decompress it.
    procDir = ""
    if ((stageFile.find(".Z") != -1) or (stageFile.find(".gz") != -1)):
        workingFile, procDir = ngamsPlugInApi.prepProcFile(srvObj.getCfg(),
                                                           stageFile)
        ngamsPlugInApi.execCmd("gunzip " + workingFile)
        if (workingFile.find(".Z") != -1):
            workingFile = workingFile[:-2]
        else:
            workingFile = workingFile[:-3]
    else:
        workingFile = stageFile

    # Check file (size + checksum).
    ngamsFitsPlugIn.checkFitsFileSize(workingFile)
    #ngamsFitsPlugIn.c_heckChecksum(parDic, workingFile)
    if 'skip_checksum' not in parDic:
        ngamsFitsPlugIn.checkFitsChecksum(reqPropsObj, workingFile)

    # Get various information about the file being handled.
    arcFile, dpId, dateDirName = ngamsFitsPlugIn.getDpIdInfo(workingFile)
    fileVersion, relPath, relFilename,\
                 complFilename, fileExists =\
                 ngamsPlugInApi.genFileInfoReg(srvObj.getDb(), srvObj.getCfg(),
                                               reqPropsObj, diskInfo,
                                               stageFile, dpId)

    # Generate status.
    logger.debug("Generating status ...")
    fileSize = ngamsPlugInApi.getFileSize(stageFile)
    if (stageFile.find(".Z") != -1):
        format = "application/x-cfits"
        compresion = "compress"
    elif (stageFile.find(".gz") != -1):
        format = "application/x-gfits"
        compresion = "gzip"
    else:
        format = "image/x-fits"
        compresion = ""
    uncomprSize = ngamsPlugInApi.getFileSize(workingFile)

    # Delete the processing directory (would be done later by the
    # Janitor Thread, but it is better to clean up explicitly).
    if (procDir): rmFile(procDir)

    logger.debug("Register Plug-In finished processing of file")
    return ngamsPlugInApi.genRegPiSuccessStat(diskInfo.getDiskId(),relFilename,
                                              dpId, fileVersion, format,
                                              fileSize, uncomprSize,compresion,
                                              relPath, diskInfo.getSlotId(),
                                              fileExists, complFilename)


# EOF
