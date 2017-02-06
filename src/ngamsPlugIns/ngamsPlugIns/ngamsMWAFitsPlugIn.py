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
"""
This Data Archiving Plug-In is used to handle reception and processing
of MWA FITS files.

Note, that the plug-in is implemented for the usage at ESO. If used in other
contexts, a dedicated plug-in matching the individual context should be
implemented and NG/AMS configured to use it.
"""

import commands
import logging
import os
import string
import time

from ngamsLib import ngamsPlugInApi
from ngamsLib.ngamsCore import TRACE, genLog
from ngamsPlugIns import ngamsFitsPlugIn


logger = logging.getLogger(__name__)

def getComprExt(comprMethod):
    """
    Determine the extension for the given type of compression specified.

    comprMethod:   Compression method e.g. 'compress' or 'gzip' (string).

    Returns:       Extension used by the given compression method (string).
    """
    T = TRACE()

    if (comprMethod.find("compress") != -1):
        return "Z"
    elif (comprMethod.find("gzip") != -1):
        return "gz"
    elif (comprMethod.find("ngamsTileCompress") != -1):
        return ""
    elif (comprMethod == ""):
        return ""
    else:
        errMsg = "Unknown compression method specified: " + comprMethod
        errMsg = genLog("NGAMS_ER_DAPI", [errMsg])
        raise Exception, errMsg


def getDpIdInfo(filename):
    """
    Generate the File ID (here DP ID) for the file.

    filename:   Name of FITS file (string).

    Returns:    Tuple containing the value of ARCFILE, the DP ID
                of the file, and the JD date. The two latter deducted from
                the ARCFILE keyword (tuple).
    """
    try:
        keyDic  = ngamsFitsPlugIn.getFitsKeys(filename, ["PROJID"])
        projectId = keyDic["PROJID"][0]

        return projectId
    except:
        err = "Did not find keyword PROJID in FITS file or PROJID illegal"
        errMsg = genLog("NGAMS_ER_DAPI_BAD_FILE", [os.path.basename(filename),
                                                   "ngamsMWAFitsPlugIn", err])
        raise Exception, errMsg


def checkFitsFileSize(filename):
    """
    Check if the size of the FITS file is a multiple of 2880. If this
    is not the case, we through an exception.

    filename:   FITS file to check (string).

    Returns:    Void.
    """
    if (string.split(filename, ".")[-1] == "fits"):
        size = ngamsPlugInApi.getFileSize(filename)
        testVal = (size / 2880.0)
        if (testVal != int(testVal)):
            errMsg = "The size of the FITS file issued " +\
                     "is not a multiple of 2880 (size: %d)! Rejecting file!"
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


def addFitsCheckSum(filename):
    """
    Add the FITS CHECKSUM/DATASUM in the given file.

    filename:  Complete name of the file to process (string).

    Returns:   Void.
    """
    cmd = "chksumAddFitsChksum %s" % filename
    stat, out = commands.getstatusoutput(cmd)
    if (stat != 0):
        errMsg = "Problem adding DATASUM/CHECKSUM keyword to file: " +\
                 filename + ". Error: " + out
        errMsg = genLog("NGAMS_ER_DAPI", [errMsg])
        raise Exception, errMsg


def prepFile(reqPropsObj,
             parDic):
    """
    Prepare the file. If it is compressed, decompress it into a temporary
    filename. Check the FITS file size and the FITS checksum if it exists.

    reqPropsObj:  NG/AMS request properties object (ngamsReqProps).

    parDic:       Dictionary with parameters for the DAPI. This is generated
                  with ngamsPlugInApi.parseDapiPlugInPars() (Dictionary).

    Returns:      Tuple containing:

                    (<DP ID>, <Date Obs. Night>, <Compr. Ext.>)   (tuple).
    """
    T = TRACE()

    # If the file is already compressed, we have to decompress it.
    tmpFn = reqPropsObj.getStagingFilename()
    if ((tmpFn.find(".Z") != -1) or (tmpFn.find(".gz") != -1)):
        ngamsPlugInApi.execCmd("gunzip " + tmpFn)
        reqPropsObj.setStagingFilename(os.path.splitext(tmpFn)[0])
    checkFitsFileSize(reqPropsObj.getStagingFilename())
    #checkChecksum(parDic, reqPropsObj.getStagingFilename())
    if 'skip_checksum' not in parDic:
        checkFitsChecksum(reqPropsObj, reqPropsObj.getStagingFilename())
    if (parDic.has_key("compression")):
        comprExt = getComprExt(parDic["compression"])
    else:
        comprExt = ""
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

    # If a compression application is specified, apply this.
    uncomprSize = ngamsPlugInApi.getFileSize(stFn)
    if (parDic["compression"] != ""):
        logger.debug("Compressing file using: %s", parDic["compression"])
        compress_start = time.time()
        exitCode, stdOut =\
                  ngamsPlugInApi.execCmd(parDic["compression"] + " " + stFn)
        if (exitCode != 0):
            errMsg = "ngamsFitsPlugIn: Problems during archiving! " +\
                     "Compressing the file failed"
            raise Exception, errMsg

        comprExt = getComprExt(parDic["compression"])
        if (comprExt != ""): stFn = stFn + "." + comprExt
        # Remember to update Staging Filename in the Request Properties Object.
        reqPropsObj.setStagingFilename(stFn)
        if (parDic["compression"].find("compress") != -1):
            fmt = "application/x-cfits"
        elif (parDic["compression"].find("ngamsTileCompress") != -1):
            fmt = "image/x-fits"
        else:
            fmt = "application/x-gfits"
        logger.debug("File compressed. Time: %.3fs", time.time() - compress_start)
    else:
        fmt = reqPropsObj.getMimeType()

    archFileSize = ngamsPlugInApi.getFileSize(reqPropsObj.getStagingFilename())
    return uncomprSize, archFileSize, fmt


# DAPI function.
def ngamsMWAFitsPlugIn(srvObj,
                    reqPropsObj):
    """
    Data Archiving Plug-In to handle archiving of MWA FITS files.

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
    ### not done for MWA
    # dpId, dateDirName, comprExt = prepFile(reqPropsObj, parDic)

    comprExt = getComprExt(parDic["compression"])

    # Get various information about the file being handled.
    projectId = getDpIdInfo(reqPropsObj.getStagingFilename())
    checkFitsFileSize(reqPropsObj.getStagingFilename())
    fileName = os.path.basename(reqPropsObj.getFileUri())
    dpId = os.path.splitext(fileName)[0]

    dateDirName = projectId
    fileVersion, relPath, relFilename,\
                 complFilename, fileExists =\
                 ngamsPlugInApi.genFileInfo(srvObj.getDb(), srvObj.getCfg(),
                                            reqPropsObj, diskInfo,
                                            reqPropsObj.getStagingFilename(),
                                            dpId, dpId, [dateDirName],
                                            [comprExt])

    # If a compression application is specified, apply this.
    uncomprSize, archFileSize, fmt = compress(reqPropsObj, parDic)

    # Generate status + return.
    logger.debug("DAPI finished processing of file - returning to main application")
    return ngamsPlugInApi.genDapiSuccessStat(diskInfo.getDiskId(), relFilename,
                                             dpId, fileVersion, fmt,
                                             archFileSize, uncomprSize,
                                             parDic["compression"], relPath,
                                             diskInfo.getSlotId(), fileExists,
                                             complFilename)


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print "Usage: ngamsMWAFitsPlugin.py <test_file>"
        sys.exit()
    try:
        fo = open(sys.argv[1],'r')
# Todo: Implement srvObj and reqPropsObj generation convenience functions.
#        status = ngamsMWAFitsPlugIn(srvObj, reqPropsObj)
        fo.close()
    except:
        raise



# EOF
