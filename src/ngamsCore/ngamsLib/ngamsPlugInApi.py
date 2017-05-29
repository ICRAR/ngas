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
# "@(#) $Id: ngamsPlugInApi.py,v 1.6 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  10/05/2001  Created
#

"""
Module containing functions to be used for implementing the different
types of NG/AMS plug-ins.
"""

import commands
import logging
import os
import shutil

from ngamsCore import NGAMS_SUCCESS, TRACE, checkCreatePath, execCmd as ngamsCoreExecCmd
import ngamsDapiStatus
import ngamsHighLevelLib, ngamsNotification
import ngamsLib


logger = logging.getLogger(__name__)

def genDapiSuccessStat(diskId,
                       relFilename,
                       fileId,
                       fileVersion,
                       format,
                       fileSize,
                       uncomprSize,
                       compression,
                       relPath,
                       slotId,
                       fileExists,
                       completeFilename):
    """
    Generates a plug-in status in a format expected by the NG/AMS Server.

    diskId:            Disk ID (string).

    relFilename:       Filename relative to mount point (string).

    fileId:            File ID (string).

    fileVersion:       Version of file (integer).

    format:            Format of file (string).

    fileSize:          File size in bytes (integer).

    uncomprSize:       Uncompressed size of file (integer).

    compression:       Compression applied on file if any (string).

    relPath:           Relative path of file (string).

    slotId:            Slot ID for disk where file was stored (string).

    fileExists:        1 if file already existed otherwise 0 (integer).

    completeFilename:  Complete name of file (string).

    Returns:           NG/AMS DAPI Status Object (ngamsDapiStatus).
    """
    return ngamsDapiStatus.ngamsDapiStatus().\
           setStatus(NGAMS_SUCCESS).setDiskId(diskId).\
           setRelFilename(relFilename).setFileId(fileId).\
           setFileVersion(fileVersion).\
           setFormat(format).setFileSize(fileSize).\
           setUncomprSize(uncomprSize).setCompression(compression).\
           setRelPath(relPath).setSlotId(slotId).\
           setFileExists(fileExists).setCompleteFilename(completeFilename)


def genRegPiSuccessStat(diskId,
                        relFilename,
                        fileId,
                        fileVersion,
                        format,
                        fileSize,
                        uncomprSize,
                        compression,
                        relPath,
                        slotId,
                        fileExists,
                        completeFilename):
    """
    Generates a plug-in status in a format expected by the NG/AMS Server.

    diskId:            Disk ID (string).

    relFilename:       Filename relative to mount point (string).

    fileId:            File ID (string).

    fileVersion:       Version of file (integer).

    format:            Format of file (string).

    fileSize:          File size in bytes (integer).

    uncomprSize:       Uncompressed size of file (integer).

    compression:       Compression applied on file if any (string).

    relPath:           Relative path of file (string).

    slotId:            Slot ID for disk where file was stored (string).

    fileExists:        1 if file already existed otherwise 0 (integer).

    completeFilename:  Complete name of file (string).

    Returns:           NG/AMS DAPI Status Object (ngamsDapiStatus).
    """
    return genDapiSuccessStat(diskId, relFilename, fileId, fileVersion,
                              format, fileSize, uncomprSize, compression,
                              relPath, slotId, fileExists, completeFilename)


def getFileSize(filename):
    """
    Get size of file referred.

    filename:   Filename - complete path (string).

    Returns:    File size (integer).
    """
    return int(os.stat(filename)[6])


def parseDapiPlugInPars(ngamsCfgObj,
                        mimeType):
    """
    Get the plug-in parameters for a Data Archiving Plug-In.

    ngamsCfgObj:  Instance of NG/AMS Configuration Class (ngamsConfig).

    mimeType:     Mime-type of request being handled (string).

    Returns:      Dictionary containing the parameters from the
                  plug-in parameters as keys referring to the corresponding
                  value of these (dictionary).
    """
    rawPars = ngamsCfgObj.getStreamFromMimeType(mimeType).getPlugInPars()
    return parseRawPlugInPars(rawPars)


def determineMimeType(ngamsCfgObj,
                      filename):
    """
    Determine mime-type of file.

    ngamsCfgObj:  Instance of NG/AMS Configuration Class (ngamsConfig).

    filename:     Filename (string).

    Return:       Mime-type (string).
    """
    return ngamsHighLevelLib.determineMimeType(ngamsCfgObj, filename)


def execCmd(cmd,
            timeOut = -1):
    """
    Execute the command given on the UNIX command line and returns a
    list with the cmd exit code and the output written on stdout and stderr.

    cmd:         Command to execute on the shell (string).

    timeOut:     Timeout waiting for the command. A timeout of "-1" means
                 that no timeout is applied (float).

    Returns:     Tuple with the exit code and output on stdout and stderr:

                   [<exit code>, <stdout>]
    """
    logger.debug("Executing command: %s", cmd)
    if (timeOut == -1):
        return commands.getstatusoutput(cmd)
    else:
        exitCode, stdout, stderr = ngamsCoreExecCmd(cmd, timeOut)
        return [exitCode, stdout]


def parseRawPlugInPars(rawPars):
    """
    Parse the plug-in parameters given in the NG/AMS Configuration
    and return a dictionary with the values.

    rawPars:    Parameters given in connection with the plug-in in
                the configuration file (string).

    Returns:    Dictionary containing the parameters from the
                plug-in parameters as keys referring to the corresponding
                value of these (dictionary).
    """
    return ngamsLib.parseRawPlugInPars(rawPars)


def notify(srvObj,
           type,
           subject,
           msg):
    """
    Send a notification e-mail to a subscriber about an event happening.

    srvObj:        Reference to the NGAS server object (ngamsServer).

    type:          Type of Notification (See NGAMS_NOTIF_* in ngams).

    subject:       Subject of message (string).

    msg:           Message to send (string).

    Returns:       Void.
    """
    ngamsNotification.notify(srvObj.getHostId(), srvObj.getCfg(), type, subject, msg)


def prepProcFile(ngamsCfgObj,
                 filename):
    """
    The function is used to create a copy of a file to be processed
    in the Processing Directory.

    It creates first a directory for this processing in the Processing
    Area, and afterwards it makes a copy of the file to be processed.
    Returned is a tuple with the complete filename of the copy of the
    file and the temporary processing directory.

    ngamsCfgObj:   Configuration object (ngamsConfig).

    filename:      Name of file to process (string).

    Returns:       Tuple with [<proc. filename>, <proc. dir.>]
                   (tuple of strings).
    """
    procDir = ngamsHighLevelLib.genProcDirName(ngamsCfgObj)
    checkCreatePath(procDir)
    procFilename = os.path.normpath("{0}/{1}".format(procDir, os.path.basename(filename)))
    if os.path.exists(filename):
        shutil.copyfile(filename, procFilename)
    else:
        open(procFilename, 'a').close()
    os.chmod(procFilename, 0775)
    return [procFilename, procDir]


def genFileInfo(dbConObj,
                ngamsCfgObj,
                reqPropsObj,
                trgDiskInfoObj,
                stagingFilename,
                fileId,
                baseFilename,
                subDirs = [],
                addExts = []):
    """
    Convenience function to generate paths, filename and version
    for the file. A tuple is returned containing the following
    information:

      [<file version>, <relative path>,
       <relative filename>, <complete filename>]

    dbConObj:          Instance of NG/AMS DB class (ngamsDb).

    ngamsCfgObj:       Instance of NG/AMS Configuration class (ngamsConfig).

    reqPropsObj:       NG/AMS request properties object (ngamsReqProps).

    diskDic:           Dictionary containing ngamsPhysDiskInfo
                       objects (dictionary).

    trgDiskInfoObj:    Disk Info Object for Target Disk (ngasmDiskInfo).

    stagingFilename:   Name of Staging File (string).

    fileId:            ID of file (string).

    baseFilename:      Desired basename of file (string).

    subDirs:           List of sub-directories to append after the
                       'mandatory' part of the path (list).

    addExts:           Additional extensions to append to the final
                       filename (list).

    Returns:           Tuple with information about file (tuple).
    """
    T = TRACE()

    if (reqPropsObj.hasHttpPar("file_version")):
        paraFV = int(reqPropsObj.getHttpPar("file_version"))
    else:
        paraFV = -1

    if (reqPropsObj.hasHttpPar("no_versioning")):
        noVersioning = int(reqPropsObj.getHttpPar("no_versioning"))
    else:
        noVersioning = 0

    if (not noVersioning): # no_versioning = 0 means do not overwrite
        if (paraFV > 0):
            # check if this version already exists
            if (dbConObj.getLatestFileVersion(fileId) >= paraFV):
                raise Exception("Version %d exists for file %s. Please use 'no_versioning=1' AND 'versioning=0' for overwrite." % (paraFV, fileId))
            fileVersion = paraFV
        else:
            fileVersion = ngamsHighLevelLib.getNewFileVersion(dbConObj, fileId)
    else: #no_versioning = 1 means overwrite either the latest or the specified version
        if (paraFV > 0):
            fileVersion = paraFV # could potentially replace this version (if it is there)
        else:
            fileVersion = dbConObj.getLatestFileVersion(fileId)
    if (fileVersion < 1): fileVersion = 1
    relPath = ngamsCfgObj.getPathPrefix()
    for subDir in subDirs:
        if (relPath != ""): relPath += "/" + subDir
    relPath = str(relPath + "/" + str(fileVersion)).strip("/")
    complPath = os.path.normpath(trgDiskInfoObj.getMountPoint()+"/"+relPath)
    newFilename = ngamsHighLevelLib.checkAddExt(ngamsCfgObj, reqPropsObj.getMimeType(),
                                  baseFilename)
    for addExt in addExts:
        if (addExt.strip() != ""): newFilename += "." + addExt
    complFilename = os.path.normpath(complPath + "/" + newFilename)
    relFilename = os.path.normpath(relPath + "/" + newFilename)
    logger.debug("Target name for file is: %s", complFilename)

    logger.debug("Checking if file exists ...")
    fileExists = ngamsHighLevelLib.\
                 checkIfFileExists(dbConObj, fileId,
                                   trgDiskInfoObj.getDiskId(),
                                   fileVersion, complFilename)
    logger.debug(" File existance (1 = existed): %s", str(fileExists))

    return [fileVersion, relPath, relFilename, complFilename, fileExists]


def genFileInfoReg(dbConObj,
                   ngamsCfgObj,
                   reqPropsObj,
                   hostDiskInfoObj,
                   filename,
                   fileId):
    """
    Convenience function to generate paths, filename and version
    for the file. A tuple is returned containing the following
    information:

      [<file version>, <relative path>,
       <relative filename>, <complete filename>]

    dbConObj:          Instance of NG/AMS DB class (ngamsDb).

    ngamsCfgObj:       Instance of NG/AMS Configuration class (ngamsConfig).

    reqPropsObj:       NG/AMS request properties object (ngamsReqProps).

    hostDiskInfoObj:   Disk Info Object for disk hosting the file
                       (ngamsDiskInfo).

    filename:          Name of file (string).

    fileId:            ID of file (string).

    Returns:           Tuple with information about file (tuple).
    """
    T = TRACE()

    if (reqPropsObj.hasHttpPar("no_versioning")):
        noVersioning = int(reqPropsObj.getHttpPar("no_versioning"))
    else:
        noVersioning = 0
    if (not noVersioning):
        fileVersion = ngamsHighLevelLib.getNewFileVersion(dbConObj, fileId)
    else:
        fileVersion = dbConObj.getLatestFileVersion(fileId)
    if (fileVersion < 1): fileVersion = 1

    relPath = filename[(len(hostDiskInfoObj.getMountPoint()) + 1):
                       (len(filename) - len(os.path.basename(filename)) - 1)]
    relFilename = filename[(len(hostDiskInfoObj.getMountPoint()) + 1):]
    complFilename = filename
    fileExists = ngamsHighLevelLib.\
                 checkIfFileExists(dbConObj, fileId,
                                   hostDiskInfoObj.getDiskId(),
                                   fileVersion, complFilename)

    return [fileVersion, relPath, relFilename, complFilename, fileExists]


def rmFile(filename):
    """
    Remove the file referenced.

    filename:   File to remove (string).

    Returns:    Void.
    """
    rmFile(filename)


def getTmpDir(ngamsCfgObj):
    """
    Get the NG/AMS Temporary Files Directory.

    ngamsCfgObj:   NG/AMS Configuration Object (ngamsConfig).

    Returns:       Name of temporary directory (string).
    """
    return ngamsHighLevelLib.getTmpDir(ngamsCfgObj)


# EOF
