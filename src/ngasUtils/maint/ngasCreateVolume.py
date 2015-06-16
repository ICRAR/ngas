import string
import sys, os, time

from ngamsLib import ngamsDiskInfo, ngamsDb, ngamsConfig, ngamsFileInfo
from ngamsLib.ngamsCore import getFileSize, info, setDebug, setLogCond
from ngamsServer import ngamsJanitorThread
from pccUt import PccUtTime


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
# "@(#) $Id: ngasCreateVolume.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  26/07/2005  Created
#
_doc =\
"""
The tool is used to create a set of files on an NGAS Disk as though these
were archived through NGAS. For the moment only FITS files can be created.
These are archived in a standard way.


The pre-defined input parameters to the tool are:

%s

"""




# Constants.

# Definition of predefined command line parameters.
_testPars = [["_startTime", time.time(), "BUILT-IN",
              "Internal Parameter: Start time for running the System Checks."],
             ["BASEFILE", None, "MANDATORY",
              "File used as basis for generating the simulated, archived " +\
              "files."],
             ["DB", None, "OPTIONAL",
              "DB connection given in the format: <Srv>:<DB>:<User>:<Pwd>. " +\
              "If given, the files 'archived' will be registered in the " +\
              "DB as well as in the DB Snapshot."],            
             ["MOUNTPOINT", None, "MANDATORY",
              "Mount point of the disk. The disk should be mounted when " +\
              "running the tool."],
             ["NOOFFILES", 1, "MANDATORY",
              "The number of files that should be 'archived' (>0)."],
             ["CREATELINK", 0, "OPTIONAL",
              "Rather than creating copies of the file to be 'archived' " +\
              "links from the source file to the 'archived files' are " +\
              "created."]]
_testParDic = {}
_parFormat = "%s [%s] (%s):\n"
_parDoc = ""
for parInfo in _testPars:
    _testParDic[parInfo[0]] = parInfo[1]
    if (parInfo[0][0] != "_"):
        _parDoc += _parFormat % (parInfo[0], str(parInfo[1]), parInfo[2])
        _parDoc += parInfo[3] + "\n\n"
__doc__ = _doc % _parDoc


def testParDic():
    """
    Return reference to test parameter dictionary.

    Returns:  Reference to test parameters dictionary (dictionary).
    """
    return _testParDic


def correctUsage():
    """
    Return the usage/online documentation in a string buffer.

    Returns:  Man-page (string).
    """
    return __doc__


def _encFileInfo(dbSnapshot,
                 fileInfo):
    """
    See ngamsJanitorThread._encFileInfo().
    """
    from ngamsLib import ngamsDb, ngamsDbCore
    tmpDic = {}
    for n in range(ngamsDb.NGAS_FILES_CREATION_DATE + 1):
        colName = ngamsDbCore._ngasFilesNameMap[n]
        colId = ngamsJanitorThread._encName(dbSnapshot, colName)
        tmpDic[colId] = fileInfo[n]
    return tmpDic


def updateDbSnapshot(dbSnapshotDbm,
                     fileInfoList):
    """
    Update the DB Snapshot DBM.

    dbSnapshotDbm:    DB Snapshot DBM (open) (ngamsDbm).

    fileInfoList:     List with file info as read from ngas_files (list).

    Returns:          Void.
    """
    encFileInfo = _encFileInfo(dbSnapshotDbm, fileInfoList)
    fileKey = ngamsJanitorThread._genFileKey(fileInfoList)
    ngamsJanitorThread._addInDbm(dbSnapshotDbm, fileKey, encFileInfo)


def createFitsFiles(testParDic):
    """
    Create the test 'archived' files according to the specified parameters.
    This function handles FITS files.

    Note:

    - The files is archived as it is. I.e., if it is not compressed, it
      remains like that.

    - Even if rearchiving the same file with new File IDs, no parameters of
      the header are updated.

    - It is assumed that the file has a valid FITS checksum.

    - It is assumed that the file has a valid ARCFILE.

    - Only new versions are created. I.e., the File ID is incremented at
      each iteration.

    testParDic:  Dictionary with parameters for running the test (dictionary).

    Returns:   Void.
    """
    from ngamsPlugIns import ngamsFitsPlugIn
    
    filesPerDay = 10000  # Max. number of files to generate per day.
    baseFile = testParDic["BASEFILE"]
    ext = baseFile.split(".")[-1]
    if ((ext == "Z") or (ext == "gz")):
        try:
            tmpFile1 = "/tmp/ngasCreateVolume_%f" % time.time()
            tmpFile2 = "%s.%s" % (tmpFile1, ext)
            os.system("cp %s %s" % (baseFile, tmpFile2))
            os.system("gunzip %s" % tmpFile2)
            arcFile, dpId, dateDir = ngamsFitsPlugIn.getDpIdInfo(tmpFile1)
            uncomprSize = getFileSize(tmpFile1)
            os.system("rm -f %s*" % tmpFile1)
        except Exception, e:
            os.system("rm -f %s*" % tmpFile1)
            raise e
    else:
        arcFile, dpId, dateDir = ngamsFitsPlugIn.getDpIdInfo(baseFile)
        uncomprSize = getFileSize(baseFile)
    insId = arcFile.split(".")[0]
    isoTime = arcFile[(arcFile.find(".") + 1):]
    baseTimeMjd = PccUtTime.TimeStamp(isoTime).getMjd()

    # Generate 'static' file info for this run.
    ngasDiskInfo = os.path.normpath("%s/NgasDiskInfo" %\
                                    testParDic["MOUNTPOINT"])
    ngasDiskInfoDoc = open(ngasDiskInfo).read()
    diskInfoObj = ngamsDiskInfo.ngamsDiskInfo().\
                  unpackXmlDoc(ngasDiskInfoDoc, ignoreVarDiskPars=1)
    diskId = diskInfoObj.getDiskId()
    fileVer = 1
    if (ext == "Z"):
        format = "application/x-cfits"
        compression = "compress -f"
    elif(ext == "gz"):
        format = "application/x-gfits"
        compression = "gzip"
    else:
        format = "image/x-fits"
        compression = "NONE"
    archFileSize = getFileSize(baseFile)
    ignore = 0
    checksumPlugIn = "ngamsGenCrc32"
    exec "import " + checksumPlugIn
    checksum = eval(checksumPlugIn + "." + checksumPlugIn +\
                    "(None, baseFile, 0)")
    fileStatus = "00000000"

    # Open DB connection if specified.
    if (testParDic["DB"] != None):
        dbSrv, dbName, dbUser, dbPwd = testParDic["DB"].split(":")
        dbCon = ngamsDb.ngamsDb(dbSrv, dbName, dbUser, dbPwd, 0)
    else:
        dbCon = None

    # Open DB Snapshot.
    ngamsCfgObj = ngamsConfig.ngamsConfig().\
                  storeVal("NgamsCfg.Permissions[1].AllowArchiveReq", 1)
    dbSnapshotDbm = ngamsJanitorThread.\
                    _openDbSnapshot(ngamsCfgObj, testParDic["MOUNTPOINT"])

    # Start 'archiving loop'.
    fileCount = 0
    step = (1.0 / filesPerDay)  # ((24 * 3600.0 / filesPerDay) / 24 * 3600.0)
    sys.stdout.write("Creating files (100 files/dot): ")
    sys.stdout.flush()
    prevBaseTimeMjd = baseTimeMjd
    while (fileCount < testParDic["NOOFFILES"]):
        dayFileCount = 0
        startMjd = baseTimeMjd
        while ((dayFileCount < filesPerDay) and
               (fileCount < testParDic["NOOFFILES"])):
            baseTimeMjd += step
            if ((baseTimeMjd - startMjd) >= 1): break
            fileId = "%s.%s" % (insId, PccUtTime.TimeStamp(baseTimeMjd).\
                                getTimeStamp())
            isoTime = fileId[(fileId.find(".") + 1):]
            dateDir = string.split(isoTime, "T")[0]            
            relPath = "saf/%s/1" % dateDir
            relFilename = "%s/%s.fits" % (relPath, fileId)
            if (ext != "fits"): relFilename += "." + ext
            complFilename = os.path.normpath("%s/%s" %\
                                             (testParDic["MOUNTPOINT"],
                                              relFilename))
            ingestionTime = PccUtTime.TimeStamp().getTimeStamp()

            # If the file already exists, we skip.
            if (os.path.exists(complFilename)): continue

            # Create file (copy or create link).
            complPath = os.path.dirname(complFilename)
            os.system("mkdir -p %s" % complPath)
            if (not testParDic["CREATELINK"]):
                os.system("cp %s %s" % (baseFile, complFilename))
            else:
                os.system("ln -s %s %s" % (baseFile, complFilename))
            
            # Update DB Snapshot Info.            
            fileInfoList = 16 * [None]
            fileInfoList[ngamsDb.NGAS_FILES_DISK_ID] = diskId
            fileInfoList[ngamsDb.NGAS_FILES_FILE_NAME] = relFilename
            fileInfoList[ngamsDb.NGAS_FILES_FILE_ID] = fileId
            fileInfoList[ngamsDb.NGAS_FILES_FILE_VER] = fileVer
            fileInfoList[ngamsDb.NGAS_FILES_FORMAT] = format
            fileInfoList[ngamsDb.NGAS_FILES_FILE_SIZE] = archFileSize
            fileInfoList[ngamsDb.NGAS_FILES_UNCOMPR_FILE_SIZE] = uncomprSize
            fileInfoList[ngamsDb.NGAS_FILES_COMPRESSION] = compression
            fileInfoList[ngamsDb.NGAS_FILES_INGEST_DATE] = ingestionTime
            fileInfoList[ngamsDb.NGAS_FILES_IGNORE] = 0
            fileInfoList[ngamsDb.NGAS_FILES_CHECKSUM] = checksum
            fileInfoList[ngamsDb.NGAS_FILES_CHECKSUM_PI] = checksumPlugIn
            fileInfoList[ngamsDb.NGAS_FILES_FILE_STATUS] = "00000000"
            fileInfoList[ngamsDb.NGAS_FILES_CREATION_DATE] = ingestionTime
            updateDbSnapshot(dbSnapshotDbm, fileInfoList)
            del fileInfoList
            
            # Update DB if requested.
            if (dbCon):
                fileInfoObj = ngamsFileInfo.ngamsFileInfo().\
                              setChecksum(checksum).\
                              setChecksumPlugIn(checksumPlugIn).\
                              setCompression(compression).\
                              setCreationDate(ingestionTime).\
                              setDiskId(diskId).\
                              setFileId(fileId).\
                              setFileSize(archFileSize).\
                              setFileStatus("00000000").\
                              setFileVersion(fileVer).\
                              setFilename(relFilename).\
                              setFormat(format).\
                              setIgnore(0).\
                              setIngestionDate(ingestionTime).\
                              setUncompressedFileSize(uncomprSize)
                fileInfoObj.write(dbCon, genSnapshot=0)
                del fileInfoObj

            dayFileCount += 1
            fileCount += 1

            # Give a signal of life ...
            if ((fileCount % 100) == 0):
                sys.stdout.write(".")
                sys.stdout.flush()
    sys.stdout.write("\n")
    sys.stdout.flush()


def createVolume(testParDic):
    """
    Create the test 'archived' files according to the specified parameters.

    testParDic:  Dictionary with parameters for running the test (dictionary).

    Returns:     Void.
    """
    info(4,"Entering createVolume() ...")
    
    # Check that <Disk Mt Pt>/NgasDiskInfo is found. I.e., that this is an
    # already registered NGAS Disk.
    ngasDiskInfo = os.path.normpath("%s/NgasDiskInfo" %
                                    testParDic["MOUNTPOINT"])
    if (not os.path.exists(ngasDiskInfo)):
        msg = "The speficied mount point: %s appears not to point to a " +\
              "registered NGAS Disk"
        raise Exception, msg % testParDic["MOUNTPOINT"]

    # Check that basefile exists.
    if (not os.path.exists(testParDic["BASEFILE"])):
        msg = "The speficied basefile: %s appears not to exist"
        raise Exception, msg % testParDic["BASEFILE"]

    if (testParDic["BASEFILE"].find(".fits") != -1):
        createFitsFiles(testParDic)
    else:
        raise Exception, "Unsupported file format!"
    info(4,"Leaving createVolume()")

  
if __name__ == '__main__':
    """
    Main function to execute the tool.
    """
    setDebug(1)
    setLogCond(0, "", 0, "", 1)
                    
    # Parse input parameters.
    testParDic = testParDic()
    idx = 1
    while idx < len(sys.argv):
        parOrg = sys.argv[idx]
        par    = parOrg.upper()
        try:
            if (par.find("--BASEFILE") == 0):
                testParDic["BASEFILE"] = sys.argv[idx].split("=")[-1]
            elif (par.find("--CREATELINK") == 0):
                testParDic["CREATELINK"] = 1
            elif (par.find("--DB") == 0):
                testParDic["DB"] = sys.argv[idx][(sys.argv[idx].find("=")+1):]
            elif (par.find("--MOUNTPOINT") == 0):
                testParDic["MOUNTPOINT"] = sys.argv[idx].split("=")[-1]
            elif (par.find("--NOOFFILES") == 0):
                testParDic["NOOFFILES"] = int(sys.argv[idx].split("=")[-1])
            elif (par.find("--VERBOSE") == 0):
                setLogCond(0, "", 0, "", int(sys.argv[idx].split("=")[-1]))
            else:
                raise Exception, "Unknown parameter: %s" % parOrg
            idx += 1
        except Exception, e:
            print "\nProblem executing the tool: %s\n" % str(e)
            print correctUsage()  
            sys.exit(1)
    try:
        if ((not testParDic["BASEFILE"]) or (not testParDic["MOUNTPOINT"]) or
            (not testParDic["NOOFFILES"])):
            print correctUsage() 
            raise Exception, "Incorrect/missing command line parameter(s)!"
        startTime = time.time()
        createVolume(testParDic)
        deltaTime = (time.time() - startTime)
        timePerFile = (deltaTime / testParDic["NOOFFILES"])
        info(1,"Time: %.3fs, %.3fs/file" % (deltaTime, timePerFile))
    except Exception, e:
        print "Problem encountered during execution:\n\n%s\n" % str(e)
        sys.exit(1)

# EOF
