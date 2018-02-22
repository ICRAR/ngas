#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2017
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
"""
A collection of methods used to deal with the DbSnapshot feature and its
bits and pieces
"""

import cPickle
import glob
import logging
import os
import time
import types

from ngamsLib import ngamsDbCore, ngamsLib, ngamsFileInfo, ngamsHighLevelLib, \
    ngamsDbm, ngamsNotification
from ngamsLib.ngamsCore import NGAMS_DB_DIR, NGAMS_DB_NGAS_FILES, \
    checkCreatePath, NGAMS_DB_CH_CACHE, rmFile, NGAMS_PICKLE_FILE_EXT, \
    NGAMS_DB_CH_FILE_DELETE, NGAMS_DB_CH_FILE_INSERT, NGAMS_DB_CH_FILE_UPDATE, \
    toiso8601, NGAMS_NOTIF_DATA_CHECK, NGAMS_TEXT_MT
from ngamsJanitorCommon import checkStopJanitorThread, StopJanitorThreadException


try:
    import bsddb
except ImportError:
    import bsddb3 as bsddb

logger = logging.getLogger(__name__)


def _addInDbm(snapShotDbObj,
              key,
              val,
              sync = 0):
    """
    Add an entry in the DB Snapshot. This entry is pickled in binary format.

    snapShotDbObj:    Snapshot DB file (bsddb).

    key:              Key in DB (string).

    val:              Value to be put in the DB (<object>).

    sync:             Sync the DB to the DB file (integer/0|1).

    Returns:          Void.
    """
    snapShotDbObj[key] = cPickle.dumps(val, 1)
    if (sync): snapShotDbObj.sync()


def _readDb(snapShotDbObj,
            key):
    """
    Read and unpickle a value referenced by its key from the file DB.

    snapShotDbObj:   Open DB object (bsddb).

    key:             Key to extract value from (string).

    Returns:         Void.
    """
    return cPickle.loads(snapShotDbObj[key])


##############################################################################
# DON'T CHANGE THESE IDs!!!
##############################################################################
NGAMS_SN_SH_ID2NM_TAG   = "___ID2NM___"
NGAMS_SN_SH_NM2ID_TAG   = "___NM2ID___"
NGAMS_SN_SH_MAP_COUNT   = "___MAP_COUNT___"

def _encName(dbSnapshot,
             name):
    """
    Encode a name and add the name and its corresponding mapping ID (integer)
    in the file DB. The mapping is such that the name itself is referred to by

      NGAMS_SN_SH_ID2NM_TAG + ID -> <Name>

    The get from the name to the corresponding ID the following mapping
    should be used:

      NGAMS_SN_SH_NM2ID_TAG + <Name> -> <ID>

    dbSnapshot:      Open DB object (bsddb).

    name:            Name to be encoded (string).

    Returns:         The ID allocated to that name (integer).
    """
    nm2IdTag = NGAMS_SN_SH_NM2ID_TAG + name
    if (dbSnapshot.has_key(nm2IdTag)):
        nameId = _readDb(dbSnapshot, nm2IdTag)
    else:
        if (dbSnapshot.has_key(NGAMS_SN_SH_MAP_COUNT)):
            count = (_readDb(dbSnapshot, NGAMS_SN_SH_MAP_COUNT) + 1)
        else:
            count = 0
        nameId = count
        id2NmTag = NGAMS_SN_SH_ID2NM_TAG + str(nameId)

        # Have to ensure that all three keys are entered in the DBM (this might
        # not be the right way, maybe there is something that can be done at
        # bsddb level.
        try:
            _addInDbm(dbSnapshot, NGAMS_SN_SH_MAP_COUNT, count)
        except:
            _addInDbm(dbSnapshot, NGAMS_SN_SH_MAP_COUNT, count)
            _addInDbm(dbSnapshot, nm2IdTag, nameId)
            _addInDbm(dbSnapshot, id2NmTag, name, 1)
            raise
        try:
            _addInDbm(dbSnapshot, nm2IdTag, nameId)
        except:
            _addInDbm(dbSnapshot, NGAMS_SN_SH_MAP_COUNT, count)
            _addInDbm(dbSnapshot, nm2IdTag, nameId)
            _addInDbm(dbSnapshot, id2NmTag, name, 1)
            raise
        _addInDbm(dbSnapshot, id2NmTag, name, 1)

    return nameId


def _encFileInfo(dbConObj,
                 dbSnapshot,
                 fileInfo):
    """
    Encode the information about a file contained in a list as read
    from the DB and generate a dictionary with these values. The column
    names are encoded and mappings between the code (ID) and name are stored
    in the file DB.

    The elements in the list can be refferred to by the 'constants'

      ngamsDbCore.NGAS_FILES_DISK_ID ... ngamsDbCore.NGAS_FILES_CREATION_DATE


    dbConObj:        DB connection object (ngamsDb).

    dbSnapshot:    Open DB object (bsddb).

    fileInfo:      List with information about file from the DB (list).

    Returns:       Dictionary with encoded column names (dictionary).
    """
    tmpDic = {}
    #for n in range(ngamsDbCore.NGAS_FILES_CREATION_DATE + 1):
    for n in range(ngamsDbCore.NGAS_FILES_IO_TIME + 1): #newly added column!
        colName = dbConObj.getNgasFilesMap()[n]
        colId = _encName(dbSnapshot, colName)
        tmpDic[colId] = fileInfo[n]
    return tmpDic


def _genFileKey(fileInfo):
    """
    Generate a dictionary key from information in the File Info object,
    which is either a list with information from ngas_files, or an
    ngamsFileInfo object.

    fileInfo:       File Info as read from the ngas_files table or
                    an instance of ngamsFileInfo (list|ngamsFileInfo).

    Returns:        File key (string).
    """
    if ((type(fileInfo) == types.ListType) or
        (type(fileInfo) == types.TupleType)):
        fileId  = fileInfo[ngamsDbCore.NGAS_FILES_FILE_ID]
        fileVer = fileInfo[ngamsDbCore.NGAS_FILES_FILE_VER]
    else:
        fileId  = fileInfo.getFileId()
        fileVer = fileInfo.getFileVersion()
    return ngamsLib.genFileKey(None, fileId, fileVer)


def _updateSnapshot(ngamsCfgObj):
    """
    Return 1 if the DB Snapshot should be updated, otherwise 0 is
    returned.

    ngamsCfgObj:   NG/AMS Configuration Object (ngamsConfig).

    Returns:       1 = update DB Snapshot, 0 = do not update DB Snapshot
                   (integer/0|1).
    """
    if (ngamsCfgObj.getAllowArchiveReq() or ngamsCfgObj.getAllowRemoveReq()):
        return 1
    else:
        return 0



def _openDbSnapshot(ngamsCfgObj,
                    mtPt):
    """
    Open a bsddb file DB. If the file exists and this is not
    a read-only NGAS system the file is opened for reading and writing.
    If this is a read-only NGAS system it is only opened for reading.

    If the file DB does not exist, a new DB is created.

    If the file DB does not exist and this is a read-only NGAS system,
    None is returned.

    The name of the DB file is:

      <Disk Mount Point>/NGAMS_DB_DIR/NGAMS_DB_NGAS_FILES

    ngamsCfgObj:    NG/AMS Configuration Object (ngamsConfig).

    mtPt:           Mount point (string).

    Returns:        File DB object (bsddb|None).
    """
    snapShotFile = os.path.normpath(mtPt + "/" + NGAMS_DB_DIR + "/" +\
                                    NGAMS_DB_NGAS_FILES)
    checkCreatePath(os.path.normpath(mtPt + "/" + NGAMS_DB_CH_CACHE))
    if (os.path.exists(snapShotFile)):
        if (_updateSnapshot(ngamsCfgObj)):
            # Open the existing DB Snapshot for reading and writing.
            snapshotDbm = bsddb.hashopen(snapShotFile, "w")
        else:
            # Open only for reading.
            snapshotDbm = bsddb.hashopen(snapShotFile, "r")
    else:
        if (_updateSnapshot(ngamsCfgObj)):
            # Create a new DB Snapshot.
            snapshotDbm = bsddb.hashopen(snapShotFile, "c")
        else:
            # There is no DB Snapshot and it is not possible to
            # create one - the check cannot be carried out.
            snapshotDbm = None

    # Remove possible, old /<mt pt>/.db/NgasFiles.xml snapshots.
    # TODO: Remove when it can be assumed that all old XML snapshots have
    #       been removed.
    rmFile(os.path.normpath(mtPt + "/" + NGAMS_DB_DIR + "/NgasFiles.xml"))

    return snapshotDbm


def _delFileEntry(hostId,
                  dbConObj,
                  fileInfoObj):
    """
    Delete a file entry in the NGAS DB. If the file does not exist,
    nothing is done. If the file exists, it will be deleted.

    dbConObj:        NG/AMS DB object (ngamsDB).

    fileInfoObj:     File Info Object (ngamsFileInfo).

    Returns:         Void.
    """
    if (dbConObj.fileInDb(fileInfoObj.getDiskId(),
                          fileInfoObj.getFileId(),
                          fileInfoObj.getFileVersion())):
        try:
            dbConObj.deleteFileInfo(hostId,
                                    fileInfoObj.getDiskId(),
                                    fileInfoObj.getFileId(),
                                    fileInfoObj.getFileVersion(), 0)
        except:
            pass


def checkDbChangeCache(srvObj,
                       diskId,
                       diskMtPt,
                       stopEvt):
    """
    The function merges the information in the DB Change Snapshot Documents
    in the DB cache area on the disk concerned, into the Main DB Snapshot
    Document in a safe way which prevents that any information is lost.

    srvObj:        Reference to NG/AMS server class object (ngamsServer).

    diskId:        ID for disk (string).

    diskMtPt:      Mount point of the disk, e.g. '/NGAS/disk1' (string).

    Returns:       Void.
    """
    if (not srvObj.getCfg().getDbSnapshot()): return
    if (not _updateSnapshot(srvObj.getCfg())): return

    snapshotDbm = None
    try:
        snapshotDbm = _openDbSnapshot(srvObj.getCfg(), diskMtPt)
        if (snapshotDbm == None):
            return

        # Remove possible, old /<mt pt>/.db/cache/*.xml snapshots.
        # TODO: Remove when it can be assumed that all old XML snapshots have
        #       been removed.
        rmFile(os.path.normpath(diskMtPt + "/" + NGAMS_DB_CH_CACHE + "/*.xml"))

        # Update the Status document with the possibly new entries.
        # TODO: Potential memory bottleneck. Use 'find > file' as for
        #       REGISTER Command.
        dbCacheFilePat = os.path.normpath("%s/%s/*.%s" %\
                                          (diskMtPt, NGAMS_DB_CH_CACHE,
                                           NGAMS_PICKLE_FILE_EXT))

        # Sort files by their creation date, to ensure we apply
        # the DB changes in the order they were generated
        def creation_date_cmp(x, y):
            d1 = os.stat(x).st_ctime
            d2 = os.stat(y).st_ctime
            return 0 if d1 == d2 else 1 if d1 > d2 else -1
        tmpCacheFiles = glob.glob(dbCacheFilePat)
        tmpCacheFiles.sort(cmp=creation_date_cmp)

        cacheStatObj = None
        count = 0
        fileCount = 0
        noOfCacheFiles = len(tmpCacheFiles)
        start = time.time()
        for cacheFile in tmpCacheFiles:
            checkStopJanitorThread(stopEvt)
            if os.lstat(cacheFile)[6] == 0:
                os.remove(cacheFile)    # sometimes there are pickle files with 0 size.
                                        # we don't want to stop on them
                continue

            cacheStatObj = ngamsLib.loadObjPickleFile(cacheFile)
            if (isinstance(cacheStatObj, types.ListType)):
                # A list type in the Temporary DB Snapshot means that the
                # file has been removed.
                cacheStatList = cacheStatObj
                tmpFileInfoObjList = [ngamsFileInfo.ngamsFileInfo().\
                                      setDiskId(cacheStatList[0]).\
                                      setFileId(cacheStatList[1]).\
                                      setFileVersion(cacheStatList[2])]
                operation = NGAMS_DB_CH_FILE_DELETE
            elif (isinstance(cacheStatObj, ngamsFileInfo.ngamsFileInfo)):
                tmpFileInfoObjList = [cacheStatObj]
                operation = cacheStatObj.getTag()
            else:
                # Assume a ngamsFileList object.
                cacheFileListObj = cacheStatObj.getFileListList()[0]
                tmpFileInfoObjList = cacheFileListObj.getFileInfoObjList()
                operation = cacheFileListObj.getComment()

            # Loop over the files in the temporary snapshot.
            for tmpFileInfoObj in tmpFileInfoObjList:
                fileKey = _genFileKey(tmpFileInfoObj)
                fileInfoList = tmpFileInfoObj.genSqlResult()
                encFileInfoDic = _encFileInfo(srvObj.getDb(), snapshotDbm,
                                              fileInfoList)
                if ((operation == NGAMS_DB_CH_FILE_INSERT) or
                    (operation == NGAMS_DB_CH_FILE_UPDATE)):
                    _addInDbm(snapshotDbm, fileKey, encFileInfoDic)
                    tmpFileInfoObj.write(srvObj.getHostId(), srvObj.getDb(), 0)
                elif (operation == NGAMS_DB_CH_FILE_DELETE):
                    if (snapshotDbm.has_key(fileKey)): del snapshotDbm[fileKey]
                    _delFileEntry(srvObj.getHostId(), srvObj.getDb(), tmpFileInfoObj)
                else:
                    # Should not happen.
                    pass
            del cacheStatObj

            # Sleep if not last iteration (or if only one file).
            fileCount += 1
            if (fileCount < noOfCacheFiles): time.sleep(0.010)

            # Synchronize the DB.
            count += 1
            if (count == 100):
                snapshotDbm.sync()
                checkStopJanitorThread(stopEvt)
                count = 0

        # Clean up, delete the temporary File Remove Status Document.
        snapshotDbm.sync()

        for cacheFile in tmpCacheFiles:
            rmFile(cacheFile)
        totTime = time.time() - start

        tmpMsg = "Handled DB Snapshot Cache Files. Mount point: %s. " +\
                 "Number of Cache Files handled: %d."
        args = [diskMtPt, fileCount]
        if (fileCount):
            tmpMsg += "Total time: %.3fs. Time per file: %.3fs."
            args += (totTime, (totTime / fileCount))
        logger.debug(tmpMsg, *args)
    finally:
        if snapshotDbm:
            snapshotDbm.close()


def updateDbSnapShots(srvObj,
                      stopEvt,
                      diskInfo = None):
    """
    Check/update the DB Snapshot Documents for all disks.

    srvObj:            Reference to NG/AMS server class object (ngamsServer).

    diskInfo:          If a Snapshot should only be updated for a specific
                       disk, this can be specifically indicated by giving
                       the Disk ID and Mount Point of the disk (list).

    Returns:           Void.
    """
    if (diskInfo):
        diskId = diskInfo[0]
        mtPt = diskInfo[1]
        if (diskId and mtPt):
            mtPt = diskInfo[1]
        else:
            mtPt = srvObj.getDb().getMtPtFromDiskId(diskId)
        if (not mtPt):
            logger.warning("No mount point returned for Disk ID: %s", diskId)
            return
        try:
            checkDbChangeCache(srvObj, diskId, mtPt, stopEvt)
        except StopJanitorThreadException:
            raise
        except Exception:
            msg = "Error checking DB Change Cache for " +\
                  "Disk ID:mountpoint: %s:%s"
            logger.exception(msg, diskId, str(mtPt))
            raise
    else:
        tmpDiskIdMtPtList = srvObj.getDb().\
                            getDiskIdsMtPtsMountedDisks(srvObj.getHostId())
        diskIdMtPtList = []
        for diskId, mtPt in tmpDiskIdMtPtList:
            diskIdMtPtList.append([mtPt, diskId])
        diskIdMtPtList.sort()
        for mtPt, diskId in diskIdMtPtList:
            logger.debug("Check/Update DB Snapshot Document for disk with " +\
                 "mount point: %s", mtPt)
            try:
                checkDbChangeCache(srvObj, diskId, mtPt, stopEvt)
            except StopJanitorThreadException:
                raise
            except Exception:
                msg = "Error checking DB Change Cache for " +\
                      "Disk ID:mountpoint: %s:%s"
                logger.exception(msg, diskId, str(mtPt))
                raise


def _encFileInfo2Obj(dbConObj,
                     dbSnapshot,
                     encFileInfoDic):
    """
    Convert an encoded file info from the snapshot into an NG/AMS File
    Info Object.

    dbConObj:        DB connection object (ngamsDb).

    dbSnapshot:      Open DB object (bsddb).

    encFileInfoDic:  Dictionary containing the encoded file information
                     (dictionary).

    Returns:         NG/AMS File Info Object (ngamsFileInfo).
    """
    sqlFileInfo = []
    #for n in range (ngamsDbCore.NGAS_FILES_CREATION_DATE + 1):
    for n in range (ngamsDbCore.NGAS_FILES_CONTAINER_ID + 1):
        sqlFileInfo.append(None)
    idxKeys = encFileInfoDic.keys()
    for idx in idxKeys:
        kid = NGAMS_SN_SH_ID2NM_TAG + str(idx)
        if (not dbSnapshot.has_key(kid)):
            logger.warning("dbSnapshot has no key '%s', is it corrupted?", str(kid))
            return None
        colName = _readDb(dbSnapshot, kid)
        sqlFileInfoIdx = dbConObj.getNgasFilesMap()[colName]
        sqlFileInfo[sqlFileInfoIdx] = encFileInfoDic[idx]
    tmpFileInfoObj = ngamsFileInfo.ngamsFileInfo().unpackSqlResult(sqlFileInfo)
    return tmpFileInfoObj


def checkUpdateDbSnapShots(srvObj, stopEvt):
    """
    Check if a DB Snapshot exists for the DB connected. If not, this is
    created according to the contents of the NGAS DB (if possible). During
    this creation it is checked if the file are physically stored on the
    disk.

    srvObj:        Reference to NG/AMS server class object (ngamsServer).

    Returns:       Void.
    """
    snapshotDbm = None
    tmpSnapshotDbm = None

    if (not srvObj.getCfg().getDbSnapshot()):
        logger.debug("NOTE: DB Snapshot Feature is switched off")
        return

    logger.debug("Generate list of disks to check ...")
    tmpDiskIdMtPtList = srvObj.getDb().getDiskIdsMtPtsMountedDisks(srvObj.getHostId())
    diskIdMtPtList = []
    for diskId, mtPt in tmpDiskIdMtPtList:
        diskIdMtPtList.append([mtPt, diskId])
    diskIdMtPtList.sort()
    logger.debug("Generated list of disks to check: %s", str(diskIdMtPtList))

    # Generate temporary snapshot filename.
    ngasId = srvObj.getHostId()
    tmpDir = ngamsHighLevelLib.getTmpDir(srvObj.getCfg())

    # Temporary DBM with file info from the DB.
    tmpSnapshotDbmName = os.path.normpath(tmpDir + "/" + ngasId + "_" +\
                                          NGAMS_DB_NGAS_FILES)

    # Temporary DBM to contain information about 'lost files', i.e. files,
    # which are registered in the DB and found in the DB Snapshot, but
    # which are not found on the disk.
    logger.debug("Create DBM to hold information about lost files ...")
    lostFileRefsDbmName = os.path.normpath(tmpDir + "/" + ngasId +\
                                           "_LOST_FILES")
    rmFile(lostFileRefsDbmName + "*")
    lostFileRefsDbm = ngamsDbm.ngamsDbm(lostFileRefsDbmName, writePerm=1)

    # Carry out the check.
    for mtPt, diskId in diskIdMtPtList:

        checkStopJanitorThread(stopEvt)

        logger.debug("Check/create/update DB Snapshot for disk with " +\
             "mount point: %s", mtPt)

        try:
            snapshotDbm = _openDbSnapshot(srvObj.getCfg(), mtPt)
            if (snapshotDbm == None):
                continue

            # The scheme for synchronizing the Snapshot and the DB is:
            #
            # - Loop over file entries in the Snapshot:
            #  - If in DB:
            #    - If file on disk     -> OK, do nothing.
            #    - If file not on disk -> Accumulate + issue collective warning.
            #
            #  - If entry not in DB:
            #    - If file on disk     -> Add entry in DB.
            #    - If file not on disk -> Remove entry from Snapshot.
            #
            # - Loop over entries for that disk in the DB:
            #  - If entry in Snapshot  -> OK, do nothing.
            #  - If entry not in Snapshot:
            #    - If file on disk     -> Add entry in Snapshot.
            #    - If file not on disk -> Remove entry from DB.

            # Create a temporary DB Snapshot with the files from the DB.
            try:
                rmFile(tmpSnapshotDbmName + "*")
                tmpSnapshotDbm = bsddb.hashopen(tmpSnapshotDbmName, "c")

                for fileInfo in srvObj.db.getFileInfoList(diskId, ignore=None):
                    fileKey = _genFileKey(fileInfo)
                    encFileInfoDic = _encFileInfo(srvObj.getDb(), tmpSnapshotDbm,
                                                  fileInfo)
                    _addInDbm(tmpSnapshotDbm, fileKey, encFileInfoDic)
                    checkStopJanitorThread(stopEvt)
                tmpSnapshotDbm.sync()
            except:
                rmFile(tmpSnapshotDbmName)
                raise

            #####################################################################
            # Loop over the possible entries in the DB Snapshot and compare
            # these against the DB.
            #####################################################################
            logger.debug("Loop over file entries in the DB Snapshot - %s ...", diskId)
            count = 0
            try:
                key, pickleValue = snapshotDbm.first()
            except Exception as e:
                msg = "Exception raised accessing DB Snapshot for disk: %s. " +\
                      "Error: %s"
                logger.debug(msg, diskId, str(e))
                key = None
                snapshotDbm.dbc = None

            # Create a DBM which is used to keep the list of files to remove
            # from the DB Snapshot.
            snapshotDelDbmName = ngamsHighLevelLib.\
                                 genTmpFilename(srvObj.getCfg(),
                                                NGAMS_DB_NGAS_FILES)
            snapshotDelDbm = ngamsDbm.ngamsDbm(snapshotDelDbmName,
                                               cleanUpOnDestr=1,
                                               writePerm=1)

            #################################################################################################
            #jagonzal: Replace looping aproach to avoid exceptions coming from the next() method underneath
            #          when iterating at the end of the table that are prone to corrupt the hash table object
            #while (key):
            for key,pickleValue in snapshotDbm.iteritems():
            #################################################################################################
                value = cPickle.loads(pickleValue)

                # Check if an administrative element, if yes add it if necessary.
                if (key.find("___") != -1):
                    if (not tmpSnapshotDbm.has_key(key)):
                        tmpSnapshotDbm[key] = pickleValue
                else:
                    tmpFileObj = _encFileInfo2Obj(srvObj.getDb(), snapshotDbm,
                                                  value)
                    if (tmpFileObj is None):
                        continue
                    complFilename = os.path.normpath(mtPt + "/" +\
                                                     tmpFileObj.getFilename())

                    # Is the file in the DB?
                    if (tmpSnapshotDbm.has_key(key)):
                        # Is the file on the disk?
                        if (not os.path.exists(complFilename)):
                            fileVer = tmpFileObj.getFileVersion()
                            tmpFileObj.setTag(complFilename)
                            fileKey = ngamsLib.genFileKey(tmpFileObj.getDiskId(),
                                                          tmpFileObj.getFileId(),
                                                          fileVer)
                            lostFileRefsDbm.add(fileKey, tmpFileObj)
                            lostFileRefsDbm.sync()
                    elif (not tmpSnapshotDbm.has_key(key)):
                        tmpFileObj = _encFileInfo2Obj(srvObj.getDb(), snapshotDbm,
                                                      value)
                        if (tmpFileObj is None):
                            continue

                        # Is the file on the disk?
                        if (os.path.exists(complFilename)):
                            # Add this entry in the NGAS DB.
                            tmpFileObj.write(srvObj.getHostId(), srvObj.getDb(), 0, 1)
                            tmpSnapshotDbm[key] = pickleValue
                        else:
                            # Remove this entry from the DB Snapshot.
                            msg = "Scheduling entry: %s in DB Snapshot " +\
                                  "for disk with ID: %s for removal"
                            logger.debug(msg, diskId, key)
                            # Add entry in the DB Snapshot Deletion DBM marking
                            # the entry for deletion.
                            if (_updateSnapshot(srvObj.getCfg())):
                                snapshotDelDbm.add(key, 1)

                        del tmpFileObj

                # Be friendly and sync the DB file every now and then
                count += 1
                if (count % 100) == 0:
                    if _updateSnapshot(srvObj.getCfg()):
                        snapshotDbm.sync()
                    checkStopJanitorThread(stopEvt)
                    tmpSnapshotDbm.sync()

                #################################################################################################
                #jagonzal: Replace looping aproach to avoid exceptions coming from the next() method underneath
                #          when iterating at the end of the table that are prone to corrupt the hash table object
                #try:
                #    key, pickleValue = snapshotDbm.next()
                #except:
                #    key = None
                #    snapshotDbm.dbc = None
                #################################################################################################

            # Now, delete entries in the DB Snapshot if there are any scheduled for
            # deletion.

            #################################################################################################
            #jagonzal: Replace looping aproach to avoid exceptions coming from the next() method underneath
            #          when iterating at the end of the table that are prone to corrupt the hash table object
            #snapshotDelDbm.initKeyPtr()
            #while (True):
            #    key, value = snapshotDelDbm.getNext()
            #    if (not key): break
            for key,value in snapshotDelDbm.iteritems():
                # jagonzal: We need to reformat the values and skip administrative elements #################
                if (str(key).find("__") != -1): continue
                #############################################################################################
                msg = "Removing entry: %s from DB Snapshot for disk with ID: %s"
                logger.debug(msg, key, diskId)
                del snapshotDbm[key]
            #################################################################################################
            del snapshotDelDbm

            logger.debug("Looped over file entries in the DB Snapshot - %s", diskId)
            # End-Loop: Check DB against DB Snapshot. ###########################
            if (_updateSnapshot(srvObj.getCfg())): snapshotDbm.sync()
            tmpSnapshotDbm.sync()

            logger.info("Checked/created/updated DB Snapshot for disk with mount point: %s", mtPt)

            #####################################################################
            # Loop over the entries in the DB and compare these against the
            # DB Snapshot.
            #####################################################################
            logger.debug("Loop over the entries in the DB - %s ...", diskId)
            count = 0
            try:
                key, pickleValue = tmpSnapshotDbm.first()
            except:
                key = None
                tmpSnapshotDbm.dbc = None

            #################################################################################################
            #jagonzal: Replace looping aproach to avoid exceptions coming from the next() method underneath
            #          when iterating at the end of the table that are prone to corrupt the hash table object
            #while (key):
            for key,pickleValue in tmpSnapshotDbm.iteritems():
            #################################################################################################
                value = cPickle.loads(pickleValue)

                # Check if it is an administrative element, if yes add it if needed
                if (key.find("___") != -1):
                    if (not snapshotDbm.has_key(key)):
                        snapshotDbm[key] = pickleValue
                else:
                    # Is the file in the DB Snapshot?
                    if (not snapshotDbm.has_key(key)):
                        tmpFileObj = _encFileInfo2Obj(srvObj.getDb(),
                                                      tmpSnapshotDbm, value)
                        if (tmpFileObj is None):
                            continue

                        # Is the file on the disk?
                        complFilename = os.path.normpath(mtPt + "/" +\
                                                         tmpFileObj.getFilename())
                        if (os.path.exists(complFilename)):
                            # Add this entry in the DB Snapshot.
                            if (_updateSnapshot(srvObj.getCfg())):
                                snapshotDbm[key] = pickleValue
                        else:
                            # Remove this entry from the DB (if it is there).
                            _delFileEntry(srvObj.getHostId(), srvObj.getDb(), tmpFileObj)
                        del tmpFileObj
                    else:
                        # We always update the DB Snapshot to ensure it is
                        # in-sync with the DB entry.
                        if (_updateSnapshot(srvObj.getCfg())):
                            snapshotDbm[key] = pickleValue

                # Be friendly and sync the DB file every now and then
                count += 1
                if (count % 100) == 0:
                    if _updateSnapshot(srvObj.getCfg()):
                        snapshotDbm.sync()
                    checkStopJanitorThread(stopEvt)

                #################################################################################################
                #jagonzal: Replace looping aproach to avoid exceptions coming from the next() method underneath
                #          when iterating at the end of the table that are prone to corrupt the hash table object
                #try:
                #    key, pickleValue = tmpSnapshotDbm.next()
                #except:
                #    key = None
                #################################################################################################
            logger.debug("Checked DB Snapshot against DB - %s", diskId)
            # End-Loop: Check DB Snapshot against DB. ###########################
            if (_updateSnapshot(srvObj.getCfg())):
                snapshotDbm.sync()

        finally:
            if snapshotDbm:
                snapshotDbm.close()

            if tmpSnapshotDbm:
                tmpSnapshotDbm.close()

    # Check if lost files found.
    logger.debug("Check if there are Lost Files ...")
    noOfLostFiles = lostFileRefsDbm.getCount()
    if (noOfLostFiles):
        statRep = os.path.normpath(tmpDir + "/" + ngasId +\
                                   "_LOST_FILES_NOTIF_EMAIL.txt")
        fo = open(statRep, "w")
        timeStamp = toiso8601()
        tmpFormat = "JANITOR THREAD - LOST FILES DETECTED:\n\n" +\
                    "==Summary:\n\n" +\
                    "Date:                       %s\n" +\
                    "NGAS Host ID:               %s\n" +\
                    "Lost Files:                 %d\n\n" +\
                    "==File List:\n\n"
        fo.write(tmpFormat % (timeStamp, srvObj.getHostId(), noOfLostFiles))

        tmpFormat = "%-32s %-32s %-12s %-80s\n"
        fo.write(tmpFormat % ("Disk ID", "File ID", "File Version",
                              "Expected Path"))
        fo.write(tmpFormat % (32 * "-", 32 * "-", 12 * "-", 80 * "-"))

        # Loop over the files an generate the report.
        lostFileRefsDbm.initKeyPtr()
        while (1):
            key, fileInfoObj = lostFileRefsDbm.getNext()
            if (not key): break
            diskId      = fileInfoObj.getDiskId()
            fileId      = fileInfoObj.getFileId()
            fileVersion = fileInfoObj.getFileVersion()
            filename    = fileInfoObj.getTag()
            fo.write(tmpFormat % (diskId, fileId, fileVersion, filename))
        fo.write("\n\n==END\n")
        fo.close()
        ngamsNotification.notify(srvObj.getHostId(), srvObj.getCfg(), NGAMS_NOTIF_DATA_CHECK,
                                 "LOST FILE(S) DETECTED", statRep,
                                 [], 1, NGAMS_TEXT_MT,
                                 "JANITOR_THREAD_LOST_FILES", 1)
        rmFile(statRep)
    logger.debug("Number of lost files found: %d", noOfLostFiles)

    # Clean up.
    del lostFileRefsDbm
    rmFile(lostFileRefsDbmName + "*")