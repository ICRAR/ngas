
#******************************************************************************
# ESO/DFS
#
# "@(#) $Id: ngasXSyncTool.py,v 1.1 2009/02/01 16:52:26 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  18/06/2008  Created
#

_doc =\
"""
The NGAS Express Synchronization Tool, ngasXSyncTool, is used to carry out
bulk data mirroring or cloning within the NGAS System in an efficient manner.

The tool works in an cluster oriented manner meaning that the target for the
synchronization is an NGAS Cluster, which name space is used to derive which
files to clone and to which target nodes the cloning can be done. An NGAS
Cluster is defined by a set of NGAS Nodes, which have the same Cluster Name
in the NGAS DB (ngas_hosts.cluster_name).

When deciding which files to clone to the target cluster, the basic criteria
applied by the tool is that there should be one file in the target cluster name
space. Determinating this can be done according to two schemes:

1. File is registered in DB (OK, Bad, Ignore).

2. The file is safely stored in the Target Cluster. This is checked by sending
a CHECKFILE Command to the Target Cluster, which will result in an explicit test
of the consistency and availability of the file in the target cluster name
space.


Files can either be refernced to by:

  1. A Volume ID (NGAS Disk ID):
  All files registered on an NGAS Volume, will be considered for
  synchronization. 

  2. An NGAS Host ID:
  All files registered on a given source node, will be considered for
  synchronization.

  3. A File List:
  The Data Objects to be synchronized are referenced specifically in a File List
  with the format:

    <Disk ID> <File ID> <File Version>
    ...

  4. An NGAS Cluster:
  In this case, all files registered in the name space of the given cluster
  will be considered for synchronization.


It should be specified with the --streams parameter how many parallel streams
the tool should control. It is important to find the 'appropriate number of
streams'. Specifying too many streams when sync'ing a HDD-based volume, may
reduce the performance haevily since the read/write head of the HDD will be
offsetting continuesly. On the other hand, it is clearly more efficient to
have a number of parallel streams sync'ing the data, compared to only one
stream, executing the batch sequentially.

After the tool has been executed on a set of files, it can also be used to
verify that the cloning was successful and can be used to correct possible
failing file synchronizations by simply executing it once more.

The tool implements persistency, such that if interrupted in the middle of
the processing of a batch, it will resume from the point where it was
interrupted. Should the files however, be lost which provides this persistency,
the tool will nonetheless be capable of resuming the processing from where
it was interrupted, after checking first all files in the set that have
been cloned successfully.

A report of the actions carried out by the tool is send out to a specified
list of recipients, given either on the command line or in the ~/.ngas
resource file. This report will contain a summary of the execution and
the entire list of files that were not processed successfully.

The tool can be instructed to send out intermediate notification reports
via email to the specified list of recipients. These intermediate reports
will contain a summary of what has been cloned already, and how much is
missing.

The tool uses a working directory under which all house keeping files will be
stored. The default value for the working directory is '/tmp', but it is
recommended to create a proper working directory for the tool, typically under
the account of the user executing the tool.

For each synchronization batch, the tool generates a Session ID for the session.
All files pertinent to one session, will be stored in a subdirectory in the
working directory:

<Working Directory>/Session ID>

The Session ID is generated as follow:

1. When synchronization volumes:
The Session ID is generated as the concatenation of the hostname and the NGAS
Slot ID for the NGAS Node hosting the volume to synchronize.

2. When synchronizing an entire node:
The Session ID is the name of the source node.

3. When synchronizing a list of files:
The Session ID is generated as the MD5 checksum of the contents of the file
list.

4. Synchronizing an entire cluster:
The Session ID will be the name of the Source Cluster.


Files and session directories in the working directory will be kept 30 days
after creation, independently of if the synchronization batch was successfully
terminated or not.


The input parameters defined for the tool are:

%s

"""

# TODO: - BUG: Possible to have multiple syncs. (after restarting the tool).
#       - Test handling of file list.
#       - Add "Real Throughput Volume" in report.
#       - Add "Real Throughput Files" in report.
#       - Test handling of failing sync requests (file list with illegal reqs)
#       - Test persistency.
#       - Clean up session directories older than 30 days.
#       - Implement synchronization of entire node (parameter --host-id=Host).
#       - Case: --target-cluster=$$$$$: Better error message.

import sys, os, time, getpass

import pcc, PccUtTime

from ngams import *
import ngamsDb, ngamsDbm, ngamsLib, ngamsFileInfo, ngamsDiskInfo, ngamsStatus
import ngamsThreadGroup
import ngamsPClient
import ngasUtils, ngasUtilsLib

# Constants.
NGAS_XSYNC_TOOL      = "NGAS_XSYNC_TOOL"
NGAS_MAX_RETRIES     = 3
NGAS_MIN_RETRY_TIME  = 60
NGAS_ALREADY_SYNCHED = "Already synchronized"


# Parameters.

# Internal parameters.
PAR_DB_CON         = "_db-con"
PAR_DISK_INFO      = "_disk-info"
PAR_SESSION_ID     = "_session-id"
PAR_START_TIME     = "_start-time"

# Parameters for statistics.
PAR_STAT_TOTAL_FILES     = "_total-files"
PAR_STAT_FILE_COUNT      = "_file-count"
PAR_STAT_LAST_FILE_COUNT = "_last-file-count"
PAR_STAT_TOTAL_VOL       = "_total-volume"
PAR_STAT_VOL_ACCU        = "_volume-accumulator"
PAR_STAT_LAST_VOL_ACCU   = "_last-volume-accumulator"


# Command line options.
PAR_CHECK_FILE     = "check-file"
PAR_CLEAN          = "clean"
PAR_FORCE          = "force"
PAR_CLUSTER_ID     = "cluster-id"
PAR_DISK_ID        = "disk-id"
PAR_HOST_ID        = "host-id"
PAR_FILE_LIST      = "file-list"
PAR_INT_NOTIF      = "intermediate-notif"
PAR_NOTIF_EMAIL    = "notif-email"
PAR_STREAMS        = "streams"
PAR_TARGET_CLUSTER = "target-cluster"
PAR_WORKING_DIR    = "working-dir"


# DBM names.
TMP_QUEUE_DBM_NAME = "_TMP_SYNC_QUEUE"
QUEUE_DBM_NAME     = "_SYNC_QUEUE"
PROC_DBM_NAME      = "_PROC_FILES"
SYNCHED_DBM_NAME   = "_SYNCHED_FILES"
FAILED_DBM_NAME    = "_FAILED_FILES"


# Definition of predefined command line parameters.
_options = [\

    [PAR_START_TIME, [], time.time(), ngasUtilsLib.NGAS_OPT_INT, "",
     "Internal Parameter: Start time for running the tool."],

    [PAR_STAT_TOTAL_FILES, [], time.time(), ngasUtilsLib.NGAS_OPT_INT, "",
     "Internal Parameter: Total number of files to synchronize."],
    [PAR_STAT_FILE_COUNT, [], time.time(), ngasUtilsLib.NGAS_OPT_INT, "",
     "Internal Parameter: File counter."],
    [PAR_STAT_LAST_FILE_COUNT, [], time.time(), ngasUtilsLib.NGAS_OPT_INT, "",
     "Internal Parameter: Last file counter at previous report."],
    [PAR_STAT_TOTAL_VOL, [], time.time(), ngasUtilsLib.NGAS_OPT_INT, "",
     "Internal Parameter: Total volume to synchronize."],
    [PAR_STAT_VOL_ACCU, [], time.time(), ngasUtilsLib.NGAS_OPT_INT, "",
     "Internal Parameter: Volume synchronized."],
    [PAR_STAT_LAST_VOL_ACCU, [], time.time(), ngasUtilsLib.NGAS_OPT_INT, "",
     "Internal Parameter: Volume accumulator at previous report."],

    [PAR_SESSION_ID, [], "", ngasUtilsLib.NGAS_OPT_INT, "",
     "Internal Parameter: ID for this session."],

    [PAR_CLEAN, [], 0, ngasUtilsLib.NGAS_OPT_OPT, 0,
     "Even though an existing session exists, start from scratch"],

    [PAR_FORCE, [], 0, ngasUtilsLib.NGAS_OPT_OPT, 0,
     "Force execution of tool."],

    [PAR_CLUSTER_ID, [], None, ngasUtilsLib.NGAS_OPT_OPT, "=<Cluster ID>",
     "The NGAS Cluster ID as defined in the NGAS Hosts DB Table."],
    
    [PAR_DISK_ID, [], None, ngasUtilsLib.NGAS_OPT_OPT, "=<Disk ID>",
     "ID for the volume to synchronize. If not given, the --fileList " +\
     "parameter must be used to specify the source files."],

    [PAR_HOST_ID, [], None, ngasUtilsLib.NGAS_OPT_OPT, "=<Host ID>",
     "ID for an entire host to synchronize. Cannot be specified in " +\
     "conjunction with --disk-id or --file-list."],

    [PAR_FILE_LIST, [], None, ngasUtilsLib.NGAS_OPT_OPT, "=<NGAS File List>",
     "List containing references to files to consider for synchronization. " +\
     "Each line must specify: <Disk ID> <File ID> <File Version>."],

    [PAR_TARGET_CLUSTER, [], None, ngasUtilsLib.NGAS_OPT_MAN,
     "=<Target Cluster Name>",
     "Name of NGAS Cluster to which the files will be synchronized."],

    [PAR_STREAMS, [], None, ngasUtilsLib.NGAS_OPT_MAN,
     "=<Number of Streams to Execute>",
     "Number of streams to execute in parallel."],

    [PAR_WORKING_DIR, [], "/tmp", ngasUtilsLib.NGAS_OPT_OPT, "=<Working Dir>", 
     "Working directory of the tool. The tool will create this " +\
     "directory, in which it will store its house-keeping files."],
    
    [PAR_NOTIF_EMAIL, [], None, ngasUtilsLib.NGAS_OPT_OPT,
     "=<Email Recipients>", "Comma separated list of email addresses."],

    [PAR_INT_NOTIF, [], None, ngasUtilsLib.NGAS_OPT_OPT,
     "=<Intermediate Notification Interval (s)>",
     "Time in seconds for which to send out an intermediate status report " +\
     "for the processing of the synchronization batch."],

    [PAR_CHECK_FILE, [], 0, ngasUtilsLib.NGAS_OPT_OPT, 0,
     "Execute a CHECKFILE Command for each file in the list in the " +\
     "Target Cluster - TO BE USED WITH CAUTION!"]
    ]

_optDic, _optDoc = ngasUtilsLib.genOptDicAndDoc(_options)
__doc__ = _doc % _optDoc


class ngasSyncRequest:
    """
    Class to hold the information for one synchronization request.
    """

    def __init__(self,
                 diskId,
                 fileId,
                 fileVersion,
                 fileSize):
        """
        Constructor.

        diskId:          Disk ID of source disk (string).
 
        fileId:          File ID of source file (string).

        fileVersion:     Version of source file (integer).

        fileSize:        File size in bytes (integer).
        """
        self.__diskId      = diskId
        self.__fileId      = fileId
        self.__fileVersion = fileVersion
        self.__fileSize    = fileSize
        self.__attempts    = 0
        self.__lastAttempt = 0.0
        self.__message     = ""
        

    def incrAttemptCount(self):
        """
        Increment the attempts counter.

        Returns:   Reference to object itself.
        """
        self.__attempts += 1
        self.__lastAttempt = time.time()
        return self
        
        
    def getAttemptCount(self):
        """
        Get the value of the attempts counter.

        Returns:   Value of attempts counter (integer).
        """
        return self.__attempts


    def getTimeLastAttempt(self):
        """
        Get the time for the last attempt.

        Returns:   Time for last attempt in seconds since epoch (float).
        """
        return self.__lastAttempt
    

    def getDiskId(self):
        """
        Return the Disk ID of the source disk.

        Returns:  Disk ID (string).
        """
        return self.__diskId
    

    def getFileId(self):
        """
        Return the File ID.

        Returns:   File ID (string).
        """
        return self.__fileId


    def getFileVersion(self):
        """
        Get the File Version.

        Returns:  File version (integer).
        """
        return self.__fileVersion


    def getSummary(self):
        """
        Generate and return summary of contents in the Syncronization Request.

        Returns:  Request summary (string).
        """
        return "%s/%s/%d" % (self.__diskId, self.__fileId, self.__fileVersion)


    def setMessage(self,
                   msg):
        """
        Set the internal message of the object.

        msg:      Message (string).

        Returns:  Reference to object itself.
        """
        self.__message = msg
        return self


    def getMessage(self):
        """
        Get the internal message of the object.

        Returns:  Message (string).
        """
        return self.__message


    def getFileSize(self):
        """
        Return the filesize.

        Returns: Filesize in bytes (integer).
        """
        return self.__fileSize
 
      
def getOptDic():
    """
    Return reference to command line options dictionary.

    Returns:  Reference to dictionary containing the command line options
              (dictionary).
    """
    return _optDic


def correctUsage():
    """
    Return the usage/online documentation in a string buffer.

    Returns:  Man-page (string).
    """
    return __doc__


def getLockFile(parDic):
    """
    Generate and return the lock file for this session of the tool.

    optDic:   Dictionary with parameters (dictionary).

    Returns:  Lock filename (string).
    """
    info(4, "Entering getLockFile() ...")
    lockFile = os.path.normpath("%s/%s/%s.lock" %\
                                (parDic[PAR_WORKING_DIR],
                                 parDic[PAR_SESSION_ID],
                                 NGAS_XSYNC_TOOL))
    info(4, "Leaving getLockFile()")
    return lockFile


def _addInFileDbm(curObj,
                  queueDbm):
    """
    Add the entries that can be retrieved via the given DB cursor.

    curObj:         Cursor object.
    
    queueDbm:       DBM in which to add the file info (ngamsDbm).

    Returns:        Void.
    """
    while (True):
        fileInfoList = curObj.fetch(10000)
        if (not fileInfoList): break
        for sqlFileInfo in fileInfoList:
            diskId      = sqlFileInfo[9]
            fileId      = sqlFileInfo[5]
            fileVersion = int(sqlFileInfo[6])
            fileSize    = float(sqlFileInfo[7])
            syncReq     = ngasSyncRequest(diskId, fileId, fileVersion, fileSize)
            key         = ngamsLib.genFileKey(None, fileId, fileVersion)
            queueDbm.add(key, syncReq)


def _getClusterNodes(dbCon,
                     clusterId):
    """
    Get the list of nodes registered in the name space of the given cluster.

    dbCon:       DB connection  (ngamsDb).

    clusterId:   NGAS Cluster ID (string).

    Returns:     List of NGAS Host IDs of the nodes in the cluster (list).
    """
    sqlQuery = "SELECT host_id from ngas_hosts WHERE cluster_name='%s'" %\
               clusterId
    res = dbCon.query(sqlQuery, ignoreEmptyRes=0)
    if (len(res[0]) == 0):
        return []
    else:
        nodeList = []
        for node in res[0]:
            nodeList.append(node[0])
        return nodeList
 

def initialize(parDic):
    """
    Initialize the tool.

    parDic:   Dictionary with parameters (dictionary).

    Returns:  Void
    """
    info(4, "Entering initialize() ...")
    
    # Extra checks of command line options.
    # =Rule 1: Can only specify one of: cluster-id, disk-id, host_id and
    #          file-list.
    sum = ((parDic[PAR_CLUSTER_ID] != None) + (parDic[PAR_DISK_ID] != None) +\
           (parDic[PAR_HOST_ID] != None) + (parDic[PAR_FILE_LIST] != None))
    if (sum > 1):
        msg = "Can only specify one of the %s, %s, %s and %s options"
        raise Exception, msg % (PAR_CLUSTER_ID, PAR_DISK_ID, PAR_HOST_ID,
                                PAR_FILE_LIST)
    # =Rule 2: Must specify either cluster-id, disk-id, host-id or file-list.
    elif (sum == 0):
        msg = "Must specify one of the %s, %s, %s or %s options"
        raise Exception, msg % (PAR_CLUSTER_ID, PAR_DISK_ID, PAR_HOST_ID,
                                PAR_FILE_LIST)

    # Connect to the RDBMS.
    server, db, user, password = ngasUtilsLib.getDbPars()
    dbCon = ngamsDb.ngamsDb(server, db, user, password, 0)
    parDic[PAR_DB_CON] = dbCon

    # Generate ID for this session:
    #
    # 1. If Disk ID is given: '<Host ID>_<Slot ID>'.
    # 2. If Host ID is given: '<Host ID>'.
    # 3. If File List is given: md5(<contents file list>).
    if (parDic[PAR_DISK_ID]):
        # Get Disk Info for the volume involved.
        diskInfoObj = ngamsDiskInfo.ngamsDiskInfo().read(parDic[PAR_DB_CON],
                                                         parDic[PAR_DISK_ID])
        parDic[PAR_DISK_INFO] = diskInfoObj
        hostId = parDic[PAR_DISK_INFO].getHostId()
        slotId = parDic[PAR_DISK_INFO].getSlotId()
        if ((hostId.strip() == "") or (slotId.strip() == "")):
            msg = "Illegal Disk ID specified: %s" % diskInfoObj.getDiskId()
            raise Exception, msg
        id = "%s_%s" % (hostId, slotId)
    elif (parDic[PAR_HOST_ID]):
        id = parDic[PAR_HOST_ID]
    elif (parDic[PAR_CLUSTER_ID]):
        id = parDic[PAR_CLUSTER_ID]
    else:
        try:
            fo = open(parDic[PAR_FILE_LIST])
            buf = fo.read()
            fo.close()
            import md5
            id = md5.new(buf).hexdigest()
        except Exception, e:
            msg = "Error loading File List: %s. Error: %s"
            raise Exception, msg % (str(parDic[PAR_FILE_LIST]), str(e))
    info(1, "Allocated session ID: %s" % id)
    parDic[PAR_SESSION_ID] = id

    # Check if an instance with this ID is running.
    if ((not parDic[PAR_FORCE]) and (os.path.exists(getLockFile(parDic)))):
        msg = "An instance of this tool with ID: %s appears to " +\
              "be running - bailing out!"
        raise Exception, msg % str(parDic[PAR_SESSION_ID])
    else:
        commands.getstatusoutput("touch %s" % getLockFile(parDic))

    # Create working directory if not existing.
    wd = os.path.normpath("%s/%s" % (parDic[PAR_WORKING_DIR],
                                     parDic[PAR_SESSION_ID]))
    # Start from a fresh if requested.
    if (parDic[PAR_CLEAN]): rmFile(wd)
    checkCreatePath(wd)

    # Create DBMs.
    tmpQueueDbmName = os.path.normpath("%s/%s" % (wd, TMP_QUEUE_DBM_NAME)) 
    queueDbmName    = os.path.normpath("%s/%s" % (wd, QUEUE_DBM_NAME))
    procDbmName     = os.path.normpath("%s/%s" % (wd, PROC_DBM_NAME))
    syncedDbmName   = os.path.normpath("%s/%s" % (wd, SYNCHED_DBM_NAME)) 
    failedDbmName   = os.path.normpath("%s/%s" % (wd, FAILED_DBM_NAME))
    
    # If Queue DBM already exists, we resuming an interrupted session. In that
    # case, just open the existin DBMs, else build up the DBMs.
    if (not os.path.exists("%s.gdbm" % queueDbmName)):
        # Create Queue, first as temporary name, then rename it when complete.
        tmpQueueDbm = ngamsDbm.ngamsDbm2(tmpQueueDbmName, writePerm = 1)
        # Put file information into the queue.
        if (parDic[PAR_DISK_ID] or parDic[PAR_HOST_ID]):
            if (parDic[PAR_DISK_ID]):
                # Dump the information for that volume from the RDBMS.
                curObj = parDic[PAR_DB_CON].\
                         getFileSummary1(diskIds = [parDic[PAR_DISK_ID]],
                                         order = 0)
            else:
                # Dump the information for that node from the RDBMS.
                curObj = parDic[PAR_DB_CON].\
                         getFileSummary1(hostId = parDic[PAR_HOST_ID],
                                         order = 0)
            _addInFileDbm(curObj, tmpQueueDbm)
        elif (parDic[PAR_CLUSTER_ID]):
            # Dump the information for an entire cluster.
            clusterNodes = _getClusterNodes(dbCon, parDic[PAR_CLUSTER_ID])
            msg = "Found the following nodes: %s in cluster: %s"
            info(1, msg % (str(clusterNodes), parDic[PAR_CLUSTER_ID]))
            for clusterNode in clusterNodes:
                curObj = parDic[PAR_DB_CON].\
                         getFileSummary1(hostId = clusterNode, order = 0)
                _addInFileDbm(curObj, tmpQueueDbm) 
                del curObj
        else:
            fileListEls = ngasUtilsLib.parseFileList(parDic[PAR_FILE_LIST])
            for fileListEl in fileListEls:
                diskId, fileId, fileVersion = fileListEl
                fileVersion = int(fileVersion)
                fileSize = 0.0
                # If intermediate reporting is on, we have to get the file
                # size for each file ...
                if (parDic[PAR_INT_NOTIF]):
                    try:
                        if (diskId != "-"):
                            tmpFileInfo = parDic[PAR_DB_CON].\
                                          getFileInfoFromFileId(fileId,
                                                                fileVersion,
                                                                diskId,
                                                                dbCursor=False,
                                                                order=False)
                        else:
                            tmpFileInfo = parDic[PAR_DB_CON].\
                                          getFileInfoFromFileId(fileId,
                                                                fileVersion,
                                                                dbCursor=False,
                                                                order=False)
                        fileSize = float(tmpFileInfo[0][5])
                    except Exception, e:
                        msg = "Error obtaining information about file: " +\
                              "%s/%s/%d. Error: %s"
                        warning(msg % (diskId, fileId, fileVersion, str(e)))
                syncReq = ngasSyncRequest(diskId, fileId, fileVersion,
                                          fileSize)
                key = ngamsLib.genFileKey(None, fileId, fileVersion)
                tmpQueueDbm.add(key, syncReq)

        # If the processing DBM already exists, this means that these files
        # were being processed while the tool was interrupted. These files are
        # copied back into the queue.
        if (os.path.exists(procDbmName)):
            procDbm = ngamsDbm.ngamsDbm2(procDbmName, writePerm = 1)
            parDic[PROC_DBM_NAME] = procDbm
            procDbm.initKeyPtr()
            while (True):
                key, val = procDbm.getNext()
                if (not key): break
                tmpQueueDbm.add(key, val)
                procDbm.rem(key)

        # Ensure all elements have been sync'ed into the DBM.
        tmpQueueDbm.sync()
        tmpQueueDbmName = tmpQueueDbm.getDbmName()
        dbmExt = os.path.splitext(tmpQueueDbmName)[-1]
        del tmpQueueDbm
        mvFile(tmpQueueDbmName, "%s%s" % (queueDbmName, dbmExt))

    # Open the DBMs.
    parDic[QUEUE_DBM_NAME]  = ngamsDbm.ngamsDbm2(queueDbmName, writePerm = 1)
    if (not parDic.has_key(PROC_DBM_NAME)):
        parDic[PROC_DBM_NAME] = ngamsDbm.ngamsDbm2(procDbmName, writePerm = 1)
    parDic[SYNCHED_DBM_NAME] = ngamsDbm.ngamsDbm2(syncedDbmName, writePerm = 1)
    parDic[FAILED_DBM_NAME] = ngamsDbm.ngamsDbm2(failedDbmName, writePerm = 1)

    # Calculate total volume.
    totalVolume = 0.0
    for dbm in [parDic[QUEUE_DBM_NAME], parDic[PROC_DBM_NAME],
                parDic[SYNCHED_DBM_NAME], parDic[FAILED_DBM_NAME]]:
        dbm.initKeyPtr()
        while (True):
            key, syncReqObj = dbm.getNext()
            if (not key): break
            totalVolume += syncReqObj.getFileSize()

    # Initialize the parameters used for statistics.
    filesInQueueDbm   = parDic[QUEUE_DBM_NAME].getCount()
    filesInProcDbm    = parDic[PROC_DBM_NAME].getCount()
    filesInSynchedDbm = parDic[SYNCHED_DBM_NAME].getCount()
    filesInFailedDbm  = parDic[FAILED_DBM_NAME].getCount()
    parDic[PAR_STAT_TOTAL_FILES]     = (filesInQueueDbm + filesInProcDbm +\
                                        filesInSynchedDbm + filesInFailedDbm)
    parDic[PAR_STAT_FILE_COUNT]      = 0
    parDic[PAR_STAT_LAST_FILE_COUNT] = 0
    parDic[PAR_STAT_TOTAL_VOL]       = totalVolume
    parDic[PAR_STAT_VOL_ACCU]        = 0.0
    parDic[PAR_STAT_LAST_VOL_ACCU]   = 0.0


def checkFile(parDic,
              client,
              diskId,
              fileId,
              fileVersion):
    """
    Send a CHECKFILE to the target cluster.

    parDic:         Dictionary with parameters (dictionary).
    
    client:         NG/AMS Python Client instance (ngamsPClient).
    
    diskId:         ID of target disk to consider in the target cluster
                    (string).
    
    fileId:         ID of file to check (string).
    
    fileVersion:    Version of file (integer).

    Returns:        Status of the checking. Can be either of:
                      1. File OK (contains string 'FILE_OK').
                      2. File Not OK (contains strng 'FILE_NOK').
                      3. Other error, typically an exception ocurred.
                                                                      (string)
    """
    try:
        statObj = client.sendCmd(NGAMS_CHECKFILE_CMD,
                                 pars = [["disk_id", diskId],
                                         ["file_id", fileId],
                                         ["file_version", fileVersion]])
        return statObj.getMessage()
    except Exception, e:
        return str(e)
    

def genIntermediateReport(threadGroupObj):
    """
    Generate intermediate report from contents of the tool internal DBM.

    threadGroupObj:   Reference to the Thread Group Object (ngamsThreadGroup).

    Returns:          Report (string).
    """
    info(2, "Generating intermediate report ...")
    
    reportFormat = "NGAS EXPRESS SYNCHRONIZATION TOOL - STATUS REPORT:\n\n" +\
                   "Time:                              %s\n" +\
                   "Host:                              %s\n" +\
                   "User:                              %s\n" +\
                   "Session ID:                        %s\n" +\
                   "Start Time:                        %s\n" +\
                   "Time Elapsed:                      %.2f hours (%.f s)\n" +\
                   "Total Files:                       %d\n" +\
                   "Total Volume:                      %.3f MB\n" +\
                   "Synchronized Files:                %d\n" +\
                   "Failed Files:                      %d\n" +\
                   "Processing Throughput Files:       %.3f files/s\n" +\
                   "Files Handled Since Last Report:   %d\n" +\
                   "Remaining Files:                   %d\n" +\
                   "Processing Throughput Volume:      %.3f MB/s\n" +\
                   "Volume Since Last Report:          %.3f MB\n" +\
                   "Remaining Volume:                  %.3f MB\n" +\
                   "Completion Percentage:             %.3f %%\n" +\
                   "Estimated Total Time:              %.2f hours (%.f s)\n" +\
                   "Estimated Completion Time:         %s\n"
    
    parDic = threadGroupObj.getParameters()[0]
    try:
        threadGroupObj.takeGenMux()
        filesInQueueDbm  = parDic[QUEUE_DBM_NAME].getCount()
        filesInProcDbm   = parDic[PROC_DBM_NAME].getCount()

        parDic[SYNCHED_DBM_NAME].initKeyPtr()
        synchedCount = 0
        while (True):
            key, syncReq = parDic[SYNCHED_DBM_NAME].getNext()
            if (not key): break
            if (syncReq.getMessage() != NGAS_ALREADY_SYNCHED):
                synchedCount += 1

        filesInSynchedDbm = parDic[SYNCHED_DBM_NAME].getCount()
        filesInFailedDbm = parDic[FAILED_DBM_NAME].getCount()
        threadGroupObj.releaseGenMux()
    except Exception, e:
        threadGroupObj.releaseGenMux()
        msg = "Error generating intermediate status report. Error: %s"
        error(msg % str(e))
        return

    # Generate the statistics.
    timeNow           = time.time()
    repTime           = PccUtTime.TimeStamp().\
                        initFromSecsSinceEpoch(timeNow).\
                        getTimeStamp()
    startTime         = PccUtTime.TimeStamp().\
                        initFromSecsSinceEpoch(parDic[PAR_START_TIME]).\
                        getTimeStamp()
    deltaTime         = (timeNow - parDic[PAR_START_TIME])
    timeElapsed       = (deltaTime / 3600.0)
    throughputFiles   = (float(parDic[PAR_STAT_FILE_COUNT]) / deltaTime)
    filesSinceLastRep = (parDic[PAR_STAT_FILE_COUNT] -
                         parDic[PAR_STAT_LAST_FILE_COUNT])
    parDic[PAR_STAT_LAST_FILE_COUNT] = parDic[PAR_STAT_FILE_COUNT]
    remainingFiles    = (parDic[PAR_STAT_TOTAL_FILES] -
                         parDic[PAR_STAT_FILE_COUNT])
    throughputVol     = float((float(parDic[PAR_STAT_VOL_ACCU]) / deltaTime) /
                              (1024 * 1024))
    volSinceLastRep   = ((parDic[PAR_STAT_VOL_ACCU] -
                          parDic[PAR_STAT_LAST_VOL_ACCU]) / (1024 * 1024))
    parDic[PAR_STAT_LAST_VOL_ACCU] = parDic[PAR_STAT_VOL_ACCU]
    remainingVol      = ((parDic[PAR_STAT_TOTAL_VOL] -
                          parDic[PAR_STAT_VOL_ACCU]) / (1024 * 1024))
    # Completion percentage is calculated as the average of completion/files
    # and completion/volume.
    if (parDic[PAR_STAT_TOTAL_FILES] > 0):
        complFiles        = (100.0 * (float(parDic[PAR_STAT_FILE_COUNT]) /
                                      float(parDic[PAR_STAT_TOTAL_FILES])))
    else:
        complFiles = -1
    if (parDic[PAR_STAT_TOTAL_VOL] > 0):
        complVol          = (100.0 * (parDic[PAR_STAT_VOL_ACCU] /
                                      parDic[PAR_STAT_TOTAL_VOL]))
    else:
        complVol = -1
    if ((complFiles != -1) or (complVol != -1)):
        complPercentage = float((complFiles + complVol) / 2.0)
    else:
        complPercentage = 0.0
    if (remainingFiles == 0):
        totalTimeSecs = deltaTime
        totalTime = float(totalTimeSecs / 3600.0)
        estimComplTime = PccUtTime.TimeStamp().\
                         initFromSecsSinceEpoch(timeNow).getTimeStamp()
    elif (complPercentage > 0.0):
        totalTimeSecs  = float(100.0 * (deltaTime / complPercentage))
        totalTime      = float(totalTimeSecs / 3600.0)
        estimComplTime = PccUtTime.TimeStamp().\
                         initFromSecsSinceEpoch(parDic[PAR_START_TIME] +\
                                                totalTimeSecs).getTimeStamp()
    else:
        totalTimeSecs = -1
        totalTime = -1
        estimComplTime = "UNDEFINED"
    report = reportFormat % (repTime,
                             getHostName(),
                             getpass.getuser(),
                             parDic[PAR_SESSION_ID],
                             startTime,
                             timeElapsed, deltaTime,
                             parDic[PAR_STAT_TOTAL_FILES],
                             float(parDic[PAR_STAT_TOTAL_VOL] / (1024 * 1024)),
                             synchedCount,
                             filesInFailedDbm,
                             throughputFiles,
                             filesSinceLastRep,
                             remainingFiles,
                             throughputVol,
                             volSinceLastRep,
                             remainingVol,
                             complPercentage,
                             totalTime, totalTimeSecs,
                             estimComplTime)
                             

    # Add the command line options in the report.
    report += 128 * "-" + "\n"
    report += "Command Line Options:\n\n"
    pars = parDic.keys()
    pars.sort()
    tmpParDic = {}
    for par in pars:
        if (par[0] == "_"): continue
        tmpParDic[par.lower()] = parDic[par]
    for par in tmpParDic.keys():
        try:
            report += "%s=%s\n" % (par, parDic[par])
        except:
            pass
    report += 128 * "=" + "\n"
                             
    return report
        

def genReport(threadGroupObj):
    """
    Generate report from contents of the tool internal DBM.

    threadGroupObj:   Reference to the Thread Group Object (ngamsThreadGroup).

    Returns:  Void.
    """
    info(4, "Entering genReport() ...")
    
    parDic = threadGroupObj.getParameters()[0]
    report = genIntermediateReport(threadGroupObj)

    # Generate list of files that failed to be syncronized (if any).
    errorRep = ""
    parDic[FAILED_DBM_NAME].initKeyPtr()
    format = "%-32s %-32s %-15s %s\n"
    while (True):
        key, syncReq = parDic[FAILED_DBM_NAME].getNext()
        if (not key): break
        errorRep += format % (syncReq.getDiskId(), syncReq.getFileId(),
                              str(syncReq.getFileVersion()),
                              syncReq.getMessage())
    if (errorRep):
        report += "Failed Synchronization Requests:\n\n"
        report += format % ("Disk ID:", "File ID:", "Version:", "Error:")
        report += 128 * "-" + "\n"
        report += errorRep
        report += "\n" + 128 * "=" + "\n"
        
    info(4, "Leaving genReport()")
    return report


def intermediateReportLoop(threadGroupObj):
    """
    Loop to produce intermediate notification reports.

    threadGroupObj:   Reference to the Thread Group Object (ngamsThreadGroup).

    Returns:          Void.
    """
    info(4, "Entering intermediateReportLoop()")
    parDic = threadGroupObj.getParameters()[0]
    period = int(parDic[PAR_INT_NOTIF])
    startTime = time.time()
    noOfStreams = int(parDic[PAR_STREAMS])
    subject = "NGAS EXPRESS SYNC TOOL - INTERMEDIATE STATUS - SESSION: %s"
    while (True):
        # Wait till the next intermediate report should be generated.
        while ((time.time() - startTime) < period):
            if (threadGroupObj.getNumberOfActiveThreads() == 1):
                info(2, "Intermediate Report Thread (%s) terminating" %\
                     threadGroupObj.getThreadId())
                threadGroupObj.terminateNormal()
            if (not threadGroupObj.checkExecute()):
                threadGroupObj.terminateNormal()
            time.sleep(0.500)

        # Generate the report and broadcast it.
        startTime = time.time()
        report = genIntermediateReport(threadGroupObj)
        info(3, "Sending intermediate report ...")
        ngasUtilsLib.sendEmail(subject % parDic[PAR_SESSION_ID],
                               parDic[PAR_NOTIF_EMAIL], report, "text/plain")
        

def getNextSyncReq(threadGroupObj):
    """
    Get the next sync request. If there are no more in the queue, return None.

    Returns:   Next sync request or None (ngasSyncRequest | None). 
    """
    parDic = threadGroupObj.getParameters()[0]
    try:
        threadGroupObj.takeGenMux()

        # Get the next request (if any).
        parDic[QUEUE_DBM_NAME].initKeyPtr()
        key, syncReq = parDic[QUEUE_DBM_NAME].getNext()
        if (not key):
            threadGroupObj.releaseGenMux()
            return None

        # Insert the element in the processing queue.
        parDic[PROC_DBM_NAME].add(key, syncReq).sync()

        # Remove the entry from the queue.
        parDic[QUEUE_DBM_NAME].rem(key).sync()

        threadGroupObj.releaseGenMux()

        return syncReq
    except Exception, e:
        threadGroupObj.releaseGenMux()
        msg = "Error requesting next synchronization request. Error: %s"
        raise Exception, msg % str(e)


def moveReqFromProcToSyncQueue(threadGroupObj,
                               syncReq):
    """
    Put a Synchronization Request back in the Sync. DBM.

    threadGroupObj:   Reference to the Thread Group Object (ngamsThreadGroup).

    syncReq:          Synchronization request (ngasSyncRequest).

    Returns:          Void.
    """
    info(4, "Entering moveReqFromProcToSyncQueue() ...")
    
    parDic = threadGroupObj.getParameters()[0]
    try:
        threadGroupObj.takeGenMux()

        key = ngamsLib.genFileKey(None, syncReq.getFileId(),
                                  syncReq.getFileVersion())
                          
        # Insert the request object in the Sync. DBM.
        parDic[QUEUE_DBM_NAME].add(key, syncReq).sync()

        # Remove it from the Processing DBM.
        try:  # TODO: Should not be needed.
            parDic[PROC_DBM_NAME].rem(key).sync()
        except:
            pass
            
        threadGroupObj.releaseGenMux()

        return syncReq
    except Exception, e:
        threadGroupObj.releaseGenMux()
        msg = "Error moving Sync. Request from Processing to Sync. DBM. " +\
              "Error: %s"
        raise Exception, msg % str(e)


def moveReqFromSyncToSynchedQueue(threadGroupObj,
                                  syncReq):
    """
    Put a synchronization request in the sync'ed DBM.

    threadGroupObj:   Reference to the Thread Group Object (ngamsThreadGroup).

    syncReq:          Synchronization request (ngasSyncRequest).

    Returns:          Void.
    """
    info(4, "Entering moveReqFromSyncToSynchedQueue() ...")
    
    parDic = threadGroupObj.getParameters()[0]
    try:
        threadGroupObj.takeGenMux()

        key = ngamsLib.genFileKey(None, syncReq.getFileId(),
                                  syncReq.getFileVersion())

        # Insert it in the Synch'ed DBM.
        parDic[SYNCHED_DBM_NAME].add(key, syncReq).sync()

        # Remove it from the Processing DBM.
        parDic[PROC_DBM_NAME].rem(key).sync()
            
        threadGroupObj.releaseGenMux()

        return syncReq
    except Exception, e:
        threadGroupObj.releaseGenMux()
        msg = "Error moving Sync. Request from Processing to Synch'ed DBM. " +\
              "Error: %s"
        raise Exception, msg % str(e)


def moveReqFromProcToFailedQueue(threadGroupObj,
                                 syncReq):
    """
    Put a synchronization request in the failed DBM.

    threadGroupObj:   Reference to the Thread Group Object (ngamsThreadGroup).

    syncReq:          Synchronization request (ngasSyncRequest).

    Returns:          Void.
    """
    info(4, "Entering moveReqFromProcToFailedQueue() ...")

    parDic = threadGroupObj.getParameters()[0]
    try:
        threadGroupObj.takeGenMux()

        key = ngamsLib.genFileKey(None, syncReq.getFileId(),
                                  syncReq.getFileVersion())
        
        # Insert it in the failed DBM.
        parDic[FAILED_DBM_NAME].add(key, syncReq).sync()

        # Remove it from the Processing DBM.
        try: # TODO: Try should not be needed.
            parDic[PROC_DBM_NAME].rem(key).sync()
        except:
            pass
        # TODO: this block should not be needed.
        try: 
            parDic[QUEUE_DBM_NAME].rem(key).sync()
        except:
            pass
            
        threadGroupObj.releaseGenMux()
        
        return syncReq
    except Exception, e:
        threadGroupObj.releaseGenMux()
        msg = "Error moving Sync. Request from Processing to Failed DBM. " +\
              "Error: %s"
        raise Exception, msg % str(e)


def cloneFile(threadGroupObj,
              client,
              syncReq):
    """
    Clone a file. 

    threadGroupObj:   Reference to the Thread Group Object (ngamsThreadGroup).

    client:           NG/AMS Python Client instance (ngamsPClient).

    syncReq:          Synchronization request (ngasSyncRequest).

    Returns:          Status object (ngamsStatus).
    """
    info(4, "Entering cloneFile() ...")
    
    try:
        fileVer = syncReq.getFileVersion()
        cmdPars = [["file_id", syncReq.getFileId()], ["file_version", fileVer]]
        if (syncReq.getDiskId() != "-"):
            cmdPars.append(["disk_id", syncReq.getDiskId()])
        statObj = client.sendCmd(NGAMS_CLONE_CMD, pars=cmdPars)
        info(4, "Leaving cloneFile() (OK)")
        return statObj
    except Exception, e:
        info(4, "Leaving cloneFile() (ERROR)")
        statObj = ngamsStatus.ngamsStatus().setStatus(NGAMS_FAILURE).\
                  setMessage(str(e))
        return statObj
    

def getClusterReadyNaus(dbCon,
                        targetCluster):
    """
    Get the list of ready NAUs in a cluster.

    dbCon:          DB connection (ngamsDb).

    targetCluster:  Target cluster name (string).

    Returns:        List of NAUs (list).
    """
    sqlQuery = "SELECT host_id, srv_port FROM ngas_hosts " +\
               "WHERE cluster_name = '%s' AND host_id in " +\
               "(SELECT host_id FROM ngas_disks WHERE completed = 0 " +\
               "AND mounted = 1) ORDER BY host_id"
    sqlQuery = sqlQuery % targetCluster
    res = dbCon.query(sqlQuery)
    if (res == [[]]):
        return []
    else:
        hostList = []
        for node in res[0]:
            hostList.append("%s:%s" % (node[0], node[1]))
        return hostList

    
def getClusterNodes(dbCon,
                    targetCluster):
    """
    Get the list of nodes in the cluster.

    dbCon:          DB connection (ngamsDb).

    targetCluster:  Target cluster name (string).

    Returns:        Tuple with three elements:
                      1. List containing node:port pairs.
                      2. String buffer with comma separated list of hostnames.
                      3. String buffer with a comma separated list of
                         host:port pairs.
                                                                    (tuple).
    """
    sqlQuery = "SELECT host_id, srv_port FROM ngas_hosts " +\
               "WHERE cluster_name = '%s'"
    sqlQuery = sqlQuery % targetCluster
    res = dbCon.query(sqlQuery)
    if (res == [[]]):
        return []
    else:
        hostList = []
        hostListStr = ""
        srvList = ""
        for node in res[0]:
            hostList.append("%s:%s" % (node[0], node[1]))
            hostListStr += "'%s'," % node[0]
            srvList += "%s:%s," % (node[0], node[1])
        return (hostList, hostListStr[:-1], srvList[:-1])


def checkIfFileInTargetCluster(dbCon,
                               clusterNodes,
                               fileId,
                               fileVersion):
    """
    Query the information for a file

    dbCon:         DB connection (ngamsDb).

    clusterNodes:  List of nodes in the target cluster (list).
     
    fileId:        ID of file to check for (string).
    
    fileVersion:   Version of file (integer).

    Returns:       Information for file as tuple:
                   (<Disk Id>, <File ID>, <File Version>) or None
                   if the file is not available in the cluster (tuple | None).
    """
    info(4, "Entering checkIfFileInTargetCluster() ...")
    
    sqlQuery = "SELECT nf.disk_id, nf.file_id, nf.file_version " +\
               "FROM ngas_files nf, ngas_disks nd " +\
               "WHERE nf.file_id = '%s' AND nf.file_version = %d AND " +\
               "nf.disk_id = nd.disk_id AND " +\
               "(nd.host_id IN (%s) OR nd.last_host_id IN (%s))"
    sqlQuery = sqlQuery % (fileId, fileVersion, clusterNodes, clusterNodes)
    res = dbCon.query(sqlQuery)
    if (res[0] != []):
        diskId, fileId, fileVersion = res[0][0]
        msg = "Leaving checkIfFileInTargetCluster() (OK: Disk ID: %s, " +\
              "File ID: %s, File Version: %s"
        info(2, msg % (str(diskId), str(fileId), str(fileVersion)))
        return (diskId, fileId, fileVersion)
    else:
        info(2, "Leaving checkIfFileInTargetCluster() (empty result)")
        return (None, None, None)


def syncLoop(threadGroupObj):
    """
    Loop that carries out the synchronization.

    threadGroupObj:   Reference to the Thread Group Object (ngamsThreadGroup).

    Returns:          Void.
    """
    info(4, "Entering syncLoop()")

    parDic = threadGroupObj.getParameters()[0]
    clusterNodeStatusTime = 0.0
    client = ngamsPClient.ngamsPClient()
    while (True):
        # Check if the execution should continue before each iteration.
        threadGroupObj.checkPauseStop()
        
        syncReq = getNextSyncReq(threadGroupObj)

        # If there are no more requests to handle, terminate the thread.
        if (not syncReq):
            info(2, "Synchronization thread: %s terminating" %\
                 threadGroupObj.getThreadId())
            threadGroupObj.terminateNormal()

        if ((time.time() - syncReq.getTimeLastAttempt()) <
            NGAS_MIN_RETRY_TIME):
            # Time waiting for next retry has not expired. Put the request
            # back in the Sync. Queue.
            moveReqFromProcToSyncQueue(threadGroupObj, syncReq)
        
        # Get list of archiving units in the Target Cluster
        timeNow = time.time()
        if ((timeNow - clusterNodeStatusTime) > 60):
            clusterNaus = getClusterReadyNaus(parDic[PAR_DB_CON],
                                              parDic[PAR_TARGET_CLUSTER])
            clusterNodes, clusterNodesStr, clusterSrvList =\
                          getClusterNodes(parDic[PAR_DB_CON],
                                          parDic[PAR_TARGET_CLUSTER])
            client.parseSrvList(clusterSrvList)
            clusterNauStatusTime = timeNow
            
        # Check if file is already in target cluster.
        fileVer = syncReq.getFileVersion()
        targClusterDiskId, targClusterFileId, targClusterFileVersion =\
                           checkIfFileInTargetCluster(parDic[PAR_DB_CON],
                                                      clusterNodesStr,
                                                      syncReq.getFileId(),
                                                      fileVer)
        if (targClusterDiskId):
            # Carry out the CHECKFILE check only if requested
            if (parDic[PAR_CHECK_FILE]):
                # File is in target cluster, send a CHECKFILE Command to
                # determine if the file is safely archived.
                res = checkFile(parDic, client, targClusterDiskId,
                                targClusterFileId, targClusterFileVersion)
            else:
                # File is in the name space of the Target Cluster, accept it.
                res = "FILE_OK"
        else:
            res = "ERROR"
        if (res.find("FILE_OK") != -1):
            msg = "File: %s is available in the target cluster - skipping"
            info(1, msg % syncReq.getSummary())
            
            # If the file is safely archived in the target cluster, insert
            # its handled into the synch'ed DBM and go to the next file.
            syncReq.setMessage(NGAS_ALREADY_SYNCHED)
            moveReqFromSyncToSynchedQueue(threadGroupObj, syncReq)
            # Update statistics.
            parDic[PAR_STAT_FILE_COUNT] += 1
            parDic[PAR_STAT_VOL_ACCU] += syncReq.getFileSize()
        elif (res.find("ERROR") != -1):
            msg = "File: %s not available in the target cluster - restoring"
            info(1, msg % syncReq.getSummary())
            
            # The file is not available in the target cluster. We have to
            # clone it to the target cluster.
            statObj = None
            try:
                errMsg = ""
                syncReq.incrAttemptCount()
                statObj = cloneFile(threadGroupObj, client, syncReq)
                if (statObj.getStatus() == NGAMS_FAILURE):
                    errMsg = "Error handling Synchronization Request: %s. " +\
                             "Error: %s"
                    errMsg = errMsg %\
                             (syncReq.getSummary(), statObj.getMessage())
                    warning(errMsg)
                else:
                    msg = "Successfully cloned Synchronization Request: %s."
                    info(1, msg % syncReq.getSummary())
            except Exception, e:
                errMsg = "Error ocurred cloning file for Synchronization " +\
                         "Request: %s. Error: %s"
                if (statObj):
                    warning(errmsg %\
                            (syncReq.getSummary(), statObj.getMessage()))
                else:
                    warning(errMsg % (syncReq.getSummary(), str(e)))
            if (errMsg):
                # Something went wrong, determine whether to abandonning
                # synchronizing this file or whether to retry later.
                syncReq.setMessage(errMsg)
                if (syncReq.getAttemptCount() >= NGAS_MAX_RETRIES):
                    moveReqFromProcToFailedQueue(threadGroupObj, syncReq)
                    # Update statistics.
                    parDic[PAR_STAT_FILE_COUNT] += 1
                    parDic[PAR_STAT_VOL_ACCU] += syncReq.getFileSize()
                else:
                    moveReqFromProcToSyncQueue(threadGroupObj, syncReq)
            else:
                # File was successfully cloned. Move to Sync'ed DBM.
                syncReq.setMessage("")
                moveReqFromSyncToSynchedQueue(threadGroupObj, syncReq)
                # Update statistics.
                parDic[PAR_STAT_FILE_COUNT] += 1
                parDic[PAR_STAT_VOL_ACCU] += syncReq.getFileSize()
        else:
            # Something went wrong, put the entry in the failed DBM.
            syncReq.setMessage(res)
            moveReqFromProcToFailedQueue(threadGroupObj, syncReq)
            # Update statistics.
            parDic[PAR_STAT_FILE_COUNT] += 1
            parDic[PAR_STAT_VOL_ACCU] += syncReq.getFileSize()

 
def syncThread(threadGroupObj):
    """
    The Synchronization Thread running within an ngamsThreadGroup object.

    The first thread (thread number 1), will also generate intermediate
    status reports if requested.

    threadGroupObj:   Reference to the Thread Group Object (ngamsThreadGroup).

    Returns:          Void.
    """
    info(4, "Entering syncThread()")
    thrGr = threadGroupObj
    parDic = thrGr.getParameters()[0]

    # Thread #1 takes care of sending out intermediate reports if specified.
    if ((thrGr.getThreadNo() == 1) and parDic[PAR_INT_NOTIF]):
        intermediateReportLoop(thrGr)
    else:
        syncLoop(thrGr)

 
def execute(parDic):
    """
    Carry out the tool execution.

    parDic:    Dictionary containing the parameters and options (dictionary).

    Returns:   Void.
    """
    info(4, "Entering execute() ...")
    
    if (parDic["help"]):
        print correctUsage()
        sys.exit(0)        
    ngasUtilsLib.getCheckAccessCode(getOptDic())

    initialize(parDic)

    # Start the threads.
    # If intermediate reporting is specified a special thread is started for
    # this.
    if (parDic[PAR_INT_NOTIF]):
        streams = (int(parDic[PAR_STREAMS]) + 1)
    else:
        streams = int(parDic[PAR_STREAMS])
    syncThreads = ngamsThreadGroup.ngamsThreadGroup(parDic[PAR_SESSION_ID],
                                                    syncThread, streams,
                                                    [parDic])
    syncThreads.start()

    # Generate final report if requested.
    if (parDic[PAR_NOTIF_EMAIL]):
        info(1, "Generating report about actions carried out ...")
        report = genReport(syncThreads)
        subject = "NGAS EXPRESS SYNCHRONIZATION TOOL - " +\
                  "FINAL STATUS REPORT - SESSION: %s"
        info(1, "Sending final report ...")
        ngasUtilsLib.sendEmail(subject % parDic[PAR_SESSION_ID],
                               parDic[PAR_NOTIF_EMAIL], report,
                               "text/plain", "NGAS-XSYNC-TOOL-REPORT.txt")

    # Clean up old working directories (if older than 30 days).
    # TODO.

    info(4, "Leaving execute()")


if __name__ == '__main__':
    """
    Main function to execute the tool.
    """
    try:
        optDic = ngasUtilsLib.parseCmdLine(sys.argv, getOptDic())
        parDic = ngasUtilsLib.optDic2ParDic(optDic)
    except Exception, e:
        print "\nProblem executing the tool:\n\n%s\n" % str(e)
        sys.exit(1) 
    if (getDebug()):
        execute(parDic)
        rmFile(getLockFile(parDic))
    else:
        try:
            execute(parDic)
            rmFile(getLockFile(parDic))
        except Exception, e:
            rmFile(getLockFile(parDic))
            if (str(e) == "0"): sys.exit(0)
            print "\nProblem executing the tool:\n\n%s\n" % str(e)
            sys.exit(1)
                  

# EOF
