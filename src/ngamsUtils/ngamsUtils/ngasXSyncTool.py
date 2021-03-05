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

# *****************************************************************************
# ESO/DFS
#
# "@(#) $Id: ngasXSyncTool.py,v 1.3 2011/11/24 13:06:42 amanning Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  18/06/2008  Created
#

# TODO: - BUG: Possible to have multiple syncs. (after restarting the tool).
#       - Test handling of file list.
#       - Add "Real Throughput Volume" in report.
#       - Add "Real Throughput Files" in report.
#       - Test handling of failing sync requests (file list with illegal reqs)
#       - Test persistence.
#       - Clean up session directories older than 30 days.
#       - Case: --target-cluster=$$$$$: Better error message.

from datetime import datetime
import hashlib
import logging
import os
import random
import sys
import time
import traceback
import getpass

from ngamsLib.ngamsCore import checkCreatePath, getHostName, mvFile, rmFile, NGAMS_CHECKFILE_CMD, NGAMS_CLONE_CMD, \
    NGAMS_FAILURE
from ngamsLib import ngamsDb
from ngamsLib import ngamsDbm
from ngamsLib import ngamsDiskInfo
from ngamsLib import ngamsLib
from ngamsLib import ngamsStatus
from ngamsLib import ngamsThreadGroup
from ngamsPClient import ngamsPClient
from . import ngasUtilsLib

LOGGING_FORMAT = "%(asctime)s %(processName)-20.20s %(levelname)-8.8s - %(message)s"
LOGGING_FILE_PATH = os.path.join(os.getcwd(), "ngas-xsync-tool.log")
logging.basicConfig(filename=LOGGING_FILE_PATH, format=LOGGING_FORMAT, level="DEBUG")
logging.getLogger(__name__).addHandler(logging.StreamHandler())
logger = logging.getLogger(__name__)

_document =\
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
space. Determining this can be done according to two schemes:

1. File is registered in DB (OK, Bad, Ignore).

2. The file is safely stored in the Target Cluster. This is checked by sending
a CHECKFILE Command to the Target Cluster, which will result in an explicit test
of the consistency and availability of the file in the target cluster name
space.

Files can either be referenced by:

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
streams'. Specifying too many streams when syncing a HDD-based volume, may
reduce the performance heavily since the read/write head of the HDD will be
offsetting continuously. On the other hand, it is clearly more efficient to
have a number of parallel streams syncing the data, compared to only one
stream, executing the batch sequentially.

After the tool has been executed on a set of files, it can also be used to
verify that the cloning was successful and can be used to correct possible
failing file synchronizations by simply executing it once more.

The tool implements persistence, such that if interrupted in the middle of
the processing of a batch, it will resume from the point where it was
interrupted. Should the files however, be lost which provides this persistence,
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

{:s}

"""

# Constants
# =========
NGAS_XSYNC_TOOL = "NGAS_XSYNC_TOOL"
NGAS_MAX_RETRIES = 3
NGAS_MIN_RETRY_TIME = 10
NGAS_ALREADY_SYNCED = "Already synchronized"

# Parameters
# ==========

# Internal parameters
PAR_DB_CON = "_db-con"
PAR_DISK_INFO = "_disk-info"
PAR_SESSION_ID = "_session-id"
PAR_START_TIME = "_start-time"

# Parameters for statistics
PAR_STAT_TOTAL_FILES = "_total-files"
PAR_STAT_FILE_COUNT = "_file-count"
PAR_STAT_LAST_FILE_COUNT = "_last-file-count"
PAR_STAT_TOTAL_VOL = "_total-volume"
PAR_STAT_VOL_ACCU = "_volume-accumulator"
PAR_STAT_LAST_VOL_ACCU = "_last-volume-accumulator"

# Command line options
PAR_CHECK_FILE = "check-file"
PAR_CLEAN = "clean"
PAR_FORCE = "force"
PAR_CLUSTER_ID = "cluster-id"
PAR_DISK_ID = "disk-id"
PAR_HOST_ID = "host-id"
PAR_FILE_LIST = "file-list"
PAR_INT_NOTIF = "intermediate-notif"
PAR_NOTIF_EMAIL = "notif-email"
PAR_STREAMS = "streams"
PAR_TARGET_CLUSTER = "target-cluster"
PAR_TARGET_NODES = "target-nodes"
PAR_WORKING_DIR = "working-dir"

# DBM names
TMP_QUEUE_DBM_NAME = "_TMP_SYNC_QUEUE"
QUEUE_DBM_NAME = "_SYNC_QUEUE"
PROC_DBM_NAME = "_PROC_FILES"
SYNCED_DBM_NAME = "_SYNCED_FILES"
FAILED_DBM_NAME = "_FAILED_FILES"

# Definition of predefined command line parameters
_options = [
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
     "ID for the volume to synchronize. If not given, the --fileList " +
     "parameter must be used to specify the source files."],

    [PAR_HOST_ID, [], None, ngasUtilsLib.NGAS_OPT_OPT, "=<Host ID>",
     "ID for an entire host to synchronize. Cannot be specified in " +
     "conjunction with --disk-id or --file-list."],

    [PAR_FILE_LIST, [], None, ngasUtilsLib.NGAS_OPT_OPT, "=<NGAS File List>",
     "List containing references to files to consider for synchronization. " +
     "Each line must specify: <Disk ID> <File ID> <File Version>."],

    [PAR_TARGET_CLUSTER, [], None, ngasUtilsLib.NGAS_OPT_OPT,
     "=<Target Cluster Name>",
     "Name of NGAS Cluster to which the files will be synchronized."],

    [PAR_TARGET_NODES, [], None, ngasUtilsLib.NGAS_OPT_OPT,
     "=<Target Node Names and Ports>",
     "Comma separated list of NGAS nodes to which the files will be synchronized." +
     "For example, \"ngas:8001,ngas:8002,ngas:8003,ngas:8004\""],

    [PAR_STREAMS, [], None, ngasUtilsLib.NGAS_OPT_MAN,
     "=<Number of Streams to Execute>",
     "Number of streams to execute in parallel."],

    [PAR_WORKING_DIR, [], "/tmp", ngasUtilsLib.NGAS_OPT_OPT, "=<Working Dir>",
     "Working directory of the tool. The tool will create this " +
     "directory, in which it will store its house-keeping files."],

    [PAR_NOTIF_EMAIL, [], None, ngasUtilsLib.NGAS_OPT_OPT,
     "=<Email Recipients>", "Comma separated list of email addresses."],

    [PAR_INT_NOTIF, [], None, ngasUtilsLib.NGAS_OPT_OPT,
     "=<Intermediate Notification Interval (s)>",
     "Time in seconds for which to send out an intermediate status report " +
     "for the processing of the synchronization batch."],

    [PAR_CHECK_FILE, [], 0, ngasUtilsLib.NGAS_OPT_OPT, 0,
     "Execute a CHECKFILE Command for each file in the list in the " +
     "Target Cluster - TO BE USED WITH CAUTION!"]
]

_option_dict, _option_document = ngasUtilsLib.generate_options_dictionary_and_document(_options)
__doc__ = _document.format(_option_document)


class NgasSyncRequest:
    """
    Class to hold the information for one synchronization request.
    """

    def __init__(self, disk_id, file_id, file_version, file_size):
        """
        Constructor

        :param disk_id: Disk ID of source disk (string)
        :param file_id: File ID of source file (string)
        :param file_version: Version of source file (integer)
        :param file_size: File size in bytes (integer)
        """
        self.__disk_id = disk_id
        self.__file_id = file_id
        self.__file_version = file_version
        self.__file_size = file_size
        self.__attempts = 0
        self.__last_attempt = 0.0
        self.__message = ""

    def increment_attempt_count(self):
        """
        Increment the attempts counter

        :return: Reference to object itself
        """
        self.__attempts += 1
        self.__last_attempt = time.time()
        return self

    def get_attempt_count(self):
        """
        Get the value of the attempts counter

        :return: Value of attempts counter (integer)
        """
        return self.__attempts

    def get_time_last_attempt(self):
        """
        Get the time for the last attempt

        :return: Time for last attempt in seconds since epoch (float)
        """
        return self.__last_attempt

    def get_disk_id(self):
        """
        Return the Disk ID of the source disk

        :return: Disk ID (string)
        """
        return self.__disk_id

    def get_file_id(self):
        """
        Return the File ID

        :return: File ID (string)
        """
        return self.__file_id

    def get_file_version(self):
        """
        Get the File Version

        :return: File version (integer)
        """
        return self.__file_version

    def get_summary(self):
        """
        Generate and return summary of contents in the synchronization request

        :return: Request summary (string)
        """
        return "{:s}/{:s}/{:d}".format(self.__disk_id, self.__file_id, self.__file_version)

    def set_message(self, message):
        """
        Set the internal message of the object

        :param message: Message (string)
        :return: Reference to object itself
        """
        self.__message = message
        return self

    def get_message(self):
        """
        Get the internal message of the object

        :return: Message (string)
        """
        return self.__message

    def get_file_size(self):
        """
        Return the file size

        :return: Filesize in bytes (integer)
        """
        return self.__file_size


def get_option_dict():
    """
    Return reference to command line options dictionary

    :return: Reference to dictionary containing the command line options (dictionary)
    """
    return _option_dict


def correct_usage():
    """
    Return the usage/online documentation in a string buffer

    :return: Help description (string)
    """
    return __doc__


def get_lock_file(param_dict):
    """
    Generate and return the lock file for this session of the tool

    :param param_dict: Dictionary with parameters (dictionary)
    :return: Lock filename (string)
    """
    logger.info("Entering get_lock_file() ...")
    lock_file = os.path.normpath(os.path.join(param_dict[PAR_WORKING_DIR], param_dict[PAR_SESSION_ID],
                                              "{:s}.lock".format(NGAS_XSYNC_TOOL)))
    logger.info("Leaving get_lock_file()")
    return lock_file


def _add_in_file_dbm(file_list_generator, queue_dbm):
    """
    Add the entries that can be retrieved via the given DB cursor

    :param file_list_generator: Cursor object
    :param queue_dbm: DBM in which to add the file info (ngamsDbm)
    """
    for sql_file_info in file_list_generator:
        disk_id = sql_file_info[ngamsDb.SUM1_DISK_ID]
        file_id = sql_file_info[ngamsDb.SUM1_FILE_ID]
        file_version = int(sql_file_info[ngamsDb.SUM1_VERSION])
        file_size = float(sql_file_info[ngamsDb.SUM1_FILE_SIZE])
        sync_req = NgasSyncRequest(disk_id, file_id, file_version, file_size)
        key = ngamsLib.genFileKey(None, file_id, file_version)
        logger.info("DBM: Adding entry in Tmp Queue DBM with key: %s", key)
        queue_dbm.add(key, sync_req)


def _get_cluster_nodes(connection, cluster_id):
    """
    Get the list of nodes registered in the name space of the given cluster

    :param connection: DB connection (ngamsDb)
    :param cluster_id: NGAS Cluster ID (string)
    :return: List of NGAS Host IDs of the nodes in the cluster (list)
    """
    sql = "select host_id from ngas_hosts where cluster_name='{:s}'".format(cluster_id)
    # result = connection.query(sql, ignoreEmptyRes=0)
    result = connection.query2(sql)
    if len(result) == 0:
        return []
    else:
        node_list = []
        for node in result[0]:
            node_list.append(node[0])
        return node_list


def get_timestamp(seconds_since_epoch=None):
    """
    Returns ISO formatted timestamp
    """
    if seconds_since_epoch:
        return datetime.fromtimestamp(seconds_since_epoch).isoformat()
    return datetime.now().isoformat()


def initialize(param_dict):
    """
    Initialize the tool

    :param param_dict: Dictionary with parameters (dictionary)
    """
    logger.info("Entering initialize() ...")

    # Extra checks of command line options
    # =Rule 1: Can only specify one of: cluster-id, disk-id, host_id and file-list
    param_sum = (param_dict[PAR_CLUSTER_ID] is not None) + (param_dict[PAR_DISK_ID] is not None) +\
                (param_dict[PAR_HOST_ID] is not None) + (param_dict[PAR_FILE_LIST] is not None)
    if param_sum > 1:
        msg = "Can only specify one of the {:s}, {:s}, {:s} and {:s} options"
        raise Exception(msg.format(PAR_CLUSTER_ID, PAR_DISK_ID, PAR_HOST_ID, PAR_FILE_LIST))
    # =Rule 2: Must specify either cluster-id, disk-id, host-id or file-list
    elif param_sum == 0:
        msg = "Must specify one of the {:s}, {:s}, {:s} or {:s} options"
        raise Exception(msg.format(PAR_CLUSTER_ID, PAR_DISK_ID, PAR_HOST_ID, PAR_FILE_LIST))

    # Connect to the RDBMS
    connection = ngasUtilsLib.get_db_connection()

    param_dict[PAR_DB_CON] = connection

    # Generate ID for this session:
    #
    # 1. If Disk ID is given: '<Host ID>_<Slot ID>'
    # 2. If Host ID is given: '<Host ID>'
    # 3. If File List is given: md5(<contents file list>)
    if param_dict[PAR_DISK_ID]:
        # Get disk info for the volume involved
        disk_info_obj = ngamsDiskInfo.ngamsDiskInfo().read(param_dict[PAR_DB_CON], param_dict[PAR_DISK_ID])
        param_dict[PAR_DISK_INFO] = disk_info_obj
        host_id = param_dict[PAR_DISK_INFO].getHostId()
        slot_id = param_dict[PAR_DISK_INFO].getSlotId()
        if host_id.strip() == "" or slot_id.strip() == "":
            raise Exception("Illegal Disk ID specified: {:s}".format(disk_info_obj.get_disk_id()))
        session_id = "{:s}_{:s}".format(host_id, slot_id)
    elif param_dict[PAR_HOST_ID]:
        session_id = param_dict[PAR_HOST_ID]
    elif param_dict[PAR_CLUSTER_ID]:
        session_id = param_dict[PAR_CLUSTER_ID]
    else:
        try:
            hash_md5 = hashlib.md5()
            with open(param_dict[PAR_FILE_LIST]) as fo:
                hash_md5.update(fo.read())
            session_id = hash_md5.hexdigest()
        except Exception as e:
            msg = "Error loading File List: {:s}. Error: {:s}"
            raise Exception(msg.format(str(param_dict[PAR_FILE_LIST]), str(e)))
    logger.info("Allocated session ID: %s", session_id)
    param_dict[PAR_SESSION_ID] = session_id

    # Check if an instance with this ID is running
    lock_file_path = get_lock_file(param_dict)
    if not param_dict[PAR_FORCE] and os.path.exists(lock_file_path):
        msg = "An instance of this tool with ID: {:s} appears to be running - bailing out!"
        raise Exception(msg.format(str(param_dict[PAR_SESSION_ID])))
    else:
        if os.path.exists(lock_file_path):
            os.utime(lock_file_path, None)

    # Create working directory if not existing
    working_directory = os.path.normpath(os.path.join(param_dict[PAR_WORKING_DIR], param_dict[PAR_SESSION_ID]))
    # Start from a fresh if requested
    if param_dict[PAR_CLEAN]:
        rmFile(working_directory)
    checkCreatePath(working_directory)

    # Create DBMs
    tmp_queue_dbm_name = os.path.normpath(os.path.join(working_directory, TMP_QUEUE_DBM_NAME))
    queue_dbm_name = os.path.normpath(os.path.join(working_directory, QUEUE_DBM_NAME))
    proc_dbm_name = os.path.normpath(os.path.join(working_directory, PROC_DBM_NAME))
    synced_dbm_name = os.path.normpath(os.path.join(working_directory, SYNCED_DBM_NAME))
    failed_dbm_name = os.path.normpath(os.path.join(working_directory, FAILED_DBM_NAME))

    # If Queue DBM already exists, we are resuming an interrupted session. In that case, just open the existing DBMs,
    # else build up the DBMs
    if not os.path.exists("{:s}.bsddb".format(queue_dbm_name)):
        # Create Queue, first as temporary name, then rename it when complete
        tmp_queue_dbm = ngamsDbm.ngamsDbm(tmp_queue_dbm_name, writePerm=1)
        # Put file information into the queue
        if param_dict[PAR_DISK_ID] or param_dict[PAR_HOST_ID]:
            if param_dict[PAR_DISK_ID]:
                # Dump the information for that volume from the RDBMS
                file_list_generator = param_dict[PAR_DB_CON].getFileSummary1(diskIds=[param_dict[PAR_DISK_ID]],
                                                                             ignore=0, order=0)
            else:
                # Dump the information for that node from the RDBMS
                file_list_generator = param_dict[PAR_DB_CON].getFileSummary1(hostId=param_dict[PAR_HOST_ID],
                                                                             ignore=0, order=0)
            _add_in_file_dbm(file_list_generator, tmp_queue_dbm)
            del file_list_generator
        elif param_dict[PAR_CLUSTER_ID]:
            # Dump the information for an entire cluster
            cluster_nodes_list = _get_cluster_nodes(connection, param_dict[PAR_CLUSTER_ID])
            msg = "Found the following nodes: {:s} in cluster: {:s}"
            logger.info(msg.format(str(cluster_nodes_list), param_dict[PAR_CLUSTER_ID]))
            for cluster_node in cluster_nodes_list:
                file_list_generator = param_dict[PAR_DB_CON].getFileSummary1(hostId=cluster_node, ignore=0, order=0)
                _add_in_file_dbm(file_list_generator, tmp_queue_dbm)
                del file_list_generator
        else:
            file_list_element_list = ngasUtilsLib.parse_file_list(param_dict[PAR_FILE_LIST])
            for file_list_element in file_list_element_list:
                disk_id, file_id, file_version = file_list_element
                file_version = int(file_version)
                file_size = 0.0
                tmp_file_info = None
                try:
                    if disk_id != "-":
                        tmp_file_info = param_dict[PAR_DB_CON].getFileInfoFromFileId(file_id, file_version,
                                                                                     disk_id, dbCursor=False,
                                                                                     order=False)
                    else:
                        tmp_file_info = param_dict[PAR_DB_CON].getFileInfoFromFileId(file_id, file_version,
                                                                                     dbCursor=False, order=False)
                    file_size = float(tmp_file_info[0][5])
                except Exception as e:
                    logger.warning("Error obtaining information about file: %s/%s/%d. Error: %s", disk_id, file_id,
                                   file_version, str(e))
                # If the file is marked to be ignored, skip it
                if tmp_file_info:
                    ignore = False
                    try:
                        if int(tmp_file_info[0][9]):
                            ignore = True
                    except Exception:
                        pass
                    if ignore:
                        logger.info("File: %s/%s/%d marked to be ignored, skipping", disk_id, file_id, file_version)
                        continue
                sync_req = NgasSyncRequest(disk_id, file_id, file_version, file_size)
                key = ngamsLib.genFileKey(None, file_id, file_version)
                logger.info("DBM: Adding entry in temporary Queue DBM: %s", key)
                tmp_queue_dbm.add(key, sync_req)

        # If the processing DBM already exists, this means that these files were being processed while the tool was
        # interrupted. These files are copied back into the queue.
        if os.path.exists(proc_dbm_name):
            proc_dbm = ngamsDbm.ngamsDbm(proc_dbm_name, writePerm=1)
            param_dict[PROC_DBM_NAME] = proc_dbm
            proc_dbm.initKeyPtr()
            while True:
                key, val = proc_dbm.getNext()
                if not key:
                    break
                logger.info("DBM: Adding entry in temporary Queue DBM: %s", key)
                tmp_queue_dbm.add(key, val)
                logger.info("DBM: Removing entry from processing DBM: %s", key)
                proc_dbm.rem(key)

        # Ensure all elements have been synced into the DBM
        tmp_queue_dbm.sync()
        tmp_queue_dbm_name = tmp_queue_dbm.getDbmName()
        dbm_ext = os.path.splitext(tmp_queue_dbm_name)[-1]
        del tmp_queue_dbm
        mvFile(tmp_queue_dbm_name, "{:s}{:s}".format(queue_dbm_name, dbm_ext))

    # Open the DBMs
    param_dict[QUEUE_DBM_NAME] = ngamsDbm.ngamsDbm(queue_dbm_name, writePerm=1)
    if PROC_DBM_NAME not in param_dict:
        param_dict[PROC_DBM_NAME] = ngamsDbm.ngamsDbm(proc_dbm_name, writePerm=1)
    param_dict[SYNCED_DBM_NAME] = ngamsDbm.ngamsDbm(synced_dbm_name, writePerm=1)
    param_dict[FAILED_DBM_NAME] = ngamsDbm.ngamsDbm(failed_dbm_name, writePerm=1)

    # Calculate total volume
    total_volume = 0.0
    for dbm in [param_dict[QUEUE_DBM_NAME], param_dict[PROC_DBM_NAME],
                param_dict[SYNCED_DBM_NAME], param_dict[FAILED_DBM_NAME]]:
        dbm.initKeyPtr()
        while True:
            key, sync_req_obj = dbm.getNext()
            if not key:
                break
            total_volume += sync_req_obj.get_file_size()

    # Initialize the parameters used for statistics
    files_in_queue_dbm = param_dict[QUEUE_DBM_NAME].getCount()
    files_in_proc_dbm = param_dict[PROC_DBM_NAME].getCount()
    files_in_synced_dbm = param_dict[SYNCED_DBM_NAME].getCount()
    files_in_failed_dbm = param_dict[FAILED_DBM_NAME].getCount()
    param_dict[PAR_STAT_TOTAL_FILES] = files_in_queue_dbm + files_in_proc_dbm + files_in_synced_dbm + \
                                       files_in_failed_dbm
    param_dict[PAR_STAT_FILE_COUNT] = 0
    param_dict[PAR_STAT_LAST_FILE_COUNT] = 0
    param_dict[PAR_STAT_TOTAL_VOL] = total_volume
    param_dict[PAR_STAT_VOL_ACCU] = 0.0
    param_dict[PAR_STAT_LAST_VOL_ACCU] = 0.0
    logger.info("Leaving initialize()")


def check_file(client, disk_id, file_id, file_version):
    """
    Send a CHECKFILE to the target cluster

    :param client: NG/AMS Python Client instance (ngamsPClient)
    :param disk_id: ID of target disk to consider in the target cluster (string)
    :param file_id: ID of file to check (string)
    :param file_version: Version of file (integer)
    :return: Status of the checking. Can be either of:
                1. File OK (contains string 'FILE_OK')
                2. File Not OK (contains string 'FILE_NOK')
                3. Other error, typically an exception occurred (string)
    """
    try:
        parameters = [["disk_id", disk_id], ["file_id", file_id], ["file_version", file_version]]
        status = client.get_status(NGAMS_CHECKFILE_CMD, pars=parameters)
        return status.get_message()
    except Exception as e:
        return str(e)


def generate_intermediate_report(thread_group_obj):
    """
    Generate intermediate report from contents of the tool internal DBM

    :param thread_group_obj: Reference to the Thread Group Object (ngamsThreadGroup)
    :return: Report (string)
    """
    logger.info("Generating intermediate report ...")

    report_format = "NGAS express synchronization tool - status report:\n\n" +\
                    "Time:                              {:s}\n" +\
                    "Host:                              {:s}\n" +\
                    "User:                              {:s}\n" +\
                    "Session ID:                        {:s}\n" +\
                    "Start Time:                        {:s}\n" +\
                    "Time Elapsed:                      {:.2f} hours ({:.3f} s)\n" +\
                    "Total Files:                       {:d}\n" +\
                    "Total Volume:                      {:.3f} MB\n" +\
                    "Synchronized Files:                {:d}\n" +\
                    "Failed Files:                      {:d}\n" +\
                    "Processing Throughput Files:       {:.3f} files/s\n" +\
                    "Files Handled Since Last Report:   {:d}\n" +\
                    "Remaining Files:                   {:d}\n" +\
                    "Processing Throughput Volume:      {:.3f} MB/s\n" +\
                    "Volume Since Last Report:          {:.3f} MB\n" +\
                    "Remaining Volume:                  {:.3f} MB\n" +\
                    "Completion Percentage:             {:.3f} %\n" +\
                    "Estimated Total Time:              {:.2f} hours ({:.3f} s)\n" +\
                    "Estimated Completion Time:         {:s}\n"

    param_dict = thread_group_obj.getParameters()[0]
    try:
        thread_group_obj.takeGenMux()
        # num_files_in_queue_dbm = param_dict[QUEUE_DBM_NAME].getCount()
        # num_files_in_proc_dbm = param_dict[PROC_DBM_NAME].getCount()

        param_dict[SYNCED_DBM_NAME].initKeyPtr()
        synced_count = 0
        while True:
            key, sync_req = param_dict[SYNCED_DBM_NAME].getNext()
            if not key:
                break
            if sync_req.get_message() != NGAS_ALREADY_SYNCED:
                synced_count += 1

        # num_files_in_synced_dbm = param_dict[SYNCED_DBM_NAME].getCount()
        num_files_in_failed_dbm = param_dict[FAILED_DBM_NAME].getCount()
        thread_group_obj.releaseGenMux()
    except Exception as e:
        thread_group_obj.releaseGenMux()
        logger.error("Error generating intermediate status report. Error: %s", str(e))
        return

    # Generate the statistics
    time_now = time.time()
    report_time = get_timestamp()
    start_time = get_timestamp()
    delta_time = time_now - param_dict[PAR_START_TIME]
    time_elapsed = delta_time / 3600.0
    throughput_files = float(param_dict[PAR_STAT_FILE_COUNT]) / delta_time
    files_since_last_report = param_dict[PAR_STAT_FILE_COUNT] - param_dict[PAR_STAT_LAST_FILE_COUNT]
    param_dict[PAR_STAT_LAST_FILE_COUNT] = param_dict[PAR_STAT_FILE_COUNT]
    remaining_files = param_dict[PAR_STAT_TOTAL_FILES] - param_dict[PAR_STAT_FILE_COUNT]
    throughput_volume = float((float(param_dict[PAR_STAT_VOL_ACCU]) / delta_time) / (1024 * 1024))
    volume_since_last_report = (param_dict[PAR_STAT_VOL_ACCU] - param_dict[PAR_STAT_LAST_VOL_ACCU]) / (1024 * 1024)
    param_dict[PAR_STAT_LAST_VOL_ACCU] = param_dict[PAR_STAT_VOL_ACCU]
    remaining_volume = (param_dict[PAR_STAT_TOTAL_VOL] - param_dict[PAR_STAT_VOL_ACCU]) / (1024 * 1024)
    # Completion percentage is calculated as the average of completion/files and completion/volume
    if param_dict[PAR_STAT_TOTAL_FILES] > 0:
        completion_files = 100.0 * (float(param_dict[PAR_STAT_FILE_COUNT]) /
                                    float(param_dict[PAR_STAT_TOTAL_FILES]))
    else:
        completion_files = -1
    if param_dict[PAR_STAT_TOTAL_VOL] > 0:
        completion_volume = 100.0 * (param_dict[PAR_STAT_VOL_ACCU] / param_dict[PAR_STAT_TOTAL_VOL])
    else:
        completion_volume = -1
    if completion_files != -1 or completion_volume != -1:
        completion_percentage = float((completion_files + completion_volume) / 2.0)
    else:
        completion_percentage = 0.0
    if remaining_files == 0:
        total_time_secs = delta_time
        total_time = float(total_time_secs / 3600.0)
        estimate_completion_time = get_timestamp()
    elif completion_percentage > 0.0:
        total_time_secs = float(100.0 * (delta_time / completion_percentage))
        total_time = float(total_time_secs / 3600.0)
        estimate_completion_time = get_timestamp(param_dict[PAR_START_TIME] + total_time_secs)
    else:
        total_time_secs = -1
        total_time = -1
        estimate_completion_time = "UNDEFINED"
    report = report_format.format(report_time,
                                  getHostName(),
                                  getpass.getuser(),
                                  param_dict[PAR_SESSION_ID],
                                  start_time,
                                  time_elapsed, delta_time,
                                  param_dict[PAR_STAT_TOTAL_FILES],
                                  float(param_dict[PAR_STAT_TOTAL_VOL] / (1024 * 1024)),
                                  synced_count,
                                  num_files_in_failed_dbm,
                                  throughput_files,
                                  files_since_last_report,
                                  remaining_files,
                                  throughput_volume,
                                  volume_since_last_report,
                                  remaining_volume,
                                  completion_percentage,
                                  total_time, total_time_secs,
                                  estimate_completion_time)

    # Add the command line options in the report
    report += 50 * "-" + "\n"
    report += "Command Line Options:\n\n"
    param_list = param_dict.keys()
    param_list.sort()
    tmp_param_dict = {}
    for param in param_list:
        if param[0] == "_":
            continue
        tmp_param_dict[param.lower()] = param_dict[param]
    for param in tmp_param_dict.keys():
        try:
            report += "{:s}={:s}\n".format(param, param_dict[param])
        except Exception:
            pass
    report += 50 * "=" + "\n"

    return report


def generate_report(thread_group_obj):
    """
    Generate report from contents of the tool internal DBM

    :param thread_group_obj: Reference to the Thread Group Object (ngamsThreadGroup)
    """
    logger.info("Entering generate_report() ...")

    param_dict = thread_group_obj.getParameters()[0]
    report = generate_intermediate_report(thread_group_obj)

    # Generate list of files that failed to be synchronized (if any)
    error_report = ""
    param_dict[FAILED_DBM_NAME].initKeyPtr()
    error_format = "{:-32s} {:-32s} {:-15s} {:s}\n"
    while True:
        key, sync_req = param_dict[FAILED_DBM_NAME].getNext()
        if not key:
            break
        error_report += error_format.format(sync_req.get_disk_id(), sync_req.get_file_id(),
                                            str(sync_req.get_file_version()), sync_req.get_message())
    if error_report:
        report += "Failed Synchronization Requests:\n\n"
        report += error_format.format("Disk ID:", "File ID:", "Version:", "Error:")
        report += 50 * "-" + "\n"
        report += error_report
        report += "\n" + 50 * "=" + "\n"

    logger.info("Leaving generate_report()")
    return report


def intermediate_report_loop(thread_group_obj):
    """
    Loop to produce intermediate notification reports

    :param thread_group_obj: Reference to the Thread Group Object (ngamsThreadGroup)
    """
    logger.info("Entering intermediate_report_loop()")
    param_dict = thread_group_obj.getParameters()[0]
    period = int(param_dict[PAR_INT_NOTIF])
    start_time = time.time()
    subject = "NGAS express sync tool - intermediate status report (session: {:s})".format(param_dict[PAR_SESSION_ID])

    while True:
        # Wait till the next intermediate report should be generated
        while (time.time() - start_time) < period:
            if thread_group_obj.getNumberOfActiveThreads() == 1:
                logger.info("Intermediate Report Thread ({:s}) terminating".format(thread_group_obj.getThreadId()))
                thread_group_obj.terminateNormal()
            if not thread_group_obj.checkExecute():
                thread_group_obj.terminateNormal()
            time.sleep(0.5)

        # Generate the report and broadcast it
        start_time = time.time()
        report = generate_intermediate_report(thread_group_obj)
        logger.info("Sending intermediate report ...")
        ngasUtilsLib.send_email(subject, param_dict[PAR_NOTIF_EMAIL], report, "text/plain", "NGAS-XSYNC-report.txt")


def get_active_requests(thread_group_obj):
    """
    Figure out how many requests are still being processed

    :return: Number of requests still in Sync or Proc Queues (integer)
    """
    param_dict = thread_group_obj.getParameters()[0]
    try:
        thread_group_obj.takeGenMux()
        active_requests = (param_dict[QUEUE_DBM_NAME].getCount() + param_dict[PROC_DBM_NAME].getCount())
        thread_group_obj.releaseGenMux()
        return active_requests
    except Exception as e:
        thread_group_obj.releaseGenMux()
        raise Exception("Error computing number of active requests. Error: %s", str(e))


def get_next_sync_request(thread_group_obj):
    """
    Get the next sync request. If there are no more in the queue, return None

    :param thread_group_obj: Reference to the Thread Group Object (ngamsThreadGroup)
    :return: Next sync request or None (ngasSyncRequest | None)
    """
    param_dict = thread_group_obj.getParameters()[0]
    try:
        thread_group_obj.takeGenMux()
        # Get the next request (if any)
        count = param_dict[QUEUE_DBM_NAME].getCount()
        if count > 100:
            count = 100
        offset = int(count * random.random())
        if offset == 0:
            offset = 1
        param_dict[QUEUE_DBM_NAME].initKeyPtr()
        for _ in range(offset):
            key, sync_req = param_dict[QUEUE_DBM_NAME].getNext()
        if not key:
            thread_group_obj.releaseGenMux()
            return None
        logger.info("DBM: Got entry from Queue DBM: %s", key)
        # Insert the element in the processing queue
        logger.info("DBM: Adding entry in processing DBM: %s", key)
        param_dict[PROC_DBM_NAME].add(key, sync_req).sync()
        # Remove the entry from the queue
        logger.info("DBM: Removing entry from queue DBM: %s", key)
        param_dict[QUEUE_DBM_NAME].rem(key).sync()
        thread_group_obj.releaseGenMux()
        return sync_req
    except Exception as e:
        thread_group_obj.releaseGenMux()
        raise Exception("Error requesting next synchronization request. Error: {:s}".format(str(e)))


def move_request_from_proc_to_sync_queue(thread_group_obj, sync_req):
    """
    Put a Synchronization Request back in the Sync. DBM

    :param thread_group_obj: Reference to the Thread Group Object (ngamsThreadGroup)
    :param sync_req: Synchronization request (ngasSyncRequest)
    """
    logger.info("Entering move_request_from_proc_to_sync_queue() ...")
    param_dict = thread_group_obj.getParameters()[0]
    try:
        thread_group_obj.takeGenMux()
        key = ngamsLib.genFileKey(None, sync_req.get_file_id(), sync_req.get_file_version())
        # Insert the request object in the sync DBM
        logger.info("DBM: Adding entry in the queue DBM: %s", key)
        param_dict[QUEUE_DBM_NAME].add(key, sync_req).sync()
        # Remove it from the Processing DBM
        logger.info("DBM: Removing entry from processing DBM: %s", key)
        param_dict[PROC_DBM_NAME].rem(key).sync()
        thread_group_obj.releaseGenMux()
        return sync_req
    except Exception as e:
        thread_group_obj.releaseGenMux()
        raise Exception("Error moving Sync. Request from Processing to Sync. DBM. Error: {:s}".format(str(e)))


def move_request_from_sync_to_synced_queue(thread_group_obj, sync_req):
    """
    Put a synchronization request in the synced DBM

    :param thread_group_obj: Reference to the Thread Group Object (ngamsThreadGroup)
    :param sync_req: Synchronization request (ngasSyncRequest)
    """
    logger.info("Entering move_request_from_sync_to_synced_queue() ...")
    param_dict = thread_group_obj.getParameters()[0]
    try:
        thread_group_obj.takeGenMux()
        key = ngamsLib.genFileKey(None, sync_req.get_file_id(), sync_req.get_file_version())
        # Insert it in the synced DBM
        logger.info("DBM: Adding entry in synced DBM: %s", key)
        param_dict[SYNCED_DBM_NAME].add(key, sync_req).sync()
        # Remove it from the Processing DBM
        logger.info("DBM: Removing entry from processing DBM: %s", key)
        param_dict[PROC_DBM_NAME].rem(key).sync()
        thread_group_obj.releaseGenMux()
        return sync_req
    except Exception as e:
        thread_group_obj.releaseGenMux()
        raise Exception("Error moving Sync. Request from Processing to Synced DBM. Error: {:s}".format(str(e)))


def move_request_from_proc_to_failed_queue(thread_group_obj, sync_req):
    """
    Put a synchronization request in the failed DBM

    :param thread_group_obj: Reference to the Thread Group Object (ngamsThreadGroup)
    :param sync_req: Synchronization request (ngasSyncRequest)
    """
    logger.info("Entering move_request_from_proc_to_failed_queue() ...")
    param_dict = thread_group_obj.getParameters()[0]
    try:
        thread_group_obj.takeGenMux()
        key = ngamsLib.genFileKey(None, sync_req.get_file_id(), sync_req.get_file_version())
        # Insert it in the failed DBM
        logger.info("DBM: Insert entry in failed DBM: %s", key)
        param_dict[FAILED_DBM_NAME].add(key, sync_req).sync()
        # Remove it from the Processing DBM
        logger.info("DBM: Removing entry from processing DBM: %s", key)
        param_dict[PROC_DBM_NAME].rem(key).sync()
        thread_group_obj.releaseGenMux()
        return sync_req
    except Exception as e:
        thread_group_obj.releaseGenMux()
        raise Exception("Error moving Sync. Request from Processing to Failed DBM. Error: {:s}".format(str(e)))


def clone_file(client, sync_req):
    """
    Clone a file

    :param client: NG/AMS Python Client instance (ngamsPClient)
    :param sync_req: Synchronization request (ngasSyncRequest)
    :return: Status object (ngamsStatus)
    """
    logger.info("Entering clone_file() ...")
    try:
        parameters = [["file_id", sync_req.get_file_id()], ["file_version", sync_req.get_file_version()]]
        if sync_req.get_disk_id() != "-":
            parameters.append(["disk_id", sync_req.get_disk_id()])
        status = client.get_status(NGAMS_CLONE_CMD, pars=parameters)
        # status = client.clone(sync_request.get_file_id(), sync_request.get_disk_id(), sync_request.get_file_version())
        logger.info("Leaving clone_file() (OK)")
        return status
    except Exception as e:
        status = ngamsStatus.ngamsStatus().setStatus(NGAMS_FAILURE).set_message(str(e))
        logger.info("Leaving clone_file() (ERROR)")
        return status


def get_cluster_ready_naus(connection, target_cluster):
    """
    Get the list of ready NAUs in a cluster

    :param connection: DB connection (ngamsDb)
    :param target_cluster: Target cluster name (string)
    :return: List of NAUs (list)
    """
    sql = "select host_id, srv_port from ngas_hosts " \
          "where cluster_name = '{:s}' and host_id in " \
          "(select host_id from ngas_disks where completed = 0 and mounted = 1) order by host_id"
    sql = sql.format(target_cluster)
    result = connection.query2(sql)
    if result == [[]]:
        return []
    else:
        host_list = []
        for node in result[0]:
            host_list.append("{:s}:{:s}".format(node[0], node[1]))
        return host_list


def get_cluster_nodes(connection, target_cluster):
    """
    Get the list of nodes in the cluster

    :param connection: DB connection (ngamsDb)
    :param target_cluster: Target cluster name (string)
    :return: Tuple with three elements:
                1. List containing node:port pairs
                2. String buffer with comma separated list of hostnames
                3. String buffer with a comma separated list of host:port pairs (tuple)
    """
    sql = "select host_id, srv_port from ngas_hosts where cluster_name = '{:s}'".format(target_cluster)
    result = connection.query2(sql)
    if result == [[]]:
        return []
    else:
        host_list = []
        host_list_str = ""
        server_list = ""
        for node in result[0]:
            host_list.append("{:s}:{:s}".format(node[0], node[1]))
            host_list_str += "'{:s}',".format(node[0])
            server_list += "{:s}:{:s},".format(node[0], node[1])
        return host_list, host_list_str[:-1], server_list[:-1]


def check_if_file_in_target_cluster(connection, cluster_nodes, file_id, file_version):
    """
    Query the information for a file

    :param connection: DB connection (ngamsDb)
    :param cluster_nodes: List of nodes in the target cluster (list)
    :param file_id: ID of file to check for (string)
    :param file_version: Version of file (integer)
    :return: Information for file as tuple:
                (<Disk Id>, <File ID>, <File Version>) or None if the file is not available in the cluster (tuple|None)
    """
    logger.info("Entering check_if_file_in_target_cluster() ...")

    # Reformat the cluster node list to a string for passing into the SQL statement
    # It should be in the format "'ngas04:7777', 'ngas04:7778'"
    cluster_nodes_sql = str(cluster_nodes.split(',')).replace('[', '').replace(']', '')

    sql = "select nf.disk_id, nf.file_id, nf.file_version " \
          "from ngas_files nf, ngas_disks nd " \
          "where nf.file_id = '{:s}' and nf.file_version = {:d} and nf.disk_id = nd.disk_id " \
          "and (nd.host_id in ({:s}) or nd.last_host_id in ({:s}))"
    sql = sql.format(file_id, file_version, cluster_nodes_sql, cluster_nodes_sql)
    result = connection.query2(sql)
    if len(result):
        disk_id, file_id, file_version = result[0][0]
        msg = "Leaving check_if_file_in_target_cluster() (OK: Disk ID: %s, File ID: %s, File Version: %s"
        logger.info(msg, str(disk_id), str(file_id), str(file_version))
        return disk_id, file_id, file_version
    else:
        logger.info("Leaving check_if_file_in_target_cluster() (empty result)")
        return None, None, None


def sync_loop(thread_group_obj):
    """
    Loop that carries out the synchronization

    :param thread_group_obj: Reference to the Thread Group Object (ngamsThreadGroup)
    """
    logger.info("Entering sync_loop()")
    param_dict = thread_group_obj.getParameters()[0]
    cluster_node_status_time = 0.0
    # client = ngamsPClient.ngamsPClient()
    client = None
    while True:
        # Check if the execution should continue before each iteration
        thread_group_obj.checkPauseStop()
        sync_req = get_next_sync_request(thread_group_obj)
        # If there are no more requests to handle, terminate the thread
        if not sync_req:
            if get_active_requests(thread_group_obj):
                # There are still requests in the queue, wait a bit and retry
                time.sleep(0.100)
                continue
            else:
                logger.info("Synchronization thread: %s terminating", thread_group_obj.getThreadId())
                thread_group_obj.terminateNormal()
        if (time.time() - sync_req.get_time_last_attempt()) < NGAS_MIN_RETRY_TIME:
            # Time waiting for next retry has not expired. Put the request back in the sync queue
            logger.info("Time for waiting for retrying entry: %s has not expired",
                        ngamsLib.genFileKey(None, sync_req.get_file_id(), sync_req.get_file_version()))
            move_request_from_proc_to_sync_queue(thread_group_obj, sync_req)
            continue

        # Get list of archiving units in the Target Cluster
        time_now = time.time()
        if (time_now - cluster_node_status_time) > 60:
            naus_server_list = ""
            cluster_nodes_str = ""
            if param_dict[PAR_TARGET_CLUSTER]:
                cluster_naus = get_cluster_ready_naus(param_dict[PAR_DB_CON], param_dict[PAR_TARGET_CLUSTER])
                cluster_nodes, cluster_nodes_str, cluster_server_list = \
                    get_cluster_nodes(param_dict[PAR_DB_CON], param_dict[PAR_TARGET_CLUSTER])
                # naus_server_list = str(cluster_naus)[1:-1].replace("'", "").replace(" ", "")
                naus_server_list = cluster_naus
            elif param_dict[PAR_TARGET_NODES]:
                naus_server_list = param_dict[PAR_TARGET_NODES]
                cluster_nodes_str = naus_server_list

            client = ngamsPClient.ngamsPClient(servers=ngasUtilsLib.get_server_list_from_string(naus_server_list))
            # cluster_naus_status_time = time_now

        # Check if file is already in target cluster
        file_version = sync_req.get_file_version()
        target_cluster_disk_id, target_cluster_file_id, target_cluster_file_version =\
            check_if_file_in_target_cluster(param_dict[PAR_DB_CON], cluster_nodes_str, sync_req.get_file_id(),
                                            file_version)

        if target_cluster_disk_id:
            # Carry out the CHECKFILE check only if requested
            if param_dict[PAR_CHECK_FILE]:
                # File is in target cluster, send a CHECKFILE Command to determine if the file is safely archived
                result = check_file(client, target_cluster_disk_id, target_cluster_file_id, target_cluster_file_version)
            else:
                # File is in the name space of the Target Cluster, accept it
                result = "FILE_OK"
        else:
            result = "ERROR"

        if result.find("FILE_OK") != -1:
            logger.info("File: %s is available in the target cluster - skipping", sync_req.get_summary())
            # If the file is safely archived in the target cluster, its handed into the synced DBM and then to next file
            sync_req.set_message(NGAS_ALREADY_SYNCED)
            move_request_from_sync_to_synced_queue(thread_group_obj, sync_req)
            # Update statistics
            param_dict[PAR_STAT_FILE_COUNT] += 1
            param_dict[PAR_STAT_VOL_ACCU] += sync_req.get_file_size()
        elif result.find("ERROR") != -1:
            logger.info("File: %s not available in the target cluster - restoring", sync_req.get_summary())

            # The file is not available in the target cluster. We have to clone it to the target cluster.
            status = None
            error_msg = ""
            try:
                sync_req.increment_attempt_count()
                status = clone_file(client, sync_req)
                if status.getStatus() == NGAMS_FAILURE:
                    error_msg = "Error handling Synchronization Request: {:s}. Error: {:s}"
                    error_msg = error_msg.format(sync_req.get_summary(), status.get_message())
                    logger.warning(error_msg)
                else:
                    logger.info("Successfully cloned Synchronization Request: %s.", sync_req.get_summary())
            except Exception as e:
                error_msg = "Error occurred cloning file for Synchronization Request: {:s}. Error: {:s}"
                if status:
                    logger.warning(error_msg.format(sync_req.get_summary(), status.getMessage()))
                else:
                    logger.warning(error_msg.format(sync_req.get_summary(), str(e)))
            if error_msg:
                # Something went wrong, determine whether to abandon synchronizing this file or whether to retry later
                sync_req.set_message(error_msg)
                if sync_req.get_attempt_count() >= NGAS_MAX_RETRIES:
                    move_request_from_proc_to_failed_queue(thread_group_obj, sync_req)
                    # Update statistics
                    param_dict[PAR_STAT_FILE_COUNT] += 1
                    param_dict[PAR_STAT_VOL_ACCU] += sync_req.get_file_size()
                else:
                    move_request_from_proc_to_sync_queue(thread_group_obj, sync_req)
            else:
                # File was successfully cloned. Move to synced DBM.
                sync_req.set_message("")
                move_request_from_sync_to_synced_queue(thread_group_obj, sync_req)
                # Update statistics
                param_dict[PAR_STAT_FILE_COUNT] += 1
                param_dict[PAR_STAT_VOL_ACCU] += sync_req.get_file_size()
        else:
            # Something went wrong, put the entry in the failed DBM
            sync_req.set_message(result)
            move_request_from_proc_to_failed_queue(thread_group_obj, sync_req)
            # Update statistics
            param_dict[PAR_STAT_FILE_COUNT] += 1
            param_dict[PAR_STAT_VOL_ACCU] += sync_req.get_file_size()


def sync_thread(thread_group_obj):
    """
    The Synchronization Thread running within an ngamsThreadGroup object

    The first thread (thread number 1), will also generate intermediate status reports if requested
    :param thread_group_obj: Reference to the Thread Group Object (ngamsThreadGroup)
    """
    logger.info("Entering sync_thread()")
    param_dict = thread_group_obj.getParameters()[0]

    # Thread #1 takes care of sending out intermediate reports if specified
    if thread_group_obj.getThreadNo() == 1 and param_dict[PAR_INT_NOTIF]:
        intermediate_report_loop(thread_group_obj)
    else:
        sync_loop(thread_group_obj)


def execute(param_dict):
    """
    Carry out the tool execution

    :param param_dict: Dictionary containing the parameters and options (dictionary)
    """
    logger.info("Entering execute() ...")

    if param_dict["help"]:
        print(correct_usage())
        sys.exit(0)
    ngasUtilsLib.get_check_access_code(get_option_dict())

    initialize(param_dict)

    # Start the threads
    # If intermediate reporting is specified a special thread is started for this
    if param_dict[PAR_INT_NOTIF]:
        streams = int(param_dict[PAR_STREAMS]) + 1
    else:
        streams = int(param_dict[PAR_STREAMS])
    sync_threads = ngamsThreadGroup.ngamsThreadGroup(param_dict[PAR_SESSION_ID], sync_thread, streams, [param_dict])
    sync_threads.start()

    # Generate final report if requested
    if param_dict[PAR_NOTIF_EMAIL]:
        logger.info("Generating report about actions carried out ...")
        report = generate_report(sync_threads)
        subject = "NGAS express sync tool - final status report (session: {:s})".format(param_dict[PAR_SESSION_ID])
        logger.info("Sending final report ...")
        ngasUtilsLib.send_email(subject, param_dict[PAR_NOTIF_EMAIL], report, "text/plain", "NGAS-XSYNC-report.txt")
    # TODO: Clean up old working directories (if older than 30 days)
    logger.info("Leaving execute()")


def main():
    """
    Main function to execute the tool
    """
    try:
        option_dict = ngasUtilsLib.parse_command_line(sys.argv, get_option_dict())
        param_dict = ngasUtilsLib.option_dictionary_to_parameter_dictionary(option_dict)
    except Exception as e:
        print("\nProblem executing the tool:\n\n{:s}\n".format(str(e)))
        print(traceback.format_exc())
        sys.exit(1)
    else:
        try:
            execute(param_dict)
            rmFile(get_lock_file(param_dict))
        except Exception as e:
            rmFile(get_lock_file(param_dict))
            if str(e) == "0":
                sys.exit(0)
            print("\nProblem executing the tool:\n\n{:s}\n".format(str(e)))
            print(traceback.print_exc())
            sys.exit(1)


if __name__ == '__main__':
    main()
