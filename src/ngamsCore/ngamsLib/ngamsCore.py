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
# "@(#) $Id: __init__.py,v 1.30 2009/11/11 13:08:02 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  12/04/2001  Created
# awicenec  29/05/2001  Added path extension
# jknudstr  11/06/2001  Added proper version + implemented getNgamsVersion()
"""
             #     #  #####        #    #    #     #  #####
             ##    # #     #      #    # #   ##   ## #     #
             # #   # #           #    #   #  # # # # #
             #  #  # #  ####    #    #     # #  #  #  #####
             #   # # #     #   #     ####### #     #       #
             #    ## #     #  #      #     # #     # #     #
             #     #  #####  #       #     # #     #  #####


%s


The NG/AMS SW provides a server to be used on the NGAS System for
handling the archival and retrieval of data, and to manage the
disk configuration of the system.

The complete functionality is implemented as a set of Python classes
that can be used in other contexts.

Also provided are functions used to built the so-called Data Handling
Plug-In Utilities which are used to handle the various types of data
files being archived.

For more detailed information about the project, consult the URL:

  http://www.eso.org/projects/ngas
"""

import calendar
import collections
import errno
import glob
import hashlib
import importlib
import logging
import math
import os
import re
import shutil
import socket
import subprocess
import threading
import time

import pkg_resources
import six

from . import utils
from . import logdefs


logger = logging.getLogger(__name__)


# Handle NG/AMS Version.
_NGAMS_SW_VER   = ""
_NGAMS_VER_DATE = ""
for line in utils.b2s(pkg_resources.resource_string('ngamsData', 'VERSION')).splitlines():
    if "NGAMS_SW_VER" in line:
        _NGAMS_SW_VER = line.split("NGAMS_SW_VER ")[1].strip()[1:-1]
    elif "VER_DATE" in line:
        _NGAMS_VER_DATE = line.split("VER_DATE ")[1].strip()[1:-1]
    if _NGAMS_SW_VER and _NGAMS_VER_DATE:
        break


def getNgamsLicense():
    """
    Read in and return the NG/AMS License Agreement.

    Returns:   Contents of license agreement (string).
    """
    return utils.b2s(pkg_resources.resource_string('ngamsData', 'COPYING'))


def prFormat1():
    """
    Return format used when dumping contents of NG/AMS objects.

    Return:    Format (string).
    """
    return "%-35s %s\n"

# Directories and Filenames.
NGAMS_BAD_FILES_DIR           = "bad-files"
NGAMS_BACK_LOG_DIR            = "back-log"
NGAMS_SUBSCR_BACK_LOG_DIR     = "subscr-back-log"
NGAMS_SUBSCR_BACK_LOG         = NGAMS_SUBSCR_BACK_LOG_DIR
NGAMS_STAGING_DIR             = "staging"
NGAMS_PROC_DIR                = "processing"
NGAMS_TMP_FILE_PREFIX         = "NGAMS_TMP_FILE___"
NGAMS_BACK_LOG_TMP_PREFIX     = "NGAMS_BACK_LOG_TMP___"
NGAMS_BAD_FILE_PREFIX         = "BAD-FILE-"
NGAMS_PICKLE_FILE_EXT         = "pickle"
NGAMS_TMP_FILE_EXT            = "tmp"

# Status.
NGAMS_FAILURE                 = "FAILURE"
NGAMS_SUCCESS                 = "SUCCESS"

# States.
NGAMS_OFFLINE_STATE       = "OFFLINE"
NGAMS_ONLINE_STATE        = "ONLINE"
NGAMS_NOT_RUN_STATE       = "NOT-RUNNING"

# Sub-States.
NGAMS_IDLE_SUBSTATE       = "IDLE"
NGAMS_BUSY_SUBSTATE       = "BUSY"

# Built-In Mime-types.
NGAMS_ARCH_REQ_MT         = "ngas/archive-request"
NGAMS_CONT_MT             = "ngas/container"
NGAMS_TEXT_MT             = "text/plain"
NGAMS_XML_MT              = "text/xml"
NGAMS_GZIP_XML_MT         = "application/x-gxml"
NGAMS_UNKNOWN_MT          = "unknown"

# HTTP Methods, etc.
NGAMS_HTTP_GET            = "GET"
NGAMS_HTTP_POST           = "POST"
NGAMS_HTTP_PUT            = "PUT"
NGAMS_HTTP_FILE_URL       = "file:"
NGAMS_HTTP_INT_AUTH_USER  = "ngas-int"

# HTTP Status Codes.
NGAMS_HTTP_SUCCESS        = 200
NGAMS_HTTP_REDIRECT       = 303
NGAMS_HTTP_BAD_REQ        = 400
NGAMS_HTTP_UNAUTH         = 401
NGAMS_HTTP_UNAUTH_STR     = "Unauthorized"
NGAMS_HTTP_SERVICE_NA     = 503 # service is not available

# Request Processing Data Types.
NGAMS_PROC_DATA           = "DATA"
NGAMS_PROC_FILE           = "FILE"
NGAMS_PROC_STREAM         = "STREAM"

# Commands Handled by NG/AMS:
NGAMS_ARCHIVE_CMD       = "ARCHIVE"
NGAMS_CACHEDEL_CMD      = "CACHEDEL"
NGAMS_CHECKFILE_CMD     = "CHECKFILE"
NGAMS_CLONE_CMD         = "CLONE"
NGAMS_CONFIG_CMD        = "CONFIG"
NGAMS_DISCARD_CMD       = "DISCARD"
NGAMS_EXIT_CMD          = "EXIT"
NGAMS_HELP_CMD          = "HELP"
NGAMS_INIT_CMD          = "INIT"
NGAMS_LABEL_CMD         = "LABEL"
NGAMS_OFFLINE_CMD       = "OFFLINE"
NGAMS_ONLINE_CMD        = "ONLINE"
NGAMS_REARCHIVE_CMD     = "REARCHIVE"
NGAMS_REGISTER_CMD      = "REGISTER"
NGAMS_REMDISK_CMD       = "REMDISK"
NGAMS_REMFILE_CMD       = "REMFILE"
NGAMS_RETRIEVE_CMD      = "RETRIEVE"
NGAMS_STATUS_CMD        = "STATUS"
NGAMS_SUBSCRIBE_CMD     = "SUBSCRIBE"
NGAMS_UNSUBSCRIBE_CMD   = "UNSUBSCRIBE"

# Some common HTTP parameters used.
NGAMS_HTTP_PAR_FILENAME      = "filename"
NGAMS_HTTP_PAR_FILE_LIST     = "file_list"
NGAMS_HTTP_PAR_FILE_LIST_ID  = "file_list_id"
NGAMS_HTTP_PAR_FROM_ING_DATE = "from_ingestion_date"
NGAMS_HTTP_PAR_MAX_ELS       = "max_elements"
NGAMS_HTTP_PAR_MIME_TYPE     = "mime_type"
NGAMS_HTTP_PAR_UNIQUE        = "unique"

# Common HTTP headers.
NGAMS_HTTP_HDR_FILE_INFO     = "NGAS-File-Info"
NGAMS_HTTP_HDR_CONTENT_TYPE  = "Content-Type"
NGAMS_HTTP_HDR_CHECKSUM      = "NGAS-File-CRC"

# Types of Notification Events.
NGAMS_NOTIF_INFO        = "InfoNotification"
NGAMS_NOTIF_ALERT       = "AlertNotification"
NGAMS_NOTIF_ERROR       = "ErrorNotification"
NGAMS_NOTIF_DISK_SPACE  = "DiskSpaceNotification"
NGAMS_NOTIF_DISK_CHANGE = "DiskChangeNotification"
NGAMS_NOTIF_NO_DISKS    = "NoDiskSpaceNotification"
NGAMS_NOTIF_DATA_CHECK  = "DataCheckNotification"

# NGAS Host Location Specifiers.
NGAMS_HOST_LOCAL        = "LOCAL"
NGAMS_HOST_CLUSTER      = "CLUSTER"
NGAMS_HOST_DOMAIN       = "DOMAIN"
NGAMS_HOST_REMOTE       = "REMOTE"

# Consistency Checking:
NGAMS_FILE_STATUS_OK    = "00000000"
NGAMS_FILE_CHK_ACTIVE   = "01000000"
NGAMS_CHECK_SEQ         = "SEQUENTIAL"
NGAMS_CHECK_RAN         = "RANDOM"

# Constants for Handling XML Documents:
NGAMS_XML_STATUS_ROOT_EL= "NgamsStatus"
NGAMS_XML_STATUS_DTD    = "ngamsStatus.dtd"

# DB actions (operations that change the DB).
NGAMS_DB_DIR            = ".db"
NGAMS_DB_CH_CACHE       = NGAMS_DB_DIR + "/" + "cache"
NGAMS_CACHE_DIR         = "cache"
NGAMS_DB_SNAPSHOT       = "DB-SNAPSHOT"
NGAMS_DBM_EXT           = "bsddb"
NGAMS_DB_NGAS_FILES     = "NgasFiles.bsddb"
NGAMS_DB_LAST_DB        = "LAST-DB"
NGAMS_DB_CH_FILE_INSERT = "FILE-INSERT"
NGAMS_DB_CH_FILE_UPDATE = "FILE-UPDATE"
NGAMS_DB_CH_FILE_DELETE = "FILE-DELETE"

# Miscelleneous.
NGAMS_DISK_INFO          = "NgasDiskInfo"
NGAMS_VOLUME_ID_FILE     = ".ngas_volume_id"
NGAMS_VOLUME_INFO_FILE   = ".ngas_volume_info"
NGAMS_DATA_CHECK_THR     = "DATA-CHECK-THREAD"
NGAMS_SUBSCRIPTION_THR   = "SUBSCRIPTION-THREAD"
NGAMS_SUBSCRIBER_THR     = "SUBSCRIBER-THREAD"
NGAMS_DELIVERY_THR       = "DELIVERY-THREAD-"
NGAMS_MIR_CONTROL_THR    = "MIRRORING-CONTROL-THREAD"
NGAMS_CLONE_THR          = "CLONE-THREAD-"
NGAMS_REGISTER_THR       = "REGISTER-THREAD-"
NGAMS_DEF_LOG_PREFIX     = "NGAS-LOG-PREFIX"
NGAMS_NOT_SET            = "NOT-SET"
NGAMS_DEFINE             = "DEFINE"
NGAMS_UNDEFINED          = "UNDEFINED"
NGAMS_MAX_FILENAME_LEN   = 128
NGAMS_MAX_SQL_QUERY_SZ   = 2048
NGAMS_SOCK_TIMEOUT_DEF   = 3600


def getNgamsVersion():
    """
    Return version identifier for NG/AMS.

    Returns:  NG/AMS version ID (string).
    """
    try:
        global _NGAMS_CVS_ID, _NGAMS_SW_VER, _NGAMS_VER_DATE
        #els = string.split(_NGAMS_CVS_ID, " ")
        #date  = re.sub("/", "-", els[4])
        #time  = els[5]
        #return _NGAMS_SW_VER + "/" + date + "T" + time
        return _NGAMS_SW_VER + "/" + _NGAMS_VER_DATE
    except:
        return "--UNDFINED--"


def ngamsCopyrightString():
    """
    Return the NG/AMS Copyright and Reference String.

    Returns:   Copyright string (string).
    """
    return utils.b2s(pkg_resources.resource_string('ngamsData', 'COPYRIGHT'))


_logDef = logdefs.LogDefHolder(pkg_resources.resource_stream('ngamsData', 'ngamsLogDef.xml'))# @UndefinedVariable
def genLog(logId, parList = []):
    """
    Generate a log line and return this.

    logId:    The Log ID for the log (string).

    parList:  List of parameters to fill into the log format (list).

    Returns:  Generated log line (string).
    """
    return _logDef.generate_log(logId, *parList)


def getAttribValue(node,
                   attributeName,
                   ignoreFailure = 0):
    """
    Get value of an attribute in connection with a DOM Node.

    node:            XML node object (Node).

    attributeName:   Name of attribute (string).

    ignoreFailure:   If a failure (execption) occurrs the function
                     will not raise an exception but will return with
                     '' as return value (integer

    Returns:         Value of attribute (string).
    """
    if not ignoreFailure and not node.hasAttribute(attributeName):
        errMsg = "Error retrieving value for attribute: %s from node: %s"
        raise Exception(errMsg % (attributeName, node.nodeName))

    return node.getAttribute(attributeName)

def ngamsGetChildNodes(parentNode,
                       childNodeName):
    """
    Get the child nodes of the parent node DOM object given, which have
    the given 'childNodeName'.

    parentNode:     Parent node DOM object (Node).

    childNodeName:  Name of chile node (element) (string).

    Returns:        List with chile node DOM objects (list/Node).
    """
    childNodes = []
    for childNode in parentNode.childNodes:
        try:
            nodeName = childNode.nodeName
        except:
            nodeName = ""
        if (nodeName == childNodeName):
            childNodes.append(childNode)
    return childNodes

def getHostName():
    """
    Return the host name of this system

    Returns:   Host name for this NGAS System (string).
    """
    return socket.gethostname()


def ignoreValue(ignoreEmptyField,
                fieldValue):
    """
    Evaluate the value of a field and indicate whether to ignore it or not.

    Internal function.

    ignoreEmptyField:     If set to 1 indicates that the field should be
                          ignored if it is an empty string (integer/0|1).

    fieldValue:           Value of field (string).

    Returns:              1 if field should be ignored, otherwise 0
                          (integer/0|1).
    """
    if isinstance(fieldValue, six.integer_types):
        if (ignoreEmptyField and (fieldValue == -1)): return 1
    elif (ignoreEmptyField and (str(fieldValue).strip() == "")):
        return 1
    return 0


def getFileSize(filename):
    """
    Get size of file referred.

    filename:   Filename - complete path (string).

    Returns:    File size (integer).
    """
    #return int(os.stat(filename)[6])
    return os.path.getsize(filename)


def getFileCreationTime(filename):
    """
    Get creation time of file referred.

    filename:   Filename - complete path (string).

    Returns:    Creation time (seconds since epoch) (integer).
    """
    # TODO: Use this: return os.path.getctime(filename)
    return int(os.stat(filename)[9])


def genUniqueId():
    """
    Generate a unique ID based on an MD5 checksum.

    Returns:  Unique ID (string).
    """
    return hashlib.md5(six.b("%.12f-%s" % (time.time(), getHostName()))).hexdigest()


def createSortDicDump(dic):
    """
    Create a sorted ASCII representation of a dictionary. The order is sorted
    according to the dictionary keys.

    dic:     Source dictionary (dictionary).

    Returns: Sorted dictionary ASCII representation (string).
    """
    if not isinstance(dic, dict):
        raise Exception("Object given is not a dictionary")
    keys = list(dic)
    keys.sort()
    asciiDic = ""
    for key in keys: asciiDic += ", '%s': '%s'" % (key, dic[key])
    asciiDic = "{" + asciiDic[2:] + "}"
    return asciiDic

_scale = {"B": 1.0, "KB": 1.0/1024.0, "MB": 1.0/1048576.0,
          "GB": 1.0/1073741824.0, "TB": 1.0/1099511627776.0}
def getDiskSpaceAvail(mountPoint, format = 'MB', smart = True):
    """
    Get the disk space available for the disk with the given mount point.

    mountPoint:  Mount point (string).

    format:      Output format (B|KB|MB|GB|TB/string).

    float:       Return the result in floating point (0|1/integer).

    smart:       Call maximum this function every 10s on a given path
                 (0|1/integer).

    Returns:     Returns available space in MB (integer).
    """
    logger.debug("Checking disk space available for path: %s", mountPoint)

    startTime = time.time()

    st = os.statvfs(mountPoint)
    diskSpace = (st.f_bavail * st.f_frsize) * _scale[format]

    msg = ("Checked disk space available for path: %s - Result (MB): %.3f"
           " Time: %.3fs")
    logger.debug(msg,  mountPoint, diskSpace, (time.time() - startTime))

    return diskSpace


def checkAvailDiskSpace(filename,
                        fileSize):
    """
    Check if there is enough space to store the file. If there is not
    enough space, an exception is raised.

    filename:   Filename - complete path (string).

    fileSize:   Filesize in bytes (integer).

    Returns:    Void.
    """
    path = os.path.dirname(filename)
    logger.debug("Checking for disk space availability for path: %s - Needed size: %s",
                 path, str(fileSize))
    if ((fileSize / 1024**2) > getDiskSpaceAvail(path, smart=True)):
        errMsg = genLog("NGAMS_ER_NO_DISK_SPACE", [filename, fileSize])
        raise Exception(errMsg)


def checkCreatePath(path):
    """
    Check if the path referred exists - if not it is created.

    path:      Path to check/create (string).

    Returns:   Void.
    """
    # Check if path exists, if not, create it.
    try:
        os.makedirs(path, 0o775)
    except OSError as e:
        # no worries!
        if e.errno == errno.EEXIST:
            return
        raise

def rmFile(filename):
    """
    Remove the file referenced.

    filename:   File to remove (string).

    Returns:    Void.
    """
    logger.debug("Removing file(s): %s", filename)
    for f in glob.glob(filename):
        if os.path.isdir(f):
            shutil.rmtree(f, True)
        else: # file, link, etc
            os.remove(f)


def _find_mount_point(path):
    path = os.path.realpath(path)
    while not os.path.ismount(path):
        path = os.path.dirname(path)
    return path


def mvFile(srcFilename,
           trgFilename):
    """
    Move a file from the source filename to the specified target filename.

    srcFilename:  Source filename (string).

    trgFilename:  Target filename (string).

    Returns:      Time in took to move file (s) (float).
    """
    logger.debug("Moving file: %s to filename: %s", srcFilename, trgFilename)
    try:
        # Make target file writable if existing.
        checkCreatePath(os.path.dirname(trgFilename))
        fileSize = getFileSize(srcFilename)

        srcfil_mntpt = _find_mount_point(srcFilename)
        trgfil_mntpt = _find_mount_point(trgFilename)
        if srcfil_mntpt != trgfil_mntpt:
            checkAvailDiskSpace(trgFilename, fileSize)

        # Don't rely on os.rename as it can cause issues when crossing 
        # disk parition boundaries
        start = time.time()
        shutil.move(srcFilename, trgFilename)
        deltaTime = time.time() - start

    except Exception as e:
        errMsg = genLog("NGAMS_AL_MV_FILE", [srcFilename, trgFilename, str(e)])
        raise Exception(errMsg)
    logger.debug("File: %s moved to filename: %s", srcFilename, trgFilename)

    return deltaTime


def cpFile(srcFilename,
           trgFilename):
    """
    Copy a file from the source filename to the specified target filename.

    srcFilename:  Source filename (string).

    trgFilename:  Target filename (string).

    Returns:      Time in took to move file (s) (float).
    """
    logger.debug("Copying file: %s to filename: %s", srcFilename, trgFilename)
    try:
        # Make target file writable if existing.
        if os.path.exists(trgFilename):
            os.chmod(trgFilename, 420)
        checkCreatePath(os.path.dirname(trgFilename))
        fileSize = getFileSize(srcFilename)
        checkAvailDiskSpace(trgFilename, fileSize)
        start = time.time()
        shutil.copyfile(srcFilename, trgFilename)
        deltaTime = time.time() - start
    except Exception as e:
        errMsg = genLog("NGAMS_AL_CP_FILE", [srcFilename, trgFilename, str(e)])
        raise Exception(errMsg)
    logger.debug("File: %s copied to filename: %s", srcFilename, trgFilename)
    return deltaTime


def to_valid_filename(fname):
    """Replaces invalid filename characters by underscores"""
    return re.sub(r"[\?=&]", "_", os.path.basename(fname))


def compressFile(srcFilename,
                 method = "gzip"):
    """
    Compress a file and return the resulting filename.

    For now only gzip is supported.

    Note: It might be that the resulting filename is the same as the input
    filename. This may happen if the file cannot be compressed.

    srcFilename:   Name of file to compress (string).

    method:        Method to apply when compressing the file (string/'gzip').

    Returns:       Name of resulting file (string).
    """
    subprocess.check_call(['gzip', '-f', srcFilename])
    trgFilename = '%s.gz' % srcFilename
    if os.path.exists(trgFilename):
        return trgFilename
    return srcFilename


def decompressFile(srcFilename,
                   method = "gzip"):
    """
    Decompress a file and return the resulting filename.

    For now only gzip is supported.

    srcFilename:   Name of file to compress (string).

    method:        Method to apply when compressing the file (string/'gzip').

    Returns:       Name of resulting file (string).
    """
    subprocess.check_call(['gunzip', '-f', srcFilename])
    trgFilename = srcFilename[:-3]
    if os.path.exists(trgFilename):
        return trgFilename
    raise Exception("Error decompressing file: %s" % srcFilename)


def isoTime2Secs(isoTime):
    """
    Converts a semi ISO 8601 style time stamp like:

        '[<days>T]<hours>:<minutes>:<seconds>[.<decimal seconds>]'

    to seconds.

    isoTime:    ISO 8601 style time stamp (string).

    Returns:    Corresponding time in seconds (float).
    """

    days = 0
    timePart = isoTime
    if "T" in isoTime:
        days, timePart = isoTime.split("T")
        days = int(days)

    timeVector = timePart.split(":")
    hours = int(timeVector[0])
    mins = int(timeVector[1])
    secs = 0
    if len(timeVector) > 2:
        secs = float(timeVector[2])

    return days*24*3600 + hours*3600 + mins*60 + secs


def getBoolean(val):
    """
    Return value of something that might be a boolean. If the given value
    cannot be recognized as boolean, an exception is thrown.

    val:       Value to evalute (string | boolean | integer).

    Returns:   True if the value can be identified as a True boolean (boolean).
    """
    val = str(val).upper()
    if (val in ("0", "F", "FALSE")):
        return False
    elif (val in ("1", "T", "TRUE")):
        return True
    else:
        msg = "Value given: %s, does not seem to be a boolean"
        raise Exception(msg % str(val))

def loadPlugInEntryPoint(plugInName, entryPointMethodName=None, returnNone=False):
    """
    Loads the entry point method of an NGAMS Plug-In. First the
    module is loaded and then the method that acts as entry point is
    also loaded and returned to the caller, who can then use
    the method reference directly.

    If `returnNone` is True and the module loads correctly but the method does
    not exist, None is returned instead.
    """

    # By default the entry point has the same name as the module
    if not entryPointMethodName:
        entryPointMethodName = plugInName.split('.')[-1]

    logger.debug("Looking for %s plug-in module", plugInName)
    try:
        logger.debug("Trying with module ngamsPlugIns.%s", plugInName)
        plugInModule = importlib.import_module('ngamsPlugIns.' + plugInName)
    except ImportError:
        logger.debug("Trying with module %s", plugInName)
        plugInModule = importlib.import_module(plugInName)

    logger.debug("Loading entry-point method %s from module %s ", entryPointMethodName,plugInModule.__name__)

    try:
        return getattr(plugInModule, entryPointMethodName)
    except AttributeError:
        if returnNone:
            return None
        raise

def is_localhost(host_or_ip):
    return host_or_ip == 'localhost' or \
           host_or_ip.startswith("127.0.") or \
           host_or_ip == getHostName()

def get_contact_ip(cfgObj):
    """
    Returns a host or IP address that can be used to contact an NGAS server
    that is running in this machine and configured with the given `cfgObj`.

    By default this method returns 'localhost' except when the server has been
    configured to listen in a given non-local interface, in which case that
    address is returned instead.
    """
    ipAddress = cfgObj.getIpAddress()
    if not ipAddress or ipAddress == '0.0.0.0' or is_localhost(ipAddress):
        return 'localhost'
    return ipAddress

def execCmd(cmd, timeOut = -1, shell=True, env=None):
    """
    Executes the command given on the UNIX command line and returns a
    list with the cmd exit code and the output written on stdout and stderr.

    timeOut:     Timeout waiting for the command in seconds. A timeout of
                 -1 means that no timeout is applied (float).

    Returns:     List with the exit code and output on stdout and stderr:

                     [<exit code>, <stdout>, <stderr>]  (list).
    """

    p = subprocess.Popen(cmd, shell=shell, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, env=env)

    if timeOut != -1:
        killed = threading.Event()
        def _kill():
            p.kill()
            killed.set()
        startTime = time.time()
        timer = threading.Timer(timeOut, _kill)
        timer.daemon = True
        timer.start()
    stdout, stderr = p.communicate()
    if timeOut != -1:
        timer.cancel()
        if killed.is_set():
            raise Exception('Command %s timed out after %.2f [s]' % (cmd, time.time() - startTime))

    return p.poll(), stdout, stderr


_formatspec = collections.namedtuple('_formatspec', 'format msecs')
FMT_DATE_ONLY        = _formatspec('%Y-%m-%d',          False)
FMT_TIME_ONLY        = _formatspec('%H:%M:%S',          True)
FMT_TIME_ONLY_NOMSEC = _formatspec('%H:%M:%S',          False)
FMT_DATETIME         = _formatspec('%Y-%m-%dT%H:%M:%S', True)
FMT_DATETIME_NOMSEC  = _formatspec('%Y-%m-%dT%H:%M:%S', False)

def fromiso8601(s, local=False, fmt=FMT_DATETIME):
    """
    Converts string `s` to the number of seconds since the epoch,
    as returned by time.time.

    `s` should be expressed in the ISO 8601 extended format indicated by the
    `fmt` flag.

    If `local` is False then `s` is assumed to represent time at UTC,
    otherwise it is interpreted as local time. In either case `s` should contain
    neither time-zone information nor the 'Z' suffix.
    """

    ms = 0.
    if fmt.msecs:
        s, ms = s.split(".")
        ms = float(ms)/1000.

    totime = calendar.timegm if not local else time.mktime
    t = totime(time.strptime(s, fmt.format))
    return t + ms

_long = int if six.PY3 else long
def toiso8601(t=None, local=False, fmt=FMT_DATETIME):
    """
    Converts the time value `t` to a string formatted using the ISO 8601
    extended format indicated by the `fmt` flag.

    `t` is expressed in numbers of seconds since the epoch, as returned by
    time.time. If `t` is not given, the current time is used instead.

    If `local` is False then the resulting string represents the time at UTC.
    If `local` is True then the resulting string represents the local time.
    In either case the string contains neither time-zone information nor the 'Z'
    suffix.
    """

    if t is None:
        t = time.time()

    totuple = time.gmtime if not local else time.localtime
    timeStamp = time.strftime(fmt.format, totuple(t))
    if fmt.msecs:
        t = (t - _long(t)) * 1000
        timeStamp += '.%03d' % int(t)

    return timeStamp

_UNIX_EPOCH_AS_MJD = 40587.
def tomjd(t=None):
    """
    Returns the Modified Julian Date for the given Unix time.
    """
    if t is None:
        t = time.time()
    return t/86400. + _UNIX_EPOCH_AS_MJD

def frommjd(mjd):
    """
    Returns the Unix time for the given Modified Julian Date.

    This algorithm is suitable at least for dates after the Unix Epoch.
    """
    # 40587 is the MJD for the Unix Epoch
    return (mjd - _UNIX_EPOCH_AS_MJD)*86400.

def terminate_or_kill(proc, timeout):
    """
    Terminates a process and waits until it has completed its execution within
    the given timeout. If the process is still alive after the timeout it is
    killed.
    """
    if proc.poll() is not None:
        return
    logger.info('Terminating %d', proc.pid)
    proc.terminate()
    wait_or_kill(proc, timeout)

def wait_or_kill(proc, timeout):
    """
    waits until it has completed its execution within the given timeout.
    If the process is still alive after the timeout it is killed.
    """
    waitLoops = 0
    max_loops = math.ceil(timeout/0.1)
    while proc.poll() is None and waitLoops < max_loops:
        time.sleep(0.1)
        waitLoops += 1

    kill9 = waitLoops == max_loops
    if kill9:
        logger.warning('Killing %s by brute force after waiting %.2f [s], BANG! :-(', proc.pid, timeout)
        proc.kill()
    proc.wait()