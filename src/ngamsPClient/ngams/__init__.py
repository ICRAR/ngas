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

_doc =\
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


To use the NG/AMS classes, the following statement should be contained
in the Python source files:

from   ngams import *

Subsequently the functions and variables defined in this module can be used
without referring to the module. The NG/AMS modules can be imported in the
standard way, e.g.:

import ngamsLib, ngamsConfig, ngamsHttpPars


HIERARCHY/STRUCTURE OF THE NG/AMS SOURCE CODE FILES:
The following diagram indicates how the hierarchy is structured for the Python
source code files contained within NG/AMS. The files (Python modules) should
only be imported by other modules, which have a lower location in the diagram,
never opposite, nor by files at the same level. This in principle is allowed by
Python, but in general such circular dependencies should be avoided in order to
keep a clear structure/architecture of the SW for maintability reasons.

Source Code File Hierarchy:

  o ngamsDb, ngamsLib, ngamsUrlLib
  o ngamsConfig
  o ngamsFileInfo, ngamsHostInfo
  o ngamsDiskInfo, ngamsDapiStatus, ngamsDppiStatus, ngamsFileList,
    ngamsPhysDiskInfo
  o ngamsReqProps, ngamsStatus
  o ngamsHighLevelLib
  o ngamsDiskUtils
  o ngamsPlugInApi

The source files contained in the sub-modules 'ngamsServer', 'ngamsPClient',
'ngamsTest' and 'ngamsCClient' are not considered in the list above. The
sub-module contaning example plug-ins 'ngamsPlugIns' is not considered either.
"""


# Debug flag.
_debug = 0

# Flag indicating if we're executing in Unit Test Mode.
_testMode = 0


# Make the NG/AMS classes available by extending the search paths.
try:
    from sys import path
    pathTup = [__path__[0] + '/../plug-ins',
               __path__[0] + '/../ngamsCClient',
               __path__[0] + '/../ngamsData',
               __path__[0] + '/../ngamsLib',
               __path__[0] + '/../ngamsPClient',
               __path__[0] + '/../ngamsPlugIns',
               __path__[0] + '/../ngamsServer',
               __path__[0] + '/../ngamsTest',
               __path__[0] + '/../ngamsUtils/src',
               __path__[0] + '/../pcc',
               __path__[0] + '/../pcc/pccLog',
               __path__[0] + '/../pcc/pccUt',
               ]
    path.extend(pathTup)
    __path__.extend(pathTup)
except Exception, e:
    print "ngams/__init__.py: Line 122"
    pass

import PccLog
import PccLogDef
import PccUtString
import PccUtTime
import md5
import os
import sys
import string
import re
import syslog
import traceback
import threading
import types
import time
import commands
import urllib
import socket

# make sure that we can get a handle to the server object
_ngamsServer = None

# NG/AMS source directory
NGAMS_SRC_DIR = os.path.realpath(__path__[0] + '/..')

#NGAMS_SRC_DIR = os.path.normpath(os.path.relpath('.') + "/../..")

# Main PID of server
NGAMS_SRV_PID = os.getpid()
NGAMS_HOST_IP = None

# Semaphore + counter to ensure unique, temporary filenames.
_uniqueNumberSem   = threading.Semaphore(1)
_uniqueNumberCount = 0


def ngamsGetSrcDir():
    """
    Return the NG/AMS source directory, i.e., the directory where
    the modules of NG/AMS are contained.

    Returns:  NG/AMS source directory (string).
    """
    global NGAMS_SRC_DIR
    return NGAMS_SRC_DIR


def getSrvPid():
    """
    Get the main PID of the Python interpreter, in which the NG/AMS Server
    or another application based on the NG/AMS library, is running.

    Returns:    Main PID of server (integer).
    """
    global NGAMS_SRV_PID
    return NGAMS_SRV_PID


# Import COPYRIGHT statement into doc page.
fo = open(os.path.normpath(ngamsGetSrcDir() + "/doc/COPYRIGHT"))
NGAMS_COPYRIGHT_TEXT = fo.read()
fo.close()
__doc__ = _doc % NGAMS_COPYRIGHT_TEXT


# Handle NG/AMS Version.
fo = open(os.path.normpath(ngamsGetSrcDir() + "/doc/VERSION"))
verBufLines = fo.readlines()
fo.close()
_NGAMS_CVS_ID   = "--UNDEFINED--"
_NGAMS_SW_VER   = "--UNDEFINED--"
_NGAMS_VER_DATE = "--UNDEFINED--"
for line in verBufLines:
    if (line.find("NGAMS_CVS_ID") != -1):
        _NGAMS_CVS_ID = line.split("NGAMS_CVS_ID ")[1].strip()[1:-1]
    elif (line.find("NGAMS_SW_VER") != -1):
        _NGAMS_SW_VER = line.split("NGAMS_SW_VER ")[1].strip()[1:-1]
    elif (line.find("VER_DATE") != -1):
        _NGAMS_VER_DATE = line.split("VER_DATE ")[1].strip()[1:-1]
    elif ((_NGAMS_CVS_ID != "") and (_NGAMS_SW_VER != "") and
          (_NGAMS_VER_DATE != "")):
        break


# Load Error Definition File
NGAMS_ERR_DEF_FILE = os.path.abspath(__path__[0] + '/../ngamsData/'+\
                                      "ngamsLogDef.xml")
_logDef = PccLogDef.PccLogDef().load(NGAMS_ERR_DEF_FILE)


# Flag used to suppress error logging on stderr.
_suppresErrorLogging = 0


# Variable indicating port number used in case multiple servers are executed
# on the same node. In this case, the Host ID used for addressing will be
# <Host Name>:<Port No> for unique addressing.
# If this feature is not used, this should be None.
_srvPortNo = None


# Log protection semaphore.
_logSem = threading.Semaphore(1)


def getNgamsLicense():
    """
    Read in and return the NG/AMS License Agreement.

    Returns:   Contents of license agreement (string).
    """
    fo = open(os.path.normpath(ngamsGetSrcDir() + "/../LICENSE"))
    license = fo.read()
    fo.close()
    return license


def prFormat1():
    """
    Return format used when dumping contents of NG/AMS objects.

    Return:    Format (string).
    """
    return "%-35s %s\n"

_CONST_FORMAT = "\n%-32s = %s"

# Directories and Filenames.
NGAMS_BAD_FILES_DIR           = "bad-files"
NGAMS_BACK_LOG_DIR            = "back-log"
NGAMS_SUBSCR_BACK_LOG_DIR     = "subscr-back-log"
NGAMS_SUBSCR_BACK_LOG         = NGAMS_SUBSCR_BACK_LOG_DIR
NGAMS_STAGING_DIR             = "staging"
NGAMS_PROC_DIR                = "processing"
NGAMS_FILE_DB_COUNTER         = "__COUNT__"
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
NGAMS_HTTP_HDR_CONTENT_TYPE  = "Content-type"
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
NGAMS_JANITOR_THR        = "JANITOR-THREAD"
NGAMS_DATA_CHECK_THR     = "DATA-CHECK-THREAD"
NGAMS_USER_SERVICE_THR   = "USER-SERVICE-THREAD"
NGAMS_SUBSCRIPTION_THR   = "SUBSCRIPTION-THREAD"
NGAMS_SUBSCRIBER_THR     = "SUBSCRIBER-THREAD"
NGAMS_DELIVERY_THR       = "DELIVERY-THREAD-"
NGAMS_MIR_CONTROL_THR    = "MIRRORING-CONTROL-THREAD"
NGAMS_CACHE_CONTROL_THR  = "CACHE-CONTROL-THREAD"
NGAMS_CACHE_CLEAN_UP_THR = "CACHE-CLEAN-UP-THREAD"
NGAMS_CLONE_THR          = "CLONE-THREAD-"
NGAMS_REGISTER_THR       = "REGISTER-THREAD-"
NGAMS_DEF_LOG_PREFIX     = "NGAS-LOG-PREFIX"
NGAMS_NOT_SET            = "NOT-SET"
NGAMS_DEFINE             = "DEFINE"
NGAMS_UNDEFINED          = "UNDEFINED"
NGAMS_MAX_FILENAME_LEN   = 128
NGAMS_MAX_SQL_QUERY_SZ   = 2048
NGAMS_SOCK_TIMEOUT_DEF   = 3600


def legalCmd(cmd):
    """
    Check that command given is a legal (recognized) command.

    cmd:       Command to check existence of (string).

    Returns:   1 if command is legal, 0 if not (integer).
    """
    if (cmd in [NGAMS_ARCHIVE_CMD, NGAMS_CACHEDEL_CMD, NGAMS_CLONE_CMD,
                NGAMS_EXIT_CMD, NGAMS_INIT_CMD, NGAMS_LABEL_CMD,
                NGAMS_OFFLINE_CMD, NGAMS_ONLINE_CMD, NGAMS_REARCHIVE_CMD,
                NGAMS_REGISTER_CMD, NGAMS_REGISTER_CMD, NGAMS_REMDISK_CMD,
                NGAMS_REMFILE_CMD, NGAMS_RETRIEVE_CMD, NGAMS_STATUS_CMD]):
        return 1
    else:
        return 0


def trim(str,
         trimChars):
    """
    Trim a string removing leading and trailing
    characters contained in the "trimChars" string.

    str:        String to trim (string).

    trimChars:  String containing characters to trim out of
                the input string (string).

    Returns:    Trimmed string (string).
    """
    return PccUtString.trimString(str, trimChars)


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


def getNgamsVersionRaw():
    """
    Return the version of the NG/AMS SW in 'raw format'.

    Returns:    Raw version string (string).
    """
    global _NGAMS_SW_VER
    return _NGAMS_SW_VER


def ngamsCopyrightString():
    """
    Return the NG/AMS Copyright and Reference String.

    Returns:   Copyright string (string).
    """
    return NGAMS_COPYRIGHT_TEXT


def setLogCond(sysLog,
               sysLogPrefix,
               locLogLevel,
               locLogFile,
               verboseLevel):
    """
    Set the global log conditions to be used by NG/AMS.

    sysLog:         Switch logging in UNIX syslgo on/off (1/0) (integer).

    sysLogPrefix:   Prefix (tag) to be used in syslog entries (string).

    locLogLevel:    Level applied for logging into local log file (integer).

    locLogFile:     Name of local log file (string).

    verboseLevel:   Level to apply to logs written on stdout (integer).

    Note: If any of the input parameters are None, the present value will
    be maintained.

    Returns:        Void.
    """
    # If any of the parameters are None - take the present value.
    if (sysLog == None):
        if (PccLog.getSysLogLogLevel() == -1):
            sysLog = 0
        else:
            sysLog = PccLog.getSysLogLogLevel()
    if (sysLogPrefix == None): sysLogPrefix = PccLog.getSysLogPrefix()
    if (locLogLevel == None): locLogLevel = PccLog.getLogLevel()
    if (locLogFile == None): locLogFile = PccLog.getLogFile()
    if (verboseLevel == None): verboseLevel = PccLog.getVerboseLevel()

    # Set up the new log conditions.
    path = os.path.dirname(locLogFile)
    if ((path != "") and (not os.path.exists(path))): os.makedirs(path)
    if (sysLog == 0):
        sysLogPrio = -1
        sysLogLevel = -1
    else:
        sysLogPrio = syslog.LOG_NOTICE
        sysLogLevel = 1
    sysLogProps = [sysLogPrio, sysLogLevel, sysLogPrefix]
    PccLog.setLogCond(int(locLogLevel), locLogFile, int(verboseLevel),
                      sysLogProps, 25, 1)


def getVerboseLevel():
    """
    Get the Verbose Level.

    Returns:   Verbose Level (integer).
    """
    return PccLog.getVerboseLevel()


def getLogLevel():
    """
    Get the Log Level.

    Returns:   Log Level (integer).
    """
    return PccLog.getLogLevel()


def getMaxLogLevel():
    """
    Returns the highest level of the Log and Verbose Levels.

    Returns:  Maximum level (integer).
    """
    if (PccLog.getLogLevel() > PccLog.getVerboseLevel()):
        return PccLog.getLogLevel()
    else:
        return PccLog.getVerboseLevel()


def genLog(logId,
           parList = []):
    """
    Generate a log line and return this.

    logId:    The Log ID for the log (string).

    parList:  List of parameters to fill into the log format (list).

    Returns:  Generated log line (string).
    """
    global _logDef
    for idx in range(len(parList)):
        par = parList[idx]
        if (type(par) == types.StringType):
            parList[idx] = par.replace("\n", " ")
    logMsg = _logDef.genLogX(logId, parList)
    return logMsg


def getThreadName():
    """
    Return the name of the thread or '' if this cannot be determined.

    Returns:    The name of the thread (string).
    """
    try:
        threadName = threading.currentThread().getName()
    except:
        threadName = ""
    return threadName


def getLocation(level = -3):
    """
    Get location of the current position in a source file of the Python
    interpreter.

    level:    Level in the stack to use as location (integer).

    Returns:  Location in the format: '<mod>:<method>:<ln no>' (string).
    """
    stackInfo =traceback.extract_stack()
    if len(stackInfo) < abs(level):
        level = -len(stackInfo)
    stackInfo = stackInfo[level]
    module = stackInfo[0].split("/")[-1]
    lineNo = stackInfo[1]
    method = stackInfo[2]
    threadName = getThreadName()
    if (threadName != ""):
        return module + ":" + method + ":" + str(lineNo) +\
               ":" + str(os.getpid()) + ":" + threadName
    else:
        return module + ":" + method + ":" + str(lineNo) +\
               ":" + str(os.getpid())


def setSuppressErrLog(state):
    """
    Set the flag indicating that error logging on stderr should be suppressed.
    Is used e.g. for the Unit Test.

    state:    State of the Suppress Error Logging Flag (integer/0|1).

    Returns:  Void.
    """
    global _suppresErrorLogging
    _suppresErrorLogging = state


def getSuppressErrLog():
    """
    Return the value of the Suppress Error Logging Flag.

    Returns:   Value of Suppress Error Logging Flag (integer/0|1).
    """
    global _suppresErrorLogging
    return _suppresErrorLogging


def takeLogSem():
    """
    Take the log protection semaphore.

    Returns:   Void.
    """
    global _logSem
    _logSem.acquire()


def relLogSem():
    """
    Release the log protection semaphore.

    Returns:  Void.
    """
    global _logSem
    _logSem.release()


def info(level,
         msg):
    """
    Generate an Information Log entry in the log targets.
    This is not written to UNIX syslog.

    level:    Level indicator for this log entry.

    msg:      Message to log (string).

    Returns:  Void.
    """
    try:
        takeLogSem()
        PccLog.info(level, msg, getLocation())
        relLogSem()
    except Exception, e:
        relLogSem()


class Trace:
    """
    Small class that can be instantiated while entering a name space.
    Upon instantiating the class, a log message is produced (from log level 4):

    TRACE: Entering (location) ...

    When leaving the name space/scope, the instantiated class is deleted. The
    destructor creates a log output as follows:

    TRACE: Leaving (location). Time: (execution time)s
    """
    def __init__(self):
        """
        Constructor method, which logs a message indicating that the current
        name space is being entered.
        """
        self.__startTime = time.time()
        self.__id        = md5.new("%.16f" % self.__startTime).hexdigest()
        self.__location  = getLocation(level = -4)
        PccLog.info(4, "TRACE:%s: Entering: %s ..." %
                    (self.__id, self.__location))

    def __del__(self):
        """
        Destructor, which logs a message indicating that there is now being
        returned from the associated name space.
        """
        stopTime = time.time()
        try:
            PccLog.info(4, "TRACE:%s: Leaving: %s. Time: %.6fs" %
                        (self.__id, self.__location,
                         (stopTime - self.__startTime)))
        except:
            pass


def TRACE(logLevel = 4):
    """
    Convenience function to use the tracing in the code.

    The function returns an instance of the Trace Class when the log level is
    above 4.

    A reference to that instance should be kept whilst in the name space, e.g.:

    def method():
        T = TRACE()
        ... execute code of method ...

    logLevel:    Logging level for this log statement (integer (0..5)).

    Returns:     Temporary trace object (Trace).
    """
    if ((PccLog.getLogLevel() >= logLevel) or
        (PccLog.getVerboseLevel() >= logLevel)):
        return Trace()
    else:
        return None


def sysLogInfo(level,
               msg,
               location = ""):
    """
    Generate a log entry in the UNIX syslog.

    msg:      Message to log (string).

    location: Optional location specifier (string).

    Returns:  Void.
    """
    try:
        takeLogSem()
        PccLog.sysLogInfo(level, msg, location)
        relLogSem()
    except Exception, e:
        relLogSem()


def notice(msg):
    """
    Log a Notice Log into the specified log targets.

    msg:      Message to log.

    Returns:  Void.
    """
    if (getSuppressErrLog()): return
    try:
        takeLogSem()
        PccLog.notice(msg, getLocation())
        relLogSem()
    except Exception, e:
        relLogSem()


def warning(msg):
    """
    Log a Warning Log into the specified log targets.

    msg:      Message to log.

    Returns:  Void.
    """
    if (getSuppressErrLog()): return
    try:
        takeLogSem()
        PccLog.warning(msg, getLocation())
        relLogSem()
    except Exception, e:
        relLogSem()


def error(msg):
    """
    Log an Error Log into the specified log targets.

    msg:      Message to log (string).

    Returns:  Void.
    """
    if (getSuppressErrLog()): return
    try:
        takeLogSem()
        PccLog.error(msg, getLocation())
        relLogSem()
    except Exception, e:
        relLogSem()


def alert(msg):
    """
    Log an Alert Log into the specified log targets.

    msg:      Message to log (string).

    Returns:  Void.
    """
    if (getSuppressErrLog()): return
    try:
        takeLogSem()
        PccLog.alert(msg, getLocation())
        relLogSem()
    except Exception, e:
        relLogSem()


def setDebug(debug):
    """
    Set Global Debug Flag.

    debug:    Debug flag. 1 = debug mode (integer).

    Returns:  Void.
    """
    global _debug
    _debug = debug


def getDebug():
    """
    Return Global Debug Flag.

    Returns:  Debug flag. 1 = debug mode (integer).
    """
    global _debug
    return _debug


def DEBUG(msg = ""):
    """
    Function to use for debugging purposes. Print a message on stdout.

    msg:       Message to print (string).

    Returns:   Void.
    """
    if (getDebug()):
        import traceback
        stackInfo = traceback.extract_stack()[-2]
        module = stackInfo[0].split("/")[-1]
        lineNo = stackInfo[1]
        method = stackInfo[2]
        print "##### DEBUG: Module: " + module + " - Line No: " +\
              str(lineNo) + " - Method: " + method + " - Message: " + str(msg)


def logFlush():
    """
    Flush the logs cached in the log manager.

    Returns:    Void.
    """
    PccLog.__logMgr.flush()


def setLogCache(size):
    """
    Set the log cache to a specific size.

    size:     Number of log entries to cache before flushing (int).

    Returns:  Void.
    """
    PccLog.__logMgr.setLogCacheFlushSize(size)


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
    if (not ignoreFailure):
        try:
            return str(node._attrs[attributeName].nodeValue)
        except Exception, e:
            errMsg = "Error retrieving value for attribute: " + attributeName+\
                     " from node: " + node.nodeName + ". Error: " + str(e)
            raise Exception, errMsg
    else:
        try:
            return str(node._attrs[attributeName].nodeValue)
        except:
            return ""


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

def getMyIpAddress():
    """
    Get the IP address of this machine as seen from the outside world.
    An external service is pretty much the only way to find this in
    a somewhat reliable way.

    INPUT:    None

    OUTPUT:   string, IP v4 address in standard notation
    """
    whatismyip = 'http://bot.whatismyipaddress.com/'
    return urllib.urlopen(whatismyip).readlines()[0]

def getIpAddress():
    global NGAMS_HOST_IP
    return NGAMS_HOST_IP

def getHostName(cfgFile=None):
    """
    Return the host name is it should be used internally in NG/AMS.

    Returns:   Host name for this NGAS System (string).
    """
    global NGAMS_HOST_IP
    ip = None
    if NGAMS_HOST_IP:
        ip= NGAMS_HOST_IP
    if cfgFile or sys.argv.count('-cfg') > 0:
        if not cfgFile: cfgFile = sys.argv[sys.argv.index('-cfg') + 1]
        from xml.dom import minidom
        dom = minidom.parse(cfgFile)
        srv = dom.getElementsByTagName('Server')
        ip = srv[0].getAttribute('IpAddress')
        if not ip or str(ip)[0] == '0':
            ip = getMyIpAddress()   #This only works if the machine can connect to the web!
    if ip:
        NGAMS_HOST_IP = str(ip)
        try:
            hostName = socket.gethostbyaddr(NGAMS_HOST_IP)[0]
        except socket.herror:
            return NGAMS_HOST_IP
    else:
        hostName = os.uname()[1]
    return hostName


def setSrvPort(portNo):
    """
    Set the server port number. Should only be set if multiple NG/AMS Servers
    are executed on the same node.

    portNo:   Server port number (integer).

    Returns:  Void.
    """
    global _srvPortNo
    _srvPortNo = int(portNo)


def getSrvPort():
    """
    Get the server port number. Is only set if multiple NG/AMS Servers
    are executed on the same node.

    Returns:  Server port number (integer|None).
    """
    global _srvPortNo
    return _srvPortNo


def getHostId(cfgFile=None):
    """
    Returns the proper NG/AMS Host ID according whether multiple servers
    can be executed on the same host.

    If multiple servers can be executed on one node, the Host ID will be:

      <Host Name>:<Port No>

    Otherwise, the Host ID will simply be the host name.

    Returns:    NG/AMS Host ID (string).
    """

    hostName = getHostName(cfgFile=cfgFile)
    if (hostName.split(".")[-1] == "local"):
        hostName = hostName.split(".")[0].split("-")[0]
    elif (re.match('^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$', hostName)):
        pass
    else:
        hostName = hostName.split(".")[0]
    if (getSrvPort()):
        return hostName + ":" + str(getSrvPort())
    else:
        return hostName


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
    if (type(fieldValue) == types.IntType):
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


def getFileAccessTime(filename):
    """
    Get last access time of file referred.

    filename:   Filename - complete path (string).

    Returns:    Last access time (seconds since epoch) (integer).
    """
    # TODO: Use this: return os.path.getatime(filename)
    return int(os.stat(filename)[7])


def getFileCreationTime(filename):
    """
    Get creation time of file referred.

    filename:   Filename - complete path (string).

    Returns:    Creation time (seconds since epoch) (integer).
    """
    # TODO: Use this: return os.path.getctime(filename)
    return int(os.stat(filename)[9])


def getFileModificationTime(filename):
    """
    Get the last modification date of the file (seconds since epch).

    filename:   Filename - complete path (string).

    Returns:    File modification date (integer).
    """
    # TODO: Use this: return os.path.getmtime(filename)
    return int(os.stat(filename)[8])


def getUniqueNo():
    """
    Generate a unique number (unique for this session of NG/AMS).

    Returns:  Unique number (integer).
    """
    global _uniqueNumberSem, _uniqueNumberCount
    _uniqueNumberSem.acquire()
    _uniqueNumberCount += 1
    count = _uniqueNumberCount
    _uniqueNumberSem.release()
    return count


def genUniqueId():
    """
    Generate a unique ID based on an MD5 checksum.

    Returns:  Unique ID (string).
    """
    return md5.new("%.12f-%s" % (time.time(), getHostName())).hexdigest()


def cleanList(lst):
    """
    Remove empty elements from a list containing strings
    (elements of length 0).

    lst:      List to be cleaned (list).

    Returns:  Cleaned list (list).
    """
    cleanLst = []
    for el in lst:
        if (len(el) > 0): cleanLst.append(el)
    return cleanLst


def createSortDicDump(dic):
    """
    Create a sorted ASCII representation of a dictionary. The order is sorted
    according to the dictionary keys.

    dic:     Source dictionary (dictionary).

    Returns: Sorted dictionary ASCII representation (string).
    """
    if (type(dic) != types.DictType):
        raise Exception, "Object given is not a dictionary"
    keys = dic.keys()
    keys.sort()
    asciiDic = ""
    for key in keys: asciiDic += ", '%s': '%s'" % (key, dic[key])
    asciiDic = "{" + asciiDic[2:] + "}"
    return asciiDic


_scale = {"B": 1024., "KB": 1., "MB": 1./1024.,
          "GB": 1./1048576., "TB": 1./1073741824.}
_diskSpaceDic = {}

def getDiskSpaceAvail(mountPoint,
                      format = "MB",
                      float = 0,
                      smart = True):
    """
    Get the disk space available for the disk with the given mount point.

    mountPoint:  Mount point (string).

    format:      Output format (B|KB|MB|GB|TB/string).

    float:       Return the result in floating point (0|1/integer).

    smart:       Call maximum this function every 10s on a given path
                 (0|1/integer).

    Returns:     Returns available space in MB (integer).
    """
    info(4,"Checking disk space available for path: " + mountPoint + " ...")

    startTime = time.time()

    # If the space was checked for a given path less than 10s ago, and the
    # smart flag is set, take the available disk space from the internal
    # dictionary.
    # This dictionary has the directory to check as key, pointing to pairs
    # of [<time last check>, <space>].
    diskSpace = None
    if (smart):
        if (_diskSpaceDic.has_key(mountPoint)):
            if ((startTime - _diskSpaceDic[mountPoint][0]) < 10):
                diskSpace = _diskSpaceDic[mountPoint][1]

    if (not diskSpace):
        if (not os.path.exists(mountPoint)): 
            try:
                os.makedirs(mountPoint)
            except:
                # go on anyhow.
                print "Illegal path encountered: " +\
                  mountPoint + " - or path could not be created."            
                # raise Exception, "Illegal path encountered: " +\
                #      mountPoint + " - or path could not be created."
        
        # TODO: UNIX specific/make plug-in?
    cmd = "df -k %s "
    if os.uname()[0] == 'Darwin':
        cmd = "df -kP %s "
    exitCode, stdOut = commands.getstatusoutput(cmd % mountPoint)
    if (exitCode):
        raise Exception, "Illegal path encountered: " + mountPoint
    cmd = str(cleanList(stdOut.replace("\n", " ").split(" "))[10]) + ".0"
    info(5,'Executing command: {0}'.format(cmd))
    diskSpace = eval(cmd)
    # Update the disk space (cache) dictionary.
    _diskSpaceDic[mountPoint] = (startTime, diskSpace)

    # Scale etc. the disk space.
    diskSpace = (diskSpace * _scale[format])

    if (not float): diskSpace = int(diskSpace + 0.5)
    msg = "Checked disk space available for path: %s - Result (%s): %s. " +\
          "Time: %.3fs"
    info(4,msg % (mountPoint, format, str(diskSpace),
                  (time.time() - startTime)))

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
    info(4,"Checking for disk space availability for path: " + path +\
         " - Needed size: " + str(fileSize) + " ...")
    if ((fileSize / 1024**2) > getDiskSpaceAvail(path, smart=True)):
        errMsg = genLog("NGAMS_ER_NO_DISK_SPACE", [filename, fileSize])
        error(errMsg)
        raise Exception, errMsg


_pathHandleSem = threading.Semaphore(1)
def checkCreatePath(path):
    """
    Check if the path referred exists - if not it is created.

    path:      Path to check/create (string).

    Returns:   Void.
    """
    # Check if path exists, if not, create it.
    global _pathHandleSem
    try:
        _pathHandleSem.acquire()
        if (os.path.exists(path) == 0):
            info(4,"Creating non-existing path: " + path)
            try:
                os.makedirs(path)
                os.chmod(path, 0775)
            except Exception, e:
                error("Error creating path: " + str(e))
                raise e
        _pathHandleSem.release()
    except Exception, e:
        _pathHandleSem.release()
        raise e


def rmFile(filename):
    """
    Remove the file referenced.

    filename:   File to remove (string).

    Returns:    Void.
    """
    info(4,"Removing file: %s" % filename)
    commands.getstatusoutput("rm -rf " + filename)


def mvFile(srcFilename,
           trgFilename):
    """
    Move a file from the source filename to the specified target filename.

    srcFilename:  Source filename (string).

    trgFilename:  Target filename (string).

    Returns:      Time in took to move file (s) (float).
    """
    info(4,"Moving file: " + srcFilename + " to filename: " + trgFilename)
    try:
        # Make target file writable if existing.
        if (os.path.exists(trgFilename)): os.chmod(trgFilename, 420)
        checkCreatePath(os.path.dirname(trgFilename))
        fileSize = getFileSize(srcFilename)
        checkAvailDiskSpace(trgFilename, fileSize)
        timer = PccUtTime.Timer()
        stat, out = commands.getstatusoutput("mv %s %s" %\
                                             (srcFilename, trgFilename))
        if (stat): raise Exception, "Error executing move command: " + str(out)
        deltaTime = timer.stop()
    except Exception, e:
        errMsg = genLog("NGAMS_AL_MV_FILE", [srcFilename, trgFilename, str(e)])
        alert(errMsg)
        raise Exception, errMsg
    info(4,"File: " + srcFilename + " moved to filename: " + trgFilename)

    return deltaTime


def cpFile(srcFilename,
           trgFilename):
    """
    Copy a file from the source filename to the specified target filename.

    srcFilename:  Source filename (string).

    trgFilename:  Target filename (string).

    Returns:      Time in took to move file (s) (float).
    """
    info(4,"Copying file: " + srcFilename + " to filename: " + trgFilename)
    try:
        # Make target file writable if existing.
        if (os.path.exists(trgFilename)): os.chmod(trgFilename, 420)
        checkCreatePath(os.path.dirname(trgFilename))
        fileSize = getFileSize(srcFilename)
        checkAvailDiskSpace(trgFilename, fileSize)
        timer = PccUtTime.Timer()
        stat, out = commands.getstatusoutput("cp %s %s" %\
                                             (srcFilename, trgFilename))
        if (stat): raise Exception, "Error executing copy command: " + str(out)
        deltaTime = timer.stop()
    except Exception, e:
        errMsg = genLog("NGAMS_AL_CP_FILE", [srcFilename, trgFilename, str(e)])
        alert(errMsg)
        raise Exception, errMsg
    info(4,"File: %s copied to filename: %s" % (srcFilename, trgFilename))
    return deltaTime


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
    T = TRACE()

    compressCmd = "gzip -f %s" % srcFilename
    stat, out = commands.getstatusoutput(compressCmd)
    if (stat != 0):
        msg = "Error compressing file: %s. Error: %s"
        raise Exception, msg % (srcFilename, str(out).replace("\n", "   "))
    trgFilename = srcFilename + ".gz"
    if (os.path.exists(trgFilename)):
        return trgFilename
    else:
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
    T = TRACE()

    decompressCmd = "gunzip -f %s" % srcFilename
    stat, out = commands.getstatusoutput(decompressCmd)
    if (stat != 0):
        msg = "Error decompressing file: %s. Error: %s"
        raise Exception, msg % (srcFilename, str(out).replace("\n", "   "))
    trgFilename = srcFilename[:-3]
    if (os.path.exists(trgFilename)):
        return trgFilename
    else:
        msg = "Error decompressing file: %s" % srcFilename
        raise Exception, msg


def timeRef2Iso8601(timeRef):
    """
    Convert a time reference into ISO 8601. This can be given as

      o ISO 8601.
      o Seconds since epoch.
      o DateTime time stamp.

    If None or an empty string is given, an empty string is returned.

    timeRef:    The reference to the time (string|int|DateTime).

    Returns:    ISO 8601 timestamp (string).
    """
    if ((timeRef == None) or (timeRef == "")):
        timeRefConv = ""
    elif (str(timeRef).find(":") != -1):
        timeRef = str(timeRef)
        # Assume ISO 8601 format or "YYYY-MM-DD HH:MM:SS[.sss]".
        if (timeRef.find("T") == -1):
            idx = timeRef.find(" ")
            timeRefConv = timeRef[0:idx] + "T" + timeRef[(idx + 1):]
        else:
            timeRefConv = timeRef
    elif (type(timeRef) in (types.IntType, types.FloatType)):
        # Assume seconds from epoch.
        timeRefConv = PccUtTime.TimeStamp().initFromSecsSinceEpoch(timeRef).\
                      getTimeStamp()
    else:
        # Assume DateTime object.
        timeRefConv = PccUtTime.TimeStamp().\
                      initFromSecsSinceEpoch(timeRef.ticks()).\
                      getTimeStamp()

    return timeRefConv


def checkIfIso8601(timestamp):
    """
    Check if value is properly formatted according to ISO 8601.

    timestamp:    Timestamp to check (string).

    Returns:      True if ISO 8601 timestamp, otherwise False (boolean).
    """
    try:
        if ((timestamp.find("-") == -1) or
            (timestamp.find("T") == -1) or
            (timestamp.find(":") == -1)):
            raise Exception, "not a timestamp"
        convTimeStamp = timeRef2Iso8601(timestamp)
        PccUtTime.TimeStamp().initFromTimeStamp(convTimeStamp)
        return True
    except Exception, e:
        return False


def getAsciiTime(timeSinceEpoch = time.time(),
                 precision = 3):
    """
    Get the time stamp on digital/ASCII form: HH:MM:SS.

    timeSinceEpoch:   Seconds since epoch (integer).

    precision:        Number of digits after the comma (integer)

    Returns:          ASCII time stamp (string).
    """
    T = TRACE()

    timeStamp = cleanList(str(time.asctime(time.gmtime(timeSinceEpoch))).\
                          split(" "))[3]
    if (precision > 0):
        decimals = (".%s" % ("%.12f" % timeSinceEpoch).\
                    split(".")[1])[0:(precision + 1)]
        timeStamp += decimals
    return timeStamp


# TODO: Consider to extend iso8601ToSecs() to handle this case.
def isoTime2Secs(isoTime):
    """
    Converts a semi ISO 8601 style time stamp like:

        '[<days>T]<hours>:<minutes>:<seconds>[.<decimal seconds>]'

    to seconds.

    isoTime:    ISO 8601 style time stamp (string).

    Returns:    Corresponding time in seconds (float).
    """
    T = TRACE()

    if (isoTime.find("T") != -1):
        datePart, timePart = isoTime.split("T")
        days = datePart
    else:
        days = 0
        timePart = isoTime
    timeVector = timePart.split(":")
    hours = timeVector[0]
    mins = timeVector[1]
    secsDec = -1
    if (len(timeVector) > 2):
        secs = timeVector[2]
        if (secs.find(".") != -1):
            secs, secsDec = secs.split(".")
    else:
        secs = 0
    try:
        totalTime = ((int(days) * 24 * 3600) + (int(hours) * 3600) +\
                     (int(mins) * 60) + int(secs))
        if (secsDec != -1): totalTime += float(".%s" % secsDec)
    except Exception, e:
        msg = "Illegal ISO 8601 time-stamp: %s. Error: %s"
        raise Exception, msg % (str(isoTime), str(e))
    return totalTime


def iso8601ToSecs(isoTimeStamp):
    """
    Convert an ISO 8601 time stamp to seconds (local time).

    isoTimeStamp:    ISO 8601 time stamp (string).

    Returns:         Seconds (float).
    """
    try:
        if (isoTimeStamp.find(".") != -1):
            ts, ms = isoTimeStamp.split(".")
        else:
            ts = isoTimeStamp
            ms = ""
        timeTuple = time.strptime(ts, "%Y-%m-%dT%H:%M:%S")
        secs = str(int(time.mktime(timeTuple)))
        if (ms): secs += "." + str(ms)
    except Exception, e:
        msg = "Illegal ISO 8601 time-stamp: %s" % isoTimeStamp
        raise Exception, msg
    return float(secs)


def padString(strBuf,
              reqLen,
              prependChr):
    """
    Prepend a certain character to generate a string of a certain length.
    E.g. if strBuf='12', reqLen=6 and prependChr='0', the resulting string
    will be '000012'.

    strBuf:         String buffer to check/change (string).

    reqLen:         Desired lenght of string (integer).

    prependChr:     Character to prepend (string/length=1).

    Returns:        Resulting string with the characters prepended (string).
    """
    noMisChars = (reqLen - len(strBuf))
    for i in range(noMisChars):
        strBuf = prependChr + strBuf
    return strBuf


def loadDoc(docName):
    """
    Load the given documation page and return the contents in a string buffer.
    Lines initiated with # in the documentation are filtered out.

    docName:  Name of documentation page, relative to the ngams module
              root directory (string).

    Returns:  Documentation page (string).
    """
    complDocName = os.path.normpath(ngamsGetSrcDir() + "/" + docName)
    fo = open(complDocName)
    docBufLines = fo.readlines()
    fo.close()
    docBuf = ""
    for docLine in docBufLines:
        if (docLine[0] != "#"): docBuf += docLine
    return docBuf


def setTestMode():
    """
    Switch on the Test Mode Flag. When set, special conditions are applied
    during execution of SW/server.

    Returns:   Void.
    """
    global _testMode
    _testMode = 1


def getTestMode():
    """
    Return the value of the Test Mode Flag. When set to 1, the server will not
    kill itself (as this would kill the test case).

    Returns:     Test Mode Flag (integer/0|1).
    """
    global _testMode
    return _testMode


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
        raise Exception, msg % str(val)


# EOF
