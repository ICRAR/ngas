

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
# "@(#) $Id: ngasArchiveClient.py,v 1.18 2009/02/06 23:23:32 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  2008-12-14  Created
#

_doc =\
"""
The NG/AMS Archive Client is a small application, which can be used to
archive files into a remote NGAS Archive System in a safe and reliable
manner.

Since the communication with NGAS takes place via HTTP, it is fairly easy
to set up such an archiving scenario, also if the data provider is located
at a geographically different location than the NGAS Archive. It must of
course be possible to build up an HTTP connection (TCP/IP socket connection)
between the two nodes involved.

Archive Queue:
The archive client is running as a small daemon, which is polling an input
directory for files to be archived according to the polling time specified
with the command line parameter '--pollTime'. Files to be archived should be
copied to this 'Archive Queue Directory'. The location of this is:

  <Root Dir>/NGAS_ARCHIVE_CLIENT/queue

It is also possible to create a link from the original file to the queue
directory if it is not desirable to copy the file.


NOTE: IF FILES ARE COPIED INTO THE QUEUE DIRECTORY THIS SHOULD BE DONE BY
CREATING THE FILE UNDER A TEMPORARY NAME (STARTING WITH DOT) AND THEN
RENAMING IT WHEN IT HAS BEEN COPIED OVER COMPLETELY.

IN ADDITION, IN ORDER FOR THE TOOL TO WORK, THE NAMES OF THE FILES SCHEDULED
INTO THE ACHIVE QUEUE MUST BE UNIQUE.


If a file cannot be archived, e.g. if it is not possible to connect to the
remote NGAS System, the archive client will retry periodically to archive it
with the given poll time until it succeeds.

Archived Files:
When a file in the Archive Queue Directory has been archived, it is moved
to the 'Archived Files Directory'. The path of this is:

  <Root Dir>/NGAS_ARCHIVE_CLIENT/archived

The archived files will be kept in this directory until the timeout
specified by the command line parameter '--cleanUpTimeout=<Timeout>' (timeout
in seconds) expires for the file.

Before removing any file from the Archived Files Directory, the archive client
contacts the remote NG/AMS System and checks that the file is available in the
NGAS Archive using the NG/AMS 'CHECKFILE' Command.

For each file archived, a file containing the associated Archive Request
Object, which again contains all information needed to handle the request,
is created. This is named:

  <Timestamp>___<Filename>___STATUS.pickle

This is used to be able to restore the internal data structures, in case
the tool is interrupted while there are still unfinished requests in the
queues.

Bad Files:
If files are rejected by the remote NG/AMS Server, if they are inconsistent,
these files are classified as 'Bad Files' and are moved to the Bad Files
Directory. The location of this is:

  <Root Dir>/NGAS_ARCHIVE_CLIENT/bad

The server will not try to clean up files in the Bad Files Directory. This
must be done by the operators of the NG/AMS Archive Client. Also in
connection with Bad Files, the pickled Archive Request Object will
be stored into a file in the Bad Files Directory with a name of the format:

  <Timestamp>___<Filename>___STATUS.pickle

By studying the error message in the internal ngamsStatus object, it is
possible to get an indication of the problem encountered for each file.

Logging:
If a Log Level higher than 0 is specified, the NG/AMS Archive Client will log
information about the actions carried out into the log file:

  <Root Dir>/NGAS_ARCHIVE_CLIENT/log/ngasArchiveClient.log

The client supports log rotation by sending a SIGHUP signal to the client.

The log file rotation can also be controlled automatically (internally) via
the command line options --logRotation, defines the time of the day to rotate
the log.

Specifying the option --logFileMaxSize, it is possible to specify the maximim
size (in MB) the log file can take.

By means of the option, --logHistory, it can be specified how many rotated
log files should be kept.

The rotated log files have a name of the form:

  <Root Dir>/NGAS_ARCHIVE_CLIENT/log/ngasArchiveClient_<ISO8601 Time Stamp>.log

Checksum Checking:
It is possible to make the tool calculate a checksum of the files to be
archived before transferring them across to the NGAS Archive. This is done
using the '--checksum' command line parameter. Together with this parameter
a Checksum Plug-In must be specified. This is a small tool that can be
executed on the shell, giving the file to be archived as input parameter.
In case of success, the generated checksum must be written on stdout. In case
of errors, the tool exits with 1 and a message with the following format is
written on stderr:

  ERROR: <Error Message>

Together with the NG/AMS Archive Client, a Checksum Plug-In is delivered.
This is named 'ngamsCrc32'. It calculates the CRC-32 checksum of the file.
This Checksum Plug-In is compatible with the CRC-32 Checksum Plug-In
used internally by NG/AMS ('ngamsGenCrc32').

Configuration/Installation:
The NG/AMS Archive Client does not require much configuration. The steps to
carry out to install this application are:

1. Compile the NG/AMS Archive Client if delivered in the form of source files:

  $ cd ngasUtils/src
  $ make clean all [install]

2. Install the NG/AMS Archive Client ('ngasArchiveClient') in a directory
where it can be executed as a shell command. Probably set up the OS init
scripts to launch the archive client automatically when the machine boots up.
Also install the Checksum Plug-In ('ngamsCrc32') if the checksum checking
feature is used.

3. Determine a proper location for the NG/AMS Archive Client Root Directory.
This should be big enough to hold the amount of data that might be buffered
here at any time. This directory must be writable for the archive client.
Note, the NG/AMS Archive Client will create the needed directory structure
automatically, the first time it is started up. This structure is:

  <Root Dir>/NGAS_ARCHIVE_CLIENT/archived
                                /bad
                                /log
                                /queue

4. Determine proper values for the other input parameters (Archive Queue
Polling Time, Archived Files Directory Clean Up Timeout, number of possible
parallel streams, log conditions, etc.).

5. Launch the NG/AMS Archive Client (possibly by rebooting the host).

6. Set up the data provider applications to deliver the files in the
Archive Queue Directory according to the requirements given above.

7. Check the log output. In case of problem it is possible to temporarily
start the server with a higher Log Level to get more information about the
problem.

The input parameters defined for the tool are:

%s

"""

import sys, os, time, threading, signal, glob, Queue, cPickle

from ngams import *
import ngamsThreadGroup
import ngamsPClient
import ngasUtils
import ngasUtilsLib


# Log functions to substitute the ones in the ngams name space to be able to
# carry out the log rotation (put the logging on hold while the file is
# being rotated).
import ngams
_logCtrl = threading.Event()
_logCtrl.set()


def info(level,
         msg):
    """
    Function encapsulating ngams.info().

    level:    Log level assigned to the log statement (integer [0; 5]).

    msg:      Log message (string).

    Returns:  Void
    """
    _logCtrl.wait()
    ngams.info(level, msg)


def notice(msg):
    """
    Function encapsulating ngams.notice().

    msg:      Log message (string).

    Returns:  Void
    """
    _logCtrl.wait()
    ngams.notice(msg)


def warning(msg):
    """
    Function encapsulating ngams.warning().

    msg:      Log message (string).

    Returns:  Void
    """
    _logCtrl.wait()
    ngams.warning(msg)


def error(msg):
    """
    Function encapsulating ngams.error().

    msg:      Log message (string).

    Returns:  Void
    """
    _logCtrl.wait()
    ngams.error(msg)


def rotateLog(logFile):
    """
    Rotate the log file.

    logFile:  Name of the log file (string).

    Returns:  Void.
    """
    T = TRACE()

    try:
        _logCtrl.clear()
        logFlush()
        rotLogTime = timeRef2Iso8601(time.time())
        rotLogFile = os.path.normpath("%s_%s.log" % (logFile[:-4], rotLogTime))
        mvFile(logFile, rotLogFile)
        ngams.info(1, "Rotated log file")
        logFlush()
        _logCtrl.set()
    except Exception, e:
        _logCtrl.set()
        msg = "Error rotating log file: %s. Error: %s"
        error(msg % (logFile, str(e)))


def logRotTimer(archCliObj):
    """
    Log rotation timer call-back function. Used together with a Python Timer
    to carry out the daily log, time controller rotation.

    archCliObj:  Reference to the instance of the NGAS Archive Client Class
                 (ngasArchiveClient).

    Returns:     Void.
    """
    T = TRACE()

    info(1, "Log Rotation Timer carrying out log rotation ...")
    logFile = archCliObj.getPar(archCliObj.PAR_INT_LOG_FILE)
    rotateLog(logFile)
    logRotTime = isoTime2Secs(archCliObj.getPar(archCliObj.PAR_LOG_ROTATION))
    timeNow = isoTime2Secs(getAsciiTime(timeSinceEpoch = time.time(),
                                        precision=0))
    time2NextRot = (logRotTime - timeNow)
    if (time2NextRot < 0): time2NextRot = ((24 * 3600) + time2NextRot)
    info(1, "Setting up Log Rotation Timer to trigger in %ds" % time2NextRot)
    timer = threading.Timer(time2NextRot, logRotTimer, [archCliObj])
    archCliObj.setLogRotTimer(timer)
    timer.start()


# Global pointer to instance of ngasArchiveClient (for signal handling
# purposes).
_archiveClient = None
def ngasSignalHandler(signalNo,
                      frameObj):
    """
    Signal handler. Currently handles the following signals:

       1. SIGHUP:             Carry out a log rotation.
       2. SIGINT/15. SIGTERM: Terminate server in a clean way.

    signalNo:    Signal number (integer).

    frameObj:    Consult Python documentation for signal.signal().

    Returns:     Void.
    """
    T = TRACE()

    notice("Received signal: %d" % signalNo)
    if (signalNo == signal.SIGHUP):
        if (_archiveClient):
            info(1, "SIGHUP: Rotating log file ...")
            logFile = _archiveClient.getPar(_archiveClient.PAR_INT_LOG_FILE)
            rotateLog(logFile)
        else:
            info(1, "SIGHUP: No reference to NGAS Archive Client defined - "+\
                 "ignoring")
    elif (_archiveClient):
        _archiveClient.stopAllThreads()


def qMonThread(threadGroupObj):
    """
    Function encapsulating the Queue Monitoring Thread implementation in
    the ngasArchiveClient object.

    threadGroupObj:   Thread Group Object to which this thread belongs
                      (ngamsThreadGroup).

    Returns:          Void.
    """
    T = TRACE()

    archiveClientObj = threadGroupObj.getParameters()[0]
    archiveClientObj.qMonThread(threadGroupObj)


def archiveThread(threadGroupObj):
    """
    Function encapsulating the Archive Thread implementation in the
    ngasArchiveClient object.

    threadGroupObj:   Thread Group Object to which this thread belongs
                      (ngamsThreadGroup).

    Returns:          Void.
    """
    T = TRACE()

    archiveClientObj = threadGroupObj.getParameters()[0]
    archiveClientObj.archiveThread(threadGroupObj)


def cleanUpThread(threadGroupObj):
    """
    Function encapsulating the Clean-Up Thread implementation in the
    ngasArchiveClient object.

    threadGroupObj:   Thread Group Object to which this thread belongs
                      (ngamsThreadGroup).

    Returns:          Void.
    """
    T = TRACE()

    archiveClientObj = threadGroupObj.getParameters()[0]
    archiveClientObj.cleanUpThread(threadGroupObj)


def janitorThread(threadGroupObj):
    """
    Function encapsulating the Janitor Thread implementation in the
    ngasArchiveClient object.

    threadGroupObj:   Thread Group Object to which this thread belongs
                      (ngamsThreadGroup).

    Returns:          Void.
    """
    T = TRACE()

    archiveClientObj = threadGroupObj.getParameters()[0]
    archiveClientObj.janitorThread(threadGroupObj)


class ngasArchiveRequest:
    """
    Class used to manage the information for one Archive Request in the
    context of the NGAS Archive Client.

    One instance is created per file to be archived. This instance is passed
    around among the worker threads.

    Note, the object is pickable, which is used to implement persistency of
    the NGAS Archive Client.
    """

    def __init__(self):
        """
        Constructor.
        """
        T = TRACE()

        self.__schedTime             = time.time()
        self.__archiveQueueFilename  = None
        self.__archivedQueueFilename = None
        self.__ngasStatDocFilename   = None
        self.__lastArchiveAttempt    = 0
        self.__lastCheckAttempt      = 0
        self.__ngasStatObj           = None


    def getSchedTime(self):
        """
        Get the scheduling time for when the request was registered.

        Returns:  Scheduling time in seconds since epoch (float).
        """
        return self.__schedTime

    def setQueueFilename(self,
                         queueFilename):
        """
        Set the staging filename of the file in the Queue Directory.

        queueFilename:  Staging filename (string).

        Returns:        Reference to object itself.
        """
        self.__archiveQueueFilename = queueFilename
        return self

    def getQueueFilename(self):
        """
        Get the staging filename of the file in the Queue Directory.

        Returns:   Staging filename (string).
        """
        return self.__archiveQueueFilename

    def setArchivedFilename(self,
                            archivedFilename):
        """
        Set the name of the staging file in the Archived Files Directory.

        archivedFilename:  Staging filename (string).

        Returns:           Reference to object itself.
        """
        self.__archivedQueueFilename = archivedFilename
        return self

    def getArchivedFilename(self):
        """
        Get the name of the staging file in the Archived Files Directory.

        Returns:   Staging filename (string).
        """
        return self.__archivedQueueFilename

    def setStatDocFilename(self,
                           statDocFilename):
        """
        Set the name of the ngasArchiveRequest Status (pickle) file.

        statDocFilename:  Name of status pickle file (string).

        Returns:          Reference to object itself.
        """
        self.__ngasStatDocFilename = statDocFilename
        return self

    def getStatDocFilename(self):
        """
        Get the name of the ngasArchiveRequest Status (pickle) file.

        Returns:   Name of status pickle file (string).
        """
        return self.__ngasStatDocFilename

    def updateLastArchiveAttempt(self):
        """
        Update the time for the last attempt to archive the file associated
        to the request.

        Returns:          Reference to object itself.
        """
        self.__lastArchiveAttempt = time.time()
        return self

    def getLastArchiveAttempt(self):
        """
        Return the time for the last attempt to archive the file associated
        to the request.

        Returns:   Time for last attempt to archive the file in seconds since
                   epoch (float).
        """
        return self.__lastArchiveAttempt

    def updateLastCheckAttempt(self):
        """
        Update the time for the last attempt to check the file associated
        to the request.

        Returns:          Reference to object itself.
        """
        self.__lastCheckAttempt = time.time()
        return self

    def getLastCheckAttempt(self):
        """
        Return the time for the last attempt to check the file associated
        to the request.

        Returns:   Time for last attempt to check the file in seconds since
                   epoch (float).
        """
        return self.__lastCheckAttempt

    def setNgasStatObj(self,
                       ngasStatObj):
        """
        Set the reference to the NGAMS Status Object, returned from the
        Archive Request sent to the remote NGAS System.

        ngasStatObj:      NGAMS Status Object (ngamsStatus).

        Returns:          Reference to object itself.
        """
        T = TRACE()

        self.__ngasStatObj = ngasStatObj
        return self

    def getNgasStatObj(self):
        """
        Get the reference to the NGAMS Status Object, returned from the
        Archive Request sent to the remote NGAS System.

        Returns:      NGAMS Status Object (ngamsStatus).
        """
        return self.__ngasStatObj


class ngasArchiveClient:
    """
    Class implementing all services of the NAGS Archive Client.
    """

    # Constants.
    ARCHIVED_DIR             = "archived"
    ARCHIVE_THREAD_ID        = "ARCHIVE_THREAD"
    ARCH_CLIENT_HOME         = "NGAS_ARCHIVE_CLIENT"
    ARCH_CLIENT_TOOL         = "ngasArchiveClient"
    BAD_DIR                  = "bad"
    CLEAN_UP_THREAD_ID       = "CLEAN_UP_THREAD"
    JANITOR_THREAD_ID        = "JANITOR_THREAD"
    LOCK_FILE_EXT            = "lock"
    LOG_DIR                  = "log"
    LOG_FILE                 = "ngasArchiveClient.log"
    QUEUE_DIR                = "queue"
    Q_MON_THREAD_ID          = "QUEUE_MON_THREAD"

    # Parameters.

    # Internal parameters.
    PAR_INT_ARCHIVED_DIR     = "_archived-dir"
    PAR_INT_BAD_DIR          = "_bad-dir"
    PAR_INT_LOG_DIR          = "_log-dir"
    PAR_INT_LOG_FILE         = "_log-file"
    PAR_INT_QUEUE_DIR        = "_queue-dir"
    PAR_INT_WORKING_DIR      = "_working-dir"

    # Command line options.
    PAR_ARCHIVE_PAR          = "archivePar"
    PAR_AUTH                 = "auth"
    PAR_CHECKSUM             = "checksum"
    PAR_CLEAN_UP_TIMEOUT     = "cleanUpTimeOut"
    PAR_LOG_FILE_MAX_SIZE    = "logFileMaxSize"
    PAR_LOG_HISTORY          = "logHistory"
    PAR_LOG_LEVEL            = "logLevel"
    PAR_LOG_ROTATION         = "logRotation"
    PAR_MIME_TYPE            = "mimeType"
    PAR_POLL_TIME            = "pollTime"
    PAR_ROOT_DIR             = "rootDir"
    PAR_SERVERS              = "servers"
    PAR_SERVER_CMD           = "server-cmd"
    PAR_STREAMS              = "streams"

    # Parameter array.
    PARAMETERS = [\

        # Internal/house holding parameters.
        [PAR_INT_WORKING_DIR, [], None, ngasUtilsLib.NGAS_OPT_INT, "",
         "Internal Parameter: Working directory."],

        [PAR_INT_LOG_FILE, [], None, ngasUtilsLib.NGAS_OPT_INT, "",
         "Internal Parameter: Log file."],

        # Command line options.
        [PAR_SERVERS, [], None, ngasUtilsLib.NGAS_OPT_MAN,
         "=<Server List>", "Comma separated list of constatc server " +\
         "nodes + ports."],

        [PAR_ROOT_DIR, [], None, ngasUtilsLib.NGAS_OPT_MAN,
         "=<Root Directory>", "Main root directory of the tool. It will " +\
         "create its home directory structure in the given root directory."],

        [PAR_MIME_TYPE, [], None, ngasUtilsLib.NGAS_OPT_OPT,
         "=<Mime-type>", "Mime-type to submit with the archive request. " +\
         "Note, all data will be archived with this type" +\
         "- NOT YET IMPLEMENTED."],

        [PAR_ARCHIVE_PAR, [], None, ngasUtilsLib.NGAS_OPT_OPT,
         "=<Archive Command Parameters>",
         "With this option it is possible to transfer parameter/value " +\
         "sets, which will be submitted  with the ARCHIVE Command sent to " +\
         "the remote NGAS system. An example is: ... -archivePar " +\
         "'no_versioning=0' - NOT YET IMPLEMENTED"],

        [PAR_AUTH, [], None, ngasUtilsLib.NGAS_OPT_OPT, "=<Access Key>",
         "Access code needed to have a request authorized in case " +\
         "authorization is used on the server side. The responsibles of " +\
         "the server side must provide the access code" +\
         "- NOT YET IMPLEMENTED."],

        [PAR_POLL_TIME, [], 30.0, ngasUtilsLib.NGAS_OPT_OPT,
         "=<Poll Period>", "Time in seconds (floating point) with which " +\
         "the NG/AMS Archive Client should poll the Archive Queue. Default " +\
         "value is 30.0s."],

        [PAR_CHECKSUM, [], None, ngasUtilsLib.NGAS_OPT_OPT,
         "=<Checksum Plug-in>", "If specified, a checksum will be " +\
         "generated and will be added in the HTTP headers of the Archive " +\
         "Request sent to the remote NGAS System. This is used at the " +\
         "server side to verify that the file has been transferred " +\
         "correctly. The Checksum Plug-In specified, must be a utility " +\
         "that can be executed on the command line - " +\
         "NOT YET IMPLEMENTED."],

        [PAR_CLEAN_UP_TIMEOUT, [], 604800, ngasUtilsLib.NGAS_OPT_MAN,
         "=<Timeout>", "Period of time (given in seconds) in which the " +\
         "successfully archived files in the Archived File Directory " +\
         "should be kept. Default value is 7 days (604800s)."],

        [PAR_LOG_FILE_MAX_SIZE, [], None, ngasUtilsLib.NGAS_OPT_OPT,
         "=<Max Size (MB)>", "The maximum size the log file can obtain " +\
         "before being rotated. Only relevant if logging into a log file " +\
         "is activated."],

        [PAR_LOG_HISTORY, [], 30, ngasUtilsLib.NGAS_OPT_OPT,
         "=<Number of Log Files>", "Number of rotated log files to keep."],

        [PAR_LOG_LEVEL, [], 0, ngasUtilsLib.NGAS_OPT_OPT,
         "=<Log Level>", "Log Level to apply when producing log file output."],

        [PAR_LOG_ROTATION, [], None, ngasUtilsLib.NGAS_OPT_OPT,
         "=<Timestamp>", "The time of the day at which the log file should "+\
         "be rotated, e.g. 12:00:00. Only relevant if logging into a log " +\
         "file is activated."],

        [PAR_STREAMS, [], 1, ngasUtilsLib.NGAS_OPT_OPT,
         "=<Numbher of Streams>", "Number of parallel streams allowed. It " +\
         "will make sense to allow two streams to make it possible to " +\
         "process data on the server side, while the next file is being " +\
         "transferred. Default is 1 stream. If more is specified, the " +\
         "Archive Requests will be scheduled such that the next Archive " +\
         "Request is  scheduled at the average time to handle " +\
         "an Archive Request divided by the number of streams allowed."],

        [PAR_SERVER_CMD, [], NGAMS_ARCHIVE_CMD, ngasUtilsLib.NGAS_OPT_OPT,
         "=<Server Command>", "Name of command to issue with the request " +\
         "to the remote server, if different from ARCHIVE."]]


    def __init__(self):
        """
        Constructor.
        """
        T = TRACE()

        self.__optDic              = {}
        self.__parDic              = {}
        self.__archiveQueue        = Queue.Queue()
        self.__archivedQueue       = Queue.Queue()
        self.__queueMap            = {}
        self.__qMonThreadGroup     = None
        self.__archiveThreadsGroup = None
        self.__cleanUpThreadsGroup = None
        self.__janitorThreadGroup  = None
        self.__logRotTimer         = None

        # Members to handle requests waiting for being ripe for deletion from
        # the queues.
        # List to keep track of elements, waiting for being ready for deletion.
        self.__cleanUpReqList = []
        # Dictionary used to refer to the associated Archive Request Object
        # for each time stamp in the archiveRequestList.
        self.__cleanUpReqDic  = {}
        # Semaphore to control access to the list/dictionary.
        self.__cleanUpSem     = threading.Semaphore(1)

    def __del__(self):
        """
        Destructor. Removes the lock file.
        """
        T = TRACE()

        try:
            rmFile(self.getLockFile())
        except:
            pass
        try:
            if (self.__logRotTimer): self.__logRotTimer.cancel()
        except:
            pass

    def setLogRotTimer(self,
                       timerObj):
        """
        Method to set the internal reference to the log rotation timer object.

        timerObj:   Timer object (Timer).

        Returns:    Reference to object itself.
        """
        self.__logRotTimer = timerObj
        return self

    def stopAllThreads(self):
        """
        Send signal to all worker threads to stop execution.

        Returns:   Void.
        """
        T = TRACE()

        for thrGr in (self.__qMonThreadGroup, self.__archiveThreadsGroup,
                      self.__cleanUpThreadsGroup, self.__janitorThreadGroup):
            if (thrGr): thrGr.stop()

    def initialize(self,
                   argv,
                   optDic):
        """
        Carry out basic initialization.

        argv:     Argument vector as defined by sys.argv (list).

        optDic:   Dictionary with information about all options (dictionary).

        Returns:  Void.
        """
        T = TRACE()

        self.__optDic = ngasUtilsLib.parseCmdLine(argv, optDic)
        self.__parDic = ngasUtilsLib.optDic2ParDic(self.__optDic)

    def setPar(self,
               par,
               value):
        """
        Set internal parameter in the parameter space.

        par:       Parameter name (string).

        value:     Value of the parameter (<Object>).

        Returns:   Reference to object itself.
        """
        T = TRACE()

        self.__parDic[par] = value
        return self

    def getPar(self,
               par):
        """
        Get internal parameter in the parameter space.

        par:       Parameter name (string).

        Returns:   Value of the parameter (<Object>).
        """
        T = TRACE(5)

        if (not self.__parDic.has_key(par)):
            msg = "Parameter: %s is unknown" % par
            raise Exception, msg

        return self.__parDic[par]

    def hasPar(self,
               par):
        """
        Probe if the  parameter space has the given parameter.

        par:       Parameter name (string).

        Returns:   True if the parameter is defined in the parameter
                   space (boolean).
        """
        return self.__parDic.has_key(par)

    def genWorkingDir(self):
        """
        Generate home directory structure of the tool.

        Returns:  Void.
        """
        T = TRACE()

        # Root Directory.
        tmpPath = "%s/%s" % (self.getPar(self.PAR_ROOT_DIR),
                             self.ARCH_CLIENT_HOME)
        self.setPar(self.PAR_INT_WORKING_DIR, os.path.normpath(tmpPath))
        checkCreatePath(self.getPar(self.PAR_INT_WORKING_DIR))

        # Queue Directory.
        tmpPath = "%s/%s" % (self.getPar(self.PAR_INT_WORKING_DIR),
                             self.QUEUE_DIR)
        self.setPar(self.PAR_INT_QUEUE_DIR, os.path.normpath(tmpPath))
        checkCreatePath(self.getPar(self.PAR_INT_QUEUE_DIR))

        # Archive Files Directory.
        tmpPath = "%s/%s" % (self.getPar(self.PAR_INT_WORKING_DIR),
                             self.ARCHIVED_DIR)
        self.setPar(self.PAR_INT_ARCHIVED_DIR, os.path.normpath(tmpPath))
        checkCreatePath(self.getPar(self.PAR_INT_ARCHIVED_DIR))

        # Bad Files Directory.
        tmpPath = "%s/%s" % (self.getPar(self.PAR_INT_WORKING_DIR),
                             self.BAD_DIR)
        self.setPar(self.PAR_INT_BAD_DIR, os.path.normpath(tmpPath))
        checkCreatePath(self.getPar(self.PAR_INT_BAD_DIR))

        # Log Files Directory.
        tmpPath = "%s/%s" % (self.getPar(self.PAR_INT_WORKING_DIR),
                             self.LOG_DIR)
        self.setPar(self.PAR_INT_LOG_DIR, os.path.normpath(tmpPath))
        checkCreatePath(self.getPar(self.PAR_INT_LOG_DIR))

    def getLockFile(self):
        """
        Return the name of the lock file for this session of the tool.

        Returns:  Lock file name (string).
        """
        T = TRACE()

        lockFile = os.path.normpath("%s/%s.%s" %\
                                    (self.getPar(self.PAR_INT_WORKING_DIR),
                                     self.ARCH_CLIENT_TOOL,
                                     self.LOCK_FILE_EXT))
        return lockFile

    def fileBeingProcessed(self,
                           stagingFilename):
        """
        Check if a file in the queue directory is already being processed.

        stagingFilename:   Name of the file in the queue (string).

        Returns:           True if the file is being processed (boolean).
        """
        T = TRACE(5)

        if (self.__queueMap.has_key(os.path.basename(stagingFilename))):
            return True
        else:
            return False

    def move2BadDir(self,
                    archiveRequest):
        """
        The file was classified as bad or as a file, which cannot be
        handled by the target system. It will be moved to the Bad Files
        Directory and de-queued.

        archiveRequest:   Archive Request instance (ngasArchiveRequest).

        Returns:          Void.
        """
        T = TRACE()

        stgFile = archiveRequest.getQueueFilename()
        info(1, "Moving file: %s to the Bad Files Directory" % stgFile)
        badFilename = os.path.normpath(self.getPar(self.PAR_INT_BAD_DIR) +\
                                       "/" + os.path.basename(stgFile))
        mvFile(stgFile, badFilename)
        statFilename = "%d___%s___STATUS.pickle" % (int(time.time() + 0.5),
                                                    os.path.basename(stgFile))
        ngasStatDocFile = os.path.\
                          normpath(self.getPar(self.PAR_INT_BAD_DIR) +\
                                   "/" + statFilename)
        archiveRequest.setStatDocFilename(ngasStatDocFile)
        try:
            fo = open(ngasStatDocFile, "w")
            fo.write(cPickle.dumps(archiveRequest, 1))
            fo.close()
        except Exception, e:
            msg = "Error generating NGAS Status pickle object file: %s"
            error(msg % ngasStatDocFile)
            raise Exception, msg
        self.deQueueArchiveReq(archiveRequest, False)

    def move2ArchivedQueue(self,
                           archiveRequest):
        """
        Move the file for a successfully archived file, to the Archive Queue
        Directory. The NG/AMS XML Status information returned from the Archive
        Request, is dumped into a file and stored in the Archive Queue as well.

        archiveRequest:    Instance of NGAS Archive Request associated to the
                           file (ngasArchiveRequest).

        Returns:           Void.
        """
        T = TRACE()

        stgFile = archiveRequest.getQueueFilename()
        newStgFile = os.path.normpath(self.getPar(self.PAR_INT_ARCHIVED_DIR) +\
                                      "/" + os.path.basename(stgFile))
        archiveRequest.setArchivedFilename(newStgFile)
        statFilename = "%d___%s___STATUS.pickle" % (int(time.time() + 0.5),
                                                    os.path.basename(stgFile))
        ngasStatDocFile = os.path.\
                          normpath(self.getPar(self.PAR_INT_ARCHIVED_DIR) +\
                                   "/" + statFilename)
        archiveRequest.setStatDocFilename(ngasStatDocFile)
        info(1, "Moving file: %s to the Archived Files Directory" % stgFile)
        mvFile(stgFile, newStgFile)
        try:
            fo = open(ngasStatDocFile, "w")
            fo.write(cPickle.dumps(archiveRequest, 1))
            fo.close()
        except Exception, e:
            msg = "Error generating NGAS Status pickle object file: %s"
            error(msg % ngasStatDocFile)
            raise Exception, msg
        self.__archivedQueue.put_nowait(archiveRequest)

    def deQueueArchiveReq(self,
                          archiveRequest,
                          rmStatusDoc = True):
        """
        Remove an Archive Request from the internal queuing system.

        archiveRequest:   Instance of NGAS Archive Request associated to the
                          file (ngasArchiveRequest).

        rmStatusDoc:      If True, remove also the the NG/AMS XML Status
                          information returned from the Archive Request, dumped
                          into a file and stored in the Archive Queue
                          (boolean).

        Returns:          Void.
        """
        T = TRACE()

        # Remove the entry for the file from the internal map.
        stgFile = os.path.basename(archiveRequest.getQueueFilename())
        if (self.__queueMap.has_key(stgFile)): del self.__queueMap[stgFile]
        # Move from the directory queues.
        if (archiveRequest.getQueueFilename()):
            rmFile(archiveRequest.getQueueFilename())
        if (archiveRequest.getArchivedFilename()):
            rmFile(archiveRequest.getArchivedFilename())
        if (rmStatusDoc and archiveRequest.getStatDocFilename()):
            rmFile(archiveRequest.getStatDocFilename())
        del archiveRequest

    def qMonThread(self,
                   threadGroupObj):
        """
        Encapsulation method that implements the business logic of the Queue
        Monitor Thread.

        threadGroupObj:   Reference to NG/AMS Thread Group Object
                          (ngamsThreadGroup).

        Returns:          Void.
        """
        T = TRACE()

        run = True
        while (run):
            try:
                self._qMonThread(threadGroupObj)
                # Thread execution stopped. Exit.
                run = False
            except Exception, e:
                if (str(e).find("__NGAMS_THR_GROUP_STOP_NORMAL__") != -1):
                    run = False
                    continue
                warning("Exception caught: %s - resuming thread execution" %\
                        str(e))
                if (getDebug()):
                    # If debug mode, print out entire stack.
                    print e

    def _qMonThread(self,
                    threadGroupObj):
        """
        Method that implements the business logic of the Queue
        Monitor Thread.

        threadGroupObj:   Reference to NG/AMS Thread Group Object
                          (ngamsThreadGroup).

        Returns:          Void.
        """
        T = TRACE()

        pollTime = float(self.getPar(self.PAR_POLL_TIME))
        if (pollTime < 0): pollTime = 0
        queueDirPat = self.getPar(self.PAR_INT_QUEUE_DIR) + "/*"
        while (True):
            threadGroupObj.checkPauseStop()
            if (pollTime <= 5):
                if (pollTime > 0): time.sleep(pollTime)
            else:
                startWaitTime = time.time()
                while ((time.time() - startWaitTime) < pollTime):
                    threadGroupObj.checkPauseStop()
                    time.sleep(1.0)
            threadGroupObj.checkPauseStop()

            # Get list of files in the queue.
            queueFileList = glob.glob(queueDirPat)
            # Schedule the files found into the Archive Queue.
            for queueFile in queueFileList:
                # If file is already in the queues, skip to the next.
                if (self.fileBeingProcessed(queueFile)): continue

                # File is not being processed, queue it into the system.
                archiveRequest = ngasArchiveRequest().\
                                 setQueueFilename(queueFile)
                # Add also the file in the queue map to indicate it is being
                # handled.
                self.__queueMap[os.path.basename(queueFile)] = archiveRequest
                # Put the request in the archive queue.
                self.__archiveQueue.put_nowait(archiveRequest)

    def archiveThread(self,
                      threadGroupObj):
        """
        Encapsulation method that implements the business logic of the Archive
        Thread.

        threadGroupObj:   Reference to NG/AMS Thread Group Object
                          (ngamsThreadGroup).

        Returns:          Void.
        """
        T = TRACE()

        ngasClient = ngamsPClient.ngamsPClient().\
                     parseSrvList(self.getPar(self.PAR_SERVERS))
        run = True
        while (run):
            try:
                self._archiveThread(threadGroupObj, ngasClient)
                # Thread execution stopped. Exit.
                run = False
            except Exception, e:
                if (str(e).find("__NGAMS_THR_GROUP_STOP_NORMAL__") != -1):
                    run = False
                    continue
                warning("Exception caught: %s - resuming thread execution" %\
                        str(e))
                if (getDebug()):
                    # If debug mode, print out entire stack.
                    print e

    def _archiveThread(self,
                       threadGroupObj,
                       ngasClient):
        """
        Method that implements the business logic of the Archive Thread.

        threadGroupObj:   Reference to NG/AMS Thread Group Object
                          (ngamsThreadGroup).

        Returns:          Void.
        """
        T = TRACE()

        while (True):
            threadGroupObj.checkPauseStop()
            archiveRequest = None
            try:
                # Wait for an Archive Request for max. 5s.
                archiveRequest = self.__archiveQueue.get(block=True,
                                                         timeout=5.0)
            except:
                # Apparently no Archive Requests in the queue.
                continue

            # If was already attempted to archive this file, within 60s, wait.
            if ((time.time() - archiveRequest.getLastArchiveAttempt()) < 60):
                # Too early to re-try, put the request back in the queue.
                self.__archiveQueue.put_nowait(archiveRequest)
                continue

            # Handle the Archive Request.
            # TODO: Handle --archivePar, --checksum, --auth
            cmdPars = []
            dataMimeType = ""
            if (self.getPar(self.PAR_MIME_TYPE)):
                dataMimeType = self.getPar(self.PAR_MIME_TYPE)
            #statObj = ngasClient.archive(archiveRequest.getQueueFilename(),
            #                             mimeType=dataMimeType, pars=cmdPars)
            statObj = ngasClient.pushFile(archiveRequest.getQueueFilename(),
                                          mimeType=dataMimeType,
                                          pars=cmdPars,
                                          cmd=self.getPar(self.PAR_SERVER_CMD))
            archiveRequest.updateLastArchiveAttempt().setNgasStatObj(statObj)

            # Archive Request after-math.
            if (statObj.getStatus() == NGAMS_SUCCESS):
                msg = "File: %s has been successfully archived, moving to " +\
                      "Archived Queue"
                info(1, msg % archiveRequest.getQueueFilename())
                if (float(self.getPar(self.PAR_CLEAN_UP_TIMEOUT)) < 1.0):
                    # The staging file can be deleted strait away.
                    info(2, "Removing staging file: %s" %\
                         archiveRequest.getQueueFilename())
                    rmFile(archiveRequest.getQueueFilename())
                else:
                    self.move2ArchivedQueue(archiveRequest)
            else:
                # A failure occurred. One the following actions are taken:
                #
                # 1. Bad File:               Move file to Bad Files Directory.
                # 2. File Back-Log Buffered: Move file to Archived Directory.
                # 3. Other Errors:           Keep file in Archive Queue.
                ngasMsg = statObj.getMessage()
                if ((ngasMsg.find("NGAMS_ER_DAPI_BAD_FILE") != -1) or
                    (ngasMsg.find("NGAMS_ER_UNKNOWN_MIME_TYPE1") != -1)):
                    error("File: %s was classified as bad by NG/AMS. Moving "
                          "to Bad Files Directory." %\
                          archiveRequest.getQueueFilename())
                    self.move2BadDir(archiveRequest)
                elif (ngasMsg.find("NGAMS_WA_BUF_DATA") != -1):
                    msg = "File: %s has been back-log buffered, moving to " +\
                          "Archived Queue"
                    notice(msg % archiveRequest.getQueueFilename())
                    self.move2ArchivedQueue(archiveRequest)
                else:
                    msg = "Archiving file: %s failed. Keeping in Archive " +\
                          "Queue Error: %s"
                    warning(msg % (archiveRequest.getQueueFilename(),
                                   statObj.getMessage()))
                    self.__archiveQueue.put_nowait(archiveRequest)

    def checkForRipeReq(self,
                        cleanUpTime):
        """
        Method to check if there is an entry ready for being deleted from the
        waiting lists.

        cleanUpTime:    Time successfully handled Archive Requests should be
                        kept, before deleting them from the system (float).

        Returns:        Reference to 'ripe' Archive Request Object found
                        ngasArchiveRequest | None).
        """
        T = TRACE()

        try:
            self.__cleanUpSem.acquire()
            if (self.__cleanUpReqList == []):
                self.__cleanUpSem.release()
                return None
            # Check if the oldest element (=the first) is ready for deletion.
            if ((time.time() - self.__cleanUpReqList[0]) >= cleanUpTime):
                archiveRequest = self.__cleanUpReqDic[self.__cleanUpReqList[0]]
                del self.__cleanUpReqDic[self.__cleanUpReqList[0]]
                del self.__cleanUpReqList[0]
            else:
                archiveRequest = None
            self.__cleanUpSem.release()
            return archiveRequest
        except Exception, e:
            self.__cleanUpSem.release()
            msg = "Error checking queue for elements to delete. Error: %s" %\
                  str(e)
            raise Exception, msg

    def insertReqInQueue(self,
                         archiveRequest):
        """
        Insert an Archive Request in the internal queue. Ensure that older
        entries in the queue are handled first.

        archiveRequest:   Reference to Archive Request Object
                          (ngasArchiveRequest).

        Returns:          Void.
        """
        T = TRACE()

        try:
            self.__cleanUpSem.acquire()

            schedTime = archiveRequest.getSchedTime()
            while (True):
                if (self.__cleanUpReqDic.has_key(schedTime)):
                    schedTime += 0.000001
                else:
                    break
            # Insert the new element in the waiting list and sort it, to
            # ensure the oldest element is the first.
            self.__cleanUpReqList.append(schedTime)
            self.__cleanUpReqList.sort()
            # Also insert reference to the archive request in the dictionary.
            self.__cleanUpReqDic[schedTime] = archiveRequest

            self.__cleanUpSem.release()
        except Exception, e:
            self.__cleanUpSem.release()
            msg = "Error inserting element waiting for deletion in queues. " +\
                  "Error: %s" % str(e)
            raise Exception, msg

    def cleanUpThread(self,
                      threadGroupObj):
        """
        Encapsulation method that implements the business logic of the Clean
        Up Thread.

        threadGroupObj:   Reference to NG/AMS Thread Group Object
                          (ngamsThreadGroup).

        Returns:          Void.
        """
        T = TRACE()

        ngasClient = ngamsPClient.ngamsPClient().\
                     parseSrvList(self.getPar(self.PAR_SERVERS))
        run = True
        while (run):
            try:
                self._cleanUpThread(threadGroupObj, ngasClient)
                # Thread execution stopped. Exit.
                run = False
            except Exception, e:
                if (str(e).find("__NGAMS_THR_GROUP_STOP_NORMAL__") != -1):
                    run = False
                    continue
                warning("Exception caught: %s - resuming thread execution" %\
                        str(e))
                if (getDebug()):
                    # If debug mode, print out entire stack.
                    print e

    def _cleanUpThread(self,
                       threadGroupObj,
                       ngasClient):
        """
        Method that implements the business logic of the Clean
        Up Thread.

        threadGroupObj:   Reference to NG/AMS Thread Group Object
                          (ngamsThreadGroup).

        Returns:          Void.
        """
        T = TRACE()

        cleanUpTime = float(self.getPar(self.PAR_CLEAN_UP_TIMEOUT))
        while (True):
            threadGroupObj.checkPauseStop()
            archiveRequest = None
            try:
                # Check if there are old element, waiting to be deleted.
                archiveRequest = self.checkForRipeReq(cleanUpTime)

                DEBUG("Queue status: %s" % str(self.__archivedQueue.empty()))

                # Wait for an Archive Request for max. 5s (if no old elements
                # found).
                if (archiveRequest == None):
                    archiveRequest = self.__archivedQueue.get(block=True,
                                                              timeout=5.0)
            except Exception, e:
                DEBUG("Exception: %s" % str(e))
                # Apparently no Archive Requests in the queue.
                continue

            # Is file 'ripe' for removal?
            info(3, "Checking if Archive Request ready for removal: %s" %\
                 str(archiveRequest.getQueueFilename()))
            if ((time.time() - archiveRequest.getSchedTime()) < cleanUpTime):
                # Nope, put it in the archive request list/dictionary.
                self.insertReqInQueue(archiveRequest)
                continue

            # If was attempted to check this file, within 60s, wait.
            if ((time.time() - archiveRequest.getLastCheckAttempt()) < 60):
                # Too early to re-try, put the request back in the queue.
                self.__archivedQueue.put_nowait(archiveRequest)
                # Make a small sleep to avoid that the Clean-Up Thread consume
                # a lot of resources if the remote NGAS system is not reachable
                # or available.
                time.sleep(0.100)
                continue

            info(3, "Checking file for deletion: %s ..." %\
                 archiveRequest.getArchivedFilename())

            # Send the CHECKFILE Command to the target NGAS Archive.
            # Note, disk_id is not specified, it is assumed enough that the
            # given file is available in the target NGAS Archive on any disk.
            diskInfoObj = archiveRequest.getNgasStatObj().\
                          getDiskStatusList()[0]
            cmdPars = [["file_id", diskInfoObj.getFileObj(0).getFileId()],
                       ["file_version",
                        diskInfoObj.getFileObj(0).getFileVersion()]]
            statObj = ngasClient.sendCmd(NGAMS_CHECKFILE_CMD, pars=cmdPars)
            archiveRequest.updateLastCheckAttempt()

            # Check if the file is consistent.
            if (statObj.getMessage().find("NGAMS_INFO_FILE_OK") != -1):
                msg = "File: %s has been successfully archived. Removing " +\
                      "from queues"
                info(1, msg % archiveRequest.getArchivedFilename())
                self.deQueueArchiveReq(archiveRequest)
                del archiveRequest
            else:
                msg = "File: %s could not be validated successfully. " +\
                      "Re-inserting in queue. Error: %s"
                warning(msg % (archiveRequest.getArchivedFilename(),
                               str(statObj.getMessage())))
                self.__archivedQueue.put_nowait(archiveRequest)

    def janitorThread(self,
                      threadGroupObj):
        """
        Encapsulation method that implements the business logic of the Janitor
        Thread.

        threadGroupObj:   Reference to NG/AMS Thread Group Object
                          (ngamsThreadGroup).

        Returns:          Void.
        """
        T = TRACE()

        run = True
        while (run):
            try:
                self._janitorThread(threadGroupObj)
                # Thread execution stopped. Exit.
                run = False
            except Exception, e:
                if (str(e).find("__NGAMS_THR_GROUP_STOP_NORMAL__") != -1):
                    run = False
                    continue
                msg = "Exception thrown in Janitor Thread. Exception: %s" +\
                      "- resuming thread execution"
                warning(msg % str(e))
                if (getDebug()):
                    # If debug mode, print out entire stack.
                    print e

    def _janitorThread(self,
                       threadGroupObj):
        """
        Method that implements the business logic of the Janitor
        Thread.

        threadGroupObj:   Reference to NG/AMS Thread Group Object
                          (ngamsThreadGroup).

        Returns:          Void.
        """
        T = TRACE()

        # Set up a timer that will trigger the log rotation at the specified
        # time (if specified).
        if (self.getPar(self.PAR_LOG_ROTATION)):
            logRotTime = isoTime2Secs(self.getPar(self.PAR_LOG_ROTATION))
            timeNow = isoTime2Secs(getAsciiTime(precision=0))
            time2NextRot = (logRotTime - timeNow)
            if (time2NextRot < 0): time2NextRot = ((24 * 3600) + time2NextRot)
            info(1, "Setting up Log Rotation Timer to trigger in %ds" %\
                 time2NextRot)
            timer = threading.Timer(time2NextRot, logRotTimer, [self])
            self.setLogRotTimer(timer)
            timer.start()

        # Get name of the log file + the maximum limit for the size of the log
        # file, before forcing a log rotation.
        logFile = self.getPar(self.PAR_INT_LOG_FILE)
        if (logFile): logDir = os.path.dirname(logFile)
        if (self.getPar(self.PAR_LOG_FILE_MAX_SIZE)):
            maxSize = int(1e6 * float(self.getPar(self.PAR_LOG_FILE_MAX_SIZE)))
        else:
            maxSize = None

        # Get the maximum number of rotated log files to keep.
        if (logFile and self.getPar(self.PAR_LOG_HISTORY)):
            logHistory = int(self.getPar(self.PAR_LOG_HISTORY))
            logHistMatchPat = "%s/*_*.log" % logDir
        else:
            logHistory = None

        # Janitor service loop.
        while (True):
            time.sleep(10.0)
            threadGroupObj.checkPauseStop()

            # Check if the log file has reached its maximum allowed size.
            if (maxSize):
                if (getFileSize(logFile) > maxSize):
                    rotateLog(logFile)

            # Check if there are more than the requested number of rotated
            # log files.
            if (logHistory):
                logFileList = glob.glob(logHistMatchPat)
                if (logFileList):
                    logFileList.sort()
                    if ((len(logFileList) - logHistory) > 0):
                        for rotLogFile in logFileList[0:(len(logFileList) -
                                                         logHistory)]:
                            info(2, "Removing rotated log file: %s" %\
                                 rotLogFile)
                            rmFile(rotLogFile)

    def restore(self):
        """
        Restore the internal state of an Archive Request, which was being
        processed during a previous (interrupted) session of the NGAS
        Archive Client.

        Returns:    Void.
        """
        T = TRACE()

        info(1, "Checking if there are requests in the queues from a " +\
             "previous session, to be restored ...")

        # Check for Archive Requests in the Archived Queue. Re-schedule the
        # elements found.
        queueDirPat = self.getPar(self.PAR_INT_ARCHIVED_DIR) +\
                      "/*___STATUS.pickle"
        # Get list of files in the queue.
        statDocFileList = glob.glob(queueDirPat)
        for statDocFile in statDocFileList:
            info(1, "Restoring information from pickle object file: %s" %\
                 statDocFile)
            fo = open(statDocFile, "r")
            statDoc = fo.read()
            fo.close()
            archiveRequest = cPickle.loads(statDoc)
            # Register the file in the Archive Request Map and put the item in
            # the Archived Queue.
            qFile = archiveRequest.getQueueFilename()
            self.__queueMap[os.path.basename(qFile)] = archiveRequest
            self.__archivedQueue.put_nowait(archiveRequest)

    def execute(self):
        """
        Carry out the tool execution.

        Returns:   Void.
        """
        T = TRACE()

        info(1, "NGAS Archive Client preparing for operation ...")

        if (self.getPar("help")):
            print __doc__
            sys.exit(0)

        self.genWorkingDir()

        # Set up log file in the working directory if the log level is given.
        if (self.getPar("logLevel")):
            self.setPar(self.PAR_INT_LOG_FILE,
                        os.path.normpath("%s/%s" %\
                                         (self.getPar(self.PAR_INT_LOG_DIR),
                                          self.LOG_FILE)))
            setLogCond(0, "", int(self.getPar("logLevel")),
                       self.getPar(self.PAR_INT_LOG_FILE),
                       int(self.getPar("verbose")))

        # If a lock file exists, bail out.
        if (os.path.exists(self.getLockFile())):
            msg = "Seems that an instance of this tool is already running " +\
                  "using the same working directory: %s"
            error(msg % (self.getPar(self.PAR_INT_WORKING_DIR)))
            raise Exception, msg

        # Create lock file, put the PID in it.
        fo = open(self.getLockFile(), "w")
        fo.write(str(os.getpid()))
        fo.close()

        # Restore the internals from a previous session.
        self.restore()

        # Start worker threads:
        # - Archive Queue Monitoring Thread
        info(2, "Creating Queue Monitoring Thread ...")
        self.__qMonThreadGroup = ngamsThreadGroup.\
                                 ngamsThreadGroup(self.Q_MON_THREAD_ID,
                                                  qMonThread, 1, [self])
        self.__qMonThreadGroup.start(wait=False)

        # - Archive Threads
        info(2, "Creating Archive Threads ...")
        streams = int(self.getPar(self.PAR_STREAMS))
        if (streams < 1): streams = 1
        self.__archiveThreadsGroup = ngamsThreadGroup.\
                                     ngamsThreadGroup(self.ARCHIVE_THREAD_ID,
                                                      archiveThread, streams,
                                                      [self])
        self.__archiveThreadsGroup.start(wait=False)

        # - Clean-Up Threads
        info(2, "Creating Clean-Up Threads ...")
        noCleanUpThreads = int((float(streams) / 4) + 0.5)
        if (noCleanUpThreads < 1): noCleanUpThreads = 1
        self.__cleanUpThreadsGroup = ngamsThreadGroup.\
                                     ngamsThreadGroup(self.CLEAN_UP_THREAD_ID,
                                                      cleanUpThread,
                                                      noCleanUpThreads, [self])
        self.__cleanUpThreadsGroup.start(wait=False)

        # - Janitor Thread
        info(2, "Creating Janitor Thread ...")
        self.__janitorThreadGroup = ngamsThreadGroup.\
                                    ngamsThreadGroup(self.JANITOR_THREAD_ID,
                                                     janitorThread, 1, [self])
        self.__janitorThreadGroup.start(wait=False)

        # Wait for all threads to terminate.
        info(1, "Serving -- waiting for worker threads to terminate " +\
             "execution ...")
        active = True
        while (active):
            count = 0
            for thrGr in [self.__qMonThreadGroup,
                          self.__archiveThreadsGroup,
                          self.__cleanUpThreadsGroup,
                          self.__janitorThreadGroup]:
                if (thrGr.getNumberOfActiveThreads() == 0): count += 1
                if (count == 3):
                    info(1, "All worker threads terminated execution")
                    active = False
                    break
                time.sleep(5)

        # Stop the log rotation timer if active.
        try:
            if (self.__logRotTimer): self.__logRotTimer.cancel()
        except:
            pass

        # Remove lock (PID) file.
        try:
            rmFile(rmFile(self.getLockFile()))
        except:
            pass


# Generate the man-page
_optDic, _optDoc = ngasUtilsLib.genOptDicAndDoc(ngasArchiveClient.PARAMETERS)
__doc__ = _doc % _optDoc


# Main function.
if __name__ == '__main__':
    """
    Main function to execute the tool.
    """
    try:
        info(3, "Instantiating ngasArchiveClient class ...")
        archiveClient = ngasArchiveClient()
        info(3, "Initializing ngasArchiveClient instance ...")
        archiveClient.initialize(sys.argv, _optDic)
        info(3, "Defining signal handlers ...")
        _archiveClient = archiveClient
        signal.signal(signal.SIGTERM, ngasSignalHandler)
        signal.signal(signal.SIGINT, ngasSignalHandler)
        signal.signal(signal.SIGHUP, ngasSignalHandler)
    except Exception, e:
        print "\nProblem executing the tool:\n\n%s\n" % str(e)
        sys.exit(1)
    if (getDebug()):
        # Exexute such that the complete stack trace is printed out in case
        # an exception is thrown (mostly for debugging purpose).
        info(3, "Starting execution ...")
        archiveClient.execute()
    else:
        # Execute such that a reduced error message is printed out in case
        # an exception is thrown.
        try:
            archiveClient.execute()
        except Exception, e:
            if (str(e) == "0"): sys.exit(0)
            print "\nProblem executing the tool:\n\n%s\n" % str(e)
            sys.exit(1)

# EOF
