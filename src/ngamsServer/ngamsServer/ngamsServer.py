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
# "@(#) $Id: ngamsServer.py,v 1.30 2009/06/02 07:44:36 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  07/05/2001  Created
#
"""
This module contains the class ngamsServer that provides the
services for the NG/AMS Server.
"""

import BaseHTTPServer
import Queue
import SocketServer
import contextlib
import logging
import multiprocessing
import math
import os
import re
import shutil
import signal
import socket
import sys
import threading
import time
import traceback
import urllib

import pkg_resources

from ngamsLib.ngamsCore import \
    genLog, TRACE,\
    rmFile, getNgamsVersion, \
    getFileSize, getDiskSpaceAvail, checkCreatePath,\
    getHostName, ngamsCopyrightString, getNgamsLicense,\
    NGAMS_HTTP_SUCCESS, NGAMS_HTTP_REDIRECT, NGAMS_HTTP_INT_AUTH_USER, NGAMS_HTTP_GET,\
    NGAMS_HTTP_BAD_REQ, NGAMS_HTTP_SERVICE_NA, NGAMS_SUCCESS, NGAMS_FAILURE, NGAMS_OFFLINE_STATE,\
    NGAMS_IDLE_SUBSTATE, NGAMS_BUSY_SUBSTATE, NGAMS_NOTIF_ERROR, NGAMS_TEXT_MT,\
    NGAMS_ARCHIVE_CMD, NGAMS_NOT_SET, NGAMS_XML_STATUS_ROOT_EL,\
    NGAMS_XML_STATUS_DTD, NGAMS_XML_MT, loadPlugInEntryPoint, isoTime2Secs,\
    toiso8601
from ngamsLib import ngamsHighLevelLib, ngamsLib, ngamsEvent, ngamsHttpUtils
from ngamsLib import ngamsDbm, ngamsDb, ngamsConfig, ngamsReqProps
from ngamsLib import ngamsStatus, ngamsHostInfo, ngamsNotification
import ngamsAuthUtils, ngamsCmdHandling, ngamsSrvUtils
import ngamsJanitorThread
import ngamsDataCheckThread
import ngamsUserServiceThread
import ngamsMirroringControlThread
import ngamsCacheControlThread


logger = logging.getLogger(__name__)

class ngamsHttpServer(SocketServer.ThreadingMixIn,
                      BaseHTTPServer.HTTPServer):
    """
    Class that provides the multithreaded HTTP server functionality.
    """
    allow_reuse_address = 1

    def __init__(self, ngamsServer, server_address):
        self._ngamsServer = ngamsServer
        BaseHTTPServer.HTTPServer.__init__(self, server_address, ngamsHttpRequestHandler)

    def process_request(self,
                        request,
                        client_address):
        """
        Start a new thread to process the request.
        """
        # Check the number of requests being handled. It is checked already
        # here to avoid starting another thread.
        noOfAliveThr = 0
        for thrObj in threading.enumerate():
            try:
                if (thrObj.isAlive()): noOfAliveThr += 1
            except Exception:
                pass

        if ((noOfAliveThr - 4) >= self._ngamsServer.getCfg().getMaxSimReqs()):
            try:
                errMsg = genLog("NGAMS_ER_MAX_REQ_EXCEEDED",
                            [self._ngamsServer.getCfg().getMaxSimReqs()])
                logger.error(errMsg)
                httpRef = self.RequestHandlerClass(request, client_address, self)
                tmpReqPropsObj = ngamsReqProps.ngamsReqProps()
                self._ngamsServer.reply(tmpReqPropsObj, httpRef, NGAMS_HTTP_SERVICE_NA,
                               NGAMS_FAILURE, errMsg)
            except IOError:
                errMsg = "Maximum number of requests exceeded and I/O ERROR encountered! Trying to continue...."
                logger.error(errMsg)
            return

        # Create a new thread to handle the request.
        t = threading.Thread(target = self.finish_request,
                             args = (request, client_address))
        t.daemon = True
        t.start()


class ngamsHttpRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    """
    Class used to handle an HTTP request.
    """

    def setup(self):
        BaseHTTPServer.BaseHTTPRequestHandler.setup(self)

        self.ngasServer = self.server._ngamsServer

        # Set the request timeout to the value given in the server configuration
        # or default to 1 minute (apache defaults to 1 minute so I assume it's
        # a sensible value)
        cfg = self.ngasServer.getCfg()
        timeout = cfg.getTimeOut()
        if timeout is None:
            timeout = 60
        self.connection.settimeout(timeout)

    def log_message(self, fmt, *args):
        """
        The default log_request is not safe (it blocks) under heavy load.
        I suggest using a Queue and another thread to read from the queue
        and write it to disk as a possible solution here. A pass works for
        now, but you get no logging.

        Comment this method out to enable (unsafe) logging.

        Returns:    Void.
        """
        pass

    def reqHandle(self):
        """
        Basic, generic request handler to handle an incoming HTTP request.

        Returns:    Void.
        """
        path = self.path.strip("?/ ")
        try:
            self.ngasServer.reqCallBack(self, self.client_address, self.command, path,
                         self.request_version, self.headers,
                         self.wfile, self.rfile)
        except socket.error:
            # BaseHTTPRequestHandler.handle does wfile.flush() after this method
            # returns. If there is a problem with the connection to the client
            # there would be further exceptions because of this, which are
            # meaningless at this point, so we don't want to know about them (or
            # at least not print them).
            # Our finish() method is already too late in the chain to catch this
            # to-be exceptions, so instead we avoid them by emptying the buffer
            # with our little trick here.
            self.wfile._wbuf = []
            self.wfile._wbuf_len = 0
        except Exception:
            logger.exception("Error while handling request", extra={'to_syslog': True})
            raise

    # The three methods we support
    do_GET  = reqHandle
    do_POST = reqHandle
    do_PUT  = reqHandle


class sizeaware_socketfile(object):
    """
    Small utility class that wraps a file object created via socket.makefile()
    and reads only a maximum amount of bytes out of it. If more are requested,
    an empty byte string is returned, thus signaling and EOF.
    """

    def __init__(self, f, size):
        self.f = f
        self.size = size
        self.readin = 0

    def read(self, n):
        if self.readin >= self.size:
            return b''
        left = self.size - self.readin
        buf = self.f.read(n if left >= n else left)
        self.readin += len(buf)
        return buf

    def __len__(self):
        return self.size

class logging_config(object):
    def __init__(self, stdout_level, file_level, logfile, logfile_rot_interval,
                 syslog, syslog_prefix, syslog_address):
        self.stdout_level = stdout_level
        self.file_level = file_level
        self.logfile = logfile
        self.logfile_rot_interval = logfile_rot_interval
        self.syslog = syslog
        self.syslog_prefix = syslog_prefix
        self.syslog_address = syslog_address


from logging.handlers import BaseRotatingHandler
class NgasRotatingFileHandler(BaseRotatingHandler):
    """
    Logging handler that rotates periodically the NGAS logfile into
    LOG-ROTATE-<date>.nglog.unsaved.
    These rotated files are later on picked up by the Janitor Thread,
    archived into this server, and re-renamed into LOG-ROTATE-<date>.nglog.
    At close() time it also makes sure the current logfile is also rotated,
    whatever its size.

    This class is basically a strip-down version of TimedRotatingFileHandler,
    without all the complexities of different when/interval combinations, etc.
    """

    def __init__(self, fname, interval):
        BaseRotatingHandler.__init__(self, fname, mode='a')
        self.interval = interval
        self.rolloverAt = self.interval + time.time()
        pass

    def shouldRollover(self, record):
        return time.time() >= self.rolloverAt

    def _rollover(self):
        if not os.path.exists(self.baseFilename):
            return

        if self.stream:
            self.stream.close()

        # It's time to rotate the current Local Log File.
        dirname = os.path.dirname(self.baseFilename)
        rotated_name = "LOG-ROTATE-%s.nglog.unsaved" % (toiso8601(),)
        rotated_name = os.path.normpath(os.path.join(dirname, rotated_name))
        shutil.move(self.baseFilename, rotated_name)

    def doRollover(self):
        self._rollover()
        self.stream = self._open()
        self.rolloverAt = time.time() + self.interval

    def close(self):
        logging.handlers.BaseRotatingHandler.close(self)
        self.stream = None
        self.acquire()
        try:
            self._rollover()
        finally:
            self.release()


def show_threads():
    """
    Log the name, ident and daemon flag of all alive threads in DEBUG level
    """
    if logger.isEnabledFor(logging.DEBUG):

        all_threads = threading.enumerate()
        max_name  = reduce(max, map(len, [t.name for t in all_threads]))
        max_ident = reduce(max, map(int, map(math.ceil, map(math.log10, [t.ident for t in all_threads]))))

        msg = ['Name' + ' '*(max_name-2) + 'Ident' + ' '*(max_ident-3) + 'Daemon',
               '='*max_name + '  ' + '=' * max_ident + '  ======']
        fmt = '%{0}.{0}s  %{1}d  %d'.format(max_name, max_ident)
        for t in threading.enumerate():
            msg.append(fmt % (t.name, t.ident, t.daemon))
        logger.debug("Threads currently alive on process %d:\n%s", os.getpid(), '\n'.join(msg))

class ngamsServer:
    """
    Class providing the functionality of the NG/AMS Server.
    """

    def __init__(self):
        """
        Constructor method.
        """
        self._serverName              = "ngamsServer"
        self.__ngamsCfg               = ""
        self.__ngamsCfgObj            = ngamsConfig.ngamsConfig()
        self.__dbCfgId                = ""
        self.__force                  = 0
        self.__autoOnline             = 0
        self.__noAutoExit             = 0
        self.__multipleSrvs           = 0
        self.__ngasDb                 = None
        self.__diskDic                = None
        self.__dynCmdDic              = {}
        self.__mimeType2PlugIn        = {}
        self.__state                  = NGAMS_OFFLINE_STATE
        self.__subState               = NGAMS_IDLE_SUBSTATE
        self.__stateSem               = threading.Semaphore(1)
        self.__subStateSem            = threading.Semaphore(1)
        self.__busyCount              = 0
        self.__sysMtPtDic             = {}
        self._pid_file_created         = False

        # Empty logging configuration.
        # It is later initialised both from the cmdline
        # and from the configuration file
        self.logcfg = logging_config(None, None, None, None, None, None, None)

        # Server list handling.
        self.__srvListDic             = {}

        self.__httpDaemon             = None

        self.__handling_exit          = False

        # General flag to control thread execution.
        self._threadRunPermission     = 0

        # Handling of the Janitor Thread.
        self._janitorThread         = None
        self._janitorThreadStopEvt  = threading.Event()
        self._janitorThreadRunCount = 0
        self._janitordbChangeSync = ngamsEvent.ngamsEvent()

        # Handling of the Janitor Queue reader Thread.
        self._janitorQueThread         = None

        # Handling of the Janitor // Processs
        self._janitorProcStopEvt     = multiprocessing.Event()

        # Handling of the Data Check Thread.
        self._dataCheckThread        = None
        self._dataCheckThreadStopEvt = threading.Event()

        # Handling of the Data Subscription.
        self._subscriberDic           = {}
        self._subscriptionThread      = None
        self._subscriptionSem         = threading.Semaphore(1)
        self._backLogAreaSem          = threading.Semaphore(1)
        self._subscriptionRunSync     = threading.Event()
        self._subscriptionFileList    = []
        self._subscriptionSubscrList  = []
        self._subscriptionStopSync    = threading.Event()
        self._subscriptionStopSyncConf= threading.Event()
        self._deliveryStopSync        = threading.Event()
        self._subscrBackLogCount      = 0
        self._subscrScheduledStatus   = {}
        self._subscrCheckedStatus     = {}
        self._subscrQueueDic          = {}
        self._subscrDeliveryThreadDic = {}
        self._subscrDeliveryThreadDicRef = {}
        self._subscrDeliveryFileDic   = {}
        self._subscrSuspendDic        = {}
        self._subscrFileCountDic      = {}
        self._subscrFileCountDic_Sem  = threading.Semaphore(1)
        self._subscrBlScheduledDic    = {}
        self._subscrBlScheduledDic_Sem = threading.Semaphore(1)
        self._subscrBackLogCount_Sem  = threading.Semaphore(1)

        # List to keep track off to which Data Providers an NG/AMS
        # Server is subscribed.
        self._subscriptionStatusList  = []

        # Handling of the Mirroring Control Thread.
        self._mirControlThreadStopEvt = threading.Event()
        self._mirControlThread        = None
        self.__mirControlTrigger      = threading.Event()
        self._pauseMirThreads         = False
        self._mirThreadsPauseCount    = 0
        self.mirroring_running        = False

        # - Mirroring Queue DBM.
        self._mirQueueDbm = None
        self._mirQueueDbmSem = threading.Semaphore(1)
        # - Error Queue DBM.
        self._errQueueDbm = None
        self._errQueueDbmSem = threading.Semaphore(1)
        # - Completed Queue DBM.
        self._complQueueDbm = None
        self._complQueueDbmSem = threading.Semaphore(1)
        # - Source Archive Info DBM.
        self._srcArchInfoDbm = None
        self._srcArchInfoDbmSem = threading.Semaphore(1)

        # Handling of User Service Plug-In.
        self._userServiceThread  = None
        self._userServiceStopEvt = threading.Event()

        # Handling of host info in ngas_hosts.
        self.__hostInfo               = ngamsHostInfo.ngamsHostInfo()

        # To indicate in the code where certain statments that could
        # influence the execution of the test should not be executed.
        ######self.__unitTest               = 0

        # Handling of Host Suspension.
        self.__lastReqStartTime         = 0.0
        self.__lastReqEndTime           = 0.0
        self.__nextDataCheckTime        = 0

        # Dictionary to keep track of the various requests being handled.
        self.__requestDbm               = None
        self.__requestDbmSem            = threading.Semaphore(1)
        self.__requestId                = 0

        # Handling of a Cache Archive.
        self._cacheArchive              = False
        self._cacheControlThread        = None
        self._cacheControlThreadStopEvt = threading.Event()

        # - Cache Contents SQLite DBMS.
        self._cacheContDbms             = None
        self._cacheContDbmsCur          = None
        self._cacheContDbmsSem          = threading.Semaphore(1)
        self._cacheNewFilesDbm          = None
        self._cacheNewFilesDbmSem       = threading.Semaphore(1)
        self._cacheCtrlPiDbm            = None
        self._cacheCtrlPiDelDbm         = None
        self._cacheCtrlPiFilesDbm       = None
        self._cacheCtrlPiThreadGr       = None
        self._dataMoverOnly             = False

        # The listening end
        self.ipAddress = None
        self.portNo    = None

        # Defined as <hostname>:<port>
        self.host_id   = None

    def getHostId(self):
        """
        Returns the NG/AMS Host ID for this server.
        It has the form <hostname>:<port>
        """
        return self.host_id

    def setup_logging(self):
        """
        Sets up logging for the process this ngams server is running on
        """

        logcfg = self.logcfg
        log_to_stdout = logcfg.stdout_level > 0
        log_to_file = logcfg.file_level > 0 and logcfg.logfile

        # 0 means off, but from now on we use the values for indexing
        stdout_level = logcfg.stdout_level - 1
        file_level = logcfg.file_level - 1

        levels = [logging.CRITICAL, logging.ERROR, logging.WARN, logging.INFO, logging.DEBUG]
        root_level = levels[max(stdout_level, file_level, 0)]
        stdout_level = levels[max(stdout_level, 0)]
        file_level = levels[max(file_level, 0)]

        # Remove all currently present handlers from the root logger
        # just in case somebody logged something before now
        for h in list(logging.root.handlers):
            logging.root.removeHandler(h)

        # If we cannot setup a syslog handler we log this
        # (after setting up all the logging)
        syslog_setup_failed = False
        if logcfg.syslog:
            from logging.handlers import SysLogHandler
            prefix = '%s: ' % logcfg.syslog_prefix if logcfg.syslog_prefix else ''
            fmt = '{0}[%(levelname)6.6s] %(message)s'.format(prefix)
            fmt = logging.Formatter(fmt)

            # User-given or default depending on the platform
            syslog_addr = logcfg.syslog_address
            if not syslog_addr:
                syslog_addr = '/dev/log'
                if sys.platform == 'darwin':
                    syslog_addr = '/var/run/syslog'

            try:
                hnd = SysLogHandler(address=syslog_addr)
                hnd.setFormatter(fmt)

                class to_syslog_filter(logging.Filter):
                    def filter(self, record):
                        return hasattr(record, 'to_syslog') and record.to_syslog
                hnd.addFilter(to_syslog_filter())
                logging.root.addHandler(hnd)
            except socket.error:
                syslog_setup_failed = True

        # We use the same format for both file and stdout
        fmt = '%(asctime)-15s.%(msecs)03d [%(threadName)10.10s] [%(levelname)6.6s] %(name)s#%(funcName)s:%(lineno)s %(message)s'
        datefmt = '%Y-%m-%dT%H:%M:%S'
        formatter = logging.Formatter(fmt, datefmt=datefmt)
        formatter.converter = time.gmtime

        if log_to_file:
            hnd = NgasRotatingFileHandler(logcfg.logfile, logcfg.logfile_rot_interval)
            hnd.setLevel(file_level)
            hnd.setFormatter(formatter)
            logging.root.addHandler(hnd)

        if log_to_stdout:
            hnd = logging.StreamHandler(sys.stdout)
            hnd.setFormatter(formatter)
            hnd.setLevel(stdout_level)
            logging.root.addHandler(hnd)

        logging.root.setLevel(root_level)

        # Our first potential logging statement
        if syslog_setup_failed:
            logger.warning("Syslog handler setup failed, no syslog messages will arrive")


    def getDb(self):
        """
        Get reference to the DB connection object.

        Returns:    Reference to DB connection object (ngamsDb).
        """
        return self.__ngasDb


    def getCachingActive(self):
        """
        Return the value of the Caching Active Flag.

        Returns:  State of Caching Active Flag (boolean).
        """
        T = TRACE()

        return self._cacheArchive

    def getDataMoverOnlyActive(self):
        """
        Return the value of the Data Mover Only Flag.

        Returns:  State of the Data Mover Only Flag (boolean).
        """
        T = TRACE()
        return self._dataMoverOnly


    def getReqDbName(self):
        """
        Get the name of the Request Info DB (BSD DB).

        Returns:    Filename of Request Info DB (string).
        """
        ngasId = self.getHostId()
        cacheDir = ngamsHighLevelLib.genCacheDirName(self.getCfg())
        return os.path.normpath(cacheDir + "/" + ngasId + "_REQUEST_INFO_DB")


    def addRequest(self,
                   reqPropsObj):
        """
        Add a request handling object in the Request List (NG/AMS Request
        Properties Object).

        reqPropsObj:     Instance of the request properties object
                         (ngamsReqProps).

        Returns:         Request ID (integer).
        """
        T = TRACE()

        try:
            self.__requestDbmSem.acquire()
            self.__requestId += 1
            if (self.__requestId >= 1000000): self.__requestId = 1
            reqPropsObj.setRequestId(self.__requestId)
            reqObj = reqPropsObj.clone().setReadFd(None).setWriteFd(None)
            self.__requestDbm.add(str(self.__requestId), reqObj)
            self.__requestDbm.sync()
            self.__requestDbmSem.release()
            return self.__requestId
        except Exception, e:
            self.__requestDbmSem.release()
            raise e

    def recoveryRequestDb(self):
        """
        Remove and recreate the request bsddb
        """
        reqDbmName = self.getReqDbName()
        rmFile(reqDbmName + "*")
        self.__requestDbm = ngamsDbm.ngamsDbm(reqDbmName, cleanUpOnDestr = 0,
                                              writePerm = 1)
        logger.debug("Recovered (Checked/created) NG/AMS Request Info DB")

    def updateRequestDb(self,
                        reqPropsObj):
        """
        Update an existing Request Properties Object in the Request DB.

        reqPropsObj: Instance of the request properties object (ngamsReqProps).

        Returns:     Reference to object itself.
        """
        T = TRACE()

        try:
            self.__requestDbmSem.acquire()
            reqId = reqPropsObj.getRequestId()
            reqObj = reqPropsObj.clone().setReadFd(None).setWriteFd(None)
            self.__requestDbm.add(str(reqId), reqObj)
            self.__requestDbm.sync()
            self.__requestDbmSem.release()
            return self
        except ngamsDbm.DbRunRecoveryError:
            self.recoveryRequestDb() # this will ensure next time the same error will not appear again, but this time, it will still throw
            self.__requestDbmSem.release()
            raise


    def getRequest(self,
                   requestId):
        """
        Return the request handle object (ngamsReqProps) for a given
        request. If request not contained in the list, None is returned.

        requestId:     ID allocated to the request (string).

        Returns:       NG/AMS Request Properties Object or None
                       (ngamsReqProps|None).
        """
        try:
            self.__requestDbmSem.acquire()
            if (self.__requestDbm.hasKey(str(requestId))):
                retVal = self.__requestDbm.get(str(requestId))
            else:
                retVal = None
            self.__requestDbmSem.release()
            return retVal
        except ngamsDbm.DbRunRecoveryError:
            self.recoveryRequestDb()
            self.__requestDbmSem.release()
            raise


    def delRequests(self, requestIds):
        """
        Delete the Request Properties Object associated to the given
        Request ID.

        requestId:     ID allocated to the request (string).

        Returns:       Reference to object itself.
        """
        with self.__requestDbmSem:
            try:
                for req_id in requestIds:
                    if self.__requestDbm.hasKey(str(req_id)):
                        self.__requestDbm.rem(str(req_id))
                self.__requestDbm.sync()
            except ngamsDbm.DbRunRecoveryError:
                self.recoveryRequestDb()
                raise


    def takeStateSem(self):
        """
        Acquire the State Semaphore to request for permission to change it.

        Returns:    Void.
        """
        self.__stateSem.acquire()


    def relStateSem(self):
        """
        Release the State Semaphore acquired with takeStateSem().

        Returns:    Void.
        """
        self.__stateSem.release()


    def takeSubStateSem(self):
        """
        Acquire the Sub-State Semaphore to request for permission to change it.

        Returns:    Void.
        """
        self.__subStateSem.acquire()


    def relSubStateSem(self):
        """
        Release the Sub-State Semaphore acquired with takeStateSem().

        Returns:    Void.
        """
        self.__subStateSem.release()


    def setState(self,
                 state,
                 updateDb = True):
        """
        Set the State of NG/AMS.

        state:      State of NG/AMS (see ngams) (string).

        updateDb:   Update the state in the DB (boolean).

        Returns:    Reference to object itself.
        """
        self.__state = state
        self.updateHostInfo(None, None, None, None, None, None, None,
                            state, updateDb)
        return self


    def getState(self):
        """
        Get the NG/AMS State.

        Returns:    State of NG/AMS (string).
        """
        return self.__state


    def setSubState(self,
                    subState):
        """
        Set the Sub-State of NG/AMS.

        subState:   Sub-State of NG/AMS (see ngams) (string).

        Returns:    Reference to object itself.
        """
        # TODO: Change the handling of the Sub-State: Use
        #       ngamsServer.getHandlingCmd() and set the Sub-State
        #       to busy if 1 other idle. Remove ngamsServer.__busyCount.
        self.takeSubStateSem()
        if (subState == NGAMS_BUSY_SUBSTATE):
            self.__busyCount = self.__busyCount + 1
        if ((subState == NGAMS_IDLE_SUBSTATE) and (self.__busyCount > 0)):
            self.__busyCount = self.__busyCount - 1
        if ((subState == NGAMS_IDLE_SUBSTATE) and (self.__busyCount == 0)):
            self.__subState = NGAMS_IDLE_SUBSTATE
        else:
            self.__subState = NGAMS_BUSY_SUBSTATE
        self.relSubStateSem()
        return self


    def getSubState(self):
        """
        Get the Sub-State of NG/AMS.

        Returns:    Sub-State of NG/AMS (string).
        """
        return self.__subState


    def checkSetState(self,
                      action,
                      allowedStates,
                      allowedSubStates,
                      newState = "",
                      newSubState = "",
                      updateDb = True):
        """
        Check and set the State and Sub-State if allowed. The method checks
        if it is allowed to set the State/Sub-State, by checking the list of
        the allowed States/Sub-States against the present State/Sub-State. If
        not, an exception is raised. Otherwise if a new State/Sub-State are
        defined this/these will become the new State/Sub-State of the system.

        action:            Action for which the state change is
                           needed (string).

        allowedStates:     Tuple containing allowed States for
                           executing the action (tuple).

        allowedSubStates:  Tuple containing allowed Sub-States for
                           executing the action (tuple).

        newState:          If specified this will become the new State
                           of NG/AMS if state conditions allows this (string).

        newSubState:       If specified this will become the new Sub-State
                           of NG/AMS if state conditions allows this (string).

        updateDb:          Update the state in the DB (boolean).

        Returns:           Void.
        """
        T = TRACE()

        self.takeStateSem()
        if ( self.getState() not in allowedStates or
             self.getSubState() not in allowedSubStates):
            errMsg = [action, self.getState(), self.getSubState(),
                      str(allowedStates), str(allowedSubStates)]
            errMsg = genLog("NGAMS_ER_IMPROPER_STATE", errMsg)
            self.relStateSem()
            logger.error(errMsg)
            raise Exception, errMsg

        if (newState != ""): self.setState(newState, updateDb)
        if (newSubState != ""): self.setSubState(newSubState)
        self.relStateSem()


    def setThreadRunPermission(self,
                               permission):
        """
        Set the Thread Run Permission Flag. A value of 1 means that
        the threads are allowed to run.

        permission:    Thread Run Permission flag (integer/0|1).

        Returns:       Reference to object itself.
        """
        self._threadRunPermission = permission
        return self


    def getThreadRunPermission(self):
        """
        Return the Thread Run Permission Flag.

        Returns:       Thread Run Permission flag (integer/0|1).
        """
        return self._threadRunPermission


    def startJanitorThread(self):
        """
        Starts the Janitor Thread.
        """
        logger.debug("Starting Janitor Thread ...")

        # Create the child process and kick it off
        self._serv_to_jan_queue = multiprocessing.Queue()
        self._jan_to_serv_queue = multiprocessing.Queue()
        self._janitorThread = multiprocessing.Process(
                                target=ngamsJanitorThread.janitorThread,
                                name="Janitor",
                                args=(self, self._janitorProcStopEvt, self._serv_to_jan_queue, self._jan_to_serv_queue))
        self._janitorThread.start()

        # Re-create the DB connections
        self.reconnect_to_db()

        # Subscribe to db-change events (which we pass down to the janitor proc)
        self.getDb().addDbChangeEvt(self._janitordbChangeSync)
        logger.info("Janitor Thread started")

        # Kick off the thread that takes care of communicating back and forth
        self._janitorQueThread = threading.Thread(target=self.janitorQueThread,
                                              name="JanitorQueReaderThread")
        self._janitorQueThread.start()
        logger.info("Janitor Queue Reader Thread started")


    def stopJanitorThread(self):
        """
        Stops the Janitor Thread.
        """

        if self._janitorThread is None:
            logger.debug("No janitor process to stop")
            return

        code = self._janitorThread.exitcode
        if code is not None:
            logger.warning("Janitor process already exited with code %d"  % (code,))

        # Set the event regardless, because our own thread also uses it
        self._janitorProcStopEvt.set()
        if not code:
            logger.debug("Stopping Janitor Thread ...")
            self._janitorThread.join(10)
            code = self._janitorThread.exitcode
            if code is None:
                logger.warning("Janitor process didn't exit cleanly, killing it")
                os.kill(self._janitorThread.pid, signal.SIGKILL)
        self._janitorThread = None
        self._janitorThreadRunCount = 0
        logger.info("Janitor Thread stopped")

        self._janitorQueThread.join(10)
        if self._janitorQueThread.is_alive():
            logger.error("Janitor queue thread is still alive")
        self._janitorQueThread = None
        logger.info("Janitor Queue thread stopped")


    def janitorQueThread(self):
        """
        This method runs in a separate thread, and implements the protocol
        which this parent process uses to communicate with the janitor process

        The protocol (so far) is simple:

         * Keep reading anything the child process sends to us. If there's
           nothing to be read keep trying until there is something.
         * If something is read, do something with it, and also check if
           a reply should be sent (and send it if required).
         * Continue like this until the janitor process is signaled to stop.
        """

        while not self._janitorProcStopEvt.is_set():

            # Reading on our end
            try:
                x = self._jan_to_serv_queue.get(timeout=0.01)
            except Queue.Empty:
                continue

            # See what we got
            inspect_db_changes = False
            name, item = x
            if name == 'log-record':
                logger.handle(item)
            elif name == 'janitor-run-count':
                # Get the thread count and send back
                # the information from the dbChangeEvent
                self._janitorThreadRunCount = item
                inspect_db_changes = True
            elif name == 'delete-requests':
                self.delRequests(item)
            else:
                raise ValueError("Unknown item in queue: name=%s, item=%r" % (name,item))

            # Writing on our end if needed
            x = None
            if inspect_db_changes:
                info = None
                if self._janitordbChangeSync.isSet():
                    info = self._janitordbChangeSync.getEventInfoList()
                    self._janitordbChangeSync.clear()
                x = ('db-change-info', info)

            if x is not None:
                try:
                    self._serv_to_jan_queue.put(x, timeout=0.01)
                except:
                    logger.exception("Problem when writing to the queue")


    def incJanitorThreadRunCount(self):
        """
        Increase the Janitor Thread run count.

        Returns:     Reference to object itself.
        """
        self._janitorThreadRunCount += 1
        return self


    def getJanitorThreadRunCount(self):
        """
        Return the Janitor Thread run count.

        Returns:     Janitor Thread Run Count (integer).
        """
        return self._janitorThreadRunCount


    def startDataCheckThread(self):
        """
        Starts the Data Check Thread.
        """
        if not self.getCfg().getDataCheckActive():
            return

        logger.debug("Starting Data Check Thread ...")
        self._dataCheckThread = threading.Thread(target=ngamsDataCheckThread.dataCheckThread,
                                                 name=ngamsDataCheckThread.NGAMS_DATA_CHECK_THR,
                                                 args=(self, self._dataCheckThreadStopEvt))
        self._dataCheckThread.start()
        logger.info("Data Check Thread started")


    def stopDataCheckThread(self):
        """
        Stop the Data Check Thread.

        srvObj:     Reference to server object (ngamsServer).

        Returns:    Void.
        """
        if not self.getCfg().getDataCheckActive():
            return
        if self._dataCheckThread is None:
            return

        logger.debug("Stopping Data Check Thread ...")
        self._dataCheckThreadStopEvt.set()
        self._dataCheckThread.join(10)
        self._dataCheckThread = None
        logger.info("Data Check Thread stopped")


    def startMirControlThread(self):
        """
        Starts the Mirroring Control Thread.
        """

        if (not self.getCfg().getMirroringActive()):
            logger.info("NGAS Mirroring not active - Mirroring Control Thread not started")
            return

        logger.debug("Starting the Mirroring Control Thread ...")
        self._mirControlThread = threading.Thread(target=ngamsMirroringControlThread.mirControlThread,
                                                  name=ngamsMirroringControlThread.NGAMS_MIR_CONTROL_THR,
                                                  args=(self, self._mirControlThreadStopEvt))
        self._mirControlThread.start()
        logger.info("Mirroring Control Thread started")


    def stopMirControlThread(self):
        """
        Stops the Mirroring Control Thread.
        """
        if self._mirControlThread is None:
            return

        logger.debug("Stopping the Mirroring Service ...")
        self._mirControlThreadStopEvt.set()
        self._mirControlThread.join(10)
        self._mirControlThread = None
        logger.info("Mirroring Control Thread stopped")


    def startUserServiceThread(self):
        """
        Start the User Service Thread.
        """
        # Start only if service is defined.
        cfg_item = "NgamsCfg.SystemPlugIns[1].UserServicePlugIn"
        userServicePlugIn = self.getCfg().getVal(cfg_item)
        logger.debug("User Service Plug-In Defined: %s" % str(userServicePlugIn))
        if not userServicePlugIn:
            return

        logger.info("Loading User Service Plug-In module: %s" % userServicePlugIn)
        userServicePlugIn = loadPlugInEntryPoint(userServicePlugIn)

        logger.debug("Starting User Service Thread ...")
        self._userServiceThread = threading.Thread(target=ngamsUserServiceThread.userServiceThread,
                                                   name=ngamsUserServiceThread.NGAMS_USER_SERVICE_THR,
                                                   args=(self, self._userServiceStopEvt, userServicePlugIn))
        self._userServiceThread.start()
        logger.info("User Service Thread started")


    def stopUserServiceThread(self):
        """
        Stops the User Service Thread.
        """
        if not self._userServiceThread:
            return

        logger.debug("Stopping User Service Thread ...")
        self._userServiceStopEvt.set()
        self._userServiceThread.join(10)
        self._userServiceThread = None
        logger.info("User Service Thread stopped")


    def startCacheControlThread(self):
        """
        Starts the Cache Control Thread.
        """

        if not self.getCachingActive():
            logger.info("NGAS Cache Service not active - will not start Cache Control Thread")
            return

        logger.debug("Starting the Cache Control Thread ...")
        try:
            check_can_be_deleted = int(self.getCfg().getVal("Caching[1].CheckCanBeDeleted"))
        except:
            check_can_be_deleted = 0

        logger.debug("Cache Control - CHECK_CAN_BE_DELETED = %d" % check_can_be_deleted)

        self._cacheControlThread = threading.Thread(target=ngamsCacheControlThread.cacheControlThread,
                                                      name=ngamsCacheControlThread.NGAMS_CACHE_CONTROL_THR,
                                                      args=(self, self._cacheControlThreadStopEvt, check_can_be_deleted))
        self._cacheControlThread.start()
        logger.info("Cache Control Thread started")


    def stopCacheControlThread(self):
        """
        Stop the Cache Control Thread.
        """
        if self._cacheControlThread is None:
            return
        logger.debug("Stopping the Cache Control Thread ...")
        self._cacheControlThreadStopEvt.set()
        self._cacheControlThread.join(10)
        self._cacheControlThread = None
        logger.info("Cache Control Thread stopped")


    def triggerSubscriptionThread(self):
        """
        Trigger the Data Subscription Thread so that it carries out a
        check to see if there are file to be delivered to Subscribers.

        Returns:   Reference to object itself.
        """
        logger.info("SubscriptionThread received trigger")
        self._subscriptionRunSync.set()
        return self

    def registerSubscriber(self, subscriberObj):
        """
        register a subscriber object to the server
        set event to true initially, indicating it is NOT suspended
        """
        self._subscriberDic[subscriberObj.getId()] = subscriberObj
        suspendSync = threading.Event()
        suspendSync.set()
        self._subscrSuspendDic[subscriberObj.getId()] = suspendSync

    def addSubscriptionInfo(self,
                            fileRefs = [],
                            subscrObjs = []):
        """
        It is possible to indicate that specific files should be checked
        to see if it should be delivered to Subscribers. This is used when
        a new file has been archived.

        It is also possible to specify that it should be checked for a
        specific Subscriber if there is data to be delivered to this
        specific Subscriber.

        If no file references are given nor Subscriber references, the
        Subscription Thread will make a general check if there are files
        to be delivered.

        fileRefs:     List of tuples of File IDs + File Versions to be checked
                      if they should be delivered to the Subscribers
                      (list/tuple/string).

        subscrObjs:   List of Subscriber Objects indicating that data
                      delivery should be investigated for each Subscriber
                      (list/ngamsSubscriber).

        Returns:      Reference to object itself.
        """
        T = TRACE()

        try:
            self._subscriptionSem.acquire()
            if (fileRefs != []):
                self._subscriptionFileList += fileRefs
            if (subscrObjs != []):
                self._subscriptionSubscrList += subscrObjs
        except Exception:
            logger.exception("Error occurred while adding subscription")
            raise
        finally:
            self._subscriptionSem.release()
        return self


    def decSubcrBackLogCount(self):
        """
        Decrease the Subscription Back-Log Counter.

        Returns:  Current value of the Subscription Back-Log Counter (integer).

        This is NOT thread safe
        """
        #self._subscrBackLogCount_Sem.acquire()
        #try:
        self._subscrBackLogCount -= 1
        #finally:
        #    self._subscrBackLogCount_Sem.release()
        return self._subscrBackLogCount

    def incSubcrBackLogCount(self):
        """
        Increase the Subscription Back-Log Counter.

        Returns:  Current value of the Subscription Back-Log Counter (integer).

        This is NOT thread safe
        """
        #self._subscrBackLogCount_Sem.acquire()
        #try:
        self._subscrBackLogCount += 1
        #finally:
        #self._subscrBackLogCount_Sem.release()
        return self._subscrBackLogCount

    def presetSubcrBackLogCount(self, num):
        """
        Preset the Subscription Back-Log Counter
        during system start-up

        num    :    The number of back-log entries
        Returns:    Reference to object itself.
        """
        self._subscrBackLogCount = num
        return self

    def resetSubcrBackLogCount(self):
        """
        Reset the Subscription Back-Log Counter.

        Returns:    Reference to object itself.
        """
        self._subscrBackLogCount = 0
        return self


    def getSubcrBackLogCount(self):
        """
        Get the value of the Subscription Back-Log Counter.

        Returns:  Current value of the Subscription Back-Log Counter (integer).
        """
        return self._subscrBackLogCount


    def getSubscrStatusList(self):
        """
        Return reference to the Subscription Status List. This list contains
        the reference to the various Data Providers to which a server has
        subscribed.

        Returns:     Reference to Subscription Status List (list).
        """
        return self._subscriptionStatusList


    def resetSubscrStatusList(self):
        """
        Reset the Subscription Status List. This list contains the reference
        to the various Data Providers to which a server has subscribed.

        Returns:     Reference to object itself.
        """
        self._subscriptionStatusList = []
        return self


    def triggerMirThreads(self):
        """
        Set (trigger) the mirroring event to signal to the Mirroring
        Threads, that there is data in the queue to be handled.

        Returns:    Reference to object itself.
        """
        self.__mirControlTrigger.set()
        return self


    def waitMirTrigger(self,
                       timeout = None):
        """
        Wait for the availability of Mirroring Requests in the queue.

        timeout:    Timeout in seconds to max wait for the event (float|None).

        Returns:    Reference to object itself.
        """
        if (timeout):
            self.__mirControlTrigger.wait(timeout)
        else:
            self.__mirControlTrigger.wait()
        self.__mirControlTrigger.clear()
        return self


    def setForce(self,
                 force):
        """
        Set the Force Execution Flag, indicating whether to force
        the server to start even if the PID file is found.

        force:    Force Flag (force = 1) (int)

        Returns:  Reference to object itself.
        """
        self.__force = int(force)
        return self


    def getForce(self):
        """
        Return the Force Execution Flag, indicating whether to force
        the server to start even if the PID file is found.

        Returns:   Force Flag (force = 1) (int)
        """
        return self.__force


    def setLastReqStartTime(self):
        """
        Register start time for handling of last request.

        Returns:      Reference to object itself.
        """
        self.__lastReqStartTime = time.time()
        return self


    def getLastReqStartTime(self):
        """
        Return the start time for last request handling initiated.

        Returns:       Absolute time for starting last request handling
                       (seconds since epoch) (float).
        """
        return self.__lastReqStartTime


    def setLastReqEndTime(self):
        """
        Register end time for handling of last request.

        Returns:      Reference to object itself.
        """
        self.__lastReqEndTime = time.time()
        return self


    def getLastReqEndTime(self):
        """
        Return the end time for last request handling initiated.

        Returns:       Absolute time for end last request handling
                       (seconds since epoch) (float).
        """
        return self.__lastReqEndTime


    def getHandlingCmd(self):
        """
        Identify if the NG/AMS Server is handling a request. In case yes
        1 is returned, otherwise 0 is returned.

        Returns:   1 = handling request, 0 = not handling a request
                   (integer/0|1).
        """
        # If the time for initiating the last request handling is later than
        # the time for finishing the previous request a request is being
        # handled.
        if (self.getLastReqStartTime() > self.getLastReqEndTime()):
            return 1
        else:
            return 0


    def setNextDataCheckTime(self,
                             nextTime):
        """
        Set the absolute time for when the next data check is due (seconds
        since epoch).

        nextTime:    Absolute time in seconds for scheduling the next
                     data check (integer).

        Returns:     Reference to object itself.
        """
        self.__nextDataCheckTime = int(nextTime)
        return self


    def getNextDataCheckTime(self):
        """
        Return the absolute time for when the next data check is due (seconds
        since epoch).

        Returns:  Absolute time in seconds for scheduling the next
                  data check (integer).
        """
        return self.__nextDataCheckTime


    def setAutoOnline(self,
                      autoOnline):
        """
        Set the Auto Online Flag, indicating whether to bring the
        server Online automatically, immediately after initializing.

        autoOnline:    Auto Online Flag (Auto Online = 1) (int)

        Returns:       Reference to object itself.
        """
        self.__autoOnline = int(autoOnline)
        return self


    def getAutoOnline(self):
        """
        Return the Auto Online Flag, indicating whether to bring the
        server Online automatically, immediately after initializing.

        Returns:    Auto Online Flag (Auto Online = 1) (int)
        """
        return self.__autoOnline


    def setNoAutoExit(self,
                      noAutoExit):
        """
        Set the No Auto Exit Flag, indicating whether the server is
        allowed to exit automatically for instance in case of problems
        in connection with going Online automatically (-autoOnline).

        autoOnline:    Auto Online Flag (Auto Online = 1) (int)

        Returns:       Reference to object itself.
        """
        self.__noAutoExit = int(noAutoExit)
        return self


    def getNoAutoExit(self):
        """
        Return the No Auto Exit Flag.

        Returns:    No Auto Exit Flag (No Auto Exit = 1) (int)
        """
        return self.__noAutoExit


    def setMultipleSrvs(self,
                        multipleSrvs):
        """
        Set the Multiple Servers Flag to indicating that several servers
        can be executed on the same node and that the Host ID should be
        composed of hostname and port number.

        multipleSrvs:    Multiple Servers Flag (integer/0|1).

        Returns:         Reference to object itself.
        """
        self.__multipleSrvs = int(multipleSrvs)
        return self


    def getMultipleSrvs(self):
        """
        Get the Multiple Servers Flag to indicating that several servers
        can be executed on the same node and that the Host ID should be
        composed of hostname and port number.

        Returns:    Multiple Servers Flag (integer/0|1).
        """
        return self.__multipleSrvs


    def setCfg(self,
               ngamsCfgObj):
        """
        Set the reference to the configuration object.

        ngamsCfgObj:  Instance of the configuration object (ngamsConfig)

        Returns:      Reference to object itself.
        """
        self.__ngamsCfgObj = ngamsCfgObj
        return self


    def getCfg(self):
        """
        Return reference to object containing the NG/AMS Configuration.

        Returns:    Reference to NG/AMS Configuration (ngamsConfig).
        """
        return self.__ngamsCfgObj


    def getSrvListDic(self):
        """
        Return reference to the Server List Dictionary.

        Returns:    Reference to Server List Dictionary (dictionary).
        """
        return self.__srvListDic


    def setDiskDic(self,
                   diskDic):
        """
        Set the Disk Dictionary of the server object.

        diskDic:    Dick Dictionary (dictionary):

        Returns:    Reference to object itself.
        """
        self.__diskDic = diskDic
        return self


    def getDiskDic(self):
        """
        Get reference to Disk Dictionary.

        Returns:   Disk Dictionary (dictionary)
        """
        return self.__diskDic


    def getDynCmdDic(self):
        """
        Get reference to Dynamic Command Module Dictionary.

        Returns:   Dynamic Command Dictionary (dictionary)
        """
        return self.__dynCmdDic


    def setCfgFilename(self,
                       filename):
        """
        Set the name of the NG/AMS Configuration File.

        filename:     Name of configuration file (string).

        Returns:      Reference to object itself.
        """
        self.__ngamsCfg = filename
        return self


    def getCfgFilename(self):
        """
        Return name of NG/AMS Configuration file.

        Returns:   Name of NG/AMS Configuration file (string).
        """
        return self.__ngamsCfg


    def getMimeTypeDic(self):
        """
        Return reference to the Mime-Type Dictionary.

        Returns:  Reference to Mime-Type Dictionary (dictionary).
        """
        return self.__mimeType2PlugIn


    def getHostInfoObj(self):
        """
        Return reference to internal host info object.

        Returns:   Reference to host info object (ngamsHostInfo).
        """
        return  self.__hostInfo


    def updateHostInfo(self,
                       version,
                       portNo,
                       allowArchiveReq,
                       allowRetrieveReq,
                       allowProcessingReq,
                       allowRemoveReq,
                       dataChecking,
                       state,
                       updateDb = True):
        """
        Update the information about this NG/AMS Server in the NGAS DB
        (ngas_hosts). If a field should not be updated it should be given
        in as None.

        If a connection to the DB is not yet created, the method returns
        without doing anything.

        version:             NG/AMS version (string).

        portNo:              Port number (integer).

        allowArchiveReq:     Allow Archive Requests Flag (integer/0|1).

        allowRetrieveReq:    Allow Retrieve Requests Flag (integer/0|1).

        allowProcessingReq:  Allow Processing Requests Flag (integer/0|1).

        allowRemoveReq:      Allow Remove Requests Flag (integer/0|1).

        dataChecking:        Data Checking Active Flag (integer/0|1).

        state:               State of NG/AMS Server (string).

        updateDb:            Update the state in the DB if True (boolean).

        Returns:             Reference to object itself.
        """
        T = TRACE(5)

        if (self.getDb() == None): return self

        if (version != None): self.getHostInfoObj().setSrvVersion(version)
        if (portNo != None): self.getHostInfoObj().setSrvPort(portNo)
        self.getHostInfoObj().\
                                setSrvArchive(allowArchiveReq).\
                                setSrvRetrieve(allowRetrieveReq).\
                                setSrvProcess(allowProcessingReq).\
                                setSrvRemove(allowRemoveReq).\
                                setSrvDataChecking(dataChecking).\
                                setSrvState(state)
        if (updateDb):
            ngamsHighLevelLib.updateSrvHostInfo(self.getDb(), self.getHostInfoObj())
        return self

    def get_remote_server_endpoint(self, hostId):
        """
        Return the IP address to which this server should connect to to
        contact ngams server `hostId`.
        """

        local_name = getHostName()
        listening_ip = self.getDb().getIpFromHostId(hostId)

        if ':' in hostId:
            remote_name, remote_port = hostId.split(':')
            remote_port = int(remote_port)
        else:
            remote_name = hostId
            remote_port = self.getDb().getPortNoFromHostId(hostId)

        # remote server is not our same machine
        if remote_name != local_name:
            host = listening_ip if listening_ip != '0.0.0.0' else remote_name

        # remote server is in the same machine
        # We always prefer the loopback interface unless it's not opened
        else:
            if listening_ip not in ('127.0.0.1', '0.0.0.0'):
                host = listening_ip
            else:
                host = '127.0.0.1'

        return host, remote_port

    def get_endpoint(self):
        """
        Return an IP address which clients can use to connect to this server.
        """
        ipAddress = self.ipAddress
        ipAddress = ipAddress if ipAddress != '0.0.0.0' else '127.0.0.1'
        return ipAddress, self.portNo

    def getSubscriberDic(self):
        """
        Returns reference to dictionary with Subscriber Objects.

        Returns:   Reference to dictionary with Subscriber info
                   (dictionary/ngamsSubscriber).
        """
        return self._subscriberDic


    def reqCallBack(self,
                    httpRef,
                    clientAddress,
                    method,
                    path,
                    requestVersion,
                    headers,
                    writeFd,
                    readFd):
        """
        Call-back to handle the HTTP request.

        httpRef:         Reference to the HTTP request handler
                         object (ngamsHttpRequestHandler).

        clientAddress:   Address of client (string).

        method:          HTTP method (string).

        path:            Path of HTTP request (URL) (string).

        requestVersion:  HTTP version (string).

        headers:         HTTP headers (dictionary).

        writeFd:         File object used to write data back to
                         client (file object).

        readFd:          File object used to read data from
                         client (file object).

        Returns:         Void.
        """
        T = TRACE()

        # Create new request handle + add this entry in the Request DB.
        reqPropsObj = ngamsReqProps.ngamsReqProps()
        self.addRequest(reqPropsObj)

        # Handle read/write FD.
        reqPropsObj.setReadFd(readFd).setWriteFd(writeFd)

        # Handle the request.
        try:
            self.handleHttpRequest(reqPropsObj, httpRef, clientAddress,
                                   method, path, requestVersion, headers)

            if not reqPropsObj.getSentReply():
                msg = "Successfully handled request"
                self.reply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS,
                           NGAMS_SUCCESS, msg)

            reqPropsObj.getWriteFd().flush()

        except Exception, e:

            # Quickly respond with a 400 status code for unexpected exceptions
            # (although it should be a 5xx code)
            # Before we were consuming the whole input stream here before
            # sending the response which wasted resources unnecessarily
            errMsg = str(e)
            if logger.level <= logging.DEBUG:
                logger.exception("Error while serving request")
            else:
                logger.error(errMsg)

            self.setSubState(NGAMS_IDLE_SUBSTATE)

            # If we fail because of a socket error there is no point on trying
            # to write the response anymore, simply bail out
            if isinstance(e, socket.error):
                raise

            # Send a response if one hasn't been send yet. Use a shorter timeout
            # if possible to avoid hanging out in here
            if not reqPropsObj.getSentReply():
                timeout = min((httpRef.wfile._sock.gettimeout(), 20))
                httpRef.wfile._sock.settimeout(timeout)
                self.reply(reqPropsObj, httpRef, NGAMS_HTTP_BAD_REQ,
                           NGAMS_FAILURE, errMsg)
        finally:
            reqPropsObj.setCompletionTime(1)
            self.updateRequestDb(reqPropsObj)
            self.setLastReqEndTime()

    def handleHttpRequest(self,
                          reqPropsObj,
                          httpRef,
                          clientAddress,
                          method,
                          path,
                          requestVersion,
                          headers):
        """
        Handle the HTTP request.

        reqPropsObj:     Request Property object to keep track of actions done
                         during the request handling (ngamsReqProps).

        httpRef:         Reference to the HTTP request handler
                         object (ngamsHttpRequestHandler).

        clientAddress:   Address of client (string).

        method:          HTTP method (string).

        path:            Path of HTTP request (URL) (string).

        requestVersion:  HTTP version (string).

        headers:         HTTP headers (dictionary).

        Returns:         Void.
        """

        # Handle the command.
        self.setLastReqStartTime()
        req_start = time.time()
        safePath = ngamsLib.hidePassword(path)
        msg = "Handling HTTP request: client_address=%s - method=%s - path=|%s|"
        logger.info(msg, str(clientAddress), method, safePath)

        reqPropsObj.unpackHttpInfo(self.getCfg(), method, path, headers)

        ngamsAuthUtils.authorize(self, reqPropsObj, httpRef)

        ngamsCmdHandling.cmdHandler(self, reqPropsObj, httpRef)

        msg = "Total time for handling request: (%s, %s ,%s, %s): %.3f [s]"
        args = [reqPropsObj.getHttpMethod(), reqPropsObj.getCmd(),
                reqPropsObj.getMimeType(), reqPropsObj.getFileUri(),
                time.time() - req_start]

        if reqPropsObj.getIoTime() > 0:
            msg += "; Transfer rate: %s MB/s"
            args += [str(reqPropsObj.getBytesReceived() / reqPropsObj.getIoTime() / 1024.0 / 1024.0)]

        logger.info(msg, *args)


    def httpReplyGen(self,
                     reqPropsObj,
                     httpRef,
                     code,
                     dataRef = None,
                     dataInFile = 0,
                     contentType = None,
                     contentLength = 0,
                     addHttpHdrs = [],
                     closeWrFo = 0):
        """
        Generate a standard HTTP reply.

        reqPropsObj:   Request Property object to keep track of actions done
                       during the request handling (ngamsReqProps).

        httpRef:       Reference to the HTTP request handler
                       object (ngamsHttpRequestHandler).

        code:          HTTP status code (integer)

        dataRef:       Data to send with the HTTP reply (string).

        dataInFile:    Data stored in a file (integer).

        contentType:   Content type (mime-type) of the data (string).

        contentLength: Length of the message. The actually message should
                       be send from the calling method (integer).

        addHttpHdrs:   List containing sub-lists with additional
                       HTTP headers to send. Format is:

                         [[<HTTP hdr>, <val>, ...]         (list)

        closeWrFo:     If set to 1, the HTTP write file object will be closed
                       by the function (integer/0|1).

        Returns:       Void.
        """
        T = TRACE()

        logger.debug("httpReplyGen(). Generating HTTP reply to: %s" \
                % str(httpRef.client_address))

        if reqPropsObj.getSentReply():
            logger.debug("Reply already sent for this request")
            return
        try:
            message = ''
            if BaseHTTPServer.BaseHTTPRequestHandler.responses.has_key(code):
                message = BaseHTTPServer.BaseHTTPRequestHandler.responses[code][0]

            protocol = BaseHTTPServer.BaseHTTPRequestHandler.protocol_version
            httpRef.wfile.write("%s %s %s\r\n" % (protocol, str(code), message))
            srvInfo = "NGAMS/%s" % getNgamsVersion()
            logger.debug("Sending header: Server: %s", srvInfo)
            httpRef.send_header("Server", srvInfo)
            httpTimeStamp = ngamsHttpUtils.httpTimeStamp()
            logger.debug("Sending header: Date: %s", httpTimeStamp)
            httpRef.send_header("Date", httpTimeStamp)
            # Expires HTTP reponse header field, e.g.:
            # Expires: Mon, 17 Sep 2001 09:21:38 GMT
            logger.debug("Sending header: Expires: %s", httpTimeStamp)
            httpRef.send_header("Expires", httpTimeStamp)

            if dataRef == None:
                dataSize = 0
            elif dataRef != None and dataInFile:
                dataSize = getFileSize(dataRef)
            elif dataRef != None:
                if len(dataRef) and not contentLength:
                    dataSize = len(dataRef)
                else:
                    dataSize = contentLength

            # Send additional headers if any.
            sentContDisp = 0
            for hdrInfo in addHttpHdrs:
                if hdrInfo[0] == "Content-Disposition":
                    sentContDisp = 1
                logger.debug("Sending header: %s:%s", hdrInfo[0], hdrInfo[1])
                httpRef.send_header(hdrInfo[0], hdrInfo[1])
            if contentType != None:
                logger.debug("Sending header: Content-Type: %s", contentType)
                httpRef.send_header("Content-Type", contentType)
            if dataRef != None:
                logger.debug("Sending header: Content-Length/1: %s", str(dataSize))
                httpRef.send_header("Content-Length", dataSize)
                if dataInFile:
                    if not sentContDisp:
                        contDisp = "attachment; filename=%s" % os.path.basename(dataRef)
                        logger.debug("Sending header: Content-Disposition: %s", contDisp)
                        httpRef.send_header("Content-Disposition", contDisp)
                    httpRef.wfile.write("\n")

                    with open(dataRef, "r") as fo:
                        dataSent = 0
                        while (dataSent < dataSize):
                            tmpData = fo.read(65536)
                            if not tmpData:
                                raise Exception('read EOF')
                            httpRef.wfile.write(tmpData)
                            dataSent += len(tmpData)
                else:
                    httpRef.wfile.write("\n%s" % dataRef)
                    if logger.level <= logging.DEBUG:
                        logger.debug("Message sent with HTTP reply=|%s|", str(dataRef).replace("\n", ""))
            elif contentLength != 0:
                logger.debug("Sending header: Content-Length/2: %s", str(contentLength))
                httpRef.send_header("Content-Length", contentLength)

        except Exception:
            errMsg = "Error occurred while sending reply to: %s" % (str(httpRef.client_address),)
            logger.exception(errMsg)
        finally:
            reqPropsObj.setSentReply(1)
            httpRef.wfile.flush()
            if closeWrFo == 1:
                httpRef.wfile.close()

        logger.debug("Generated HTTP reply to: %s" % str(httpRef.client_address))


    def httpReply(self,
                  reqPropsObj,
                  httpRef,
                  code,
                  msg = None,
                  contentType = NGAMS_TEXT_MT,
                  addHttpHdrs = []):
        """
        Generate standard HTTP reply.

        reqPropsObj:   Request Property object to keep track of
                       actions done during the request handling
                       (ngamsReqProps).

        httpRef:       Reference to the HTTP request handler
                       object (ngamsHttpRequestHandler).

        code:          HTTP status code (integer)

        msg:           Message to send as data with the HTTP reply (string).

        contentType:   Content type (mime-type) of the msg (string).

        addHttpHdrs:   List containing sub-lists with additional
                       HTTP headers to send. Format is:

                         [[<HTTP hdr>, <val>, ...]         (list)

        Returns:       Void.
        """
        T = TRACE()

        if msg is None: msg = ''

        if (reqPropsObj.getSentReply()):
            logger.info("Reply already sent for this request")
            return
        self.httpReplyGen(reqPropsObj, httpRef, code, msg, 0, contentType,
                          len(msg), addHttpHdrs)
        httpRef.wfile.write("\r\n")
        logger.info("HTTP reply sent to: %s", str(httpRef.client_address))


    def httpRedirReply(self,
                       reqPropsObj,
                       httpRef,
                       redirHost,
                       redirPort):
        """
        Generate an HTTP Redirection Reply and send this back to the
        requestor.

        reqPropsObj:   Request Property object to keep track of actions done
                       during the request handling (ngamsReqProps).

        httpRef:       Reference to the HTTP request handler
                       object (ngamsHttpRequestHandler).

        redirHost:     NGAS host to which to redirect the request (string).

        redirPort:     Port number of the NG/AMS Server to which to redirect
                       the request (integer).

        Returns:       Void.
        """
        T = TRACE()

        pars = ""
        for par in reqPropsObj.getHttpParNames():
            pars += par + "=" + reqPropsObj.getHttpPar(par) + "&"
        pars = pars[0:-1]
        redirectUrl = "http://" + redirHost + ":" + str(redirPort) + "/" +\
                      reqPropsObj.getCmd() + "?" + pars
        msg = genLog("NGAMS_INFO_REDIRECT", [redirectUrl])
        logger.info(msg)
        addHttpHdrs = [["Location", redirectUrl]]
        self.reply(reqPropsObj, httpRef, NGAMS_HTTP_REDIRECT, NGAMS_SUCCESS,
                   msg, addHttpHdrs)


    def forwardRequest(self, reqPropsObj, httpRefOrg,
                       host_id, host, port,
                       autoReply = 1, mimeType = ""):
        """
        Forward an HTTP request to the given host + port and handle the reply
        from the remotely, contacted NGAS node. If the host to contact for
        handling the request is different that the actual target host, the
        proper contact host (e.g. cluster main node) is resolved internally.

        reqPropsObj:    Request Property object to keep track of actions done
                        during the request handling (ngamsReqProps).

        httpRefOrg:     Reference to the HTTP request handler object for
                        the request received from the originator
                        (ngamsHttpRequestHandler).

        forwardHost:    Host ID to where the request should be forwarded
                        (string).

        forwardPort:    Port number of the NG/AMS Server on the remote
                        host (integer).

        autoReply:      Send back reply to originator of the request
                        automatically (integer/0|1).

        mimeType:       Mime-type of possible data to forward (string).

        Returns:        Tuple with the following information:

                          (<HTTP Status>, <HTTP Status Msg>, <HTTP Hdrs>,
                           <Data>)  (tuple).
        """

        pars = []
        for par in reqPropsObj.getHttpParNames():
            pars.append((par, reqPropsObj.getHttpPar(par)))

        if logger.isEnabledFor(logging.INFO):
            msg = "Forwarding %s?%s to %s:%d (corresponding to hostId %s)"
            urlpars = urllib.urlencode(pars, doseq=1)
            logger.info(msg, reqPropsObj.getCmd(), urlpars, host, port, host_id)

        try:
            # If target host is suspended, wake it up.
            if (self.getDb().getSrvSuspended(host_id)):
                ngamsSrvUtils.wakeUpHost(self, host_id)

            # If the NGAS Internal Authorization User is defined generate
            # an internal Authorization Code.
            if (self.getCfg().hasAuthUser(NGAMS_HTTP_INT_AUTH_USER)):
                authHttpHdrVal = self.getCfg().\
                                 getAuthHttpHdrVal(NGAMS_HTTP_INT_AUTH_USER)
            else:
                authHttpHdrVal = ""

            # Make sure the time_out parameters is positive if given; otherwise
            # a sane default
            def_timeout = 300 # 3 [min]
            reqTimeOut = def_timeout
            if 'time_out' in reqPropsObj and reqPropsObj['time_out']:
                reqTimeOut = float(reqPropsObj.getHttpPar("time_out"))
                reqTimeOut = reqTimeOut if reqTimeOut >= 0 else def_timeout

            # Forward GET or POST request.
            if (reqPropsObj.getHttpMethod() == NGAMS_HTTP_GET):
                resp = ngamsHttpUtils.httpGet(host, port, reqPropsObj.getCmd(),
                                       pars=pars, timeout=reqTimeOut,
                                       auth=authHttpHdrVal)
                with contextlib.closing(resp):
                    httpStatCode, httpStatMsg, data = resp.status, resp.reason, resp.read()
                httpHdrs = {h[0]: h[1] for h in resp.getheaders()}
            else:
                # It's a POST request, forward request + possible data.
                contLen = reqPropsObj.getSize()
                if ((reqPropsObj.getCmd() == NGAMS_ARCHIVE_CMD) and
                    (contLen <= 0)):
                    raise Exception, "Must specify a content-length when " +\
                          "forwarding Archive Requests (Archive Proxy Mode)"

                # During HTTP post we need to pass down a EOF-aware,
                # read()-able object
                data = sizeaware_socketfile(reqPropsObj.getReadFd(), contLen)
                httpStatCode, httpStatMsg, httpHdrs, data =\
                            ngamsHttpUtils.httpPost(host, port,
                                                    reqPropsObj.getCmd(),
                                                    data, mimeType,
                                                    pars=pars,
                                                    auth=authHttpHdrVal,
                                                    timeout=reqTimeOut)

            # If auto-reply is selected, the reply from the remote server
            # is send back to the originator of the request.
            if (autoReply):
                tmpReqObj = ngamsReqProps.ngamsReqProps().\
                            unpackHttpInfo(self.getCfg(),
                                           reqPropsObj.getHttpMethod(), "",
                                           httpHdrs)
                mimeType = tmpReqObj.getMimeType()
                if (tmpReqObj.getFileUri()):
                    attachmentName = os.path.basename(tmpReqObj.getFileUri())
                    httpHdrs = [["Content-Disposition",
                                 "attachment; filename=" + attachmentName]]
                else:
                    httpHdrs = []
                self.httpReply(reqPropsObj, httpRefOrg, httpStatCode, data,
                               mimeType, httpHdrs)

            return httpStatCode, httpStatMsg, httpHdrs, data
        except Exception:
            logger.exception("Problem occurred forwarding command %s", reqPropsObj.getCmd())
            raise


    def genStatus(self,
                  status,
                  msg):
        """
        Generate an NG/AMS status object with the basic fields set.

        status:   Status: OK/FAILURE (string).

        msg:      Message for status (string).

        Returns:  Status object (ngamsStatus).
        """
        return ngamsStatus.ngamsStatus().\
               setDate(toiso8601()).\
               setVersion(getNgamsVersion()).setHostId(self.getHostId()).\
               setStatus(status).setMessage(msg).setState(self.getState()).\
               setSubState(self.getSubState())


    def reply(self,
              reqPropsObj,
              httpRef,
              code,
              status,
              msg,
              addHttpHdrs = []):
        """
        Standard reply to HTTP request.

        reqPropsObj:   Request Property object to keep track of
                       actions done during the request handling
                       (ngamsReqProps).

        httpRef:       Reference to the HTTP request handler
                       object (ngamsHttpRequestHandler).

        code:          HTTP status code (integer)

        status:        Status: OK/FAILURE (string).

        msg:           Message for status (string).

        addHttpHdrs:   List containing sub-lists with additional
                       HTTP headers to send. Format is:

                         [[<HTTP hdr>, <val>, ...]         (list)

        Returns:       Void.
        """
        T = TRACE()

        if (reqPropsObj.getSentReply()):
            logger.info("Reply already sent for this request")
            return
        status = self.genStatus(status, msg).\
                 setReqStatFromReqPropsObj(reqPropsObj).\
                 setCompletionTime(reqPropsObj.getCompletionTime())
        xmlStat = status.genXmlDoc()
        xmlStat = ngamsHighLevelLib.\
                  addDocTypeXmlDoc(self, xmlStat, NGAMS_XML_STATUS_ROOT_EL,
                                   NGAMS_XML_STATUS_DTD)
        self.httpReply(reqPropsObj, httpRef, code, xmlStat, NGAMS_XML_MT,
                       addHttpHdrs)


    def ingestReply(self,
                    reqPropsObj,
                    httpRef,
                    code,
                    status,
                    msg,
                    diskInfoObj):
        """
        Standard HTTP reply to archive ingestion action.

        reqPropsObj:   Request Property object to keep track of actions done
                       during the request handling (ngamsReqProps).

        httpRef:       Reference to the HTTP request handler
                       object (ngamsHttpRequestHandler).

        code:          HTTP status code (integer)

        status:        Status: OK/FAILURE (string).

        msg:           Message to send as data with the HTTP reply (string).

        diskInfoObj:   Disk info object containing status for disk
                       where file were stored (Main Disk) (ngamsDiskInfo).
        """
        T = TRACE()

        statusObj = self.genStatus(status, msg).addDiskStatus(diskInfoObj).\
                    setReqStatFromReqPropsObj(reqPropsObj)
        xmlStat = statusObj.genXmlDoc(0, 1, 1)
        xmlStat = ngamsHighLevelLib.\
                  addDocTypeXmlDoc(self, xmlStat, NGAMS_XML_STATUS_ROOT_EL,
                                   NGAMS_XML_STATUS_DTD)
        self.httpReply(reqPropsObj, httpRef, code, xmlStat, NGAMS_XML_MT)


    def checkDiskSpaceSat(self,
                          minDiskSpaceMb = None):
        """
        This method checks the important mount points used by NG/AMS for
        the operation. If the amount of free disk space goes below 10 GB
        this is signalled by raising an exception.

        minDiskSpaceDb:   The amount of minimum free disk space (integer).

        Returns:          Void.
        """
        T = TRACE()

        if (not minDiskSpaceMb):
            minDiskSpaceMb = self.getCfg().getMinSpaceSysDirMb()
        for mtPt in self.__sysMtPtDic.keys():
            diskSpace = getDiskSpaceAvail(mtPt)
            if (diskSpace < minDiskSpaceMb):
                dirErrMsg = "("
                for dirInfo in self.__sysMtPtDic[mtPt]:
                    dirErrMsg += "Directory: %s - Info: %s, " %\
                                 (dirInfo[0], dirInfo[1])
                dirErrMsg = dirErrMsg[0:-2] + ")"
                errMsg = genLog("NGAMS_AL_DISK_SPACE_SAT",
                                [minDiskSpaceMb, dirErrMsg])
                raise Exception, errMsg


    def init(self, argv):
        """
        Initialize the NG/AMS Server.

        argv:       Tuple containing the command line parameters to
                    the server (tuple).

        Returns:    Reference to object itself.
        """
        # Parse input parameters, set up signal handlers, connect to DB,
        # load NGAMS configuration, start NG/AMS HTTP server.
        self.parseInputPars(argv)

        logger.info("NG/AMS Server version: %s", getNgamsVersion())
        logger.info("Python version: %s", re.sub("\n", "", sys.version))

        # Set up signal handlers.
        logger.debug("Setting up signal handler for SIGTERM ...")
        signal.signal(signal.SIGTERM, self.ngamsExitHandler)
        logger.debug("Setting up signal handler for SIGINT ...")
        signal.signal(signal.SIGINT, self.ngamsExitHandler)

        try:
            self.handleStartUp()
        except Exception, e:
            try:
                errMsg = genLog("NGAMS_ER_INIT_SERVER", [str(e)])
                ngamsNotification.notify(self.getHostId() or '', self.getCfg(), NGAMS_NOTIF_ERROR,
                                         "PROBLEMS INITIALIZING NG/AMS SERVER",
                                         errMsg, [], 1)
            except:
                print("Error while notifying about problems in server initialization")

            self.terminate()
            raise

    def pidFile(self):
        """
        Return the name of the PID file in which NG/AMS stores its PID.

        Returns:   Name of PID file (string).
        """
        # Generate a PID file with the  name: <mt root dir>/.<NGAS ID>
        if ((not self.getCfg().getRootDirectory()) or \
            (self.getCfg().getPortNo() < 1)): return ""
        try:
            pidFile = os.path.join(self.getCfg().getRootDirectory(), "." +
                                   self.getHostId()
                                   + ".pid")
        except Exception, e:
            errMsg = "Error occurred generating PID file name. Check " +\
                     "Mount Root Directory + Port Number in configuration. "+\
                     "Error: " + str(e)
            raise Exception, errMsg
        return pidFile


    def loadCfg(self):
        """
        Load the NG/AMS Configuration.

        Returns:   Reference to object itself.
        """

        cfg = self.getCfg()
        logger.info("Loading NG/AMS Configuration: " + self.getCfgFilename()+" ...")
        cfg.load(self.getCfgFilename())

        self.reconnect_to_db()

        # Check if we should load a configuration from the DB.
        if (self.__dbCfgId):
            cfg.loadFromDb(self.__dbCfgId, self.__ngasDb)

        cfg._check()

        logger.info("Successfully loaded NG/AMS Configuration")


    def reconnect_to_db(self):

        if self.__ngasDb:
            self.__ngasDb.close()

        cfg = self.__ngamsCfgObj
        self.__ngasDb = ngamsDb.from_config(cfg)
        ngasTmpDir = ngamsHighLevelLib.getNgasTmpDir(cfg)
        self.__ngasDb.setDbTmpDir(ngasTmpDir)

    def handleStartUp(self):
        """
        Initialize the NG/AMS Server. This implies loading the NG/AMS
        Configuration, setting up DB connection, checking disk configuration,
        and starting the HTTP server.

        serve:      If set to 1, the server will start serving on the
                    given HTTP port (integer/0|1).

        Returns:    Void.
        """
        # Remember to set the time for the last request initially to the
        # start-up time to avoid that the host is suspended immediately.
        self.setLastReqEndTime()

        # Load NG/AMS Configuration (from XML Document/DB).
        self.loadCfg()

        # IP address defaults to localhost
        ipAddress = self.getCfg().getIpAddress()
        self.ipAddress = ipAddress or '127.0.0.1'

        # Port number defaults to 7777
        portNo = self.getCfg().getPortNo()
        self.portNo = portNo if portNo != -1 else 7777
        self.host_id = "%s:%d" % (getHostName(), self.portNo)

        # Set up missing logging conditions from configuration file
        logcfg = self.logcfg
        if logcfg.file_level is None:
            logcfg.file_level = self.getCfg().getLocalLogLevel()
        if logcfg.logfile is None:
            logcfg.logfile = self.getCfg().getLocalLogFile()
        if logcfg.syslog is None:
            logcfg.syslog = self.getCfg().getSysLog()
        if logcfg.syslog_prefix is None:
            logcfg.syslog_prefix = self.getCfg().getSysLogPrefix()
        if logcfg.syslog_address is None:
            logcfg.syslog_address = self.getCfg().getSysLogAddress()
        if logcfg.stdout_level is None:
            logcfg.stdout_level = 0
        if logcfg.logfile_rot_interval is None:
            logcfg.logfile_rot_interval = isoTime2Secs(self.getCfg().getLogRotateInt())
            if not logcfg.logfile_rot_interval:
                logcfg.logfile_rot_interval = 600

        try:
            self.setup_logging()
        except Exception, e:
            errMsg = genLog("NGAMS_ER_INIT_LOG", [logcfg.logfile, str(e)])
            ngamsNotification.notify(self.getHostId(), self.getCfg(), NGAMS_NOTIF_ERROR,
                                     "PROBLEM SETTING UP LOGGING", errMsg)
            raise

        # Extend the system path to include anything specified in the config
        plugins_path = self.getCfg().getPluginsPath()
        if plugins_path:
            if not os.path.exists(plugins_path):
                raise ValueError("Plugins path %s doesn't exist, check your configuration" % (plugins_path,))
            sys.path.insert(0, plugins_path)
            logger.info("Added %s to the system path", plugins_path)

        # Check if there is an entry for this node in the ngas_hosts
        # table, if not create it.
        hostInfo = self.getDb().getHostInfoFromHostIds([self.getHostId()])
        if not hostInfo:
            tmpHostInfoObj = ngamsHostInfo.ngamsHostInfo()

            # If we specified a Proxy Name/IP in the configuration we use that
            # to save our IP address in the database so it becomes visible to
            # external users
            # TODO: This still needs to be properly done

            domain = ngamsLib.getDomain() or NGAMS_NOT_SET
            tmpHostInfoObj.\
                             setHostId(self.getHostId()).\
                             setDomain(domain).\
                             setIpAddress(self.ipAddress).\
                             setSrvPort(self.portNo).\
                             setMacAddress(NGAMS_NOT_SET).\
                             setNSlots(-1).\
                             setClusterName(self.getHostId()).\
                             setInstallationDate(time.time())
            logger.info("Creating entry in NGAS Hosts Table for this node: %s" %\
                 self.getHostId())
            self.getDb().writeHostInfo(tmpHostInfoObj)

        # Should be possible to execute several servers on one node.
        self.getHostInfoObj().setHostId(self.getHostId())

        # Log some essential information.
        allowArchiveReq    = self.getCfg().getAllowArchiveReq()
        allowRetrieveReq   = self.getCfg().getAllowRetrieveReq()
        allowProcessingReq = self.getCfg().getAllowProcessingReq()
        allowRemoveReq     = self.getCfg().getAllowRemoveReq()
        logger.info("Allow Archiving Requests: %d", allowArchiveReq)
        logger.info("Allow Retrieving Requests: %d", allowRetrieveReq)
        logger.info("Allow Processing Requests: %d", allowProcessingReq)
        logger.info("Allow Remove Requests: %d", allowRemoveReq)
        self.getHostInfoObj().\
                                setSrvArchive(allowArchiveReq).\
                                setSrvRetrieve(allowRetrieveReq).\
                                setSrvProcess(allowProcessingReq).\
                                setSrvRemove(allowRemoveReq).\
                                setSrvDataChecking(0)

        # Check if there is already a PID file.
        logger.debug("Check if NG/AMS PID file is existing ...")
        if (not self.getForce() and os.path.exists(self.pidFile())):
            errMsg = genLog("NGAMS_ER_MULT_INST")
            ngamsNotification.notify(self.getHostId(), self.getCfg(), NGAMS_NOTIF_ERROR,
                                     "CONFLICT STARTING NG/AMS SERVER", errMsg)
            raise Exception(errMsg)

        # Store the PID of this process in a PID file.
        logger.debug("Creating PID file for this session: %s", self.pidFile())
        checkCreatePath(os.path.dirname(self.pidFile()))
        with open(self.pidFile(), "w") as fo:
            fo.write(str(os.getpid()))
        self._pid_file_created = True
        logger.debug("PID file for this session created")

        # Check/create the NG/AMS Temporary and Cache Directories.
        checkCreatePath(ngamsHighLevelLib.getTmpDir(self.getCfg()))
        checkCreatePath(ngamsHighLevelLib.genCacheDirName(self.getCfg()))

        # Remove Request DB (DBM file).
        rmFile(self.getReqDbName() + "*")

        # Find the directories (mount directoties) to monitor for a minimum
        # amount of disk space. This is resolved from the various
        # directories defined in the configuration.
        logger.debug("Find NG/AMS System Directories to monitor for disk space ...")
        dirList = [(self.getCfg().getRootDirectory(),
                    "Mount Root Directory (Ngams:RootDirectory"),
                   (self.getCfg().getBackLogBufferDirectory(),
                    "Back-Log Buffer Directory " +\
                    "(Ngams:BackLogBufferDirectory)"),
                   (self.getCfg().getProcessingDirectory(),
                    "Processing Directory (FileHandling:ProcessingDirectory"),
                   (self.getCfg().getLocalLogFile(),
                    "Local Log File (Log:LocalLogFile)")]

        for dirInfo in dirList:
            path = os.path.abspath(dirInfo[0])
            while not os.path.ismount(path):
                path = os.path.dirname(path)
            if not os.path.ismount(path):
                continue
            if not self.__sysMtPtDic.has_key(path):
                self.__sysMtPtDic[path] = []
            self.__sysMtPtDic[path].append(dirInfo)

        logger.debug("Found NG/AMS System Directories to monitor for disk space")

        logger.debug("Check/create NG/AMS Request Info DB ...")
        reqDbmName = self.getReqDbName()
        self.__requestDbm = ngamsDbm.ngamsDbm(reqDbmName, cleanUpOnDestr = 0,
                                              writePerm = 1)
        logger.debug("Checked/created NG/AMS Request Info DB")

        #if (self.getCfg().getLogBufferSize() != -1):
        #    setLogCache(self.getCfg().getLogBufferSize())

        msg = genLog("NGAMS_INFO_STARTING_SRV",
                     [getNgamsVersion(), self.getHostId(),
                     self.getCfg().getPortNo()])
        logger.info(msg, extra={'to_syslog': True})

        # Reset the parameters for the suspension.
        self.getDb().resetWakeUpCall(self.getHostId(), 1)

        # Create a mime-type to DAPI dictionary
        for stream in self.getCfg().getStreamList():
            self.getMimeTypeDic()[stream.getMimeType()] = stream.getPlugIn()

        # Throw this info again to have it in the log-file as well
        logger.info("PID file for this session created: %s", self.pidFile())

        # If Auto Online is selected, bring the Server Online
        if (self.getAutoOnline()):
            logger.info("Auto Online requested - server going to Online State ...")
            try:
                ngamsSrvUtils.handleOnline(self)
            except:
                if (not self.getNoAutoExit()):
                    raise
        else:
            logger.info("Auto Online not requested - server remaining in Offline State")

        # Update the internal ngamsHostInfo object + ngas_hosts table.
        clusterName = self.getDb().getClusterNameFromHostId(self.getHostId())
        self.getHostInfoObj().setClusterName(clusterName)
        self.updateHostInfo(getNgamsVersion(), self.getCfg().getPortNo(),
                            self.getCfg().getAllowArchiveReq(),
                            self.getCfg().getAllowRetrieveReq(),
                            self.getCfg().getAllowProcessingReq(),
                            self.getCfg().getAllowRemoveReq(),
                            0, None)

        # Start HTTP server.
        logger.info("Initializing HTTP server ...")
        try:
            self.serve()
        except Exception, e:
            errMsg = genLog("NGAMS_ER_OP_HTTP_SERV", [str(e)])
            logger.exception(errMsg)
            ngamsNotification.notify(self.getHostId(), self.getCfg(), NGAMS_NOTIF_ERROR,
                                     "PROBLEM ENCOUNTERED STARTING " +\
                                     "SERVER", errMsg)
            raise


    def reqWakeUpCall(self,
                      wakeUpHostId,
                      wakeUpTime):
        """
        Request a Wake-Up Call via the DB.

        wakeUpHostId:  Name of host where the NG/AMS Server requested for
                       the Wake-Up Call is running (string).

        wakeUpTime:    Absolute time for being woken up (seconds since
                       epoch) (integer).

        Returns:       Reference to object itself.
        """
        self.getDb().reqWakeUpCall(self.getHostId(), wakeUpHostId, wakeUpTime)
        self.getHostInfoObj().\
                                setSrvSuspended(1).\
                                setSrvReqWakeUpSrv(wakeUpHostId).\
                                setSrvReqWakeUpTime(wakeUpTime)
        return self

    def serve(self):
        """
        Start to serve.

        Returns:  Void.
        """
        hostName = getHostName()
        logger.info("Setting up NG/AMS HTTP Server (Host: %s - IP: %s - Port: %d)",
                    hostName, self.ipAddress, self.portNo)
        self.__httpDaemon = ngamsHttpServer(self, (self.ipAddress, self.portNo))
        logger.info("NG/AMS HTTP Server ready")

        self.__httpDaemon.serve_forever()

    def stopServer(self):
        if self.__httpDaemon:
            self.__httpDaemon.shutdown()

    def ngamsExitHandler(self,
                         signalNo,
                         killServer = 1,
                         exitCode = 0,
                         delPidFile = 1):
        """
        NG/AMS Exit Handler Function. Is invoked when the NG/AMS Server
        is killed/terminated.

        signalNo:     Number of signal received.

        killServer:   1 = kill the server (integer).

        exitCode:     Exit code with which the server should exit (integer).

        delPidFile:   Flag indicating if NG/AMS PID file should be deleted or
                      not (integer/0|1).

        Returns:      Void.
        """

        if self.__handling_exit:
            logger.info('Already handling exit signal')
            return

        self.__handling_exit = True
        logger.info("In NG/AMS Exit Handler - received signal: %d", signalNo)
        self.terminate()

    def terminate(self):
        """
        Terminates the server process.

        If this server is listening for HTTP requests it is first stopped. Then
        the server is taken to the OFFLINE state.

        It flushes the logging system and renames the
        local log file so it gets automatically archived later.

        Returns:     Void.
        """
        t = threading.Thread(target=self._terminate, name="Shutdown")
        t.daemon = False
        t.start()

    def _terminate(self):
        msg = genLog("NGAMS_INFO_TERM_SRV", [getNgamsVersion(), getHostName(),
                                             self.getCfg().getPortNo()])
        logger.info(msg, extra={'to_syslog': True})

        # show+threads is useful to know if there are any hanging threads
        # after we stop the server
        show_threads()
        self.stopServer()
        ngamsSrvUtils.ngamsBaseExitHandler(self)
        show_threads()

        # Shut down logging. This will flush all pending logs in the system
        # and will ensure that the last logfile gets rotated
        logging.shutdown()

        # Remove PID file to allow future instances to be run
        try:
            if self._pid_file_created:
                os.unlink(self.pidFile())
        except OSError:
            print("Error while deleting PID file %s", self.pidFile())
            traceback.print_exc()

    def killServer(self):
        """
        Kills this process with SIGKILL
        """
        logger.warning("About to commit suicide... good-by cruel world")

        #First kill the janitor process created by this ngamsServer
        if self._janitorThread is not None:
            try:
                os.kill(self._janitorThread.pid, signal.SIGKILL)
            except:
                logger.warning("No Janitor process was found: %s. ")

        #Now kill the server itself
        pid = os.getpid()
        os.kill(pid, signal.SIGKILL)

    def _incCheckIdx(self,
                     idx,
                     argv):
        """
        Increment and check index for command line parameters.

        idx:       Present index to increment (integer).

        argv:      Tuple containing command line arguments (tuple).

        Returns:   Increment index.
        """
        idx = idx + 1
        if (idx == len(argv)): self.correctUsage()
        return idx


    def correctUsage(self):
        """
        Print out correct usage message.

        Returns:    Void.
        """
        manPage = pkg_resources.resource_string(__name__, 'ngamsServer.txt')  # @UndefinedVariable
        print manPage
        print ngamsCopyrightString()


    def parseInputPars(self, argv):
        """
        Parse input parameters.

        argv:       Tuple containing command line parameters (tuple)

        Returns:
        """
        exitValue = 1
        silentExit = 0
        idx = 1
        extra_paths = []

        while idx < len(argv):
            par = argv[idx].upper()
            try:
                if (par == "-CFG"):
                    idx = self._incCheckIdx(idx, argv)
                    self.setCfgFilename(argv[idx])
                elif (par == "-CACHE"):
                    self._cacheArchive = True
                elif par == '-DATAMOVER':
                    self._dataMoverOnly = True
                elif (par == "-DBCFGID"):
                    idx = self._incCheckIdx(idx, argv)
                    self.__dbCfgId = argv[idx]
                elif (par == "-V"):
                    idx = self._incCheckIdx(idx, argv)
                    self.logcfg.stdout_level = int(argv[idx])
                elif (par == "-LOCLOGFILE"):
                    idx = self._incCheckIdx(idx, argv)
                    self.logcfg.logfile = argv[idx]
                elif (par == "-LOCLOGLEVEL"):
                    idx = self._incCheckIdx(idx, argv)
                    self.logcfg.file_level = int(argv[idx])
                elif (par == "-SYSLOG"):
                    idx = self._incCheckIdx(idx, argv)
                    self.logcfg.syslog = bool(argv[idx])
                elif (par == "-SYSLOGPREFIX"):
                    idx = self._incCheckIdx(idx, argv)
                    self.logcfg.syslog_prefix = argv[idx]
                elif (par == "-VERSION"):
                    print getNgamsVersion()
                    exitValue = 0
                    silentExit = 1
                    sys.exit(0)
                elif (par == "-LICENSE"):
                    print getNgamsLicense()
                    exitValue = 0
                    silentExit = 1
                    sys.exit(0)
                elif (par == "-FORCE"):
                    self.setForce(1)
                elif (par == "-AUTOONLINE"):
                    self.setAutoOnline(1)
                elif (par == "-NOAUTOEXIT"):
                    self.setNoAutoExit(1)
                elif (par == "-MULTIPLESRVS"):
                    self.setMultipleSrvs(1)
                elif par == "-PATH":
                    idx = self._incCheckIdx(idx, argv)
                    extra_paths = set(filter(None, argv[idx].split(os.pathsep)))
                else:
                    self.correctUsage()
                    silentExit = 1
                    sys.exit(1)
                idx = idx + 1
            except Exception:
                if (not silentExit): self.correctUsage()
                sys.exit(exitValue)

        # Check correctness of the command line parameters.
        if (self.getCfgFilename() == ""):
            self.correctUsage()
            sys.exit(1)

        # Add extra paths at the beginning of the sys.path
        for p in extra_paths:
            p = os.path.expanduser(p)
            if not os.path.exists(p):
                raise ValueError("Path %s doesn't exist" % (p,))
            sys.path.insert(0,p)

    ########################################################################
    # The following methods are used for the NG/AMS Unit Tests.
    # The method do not contain any code, but in the Unit Test code it is
    # possible to override these methods to give the server a specific,
    # usually abnormal, behavior, e.g. to simulate that the server crashes.
    ########################################################################
    def test_AfterSaveInStagingFile(self):
        """
        Method invoked in NG/AMS Server immediately after saving data in a
        Staging File while handling an Archive Request.

        Returns:   Void.
        """
        pass

    def test_AfterCreateTmpPropFile(self):
        """
        Method invoked in NG/AMS Server immediately after having created
        the Temp. Req. Prop. File while handling an Archive Request.

        Returns:   Void.
        """
        pass

    def test_BeforeDapiInvocation(self):
        """
        Test method invoked in NG/AMS Server immediately before invoking
        the DAPI during the handling of the Archive Request.

        Returns:   Void.
        """
        pass

    def test_AfterDapiInvocation(self):
        """
        Test method invoked in NG/AMS Server immediately after having invoked
        the DAPI during the handling of the Archive Request.

        Returns:   Void.
        """
        pass

    def test_AfterMovingStagingFile(self):
        """
        Test method invoked in NG/AMS Server immediately moving the
        Processing Staging File to its final destination (Main File).

        Returns:   Void.
        """
        pass

    def test_BeforeRepFile(self):
        """
        Test method invoked in NG/AMS Server after handling the Main File
        (before handling the Replication File).

        Returns:   Void.
        """
        pass

    def test_BeforeDbUpdateRepFile(self):
        """
        Test method invoked in NG/AMS Server during handling of the Replication
        File, after creating the Replication Copy, before updating its info in
        the DB.

        Returns:   Void.
        """
        pass

    def test_BeforeArchCleanUp(self):
        """
        Test method invoked in NG/AMS Server during the Archive handling,
        before deleting the Original Staging File and the Request Propeties
        File.
        """
        pass
    ########################################################################

def main(argv=sys.argv):
    """
    Main function instantiating the NG/AMS Server Class and starting the server.
    """
    ngams = ngamsServer()
    ngams.init(argv)

if __name__ == '__main__':
    main()
