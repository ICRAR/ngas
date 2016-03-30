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

import os, sys, re, threading, time, pkg_resources
import traceback
import SocketServer, BaseHTTPServer, socket, signal

from pccUt import PccUtTime

from ngamsLib.ngamsCore import \
    genLog, error, info, alert, setLogCache, logFlush, sysLogInfo, TRACE,\
    rmFile, trim, getNgamsVersion, getDebug, getTestMode, setDebug, setTestMode, \
    getFileSize, getDiskSpaceAvail, setLogCond, checkCreatePath,\
    getHostName, ngamsCopyrightString, getNgamsLicense,\
    NGAMS_HTTP_SUCCESS, NGAMS_HTTP_REDIRECT, NGAMS_HTTP_INT_AUTH_USER, NGAMS_HTTP_GET,\
    NGAMS_HTTP_BAD_REQ, NGAMS_HTTP_SERVICE_NA, NGAMS_SUCCESS, NGAMS_FAILURE, NGAMS_OFFLINE_STATE,\
    NGAMS_IDLE_SUBSTATE, NGAMS_DEF_LOG_PREFIX, NGAMS_BUSY_SUBSTATE, NGAMS_NOTIF_ERROR, NGAMS_TEXT_MT,\
    NGAMS_ARCHIVE_CMD, NGAMS_NOT_SET, NGAMS_XML_STATUS_ROOT_EL,\
    NGAMS_XML_STATUS_DTD, NGAMS_XML_MT
from ngamsLib import ngamsHighLevelLib, ngamsLib
from ngamsLib import ngamsDbm, ngamsDb, ngamsConfig, ngamsReqProps
from ngamsLib import ngamsStatus, ngamsHostInfo, ngamsNotification
import ngamsAuthUtils, ngamsCmdHandling, ngamsSrvUtils


class ngamsSimpleRequest:
    """
    Small class to provide minimal HTTP Request Handler functionality.
    """

    def __init__(self,
                 request,
                 clientAddress):
        """
        Constructor method.
        """
        self.rbufsize = -1
        self.wbufsize = 0
        self.connection = request
        self.client_address = clientAddress
        self.rfile = self.connection.makefile('rb', self.rbufsize)
        self.wfile = self.wfile = self.connection.makefile('wb', self.wbufsize)


    def send_header(self,
                    keyword,
                    value):
        """
        Send an HTTP header.

        keyword:    HTTP header keyword (string).

        value:      Value for the HTTP header keyword (string).
        """
        self.wfile.write("%s: %s\r\n" % (keyword, value))


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
                error(errMsg)
                httpRef = self.RequestHandlerClass(request, client_address, self)
                tmpReqPropsObj = ngamsReqProps.ngamsReqProps()
                self._ngamsServer.reply(tmpReqPropsObj, httpRef, NGAMS_HTTP_SERVICE_NA,
                               NGAMS_FAILURE, errMsg)
            except IOError:
                errMsg = "Maximum number of requests exceeded and I/O ERROR encountered! Trying to continue...."
                error(errMsg)
            return

        # Create a new thread to handle the request.
        t = threading.Thread(target = self.finish_request,
                             args = (request, client_address))
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

    def finish(self):
        """
        Finish the handling of the HTTP request.

        Returns:    Void.
        """
        try:
            BaseHTTPServer.BaseHTTPRequestHandler.finish(self)
        finally:
            logFlush()

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
        path = trim(self.path, "?/ ")
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
        except Exception, e:
            error(str(e))
            sysLogInfo(1,str(e))
            raise

    # The three methods we support
    do_GET  = reqHandle
    do_POST = reqHandle
    do_PUT  = reqHandle

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
        self.__verboseLevel           = -1
        self.__locLogLevel            = -1
        self.__locLogFile             = None
        self.__sysLog                 = -1
        self.__sysLogPrefix           = NGAMS_DEF_LOG_PREFIX
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

        # Server list handling.
        self.__srvListDic             = {}

        self.__httpDaemon             = None

        self.__handling_exit          = False

        # General flag to control thread execution.
        self._threadRunPermission     = 0

        # Handling of the Janitor Thread.
        self._janitorThread           = None
        self._janitorThreadRunning    = 0
        self._janitorThreadRunSync    = threading.Event()
        self._janitorThreadRunCount   = 0

        # Handling of the Data Check Thread.
        self._dataCheckThread         = None
        self._dataCheckRunSync        = threading.Event()

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
        self._mirControlThread        = None
        self._mirControlThreadRunning = 0
        self.__mirControlTrigger      = threading.Event()
        self._pauseMirThreads         = False
        self._mirThreadsPauseCount    = 0
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
        self._userServicePlugIn       = None
        self._userServiceRunSync      = threading.Event()

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
        self._cacheControlThreadRunning = False
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

    def getHostId(self):
        """
        Returns the proper NG/AMS Host ID according whether multiple servers
        can be executed on the same host, and this server is one of those.

        If multiple servers can be executed on one node, the Host ID will be
        <Host Name>:<Port No>. Otherwise, the Host ID will be the hostname.
        """
        hostname = getHostName()
        if self.__multipleSrvs:
            return hostname + ":" + str(self.portNo)
        return hostname

    def getLogFilename(self):
        """
        Return the filename of the Local Log File.

        Returns:   Name of the Local Log File (string).
        """
        return self.__locLogFile


    def setDb(self,
              dbObj):
        """
        Set reference to the DB connection object.

        dbObj:      Valid NG/AMS DB Connection object (ngamsDb).

        Returns:    Reference to object itself.
        """
        self.__ngasDb = dbObj
        return self


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

    def recoveryRequestDb(self, err):
        """
        If the bsddb needs recover, i.e. err is something like
         (-30974, 'DB_RUNRECOVERY: Fatal error, run database recovery -- PANIC: fatal region error detected; run recovery')

        then remove and recreate the request bsddb

        err:    the error message (string)
        return: 0 - the error is not about recovery
                1 - the error is about recovery, which succeeded
               -1 - the error is about recovery, which failed
        """
        T = TRACE()

        if (err.find('DB_RUNRECOVERY') > -1):
            reqDbmName = self.getReqDbName()
            rmFile(reqDbmName + "*")
            info(4,"Recover (Check/create) NG/AMS Request Info DB ...")
            self.__requestDbm = ngamsDbm.ngamsDbm(reqDbmName, cleanUpOnDestr = 0,
                                                  writePerm = 1)
            info(4,"Recovered (Checked/created) NG/AMS Request Info DB")
            return 1
        else:
            return 0

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
        except Exception, e:
            self.recoveryRequestDb(str(e)) # this will ensure next time the same error will not appear again, but this time, it will still throw
            self.__requestDbmSem.release()
            raise e


    def getRequestIds(self):
        """
        Return a list with the Request IDs.

        Returns:    List with Request IDs (list).
        """
        return self.__requestDbm.keys()


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
        except Exception, e:
            self.recoveryRequestDb(str(e))
            self.__requestDbmSem.release()
            raise e


    def delRequest(self,
                   requestId):
        """
        Delete the Request Properties Object associated to the given
        Request ID.

        requestId:     ID allocated to the request (string).

        Returns:       Reference to object itself.
        """
        try:
            self.__requestDbmSem.acquire()
            if (self.__requestDbm.hasKey(str(requestId))):
                self.__requestDbm.rem(str(requestId))
                self.__requestDbm.sync()
            self.__requestDbmSem.release()
            return self
        except Exception, e:
            self.recoveryRequestDb(str(e))
            self.__requestDbmSem.release()
            raise e


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
        if ((ngamsLib.searchList(allowedStates, self.getState()) == -1) or
            (ngamsLib.searchList(allowedSubStates, self.getSubState()) == -1)):
            errMsg = [action, self.getState(), self.getSubState(),
                      str(allowedStates), str(allowedSubStates)]
            errMsg = genLog("NGAMS_ER_IMPROPER_STATE", errMsg)
            self.relStateSem()
            error(errMsg)
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


    def setJanitorThreadRunning(self,
                                running):
        """
        Set the Janitor Thread Running Flag to indicate if the Janitor
        Thread is running or not.

        running:     Janitor Thread Running Flag (integer/0|1).

        Returns:     Reference to object itself.
        """
        self._janitorThreadRunning = running
        return self


    def getJanitorThreadRunning(self):
        """
        Get the Janitor Thread Running Flag to indicate if the Janitor
        Thread is running or not.

        Returns:    Janitor Thread Running Flag (integer/0|1).
        """
        return self._janitorThreadRunning


    def incJanitorThreadRunCount(self):
        """
        Increase the Janitor Thread run count.

        Returns:     Reference to object itself.
        """
        self._janitorThreadRunCount += 1
        return self


    def resetJanitorThreadRunCount(self):
        """
        Reset the Janitor Thread run count.

        Returns:     Reference to object itself.
        """
        self._janitorThreadRunCount = 0
        return self


    def getJanitorThreadRunCount(self):
        """
        Return the Janitor Thread run count.

        Returns:     Janitor Thread Run Count (integer).
        """
        return self._janitorThreadRunCount


    def triggerSubscriptionThread(self):
        """
        Trigger the Data Subscription Thread so that it carries out a
        check to see if there are file to be delivered to Subscribers.

        Returns:   Reference to object itself.
        """
        T = TRACE()
        msg = "SubscriptionThread received trigger"
        info(3, msg)

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
        except Exception, e:
            errMsg = "Error occurred in ngamsServer." +\
                     "addSubscriptionInfo(). Exception: " + str(e)
            alert(errMsg)
            raise Exception, errMsg
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


    def setMirControlThreadRunning(self,
                                   running):
        """
        Set the Mirroring Control Thread Running Flag to indicate if the
        thread.

        running:     Mirroring Control Running Flag (integer/0|1).

        Returns:     Reference to object itself.
        """
        self._mirControlThreadRunning = running
        return self


    def getMirControlThreadRunning(self):
        """
        Get the Mirroring Control Thread Running Flag to indicate if the
        Mirroring Control Thread is running or not.

        Returns:    Mirroring Control Thread Running Flag (integer/0|1).
        """
        return self._mirControlThreadRunning


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


    def setCacheControlThreadRunning(self,
                                     running):
        """
        Set the Cache Control Thread Running Flag to indicate if the thread.

        running:     Cache Control Running Flag (integer/0|1).

        Returns:     Reference to object itself.
        """
        self._cacheControlThreadRunning = running
        return self


    def getCacheControlThreadRunning(self):
        """
        Get the Cache Control Thread Running Flag to indicate if the
        Cache Control Thread is running or not.

        Returns:    Cache Control Thread Running Flag (integer/0|1).
        """
        return self._cacheControlThreadRunning


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
            ngamsHighLevelLib.updateSrvHostInfo(self.getDb(),
                                                self.getHostInfoObj(), 1)
        return self


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
            if getDebug():
                traceback.print_exc(file = sys.stdout)

            errMsg = str(e)
            error(errMsg)
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
        T = TRACE()

        # If running in Unit Test Mode, check if the server is suspended.
        # In case yes, raise an exception indicating this.
        if (getTestMode()):
            if (self.getDb().getSrvSuspended(self.getHostId())):
                raise Exception, "UNIT-TEST: This server is suspended"

        # Handle the command.
        self.setLastReqStartTime()
        reqTimer = PccUtTime.Timer()
        safePath = ngamsLib.hidePassword(path)
        msg = "Handling HTTP request: client_address=%s - method=%s - path=|%s|" %\
                (str(clientAddress), method, safePath)
        info(1, msg)

        reqPropsObj.unpackHttpInfo(self.getCfg(), method, path, headers)

        ngamsAuthUtils.authorize(self, reqPropsObj, httpRef)

        ngamsCmdHandling.cmdHandler(self, reqPropsObj, httpRef)

        msg = "Total time for handling request: (%s, %s ,%s, %s): %ss" %\
                (reqPropsObj.getHttpMethod(), reqPropsObj.getCmd(),
                reqPropsObj.getMimeType(), reqPropsObj.getFileUri(),
                str(int(1000.0 * reqTimer.stop()) / 1000.0))

        if reqPropsObj.getIoTime() > 0:
            msg += "; Transfer rate: %s MB/s" % \
            str(reqPropsObj.getBytesReceived() / reqPropsObj.getIoTime() / 1024.0 / 1024.0)

        info(2, msg)
        logFlush()


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

        info(4, "httpReplyGen(). Generating HTTP reply to: %s" \
                % str(httpRef.client_address))

        if reqPropsObj.getSentReply():
            info(3,"Reply already sent for this request")
            return
        try:
            message = ''
            if BaseHTTPServer.BaseHTTPRequestHandler.responses.has_key(code):
                message = BaseHTTPServer.BaseHTTPRequestHandler.responses[code][0]

            protocol = BaseHTTPServer.BaseHTTPRequestHandler.protocol_version
            httpRef.wfile.write("%s %s %s\r\n" % (protocol, str(code), message))
            srvInfo = "NGAMS/%s" % getNgamsVersion()
            info(4,"Sending header: Server: %s" % srvInfo)
            httpRef.send_header("Server", srvInfo)
            httpTimeStamp = ngamsLib.httpTimeStamp()
            info(4,"Sending header: Date: %s" % httpTimeStamp)
            httpRef.send_header("Date", httpTimeStamp)
            # Expires HTTP reponse header field, e.g.:
            # Expires: Mon, 17 Sep 2001 09:21:38 GMT
            info(4,"Sending header: Expires: %s" % httpTimeStamp)
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
                info(4,"Sending header: %s:%s" % (hdrInfo[0], hdrInfo[1]))
                httpRef.send_header(hdrInfo[0], hdrInfo[1])
            if contentType != None:
                info(4,"Sending header: Content-Type: %s" % contentType)
                httpRef.send_header("Content-Type", contentType)
            if dataRef != None:
                info(4,"Sending header: Content-Length/1: %s" % str(dataSize))
                httpRef.send_header("Content-Length", dataSize)
                if dataInFile:
                    if not sentContDisp:
                        contDisp = "attachment; filename=%s" % os.path.basename(dataRef)
                        info(4,"Sending header: Content-Disposition: %s" % contDisp)
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
                    info(5,"Message sent with HTTP reply=|%s|" \
                            % str(dataRef).replace("\n", ""))
            elif contentLength != 0:
                info(4,"Sending header: Content-Length/2: %s" % str(contentLength))
                httpRef.send_header("Content-Length", contentLength)

        except Exception as e:
            errMsg = "Error occurred while sending reply to: %s Error: %s" \
                    % (str(httpRef.client_address), str(e))
            error(errMsg)
        finally:
            reqPropsObj.setSentReply(1)
            httpRef.wfile.flush()
            if closeWrFo == 1:
                httpRef.wfile.close()

        info(4,"Generated HTTP reply to: %s" % str(httpRef.client_address))


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
            info(3,"Reply already sent for this request")
            return
        self.httpReplyGen(reqPropsObj, httpRef, code, msg, 0, contentType,
                          len(msg), addHttpHdrs)
        httpRef.wfile.write("\r\n")
        info(3,"HTTP reply sent to: " + str(httpRef.client_address))


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
            if (par != "initiator"):
                pars += par + "=" + reqPropsObj.getHttpPar(par) + "&"
        pars = pars[0:-1]
        redirectUrl = "http://" + redirHost + ":" + str(redirPort) + "/" +\
                      reqPropsObj.getCmd() + "?" + pars
        msg = genLog("NGAMS_INFO_REDIRECT", [redirectUrl])
        info(1,msg)
        addHttpHdrs = [["Location", redirectUrl]]
        self.reply(reqPropsObj, httpRef, NGAMS_HTTP_REDIRECT, NGAMS_SUCCESS,
                   msg, addHttpHdrs)


    def forwardRequest(self,
                       reqPropsObj,
                       httpRefOrg,
                       forwardHost,
                       forwardPort,
                       autoReply = 1,
                       mimeType = ""):
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
        T = TRACE()

        # Resolve the proper contact host/port if needed/possible.
        hostDic = ngamsHighLevelLib.\
                  resolveHostAddress(self.getHostId(), self.getDb(),self.getCfg(),[forwardHost])
        if (hostDic[forwardHost] != None):
            contactHost = hostDic[forwardHost].getHostId()
            contactAddr = hostDic[forwardHost].getIpAddress()
            contactPort = hostDic[forwardHost].getSrvPort()
        else:
            contactHost = forwardHost
            contactAddr = forwardHost
            contactPort = forwardPort
        pars = []
        for par in reqPropsObj.getHttpParNames():
            if (par != "initiator"):
                val = reqPropsObj.getHttpPar(par)
                pars.append([par, val])
        cmdInfo = reqPropsObj.getCmd() + "/Parameters: " +\
                  str(pars)[1:-1] + " to server defined " +\
                  "by host/port: %s/%s." % (forwardHost, str(forwardPort))
        cmdInfo += " Contact address: %s/%s." % (contactAddr, str(contactPort))
        info(2,"Forwarding command: %s" % cmdInfo)
        try:
            # If target host is suspended, wake it up.
            if (self.getDb().getSrvSuspended(contactHost)):
                ngamsSrvUtils.wakeUpHost(self, contactHost)

            # If the NGAS Internal Authorization User is defined generate
            # an internal Authorization Code.
            if (self.getCfg().hasAuthUser(NGAMS_HTTP_INT_AUTH_USER)):
                authHttpHdrVal = self.getCfg().\
                                 getAuthHttpHdrVal(NGAMS_HTTP_INT_AUTH_USER)
            else:
                authHttpHdrVal = ""

            # Forward GET or POST request.
            # Make sure the time_out parameters is positive if given; otherwise
            # a sane default
            def_timeout = 300 # 3 [min]
            reqTimeOut = def_timeout
            if reqPropsObj.hasHttpPar('time_out'):
                reqTimeOut = reqPropsObj.getHttpPar("time_out")
                reqTimeOut = float(reqTimeOut) if reqTimeOut else def_timeout
                reqTimeOut = reqTimeOut if reqTimeOut >= 0 else def_timeout
            if (reqPropsObj.getHttpMethod() == NGAMS_HTTP_GET):
                httpStatCode, httpStatMsg, httpHdrs, data =\
                              ngamsLib.httpGet(contactAddr, contactPort,
                                               reqPropsObj.getCmd(), 1, pars,
                                               "",self.getCfg().getBlockSize(),
                                               timeOut=reqTimeOut,
                                               returnFileObj=0,
                                               authHdrVal=authHttpHdrVal)
            else:
                # It's a POST request, forward request + possible data.
                fn = reqPropsObj.getFileUri()
                contLen = reqPropsObj.getSize()
                if ((reqPropsObj.getCmd() == NGAMS_ARCHIVE_CMD) and
                    (contLen <= 0)):
                    raise Exception, "Must specify a content-length when " +\
                          "forwarding Archive Requests (Archive Proxy Mode)"
                httpStatCode, httpStatMsg, httpHdrs, data =\
                                  ngamsLib.httpPost(contactAddr, contactPort,
                                                    reqPropsObj.getCmd(),
                                                    mimeType,
                                                    reqPropsObj.getReadFd(),
                                                    "FD", pars,
                                                    authHdrVal=authHttpHdrVal,
                                                    fileName=fn,
                                                    dataSize=contLen,
                                                    timeOut=reqTimeOut)

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
        except Exception, e:
            errMsg = "Problem occurred forwarding command: " + cmdInfo +\
                     ". Error: " + str(e)
            raise Exception, errMsg


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
               setDate(PccUtTime.TimeStamp().getTimeStamp()).\
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
            info(3,"Reply already sent for this request")
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


    def init(self, argv, extlogger=None):
        """
        Initialize the NG/AMS Server.

        argv:       Tuple containing the command line parameters to
                    the server (tuple).

        Returns:    Reference to object itself.
        """
        if extlogger: extlogger("INFO", "Inside init()")
        # Parse input parameters, set up signal handlers, connect to DB,
        # load NGAMS configuration, start NG/AMS HTTP server.
        self.parseInputPars(argv, extlogger = extlogger)
        info(1,"NG/AMS Server version: " + getNgamsVersion())
        info(1,"Python version: " + re.sub("\n", "", sys.version))
        if extlogger: extlogger("INFO", "NG/AMS Server version: " + getNgamsVersion())

        # Set up signal handlers.
        info(4,"Setting up signal handler for SIGTERM ...")
        signal.signal(signal.SIGTERM, self.ngamsExitHandler)
        info(4,"Setting up signal handler for SIGINT ...")
        signal.signal(signal.SIGINT, self.ngamsExitHandler)

        if (getDebug()):
            self.handleStartUp()
        else:
            try:
                self.handleStartUp()
                if extlogger:
                    extlogger("INFO", "Successfully returned from handleStartup")
            except Exception, e:
                errMsg = genLog("NGAMS_ER_INIT_SERVER", [str(e)])
                if extlogger:
                    extlogger("INFO", errMsg)
                error(errMsg)
                ngamsNotification.notify(self.getHostId(), self.getCfg(), NGAMS_NOTIF_ERROR,
                                         "PROBLEMS INITIALIZING NG/AMS SERVER",
                                         errMsg, [], 1)
                self.terminate()

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
        info(1,"Loading NG/AMS Configuration: " + self.getCfgFilename()+" ...")
        cfg.load(self.getCfgFilename())

        # Connect to the DB.
        db = self.getDb()
        if not db:
            db = ngamsDb.from_config(cfg)
            self.setDb(db)

        # Check if we should load a configuration from the DB.
        if (self.__dbCfgId):
            cfg.loadFromDb(self.__dbCfgId, db)

        cfg._check()

        ngasTmpDir = ngamsHighLevelLib.getNgasTmpDir(cfg)
        self.__ngasDb.setDbTmpDir(ngasTmpDir)

        info(1,"Successfully loaded NG/AMS Configuration")


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

        # Set up final logging conditions.
        if (self.__locLogLevel == -1):
            self.__locLogLevel = self.getCfg().getLocalLogLevel()
        if ((self.__locLogFile != "") and (self.getCfg().getLocalLogFile())):
            self.__locLogFile = self.getCfg().getLocalLogFile()
        if (self.__sysLog == -1):
            self.__sysLog = self.getCfg().getSysLog()
        if (self.__sysLogPrefix == NGAMS_DEF_LOG_PREFIX):
            self.__sysLogPrefix = self.getCfg().getSysLogPrefix()
        try:
            setLogCond(self.__sysLog, self.__sysLogPrefix, self.__locLogLevel,
                       self.__locLogFile, self.__verboseLevel)
            msg = "Logging properties for NGAS Node: %s " +\
                  "defined as: Sys Log: %s " +\
                  "- Sys Log Prefix: %s  - Local Log File: %s " +\
                  "- Local Log Level: %s - Verbose Level: %s"
            info(1, msg % (self.getHostId(), str(self.__sysLog),
                           self.__sysLogPrefix, self.__locLogFile,
                           str(self.__locLogLevel), str(self.__verboseLevel)))
        except Exception, e:
            errMsg = genLog("NGAMS_ER_INIT_LOG", [self.__locLogFile, str(e)])
            error(errMsg)
            ngamsNotification.notify(self.getHostId(), self.getCfg(), NGAMS_NOTIF_ERROR,
                                     "PROBLEM SETTING UP LOGGING", errMsg)
            raise Exception, errMsg

        # Check if there is an entry for this node in the ngas_hosts
        # table, if not create it.
        hostInfo = self.getDb().getHostInfoFromHostIds([self.getHostId()])
        if (hostInfo == []):
            tmpHostInfoObj = ngamsHostInfo.ngamsHostInfo()

            # If we specified a Proxy Name/IP in the configuration we use that
            # to save our IP address in the database so it becomes visible to
            # external users
            # TODO: This still needs to be properly done

            domain = ngamsLib.getDomain() or NGAMS_NOT_SET
            tmpHostInfoObj.\
                             setHostId(self.getHostId()).\
                             setDomain(domain).\
                             setIpAddress(ipAddress).\
                             setMacAddress(NGAMS_NOT_SET).\
                             setNSlots(-1).\
                             setClusterName(self.getHostId()).\
                             setInstallationDateFromSecs(time.time())
            info(1,"Creating entry in NGAS Hosts Table for this node: %s" %\
                 self.getHostId())
            self.getDb().writeHostInfo(tmpHostInfoObj)

        # Should be possible to execute several servers on one node.
        self.getHostInfoObj().setHostId(self.getHostId())

        # Log some essential information.
        allowArchiveReq    = self.getCfg().getAllowArchiveReq()
        allowRetrieveReq   = self.getCfg().getAllowRetrieveReq()
        allowProcessingReq = self.getCfg().getAllowProcessingReq()
        allowRemoveReq     = self.getCfg().getAllowRemoveReq()
        info(1,"Allow Archiving Requests: %d"  % allowArchiveReq)
        info(1,"Allow Retrieving Requests: %d" % allowRetrieveReq)
        info(1,"Allow Processing Requests: %d" % allowProcessingReq)
        info(1,"Allow Remove Requests: %d"     % allowRemoveReq)
        self.getHostInfoObj().\
                                setSrvArchive(allowArchiveReq).\
                                setSrvRetrieve(allowRetrieveReq).\
                                setSrvProcess(allowProcessingReq).\
                                setSrvRemove(allowRemoveReq).\
                                setSrvDataChecking(0)

        # Check if there is already a PID file.
        info(5,"Check if NG/AMS PID file is existing ...")
        if (not self.getForce() and os.path.exists(self.pidFile())):
            errMsg = genLog("NGAMS_ER_MULT_INST")
            error(errMsg)
            ngamsNotification.notify(self.getHostId(), self.getCfg(), NGAMS_NOTIF_ERROR,
                                     "CONFLICT STARTING NG/AMS SERVER", errMsg)
            self.terminate()

        # Store the PID of this process in a PID file.
        info(4,"Creating PID file for this session: {0}".format(self.pidFile()))
        checkCreatePath(os.path.dirname(self.pidFile()))
        with open(self.pidFile(), "w") as fo:
            fo.write(str(os.getpid()))
        info(4,"PID file for this session created")

        # Check/create the NG/AMS Temporary and Cache Directories.
        checkCreatePath(ngamsHighLevelLib.getTmpDir(self.getCfg()))
        checkCreatePath(ngamsHighLevelLib.genCacheDirName(self.getCfg()))

        # Remove Request DB (DBM file).
        rmFile(self.getReqDbName() + "*")

        # Find the directories (mount directoties) to monitor for a minimum
        # amount of disk space. This is resolved from the various
        # directories defined in the configuration.
        info(4,"Find NG/AMS System Directories to monitor for disk space ...")
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

        info(4,"Found NG/AMS System Directories to monitor for disk space")

        info(4,"Check/create NG/AMS Request Info DB ...")
        reqDbmName = self.getReqDbName()
        self.__requestDbm = ngamsDbm.ngamsDbm(reqDbmName, cleanUpOnDestr = 0,
                                              writePerm = 1)
        info(4,"Checked/created NG/AMS Request Info DB")

        if (self.getCfg().getLogBufferSize() != -1):
            setLogCache(self.getCfg().getLogBufferSize())

        sysLogInfo(1, genLog("NGAMS_INFO_STARTING_SRV",
                             [getNgamsVersion(), self.getHostId(),
                              self.getCfg().getPortNo()]))

        # Reset the parameters for the suspension.
        self.getDb().resetWakeUpCall(self.getHostId(), 1)

        # Create a mime-type to DAPI dictionary
        for stream in self.getCfg().getStreamList():
            self.getMimeTypeDic()[stream.getMimeType()] = stream.getPlugIn()

        # Throw this info again to have it in the log-file as well
        info(3,"PID file for this session created: {0}".format(self.pidFile()))

        # If Auto Online is selected, bring the Server Online
        if (self.getAutoOnline()):
            info(2,"Auto Online requested - server going to Online State ...")
            try:
                ngamsSrvUtils.handleOnline(self)
            except Exception, e:
                if (not self.getNoAutoExit()): raise e
        else:
            info(2,"Auto Online not requested - " +\
                 "server remaining in Offline State")

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
        info(1,"Initializing HTTP server ...")
        try:
            self.serve()
        except Exception, e:
            traceback.print_exc()
            errMsg = genLog("NGAMS_ER_OP_HTTP_SERV", [str(e)])
            error(errMsg)
            ngamsNotification.notify(self.getHostId(), self.getCfg(), NGAMS_NOTIF_ERROR,
                                     "PROBLEM ENCOUNTERED STARTING " +\
                                     "SERVER", errMsg)
            self.terminate()


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
        info(1, "Setting up NG/AMS HTTP Server (Host: {0} - IP: {1} - Port: {2})".\
             format(hostName, self.ipAddress, self.portNo))
        self.__httpDaemon = ngamsHttpServer(self, (self.ipAddress, self.portNo))
        info(1,"NG/AMS HTTP Server ready")

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
            info(1, 'Already handling exit signal')
            return

        self.__handling_exit = True
        info(1,"In NG/AMS Exit Handler - received signal: " + str(signalNo))
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
        t = threading.Thread(target=self._terminate)
        t.start()

    def _terminate(self):
        msg = genLog("NGAMS_INFO_TERM_SRV", [getNgamsVersion(), getHostName(),
                                             self.getCfg().getPortNo()])
        sysLogInfo(1, msg)
        info(1,msg)

        self.stopServer()
        ngamsSrvUtils.ngamsBaseExitHandler(self)

        # Rotate the log file; it's called .unsaved because the next time NGAS
        # starts it will pick them up and save them into itself
        # logFile might be empty/None if self.getCfg() returns the empty config
        # created at __init__ time, meaning that this _terminate is being called
        # due to an error before or during self.loadCfg()
        logFile = self.getCfg().getLocalLogFile()
        if logFile:
            try:
                logPath = os.path.dirname(logFile)
                rotLogFile = "LOG-ROTATE-%s.nglog.unsaved" % (PccUtTime.TimeStamp().getTimeStamp(),)
                rotLogFile = os.path.normpath(logPath + "/" + rotLogFile)
                info(1, "Closing log file: %s -> %s" % (logFile, rotLogFile))
                logFlush()
                os.rename(logFile, rotLogFile)
            except Exception, e:
                error("Server encountered problem while rotating logfile: " + str(e))

        # Avoid last logs going into the local file
        setLogCond(self.__sysLog, self.__sysLogPrefix, 0,
                       self.__locLogFile, self.__verboseLevel)
        info(1,"Terminated NG/AMS Server")

    def killServer(self):
        """
        Kills this process with SIGKILL
        """
        info(1,"About to commit suicide... good-by cruel world")
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
        manPage = manPage.replace("ngamsServer", self._serverName)
        print manPage
        print ngamsCopyrightString()


    def parseInputPars(self,
                       argv, extlogger = None):
        """
        Parse input parameters.

        argv:       Tuple containing command line parameters (tuple)

        Returns:
        """
        if extlogger: extlogger("INFO", "Entering parseInputPars")
        if extlogger: extlogger("INFO", "Arguments: {0}".format(' '.join(argv)))
        setLogCache(10)
        exitValue = 1
        silentExit = 0
        idx = 1
        while idx < len(argv):
            par = argv[idx].upper()
            try:
                if (par == "-CFG"):
                    idx = self._incCheckIdx(idx, argv)
                    info(1,"Configuration specified: %s" % argv[idx])
                    self.setCfgFilename(argv[idx])
                elif (par == "-DBCFGID"):
                    idx = self._incCheckIdx(idx, argv)
                    info(1,"Configuration DB ID specified: %s" % argv[idx])
                    self.__dbCfgId = argv[idx]
                elif (par == "-V"):
                    idx = self._incCheckIdx(idx, argv)
                    self.__verboseLevel = int(argv[idx])
                    setLogCond(self.__sysLog, self.__sysLogPrefix,
                               self.__locLogLevel, self.__locLogFile,
                               self.__verboseLevel)
                elif (par == "-LOCLOGFILE"):
                    idx = self._incCheckIdx(idx, argv)
                    self.__locLogFile = argv[idx]
                    setLogCond(self.__sysLog, self.__sysLogPrefix,
                               self.__locLogLevel, self.__locLogFile,
                               self.__verboseLevel)
                elif (par == "-LOCLOGLEVEL"):
                    idx = self._incCheckIdx(idx, argv)
                    self.__locLogLevel = int(argv[idx])
                    setLogCond(self.__sysLog, self.__sysLogPrefix,
                               self.__locLogLevel, self.__locLogFile,
                               self.__verboseLevel)
                elif (par == "-SYSLOG"):
                    idx = self._incCheckIdx(idx, argv)
                    self.__sysLogLevel = argv[idx]
                    setLogCond(self.__sysLog, self.__sysLogPrefix,
                               self.__locLogLevel, self.__locLogFile,
                               self.__verboseLevel)
                elif (par == "-SYSLOGPREFIX"):
                    idx = self._incCheckIdx(idx, argv)
                    self.__sysLogPrefix = argv[idx]
                    setLogCond(self.__sysLog, self.__sysLogPrefix,
                               self.__locLogLevel, self.__locLogFile,
                               self.__verboseLevel)
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
                elif (par == "-D"):
                    info(1,"Debug Mode enabled")
                    setDebug(1)
                elif (par == "-FORCE"):
                    info(1,"Forced Mode requested")
                    self.setForce(1)
                elif (par == "-AUTOONLINE"):
                    info(1,"Auto Online requested")
                    self.setAutoOnline(1)
                elif (par == "-NOAUTOEXIT"):
                    info(1,"Auto Exit is off")
                    self.setNoAutoExit(1)
                elif (par == "-MULTIPLESRVS"):
                    info(1,"Running in Multiple Servers Mode")
                    self.setMultipleSrvs(1)
                elif (par == "-TEST"):
                    info(1,"Running server in Test Mode")
                    setTestMode()
                else:
                    self.correctUsage()
                    silentExit = 1
                    if extlogger: extlogger("INFO", "ngamsServer call incomplete")
                    sys.exit(1)
                idx = idx + 1
                if extlogger: extlogger("INFO", "Parser parsed {0}".format(par))
                logFlush()
            except Exception, e:
                if (str(e) == "0"):
                    if extlogger: extlogger("INFO",\
                         "Problem encountered parsing command line ")
                    logFlush()
                    sys.exit(0)
                if (str(1) != "1"):
                    if extlogger: extlogger("INFO",\
                       "Problem encountered parsing command line " +\
                          "parameters: "+ str(e))
                    info(1,str(e))
                if (not silentExit): self.correctUsage()
                sys.exit(exitValue)

        # Check correctness of the command line parameters.
        if (self.getCfgFilename() == ""):
            self.correctUsage()
            sys.exit(1)
        if extlogger: extlogger("INFO","Leaving parseInputPars")

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
