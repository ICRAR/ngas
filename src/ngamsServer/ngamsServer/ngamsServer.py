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

import argparse
import collections
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
import uuid

import six
from six.moves import reduce # @UnresolvedImport
from six.moves import socketserver # @UnresolvedImport
from six.moves import BaseHTTPServer  # @UnresolvedImport
from six.moves import queue as Queue  # @UnresolvedImport

import netifaces

from ngamsLib.ngamsCore import genLog, getNgamsVersion, \
    getFileSize, getDiskSpaceAvail, checkCreatePath,\
    getHostName, ngamsCopyrightString, getNgamsLicense,\
    NGAMS_HTTP_REDIRECT, NGAMS_HTTP_INT_AUTH_USER, \
    NGAMS_SUCCESS, NGAMS_FAILURE, NGAMS_OFFLINE_STATE,\
    NGAMS_IDLE_SUBSTATE, NGAMS_BUSY_SUBSTATE, NGAMS_NOTIF_ERROR,\
    NGAMS_NOT_SET, NGAMS_XML_MT, loadPlugInEntryPoint, isoTime2Secs,\
    toiso8601
from ngamsLib import ngamsHighLevelLib, ngamsLib, ngamsEvent, ngamsHttpUtils,\
    utils, logutils
from ngamsLib import ngamsDb, ngamsConfig, ngamsReqProps, pysendfile
from ngamsLib import ngamsStatus, ngamsHostInfo, ngamsNotification
from . import janitor
from . import InvalidParameter, NoSuchCommand
from . import ngamsAuthUtils, ngamsCmdHandling, ngamsSrvUtils
from . import ngamsDataCheckThread
from . import ngamsUserServiceThread
from . import ngamsMirroringControlThread
from . import ngamsCacheControlThread
from . import request_db


logger = logging.getLogger(__name__)

def get_all_ipaddrs():
    """
    Returns a list of all the IPv4 addresses found in this computer
    """
    proto = netifaces.AF_INET
    iface_addrs = [netifaces.ifaddresses(iface) for iface in netifaces.interfaces()]
    inet_addrs = [addrs[proto] for addrs in iface_addrs if proto in addrs]
    return [addr['addr'] for addrs in inet_addrs for addr in addrs if 'addr' in addr]

class thread_pool_mixin(socketserver.ThreadingMixIn):
    """Uses a thread pool to process requests"""

    def __init__(self, ngamsServer):
        import multiprocessing.pool

        max_reqs = ngamsServer.cfg.getMaxSimReqs()
        self._ngamsServer = ngamsServer
        self._pool = multiprocessing.pool.ThreadPool(processes=max_reqs)

        # The length of the backlog of connections that are being accepted
        # but haven't been picked up yet, declared in TCPServer
        self.request_queue_size = max_reqs

    def process_request(self, request, client_address):
        """process the request in a thread of the pool"""

        if self._ngamsServer.serving_count >= self.request_queue_size:
            logger.error("Maximum number of serving threads reached, rejecting request")
            wfile = request.makefile('wb')
            wfile.write(b'HTTP/1.0 503 Service Unavailable\r\n\r\n')
            return

        self._pool.apply_async(self.process_request_thread, args=(request, client_address))

class ngamsHttpServer(thread_pool_mixin, BaseHTTPServer.HTTPServer):
    """Class providing pooled multithreaded HTTP server functionality"""

    allow_reuse_address = 1

    def __init__(self, ngamsServer, server_address):
        thread_pool_mixin.__init__(self, ngamsServer)
        BaseHTTPServer.HTTPServer.__init__(self, server_address, ngamsHttpRequestHandler)

        if ngamsServer._cert is not None:
            import ssl
            logger.info("Using TLS for testing")
            context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            context.load_cert_chain(certfile=ngamsServer._cert)
            self.socket = context.wrap_socket(self.socket, server_side=True)


class _atomic_counter(object):
    """A simple atomic counter"""

    def __init__(self, val):
        self.val = val
        self.lock = threading.Lock()

    def inc(self):
        with self.lock:
            val = self.val
            self.val += 1
            return val
            
class ngamsHttpRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    """
    Class used to handle an HTTP request. The various ``send_*`` methods
    should make it easy for the rest of the code to send different kind of
    replies to users
    """

    server_version = "NGAMS/" + getNgamsVersion()
    req_count = _atomic_counter(0)

    def setup(self):

        self.ngasServer = self.server._ngamsServer

        # Set the request timeout to the value given in the server configuration
        # or default to 1 minute (apache defaults to 1 minute so I assume it's
        # a sensible value)
        cfg = self.ngasServer.getCfg()
        self.timeout = cfg.getTimeOut() or 60

        # Make the name of the current thread more unique
        # This is important because we currently use the thread name to uniquely
        # map log statements to individual requests. Log statements use
        req_num = self.req_count.inc()
        threading.current_thread().setName('R-%d' % req_num)

        BaseHTTPServer.BaseHTTPRequestHandler.setup(self)

    def version_string(self):
        return self.server_version

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

        # This is set by send_response below to prevent multiple replies being
        # send during the same HTTP request
        self.reply_sent = False
        self.headers_sent = False

        path = self.path.strip("?/ ")
        try:
            self.ngasServer.reqCallBack(self, self.client_address, self.command,
                                        path, self.headers)
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

    # Richer end_headers method to keep track of call
    def end_headers(self):
        BaseHTTPServer.BaseHTTPRequestHandler.end_headers(self)
        self.headers_sent = True

    # Richer send_response method to pass down headers
    def send_response(self, code, message=None, hdrs={}):
        """Sends the initial status line plus headers to the client, can't be called twice"""

        # Prevent multiple replies being sent
        if self.reply_sent:
            raise Exception("Tried to send two responses :(")
        self.reply_sent = True

        BaseHTTPServer.BaseHTTPRequestHandler.send_response(self, code, message=message)
        for k, v in hdrs.items():
            self.send_header(k, v)

    def redirect(self, host, port):
        """Redirects the client to the requested path, but on ``host``:``port``"""

        path = self.path
        if not path.startswith('/'):
            path = '/' + path
        location = 'http://%s:%d%s' % (host, port, path)

        logger.info("Redirecting client to %s", location)
        self.send_response(NGAMS_HTTP_REDIRECT, hdrs={'Location': location})
        self.end_headers()

    def redirect_to_url(self, url, http_status=NGAMS_HTTP_REDIRECT):
        """Permanent Redirects the client to the requested url"""

        logger.info("Redirecting client to %s", url)
        self.send_response(http_status, hdrs={'Location': url})
        self.end_headers()

    def send_file(self, f, mime_type, start_byte=0, fname=None, hdrs={}):
        """
        Sends file ``f`` of type ``mime_type`` to the client. Optionally a different
        starting byte to start the transmission from, and a different name for
        the file to present the data to the user can be given.
        """

        fname = fname or os.path.basename(f)
        size = getFileSize(f)

        self.send_file_headers(fname, mime_type, size, start_byte, hdrs=hdrs)
        self.write_file_data(f, size, start_byte)

    def send_file_headers(self, fname, mime_type, size, start_byte=0, hdrs={}):
        """Sends the headers advertising file ``fname``, but without its data.
        Headers set by this method take precedence over values given by the
        caller via the ``hdrs`` optional argument"""
        hdrs = dict(hdrs)
        if start_byte:
            hdrs['Accept-Ranges'] = 'bytes'
            hdrs["Content-Range"] = "bytes %d-%d/%d" % (start_byte, size - 1, size)
        self.write_headers(length=(size - start_byte), mime_type=mime_type,
                           code=200, fname=fname, hdrs=hdrs)

    def write_file_data(self, f, size, start_byte=0):
        """sends file ``f``, hopefully using ``sendfile(2)``"""

        if not self.headers_sent:
            raise RuntimeError('Trying to send file data but HTTP headers not sent')

        self.wfile.flush()
        logger.info("Sending %s (%d bytes) to client, starting at byte %d", f, size, start_byte)
        with open(f, 'rb') as fin:
            st = time.time()
            if self.ngasServer.get_server_access_proto() == "https":
                pysendfile.sendfile_send(self.connection, fin, start_byte)
            else:
                pysendfile.sendfile(self.connection, fin, start_byte)
            howlong = time.time() - st
            size_mb = size / 1024. / 1024.
        logger.info("Sent %s at %.3f [MB/s]", f, size_mb / howlong)

    def send_data(self, data, mime_type, code=200, message=None, fname=None, hdrs={}):
        """
        Sends back ``data``, which is of type ``mime_type``. If ``fname`` is given
        then the data is sent as an attachment.
        """
        self.write_headers(length=len(data), mime_type=mime_type, code=code,
                           message=message, fname=fname, hdrs=hdrs)
        self.write_data(data)

    def write_headers(self, length=None, mime_type=None, code=200, message=None, fname=None, hdrs={}):
        """
        Writes `length`, `mime_type` and `fname` (when given) into their
        corresponding HTTP headers, initiating an HTTP response. After a
        succesful call the body of the response should be written.
        If `lenght` is not given, the corresponding header must be set already.
        """
        # Check length is somehow given to us, and not twice
        # We pop to make sure only one Content-Length is sent
        hdr_length = hdrs.pop('Content-Length', None)
        if hdr_length is None:
            hdr_length = hdrs.pop('content-length', None)
        if length is None:
            length = hdr_length
        if length is None:
            raise ValueError('No length information given for response')
        elif hdr_length is not None and length != hdr_length:
            raise ValueError('Length information given twice (%d v/s %d)' % (length, hdr_length))

        hdrs = dict(hdrs)
        hdrs['Content-Length'] = length
        if mime_type:
            hdrs['Content-Type'] = mime_type
        if fname:
            hdrs['Content-Disposition'] = 'attachment; filename="%s"' % fname

        logger.info("Sending %d bytes of data of type %s and headers %r", length, mime_type, hdrs)
        self.send_response(code, message=message, hdrs=hdrs)
        self.end_headers()

    def write_data(self, data):
        """Writes ``data`` into the HTTP response body"""

        if not self.headers_sent:
            raise RuntimeError('Trying to send data but HTTP headers not sent')

        # Support for file-like objects (but files should be sent via write_file_data)
        if hasattr(data, 'read'):
            shutil.copyfileobj(data, self.wfile)
            return

        self.wfile.write(data)

    def send_status(self, message, status=NGAMS_SUCCESS, code=None, http_message=None, hdrs={}):
        """Creates and sends an NGAS status XML document back to the client"""


        if code is None:
            code = 200 if status == NGAMS_SUCCESS else 400

        logger.info("Returning status %s with message %s and HTTP code %d", status, message, code)

        status = self.ngasServer.genStatus(status, message)
        xml = ngamsHighLevelLib.addStatusDocTypeXmlDoc(self.ngasServer, status.genXmlDoc())
        self.send_data(six.b(xml), NGAMS_XML_MT, code=code, message=http_message, hdrs=hdrs)

    def send_ingest_status(self, msg, disk_info):
        """Reply to the client with a standard ingest status XML document"""
        status = self.ngasServer.genStatus(NGAMS_SUCCESS, msg).addDiskStatus(disk_info).\
                 setReqStatFromReqPropsObj(self.ngas_request)
        xml = status.genXmlDoc(0, 1, 1)
        xml = ngamsHighLevelLib.addStatusDocTypeXmlDoc(self.ngasServer, xml)
        self.send_data(six.b(xml), NGAMS_XML_MT)

    def proxy_request(self, host_id, host, port, timeout=300):
        """Proxy the current request to ``host``:``port``"""

        # Calculate target URL
        path = self.path
        if not path.startswith('/'):
            path = '/' + path
        url = 'http://%s:%d%s' % (host, port, path)
        logger.info("Proxying request for %s to %s:%d (corresponding to server %s)",
                    path, host, port, host_id)

        # If target host is suspended, wake it up.
        srv = self.ngasServer
        if (srv.db.getSrvSuspended(host_id)):
            ngamsSrvUtils.wakeUpHost(srv, host_id)

        # If the NGAS Internal Authorization User is defined generate
        # an internal Authorization Code.
        authHttpHdrVal = ""
        if srv.cfg.hasAuthUser(NGAMS_HTTP_INT_AUTH_USER):
            authHttpHdrVal = srv.cfg.getAuthHttpHdrVal(NGAMS_HTTP_INT_AUTH_USER)

        # Make sure the time_out parameters is within proper boundaries
        timeout = min(max(timeout, 0), 1200)

        # Cleanup any headers that we know we'll set again
        # (including "Host", we are not a *realy* proxy)
        _STD_HDRS = ('host', 'content-type', 'content-length', 'content-disposition',
                     'accept-encoding', 'transfer-encoding', 'authorization')
        hdrs = {k: v for k, v in self.headers.items() if k.lower() not in _STD_HDRS}

        # Forward GET or POST request, get back code/msg/hdrs/data
        if self.command == 'GET':
            resp = ngamsHttpUtils.httpGetUrl(url, hdrs=hdrs, timeout=timeout,
                                             auth=authHttpHdrVal)
            with contextlib.closing(resp):
                code, data = resp.status, resp.read()
            hdrs = {h[0]: h[1] for h in resp.getheaders()}

        else:
            # During HTTP post we need to pass down a EOF-aware,
            # read()-able object
            data = ngamsHttpUtils.sizeaware(self.rfile, int(self.headers['content-length']))

            mime_type = ''
            if 'content-type' in self.headers:
                mime_type = self.headers['content-type']

            code, _, hdrs, data = ngamsHttpUtils.httpPostUrl(url, data, mime_type,
                                                             hdrs=hdrs,
                                                             timeout=timeout,
                                                             auth=authHttpHdrVal)

        # Our code calculates the content length already, let's not send it twice
        # Similarly, let's avoid Content-Type rewriting
        # Here "hdrs" is not a one of the nice email.message.Message objects
        # which allows for case-insensitive lookup, but simply a dictionary,
        # so we are forced to perform a case-sensitive lookup
        logger.info("Received response from %s:%d, sending to client", host, port)
        logger.info("Headers from response: %r", hdrs)
        if 'content-length' in hdrs:
            del hdrs['content-length']
        if 'Content-Length' in hdrs:
            del hdrs['Content-Length']
        mime_type = ''
        if 'content-type' in hdrs:
            mime_type = hdrs['content-type']
            del hdrs['content-type']
        if 'Content-Type' in hdrs:
            mime_type = hdrs['Content-Type']
            del hdrs['Content-Type']

        self.send_data(data, mime_type, code=code, hdrs=hdrs)



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


def show_threads():
    """
    Log the name, ident and daemon flag of all alive threads in DEBUG level
    """
    if logger.isEnabledFor(logging.DEBUG):

        all_threads = threading.enumerate()
        max_name  = reduce(max, map(len, [t.name for t in all_threads]))
        max_ident = reduce(max, map(int, map(math.ceil, map(math.log10, [t.ident for t in all_threads if t.ident is not None]))))

        msg = ['Name' + ' '*(max_name-2) + 'Ident' + ' '*(max_ident-3) + 'Daemon',
               '='*max_name + '  ' + '=' * max_ident + '  ======']
        fmt = '%{0}.{0}s  %{1}d  %d'.format(max_name, max_ident)
        for t in threading.enumerate():
            msg.append(fmt % (t.name, t.ident, t.daemon))
        logger.debug("Threads currently alive on process %d:\n%s", os.getpid(), '\n'.join(msg))

archive_event = collections.namedtuple('archive_event', 'file_id file_version')

class ngamsServer(object):
    """
    Class providing the functionality of the NG/AMS Server.
    """

    # These are here purely for documentation reasons
    db = None
    """A reference to the underlying database of type
    :py:class:`ngamsDb <ngamsLib.ngamsDb.ngamsDb>`"""

    cfg = None
    """The underlying configuration object of type
    :py:class:`ngamsConfig <ngamsLib.ngamsConfig.ngamsConfig>`"""

    def __init__(self, cfg_fname, _cert=None):
        """
        Constructor method.
        """
        self.cfg_fname                = cfg_fname
        self.cfg                      = ngamsConfig.ngamsConfig()
        self.__dbCfgId                = ""
        self.force_start              = False
        self.autoonline               = False
        self.no_autoexit              = False
        self.db                       = None
        self.__diskDic                = None
        self.__mimeType2PlugIn        = {}
        self.__state                  = NGAMS_OFFLINE_STATE
        self.__subState               = NGAMS_IDLE_SUBSTATE
        self.__stateSem               = threading.Semaphore(1)
        self.__subStateSem            = threading.Semaphore(1)
        self.__busyCount              = 0
        self.__sysMtPtDic             = {}
        self._pid_file_created         = False
        self._cert                     = _cert

        # Keep track of how many requests are being served,
        # This is slightly different from keeping track of the server's
        # sub-state because many commands don't bother changing it
        self.serving_count = 0
        self.serving_count_lock = threading.Lock()

        # Whether background threads *started by commands* are allowed to run
        # or not. Background tasks like the janitor, data check or subscription
        # threads use their own synchronization mechanism
        self.run_async_commands = False

        # Coalesces whether requests are being served at all or not.
        # When the value changes, listeners are notified
        # The value is get and set via a property that encapsulates
        # the listener notification
        self._serving = False
        self.serving_listeners = []

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
        self._janitorThread = utils.Task("Janitor", janitor.janitorThread, mode=utils.Task.PROCESS)
        self._janitorQueThread = utils.Task("JanitorQueReaderThread", self.janitorQueThread, stop_evt=self._janitorThread.stop_evt)
        self._janitorThreadRunCount = 0
        self._janitordbChangeSync = ngamsEvent.ngamsEvent()

        # Handling of the Data Check Thread.
        self._data_check_thread = utils.Task(ngamsDataCheckThread.NGAMS_DATA_CHECK_THR,
                                             ngamsDataCheckThread.dataCheckThread)

        # The events that control the execution of the checksum calculation
        # The allowed event is initially set because initially the server
        # is serving no requests.
        self.checksum_allow_evt      = multiprocessing.Event()
        self.checksum_allow_evt.set()
        self.checksum_stop_evt       = multiprocessing.Event()

        # Handling of the Data Subscription.
        self._subscriberDic           = {}
        self._subscriptionThread      = None
        self._subscriptionSem         = threading.Semaphore(1)
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

        # List to keep track off to which Data Providers an NG/AMS
        # Server is subscribed.
        self._subscriptionStatusList  = []

        # Handling of the Mirroring Control Thread.
        self._mir_control_thread = utils.Task(ngamsMirroringControlThread.NGAMS_MIR_CONTROL_THR,
                                              ngamsMirroringControlThread.mirControlThread)
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
        self._user_service_thread = utils.Task('USER-SERVICE-THREAD', ngamsUserServiceThread.userServiceThread)

        # Handling of host info in ngas_hosts.
        self.__hostInfo               = ngamsHostInfo.ngamsHostInfo()

        # To indicate in the code where certain statments that could
        # influence the execution of the test should not be executed.
        ######self.__unitTest               = 0

        # Handling of Host Suspension.
        self.__lastReqStartTime         = 0.0
        self.__lastReqEndTime           = 0.0
        self.__nextDataCheckTime        = 0

        # The requests database. Which back-end is used depends on the
        # configuration
        self.request_db = None

        # Handling of a Cache Archive.
        self._cache_control_thread = utils.Task(ngamsCacheControlThread.NGAMS_CACHE_CONTROL_THR,
                                                ngamsCacheControlThread.cacheControlThread)

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

        # A list of all the IPv4 addresses in this computer
        # Used only for comparison purposes
        self.all_ip_addresses = []

        # Defined as <hostname>:<port>
        self.host_id   = None

        # Worker processes
        self.workers_pool = None

        # Archive subscribers. Each of these gets notified when a new archive
        # takes place
        self.archive_event_subscribers = []


        # Created by ngamsSrvUtils.handleOnline
        self.remote_subscription_creation_task = None

    def get_server_access_proto(self):
        if self._cert is not None:
            return 'https'
        return 'http'

    def load_archive_event_subscribers(self):

        # Built-in event subscriber that triggers the subscription thread
        def trigger_subscription(evt):
            logger.info("Triggering subscription thread for file %s", evt.file_id)
            self.addSubscriptionInfo([(evt.file_id, evt.file_version)], [])
            self.triggerSubscriptionThread()

        self.archive_event_subscribers = [trigger_subscription]
        for (module, clazz), pars in self.cfg.archive_evt_plugins.items():
            pars = ngamsLib.parseRawPlugInPars(pars) if pars else {}
            plugin = loadPlugInEntryPoint(module, clazz)(**pars)
            self.archive_event_subscribers.append(plugin.handle_event)

    def fire_archive_event(self, file_id, file_version):
        """Passes down the archive event to each of the archive event subscriber"""
        evt = archive_event(file_id, file_version)
        for s in self.archive_event_subscribers:
            try:
                s(evt)
            except:
                msg = ("Error while trigerring archiving event subscriber, "
                       "will continue with the rest anyway")
                logger.exception(msg)

    @property
    def serving(self):
        return self._serving

    @serving.setter
    def serving(self, value):
        if self._serving == value:
            return

        self._serving = value
        for l in self.serving_listeners:
            l(value)

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
        stdout_level = min(logcfg.stdout_level - 1, 4)
        file_level = min(logcfg.file_level - 1, 4)

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
        formatter = logutils.get_formatter(include_pid=True, include_thread_name=True)

        if log_to_file:
            checkCreatePath(os.path.dirname(logcfg.logfile))
            hnd = logutils.RenamedRotatingFileHandler(logcfg.logfile, logcfg.logfile_rot_interval, "LOG-ROTATE-%s.nglog.unsaved")
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
        return self.db


    def getCachingActive(self):
        """
        Return the value of the Caching Active Flag.

        Returns:  State of Caching Active Flag (boolean).
        """
        return self.getCfg().getCachingEnabled()

    def getDataMoverOnlyActive(self):
        """
        Return the value of the Data Mover Only Flag.

        Returns:  State of the Data Mover Only Flag (boolean).
        """
        return self._dataMoverOnly


    def updateRequestDb(self,
                        reqPropsObj):
        """
        Update an existing Request Properties Object in the Request DB.

        reqPropsObj: Instance of the request properties object (ngamsReqProps).

        Returns:     Reference to object itself.
        """

        self.request_db.update(reqPropsObj)


    def getRequest(self,
                   requestId):
        """
        Return the request handle object (ngamsReqProps) for a given
        request. If request not contained in the list, None is returned.

        requestId:     ID allocated to the request (string).

        Returns:       NG/AMS Request Properties Object or None
                       (ngamsReqProps|None).
        """
        return self.request_db.get(requestId)

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
        self.takeStateSem()
        if ( self.getState() not in allowedStates or
             self.getSubState() not in allowedSubStates):
            errMsg = [action, self.getState(), self.getSubState(),
                      str(allowedStates), str(allowedSubStates)]
            errMsg = genLog("NGAMS_ER_IMPROPER_STATE", errMsg)
            self.relStateSem()
            logger.error(errMsg)
            raise Exception(errMsg)

        if (newState != ""): self.setState(newState, updateDb)
        if (newSubState != ""): self.setSubState(newSubState)
        self.relStateSem()


    def startJanitorThread(self):
        """
        Starts the Janitor Thread.
        """
        logger.debug("Starting Janitor Thread ...")

        # Create the child process and kick it off
        self._serv_to_jan_queue = multiprocessing.Queue()
        self._jan_to_serv_queue = multiprocessing.Queue()
        self._janitorThread.start(self, self._serv_to_jan_queue, self._jan_to_serv_queue)

        # Re-create the DB connections
        self.reconnect_to_db()

        # Subscribe to db-change events (which we pass down to the janitor proc)
        self.getDb().addDbChangeEvt(self._janitordbChangeSync)

        # Kick off the thread that takes care of communicating back and forth
        self._janitorQueThread.start()


    def stopJanitorThread(self):
        """
        Stops the Janitor Thread.
        """
        self._janitorThread.stop(10)
        self._janitorQueThread.stop(10)
        self._janitorThreadRunCount = 0


    def janitorQueThread(self, stop_evt):
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

        UNSET = object()
        while not stop_evt.is_set():

            # Reading on our end
            try:
                name, item = self._jan_to_serv_queue.get(timeout=0.01)
            except Queue.Empty:
                continue


            # See what we got
            # TODO: This is obviously not scalable
            # janitor plug-ins should be able to provide this logic as a method
            # that can be invoked on them from the main server
            reply = UNSET
            if name == 'log-record':
                logger.handle(item)

            elif name == 'janitor-run-count':
                self._janitorThreadRunCount = item

            elif name == 'event-info-list':
                info = None
                if self._janitordbChangeSync.isSet():
                    info = self._janitordbChangeSync.getEventInfoList()
                    self._janitordbChangeSync.clear()
                reply = info

            elif name == 'get-request-ids':
                reply = self.request_db.keys()

            elif name == 'get-request':
                reply = self.request_db.get(item)

            elif name == 'delete-requests':
                self.request_db.delete(item)
                reply = None

            else:
                raise ValueError("Unknown item in queue: name=%s, item=%r" % (name,item))

            # Writing on our end if needed
            if reply is not UNSET:
                try:
                    self._serv_to_jan_queue.put(reply, timeout=0.01)
                except:
                    logger.exception("Problem when writing to the queue")

    def janitor_send(self, name, item=None):
        """Used by the Janitor Thread to send data to the main process"""
        self._jan_to_serv_queue.put_nowait((name, item))

    def janitor_communicate(self, name, item=None, timeout=None):
        """Used by the Janitor Thread to send data to and wait for a reply from the main process"""
        self.janitor_send(name, item)
        return self._serv_to_jan_queue.get(timeout=timeout)


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
        self._data_check_thread.start(self, self.checksum_allow_evt, self.checksum_stop_evt)


    def stopDataCheckThread(self):
        """Stop the Data Check Thread"""
        self._data_check_thread.stop(10)


    def startMirControlThread(self):
        """Starts the Mirroring Control Thread"""
        if (not self.getCfg().getMirroringActive()):
            logger.info("NGAS Mirroring not active - Mirroring Control Thread not started")
            return
        self._mir_control_thread.start(self)


    def stopMirControlThread(self):
        """Stops the Mirroring Control Thread"""
        self._mir_control_thread.stop()
        # This should bring the sub-threads to an end too, although we should
        # probably make this part of the main mirroring control thread itself
        self.triggerMirThreads()


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

        self._user_service_thread.start(self, userServicePlugIn)

    def stopUserServiceThread(self):
        """Stops the User Service Thread"""
        self._user_service_thread.stop(10)

    def startCacheControlThread(self):
        """
        Starts the Cache Control Thread.
        """

        if not self.getCachingActive():
            logger.info("NGAS Cache Service not active - will not start Cache Control Thread")
            return

        try:
            check_can_be_deleted = int(self.getCfg().getVal("Caching[1].CheckCanBeDeleted"))
        except:
            check_can_be_deleted = 0

        logger.debug("Cache Control - CHECK_CAN_BE_DELETED = %d" % check_can_be_deleted)

        ready_evt = threading.Event()
        self._cache_control_thread.start(self, ready_evt, check_can_be_deleted)
        if not ready_evt.wait(10):
            msg = ('Cache Control Thread took longer than expected to start. '
                   'This *might* cause issues during archiving, but not necessarily. Beware!')
            logger.warning(msg)


    def stopCacheControlThread(self):
        """Stop the Cache Control Thread"""
        self._cache_control_thread.stop(10)


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
        self._subscrBackLogCount -= 1
        return self._subscrBackLogCount

    def incSubcrBackLogCount(self):
        """
        Increase the Subscription Back-Log Counter.

        Returns:  Current value of the Subscription Back-Log Counter (integer).

        This is NOT thread safe
        """
        self._subscrBackLogCount += 1
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
        """
        return self.serving


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


    def getCfg(self):
        """
        Return reference to object containing the NG/AMS Configuration.

        Returns:    Reference to NG/AMS Configuration (ngamsConfig).
        """
        return self.cfg


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

    def get_self_endpoint(self):
        """
        Return an IP address that the server can use to connect to itself.
        """
        ipAddress = self.ipAddress
        ipAddress = ipAddress if ipAddress != '0.0.0.0' else '127.0.0.1'
        return ipAddress, self.portNo

    def is_it_us(self, host, port):
        """
        True if the host/port combination corresponds to an address exposed by
        this server
        """
        if port != self.portNo:
            return False

        our_ip = self.ipAddress
        if our_ip != '0.0.0.0' and our_ip == host:
            return True

        # We are exposed to all interfaces and `host` might be one of them
        # gethostbyname_ex[2] is a list of addresses
        for h in socket.gethostbyname_ex(host)[2]:
            if h in self.all_ip_addresses:
                return True
        return False

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
                    headers):
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

        # Keep track of how many requests we are serving
        with self.serving_count_lock:
            self.serving_count += 1
            self.serving = True

        # Create new request handle + add this entry in the Request DB.
        reqPropsObj = ngamsReqProps.ngamsReqProps()
        reqPropsObj.setRequestId(str(uuid.uuid4()))
        self.request_db.add(reqPropsObj)
        httpRef.ngas_request = reqPropsObj

        # Handle the request.
        try:
            self.handleHttpRequest(reqPropsObj, httpRef, clientAddress,
                                   method, path, headers)

            if not httpRef.reply_sent:
                httpRef.send_status("Successfully handled request")
        except NoSuchCommand as e:
            httpRef.send_status("Command not found", status=NGAMS_FAILURE, code=404)
        except InvalidParameter as e:
            httpRef.send_status("Invalid parameter: %s" % e.args[0], status=NGAMS_FAILURE, code=400)
        except Exception as e:

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
            if not httpRef.reply_sent:
                timeout = min((httpRef.connection.gettimeout(), 20))
                httpRef.connection.settimeout(timeout)
                httpRef.send_status(errMsg, status=NGAMS_FAILURE, code=400)

        finally:
            reqPropsObj.setCompletionTime(1)
            self.request_db.update(reqPropsObj)
            self.setLastReqEndTime()

            with self.serving_count_lock:
                self.serving_count -= 1
                if not self.serving_count:
                    self.serving = False


    def handleHttpRequest(self,
                          reqPropsObj,
                          httpRef,
                          clientAddress,
                          method,
                          path,
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

        reqPropsObj.client_addr = clientAddress[0]
        reqPropsObj.unpackHttpInfo(self.getCfg(), method, path, headers)

        try:
            ngamsAuthUtils.authorize(self.cfg, reqPropsObj)
        except ngamsAuthUtils.UnauthenticatedError as e:
            logger.warning("Unauthenticated access denied: %s", e.msg)
            httpRef.send_status(e.msg, status=NGAMS_FAILURE, code=401)
            return
        except ngamsAuthUtils.UnauthorizedError:
            msg = 'Unauthorized access denied'
            httpRef.send_status(msg, status=NGAMS_FAILURE, code=403)
            return

        ngamsCmdHandling.handle_cmd(self, reqPropsObj, httpRef)

        msg = "Total time for handling request: (%s, %s ,%s, %s): %.3f [s]"
        args = [reqPropsObj.getHttpMethod(), reqPropsObj.getCmd(),
                reqPropsObj.getMimeType(), reqPropsObj.getFileUri(),
                time.time() - req_start]

        if reqPropsObj.getIoTime() > 0:
            msg += "; Transfer rate: %s MB/s"
            args += [str(reqPropsObj.getBytesReceived() / reqPropsObj.getIoTime() / 1024.0 / 1024.0)]

        logger.info(msg, *args)


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


    def checkDiskSpaceSat(self):
        """
        This method checks the important mount points used by NG/AMS for
        the operation. If the amount of free disk space goes below 10 GB
        this is signalled by raising an exception.

        minDiskSpaceDb:   The amount of minimum free disk space (integer).

        Returns:          Void.
        """

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
                raise Exception(errMsg)


    def init(self):
        """
        Initialize the NG/AMS Server.

        argv:       Tuple containing the command line parameters to
                    the server (tuple).

        Returns:    Reference to object itself.
        """
        logger.info("NG/AMS Server version: %s", getNgamsVersion())
        logger.info("Python version: %s", re.sub("\n", "", sys.version))

        # Set up signal handlers.
        logger.debug("Setting up signal handler for SIGTERM ...")
        signal.signal(signal.SIGTERM, self.ngamsExitHandler)
        logger.debug("Setting up signal handler for SIGINT ...")
        signal.signal(signal.SIGINT, self.ngamsExitHandler)

        try:
            self.handleStartUp()
        except Exception as e:

            logger.exception("Error during startup, shutting system down")

            try:
                errMsg = genLog("NGAMS_ER_INIT_SERVER", [str(e)])
                ngamsNotification.notify(self.host_id or '', self.cfg, NGAMS_NOTIF_ERROR,
                                         "PROBLEMS INITIALIZING NG/AMS SERVER",
                                         errMsg, force=1)
            except:
                logger.exception("Error while notifying about problems in server initialization")

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
        except Exception as e:
            errMsg = "Error occurred generating PID file name. Check " +\
                     "Mount Root Directory + Port Number in configuration. "+\
                     "Error: " + str(e)
            raise Exception(errMsg)
        return pidFile


    def loadCfg(self):
        """
        Load the NG/AMS Configuration.

        Returns:   Reference to object itself.
        """

        logger.info("Loading NG/AMS Configuration from %s", self.cfg_fname)
        cfg = self.cfg
        cfg.load(self.cfg_fname)

        # Check if we should load a configuration from the DB.
        # To bootstrap this, the configuration we just loaded needs of course to
        # have at least a Db XML element so we can create the connection to the
        # database. We don't need more than one connection to the database though,
        # so we hardcode that
        if (self.__dbCfgId):
            with contextlib.closing(ngamsDb.from_config(self.cfg, maxpool=1)) as db:
                self.cfg.loadFromDb(self.__dbCfgId, db)

        cfg._check()

        logger.info("Successfully loaded NG/AMS Configuration")

    def connect_to_db(self):
        """Connect to the database"""
        self.db = ngamsDb.from_config(self.cfg)
        ngasTmpDir = ngamsHighLevelLib.getNgasTmpDir(self.cfg)
        self.db.setDbTmpDir(ngasTmpDir)

    def close_db(self):
        """Close the connections to the database"""
        if self.db:
            self.db.close()

    def reconnect_to_db(self):
        """Re-connect to the database"""
        self.close_db()
        self.connect_to_db()

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

        # Make sure we have a root directory, which is quite basic
        checkCreatePath(self.cfg.getRootDirectory())

        # Extend the system path to include anything specified in the config
        plugins_path = self.getCfg().getPluginsPath()
        if plugins_path:
            for p in plugins_path.split(':'):
                if not os.path.exists(p):
                    raise ValueError("Plugins path %s doesn't exist, check your configuration" % (p,))
                sys.path.insert(0, p)
                logger.info("Added %s to the system path", p)

        # Exactly what the name implies
        self.connect_to_db()

        # Do we need data check workers?
        if self.getCfg().getDataCheckActive():

            # When the server is idle we allow checksums to progress
            def serving_listener(serving):
                if serving:
                    logger.info("Disabling checksum calculation due to server serving requests")
                    self.checksum_allow_evt.clear()
                else:
                    logger.info("Enabling checksum calculations due to idle server")
                    self.checksum_allow_evt.set()
            self.serving_listeners.append(serving_listener)

            # Store the events globally for later usage in the process pool
            # (they cannot be passed via pool.apply or pool.map).
            # Then reset signal handlers and shutdown DB connection
            # on newly created worker processes
            def init_subproc(srvObj):

                ngamsDataCheckThread.checksum_allow_evt = srvObj.checksum_allow_evt
                ngamsDataCheckThread.checksum_stop_evt = srvObj.checksum_stop_evt

                def noop(*args):
                    pass

                signal.signal(signal.SIGTERM, noop)
                signal.signal(signal.SIGINT, noop)
                srvObj.close_db()

            n_workers = self.getCfg().getDataCheckMaxProcs()
            self.workers_pool = multiprocessing.Pool(n_workers,
                                                     initializer=init_subproc,
                                                     initargs=(self,))

        # IP address defaults to localhost
        ipAddress = self.getCfg().getIpAddress()
        self.ipAddress = ipAddress or '127.0.0.1'
        self.all_ip_addresses = get_all_ipaddrs()

        # Port number defaults to 7777
        portNo = self.getCfg().getPortNo()
        self.portNo = portNo if portNo != -1 else 7777
        self.host_id = "%s:%d" % (getHostName(), self.portNo)

        # Should be possible to execute several servers on one node.
        self.getHostInfoObj().setHostId(self.host_id)

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
        except Exception as e:
            errMsg = genLog("NGAMS_ER_INIT_LOG", [logcfg.logfile, str(e)])
            ngamsNotification.notify(self.getHostId(), self.getCfg(), NGAMS_NOTIF_ERROR,
                                     "PROBLEM SETTING UP LOGGING", errMsg)
            raise

        # Pretty clear what this does...
        self.load_archive_event_subscribers()

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

        if allowProcessingReq:
            checkCreatePath(self.cfg.getProcessingDirectory())


        # Check if there is already a PID file.
        logger.debug("Check if NG/AMS PID file is existing ...")
        if not self.force_start and os.path.exists(self.pidFile()):
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
            if path not in self.__sysMtPtDic:
                self.__sysMtPtDic[path] = []
            self.__sysMtPtDic[path].append(dirInfo)

        logger.debug("Found NG/AMS System Directories to monitor for disk space")

        # Initialize the request DB
        request_db_backend = self.getCfg().getRequestDbBackend()
        if request_db_backend == 'null':
            self.request_db = request_db.NullRequestDB()
        elif request_db_backend == 'memory':
            self.request_db = request_db.InMemoryRequestDB()
        elif request_db_backend == 'bsddb':
            cache_dir = ngamsHighLevelLib.getNgasChacheDir(self.getCfg())
            dbm_fname = os.path.join(cache_dir, '%s_REQUEST_INFO_DB' % self.host_id)
            self.request_db = request_db.DBMRequestDB(dbm_fname)
        else:
            raise Exception("Unsupported backend: %s" % request_db_backend)

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
        if self.autoonline:
            logger.info("Auto Online requested - server going to Online State ...")
            try:
                ngamsSrvUtils.handleOnline(self)
            except:
                if not self.no_autoexit:
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
        except Exception as e:
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

    def ngamsExitHandler(self, signal_no, stack_frame):
        """NGAS exit Handler function, given to signal.signal"""

        if self.__handling_exit:
            logger.info('Already handling exit signal')
            return
        self.__handling_exit = True
        logger.info("In NG/AMS Exit Handler - received signal: %d", signal_no)
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
        if self.workers_pool:
            self.workers_pool.close()
            self.workers_pool.join()
        show_threads()

        # Close all connections to the database, please
        self.close_db()

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
        if self._janitorThread._bg_task is not None:
            try:
                os.kill(self._janitorThread._bg_task.pid, signal.SIGKILL)
            except:
                logger.warning("No Janitor process was found: %s. ")

        #Now kill the server itself
        pid = os.getpid()
        os.kill(pid, signal.SIGKILL)

    ########################################################################
    # The following methods are used for the NG/AMS Unit Tests.
    # The method do not contain any code, but in the Unit Test code it is
    # possible to override these methods to give the server a specific,
    # usually abnormal, behavior, e.g. to simulate that the server crashes.
    ########################################################################
    def test_BeforeArchCleanUp(self):
        """
        Test method invoked in NG/AMS Server during the Archive handling,
        before deleting the Original Staging File and the Request Propeties
        File.
        """
        pass
    ########################################################################


def _parse_and_run(args, prog, server_class):

    # The old parser supported case-insensitive argument names.
    # We want to be nice, so we support them too (for a while)
    _lower = ('-version', '-license', '-cfg', '-path', '-autoonline', '-force',
              '-v', '-loclogfile', '-locloglevel', '-syslog', '-syslogprefix',
              '-dbcfgid', '-noautoexit', '-datamover')
    modified_args = []
    for arg in args:
        larg = arg.lower()
        if larg in _lower:
            if larg != arg:
                print("WARNING: case-insenstive command-line option names are "
                      "deprecated and will be removed in the future. To avoid this "
                      "message change '%s' by '%s'" % (arg, larg))
            modified_args.append(larg)
        else:
            modified_args.append(arg)

    parser = argparse.ArgumentParser(prog=prog, epilog=ngamsCopyrightString())

    genopts = parser.add_argument_group('General options')
    genopts.add_argument('-version', action='store_true', help='Show version and exit')
    genopts.add_argument('-license', action='store_true', help='Show license and exit')

    startopts = parser.add_argument_group('Startup options')
    startopts.add_argument('-cfg', help='Path to server XML configuration file')
    startopts.add_argument('-path', help='Colon-separated list of extra directories containing NGAS plugins')
    startopts.add_argument('-autoonline', action='store_true', help='Automatically set the server in the ONLINE state when starting')
    startopts.add_argument('-force', action='store_true', help='Force the start of the server, even if a PID file is present')

    logopts = parser.add_argument_group('Logging options')
    logopts.add_argument('-v', type=int, help='stdout verbosity level (5=DEBUG, 4=INFO, 3=WARN, 2=ERROR, 1=CRITICAL)', default=3)
    logopts.add_argument('-loclogfile', help='The location of the server logfile')
    logopts.add_argument('-locloglevel', type=int, help='logfile verbosity level (see -v), defaults to configuration file setting', default=None)
    logopts.add_argument('-syslog', action='store_true', help='Enable syslog logging')
    logopts.add_argument('-syslogprefix', help='Syslog log prefix')

    advopts = parser.add_argument_group('Advanced options')
    advopts.add_argument('-dbcfgid', help='ID of the configuration in the database that should be loaded')
    advopts.add_argument('-noautoexit', action='store_true', help='Do not automatically shutdown the server on startup failures')
    advopts.add_argument('-datamover', action='store_true', help="Start this server as a DataMover server")
    advopts.add_argument('-cert', action='store', help="Use cert for tls tests PRIVATE DON'T USE")

    opts = parser.parse_args(modified_args)
    if opts.version:
        print(getNgamsVersion())
        return 0
    if opts.license:
        print(getNgamsLicense())
        return 0

    if not opts.cfg:
        parser.error('No configuration file specified via -cfg')

    if opts.path:
        for p in set(filter(None, opts.path.split(os.pathsep))):
            p = os.path.expanduser(p)
            if not os.path.exists(p):
                raise ValueError("Path %s doesn't exist" % (p,))
            sys.path.insert(0, p)

    server = server_class(opts.cfg, _cert=opts.cert)
    server.autoonline = opts.autoonline
    server.force_start = opts.force

    server.logcfg.stdout_level = opts.v
    server.logcfg.file_level = opts.locloglevel
    server.logcfg.logfile = opts.loclogfile
    server.logcfg.syslog = opts.syslog
    server.logcfg.syslog_prefix = opts.syslogprefix

    server.no_autoexit = opts.noautoexit
    server._dataMoverOnly = opts.datamover
    server.__dbCfgId = opts.dbcfgid

    server.init()

def main(args=None, prog='ngamsServer', server_class=ngamsServer):
    if args is None:
        args = sys.argv[1:]
    _parse_and_run(args, prog, server_class)

if __name__ == '__main__':
    main()