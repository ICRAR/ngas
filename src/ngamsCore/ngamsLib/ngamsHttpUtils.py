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
Module containing HTTP utility code (mostly client-side)
"""

import contextlib
import errno
import io
import logging
import os
import socket
import time
import sys

from six.moves import http_client as httplib  # @UnresolvedImport
from six.moves.urllib import parse as urlparse  # @UnresolvedImport

logger = logging.getLogger(__name__)



_connect_retries = 5
_connect_retries_period_ms = 10
if 'NGAS_HTTP_CONNECT_RETRIES' in os.environ:
    _connect_retries = int(os.environ['NGAS_HTTP_CONNECT_RETRIES'])
if 'NGAS_HTTP_CONNECT_RETRIES_PERIOD_MS' in os.environ:
    _connect_retries_period_ms = int(os.environ['NGAS_HTTP_CONNECT_RETRIES_PERIOD_MS'])

def _connect(conn):
    # If the server on the other side has its backlog of connections full
    # it will react differently depending on the OS it is running on.
    # Linux will simply not respond the SYN packet sent by this client,
    # triggering a few internal retries before giving up. On the other
    # hand BSDs (including MacOS) will respond with RST, issuing a
    # ECONNRESET error here. We thus deal with that particular error at
    # this level, re-trying a few times before fully giving up
    ntry = 0
    while True:
        try:
            conn.connect()
            return
        except socket.error as e:

            if e.errno != errno.ECONNRESET:
                raise

            ntry += 1
            if ntry == _connect_retries:
                raise

            # We do increasing sleeps, kind of "a la TCP"
            ms = _connect_retries_period_ms * ntry
            logger.warning('Server did not accept() connection, retying in %d [ms] (try %d/%d)', ms, ntry,  _connect_retries)
            time.sleep(0.001 * ms)


def _http_response(host, port, method, cmd,
                 data=None, timeout=None,
                 pars=[], hdrs={}):

    # Prepare all headers that need to be sent
    hdrs = dict(hdrs)

    # In python 3.6 the http.client module changed how it uses the body of a
    # request to automatically calculate the Content-Length header, if none has
    # been previously specified.
    # In particular, file objects before 3.6 were previously automatically
    # handled by calling fstat(f).st_size on them. In 3.6 they now do not yield
    # a Content-Length header, but instead are they are sent using chunked
    # transfer encoding, which we do not support explicitly on the server side)
    #
    # In several places throughout the code we trusted on the pre-3.6 rules,
    # so here we exercise them manually for 3.6+
    if (data is not None and
        sys.version_info >= (3, 6, 0) and
        'content-length' not in hdrs and
        'Content-Length' not in hdrs):
        try:
            thelen = len(data)
        except (TypeError, AttributeError):
            try:
                thelen = os.fstat(data.fileno()).st_size
            except (AttributeError, OSError):
                thelen = None
        if thelen is not None:
            hdrs['Content-Length'] = thelen

    url = cmd
    if pars:
        # urlib.urlencode expects tuple elements (if pars is a list)
        if not hasattr(pars, 'items'):
            pars = [(p[0], p[1]) for p in pars]
        pars = urlparse.urlencode(pars)
        url += '?' + pars

    # Go, go, go!
    logger.info("About to %s to %s:%d/%s", method, host, port, url)
    conn = httplib.HTTPConnection(host, port, timeout = timeout)
    _connect(conn)

    try:
        conn.request(method, url, body=data, headers=hdrs)
        logger.debug("%s request sent to, waiting for a response", method)
    except socket.error as e:

        # If the server closes the connection while we write data
        # we still try to read the response, if any
        #
        # In OSX >= 10.10 this error can come up as EPROTOTYPE instead of EPIPE
        # (although the error code is not mentioned in send(2)). The actual
        # error recognised by the kernel in this situation is slightly different,
        # but still due to remote end closing the connection. For a full, nice
        # explanation of this see:
        #
        # https://erickt.github.io/blog/2014/11/19/adventures-in-debugging-a-potential-osx-kernel-bug/
        tolerate = e.errno in (errno.EPROTOTYPE, errno.EPIPE)
        if not tolerate:
            try:
                conn.close()
            except:
                pass
            raise

    start = time.time()
    response = conn.getresponse()
    logger.debug("Response to %s request received within %.4f [s]", method, time.time() - start)

    return response


def httpPost(host, port, cmd, data, mimeType, pars=[], hdrs={},
             timeout=None, contDisp=None, auth=None):
    """
    Sends `data` via HTTP POST to http://host:port/cmd.

    If a `timeout` is specified, it is applied to the HTTP connection; otherwise
    the system-wide default timeout will apply.
    `mimeType` is the value for the 'Content-Type` header, and is required.
    `auth` and `contDisp` are the (optional) values for the 'Authentication' and
    'Content-Disposition' headers respectively.
    Additional HTTP parameters can be passed as a list of 2-elements tuples
    via `pars`.
    Additional headers can be passed as a dictionary via `hdrs`.
    """

    logger.debug("About to POST to %s:%d/%s", host, port, cmd)

    # Prepare all headers that need to be sent
    hdrs = dict(hdrs)
    hdrs["Content-Type"] = mimeType
    if contDisp:
        hdrs["Content-Disposition"] = contDisp
    if auth:
        hdrs["Authorization"] = auth.strip()

    resp = _http_response(host, port, 'POST', cmd, data, timeout, pars, hdrs)
    with contextlib.closing(resp):

        # Receive + unpack reply.
        reply, msg, hdrs = resp.status, resp.reason, resp.getheaders()

        # Dump HTTP headers if Verbose Level >= 4.
        hdrs = {h[0]: h[1] for h in hdrs}
        logger.debug("HTTP Header: HTTP/1.0 %d %s", reply, msg)
        if logger.isEnabledFor(logging.DEBUG):
            for hdr in hdrs.keys():
                logger.debug("HTTP Header: %s: %s", hdr, hdrs[hdr])

        # How much do we need to read?
        size = 0
        if "content-length" in resp.msg:
            size = int(resp.msg["content-length"])

        # Accumulate the incoming stream and return it whole in `data`
        bs = 65536
        with contextlib.closing(io.BytesIO()) as out:
            readin = 0
            while readin < size:
                left = size - readin
                buff = resp.read(bs if left >= bs else left)
                if not buff:
                    raise Exception('error reading data')
                out.write(buff)
                readin += len(buff)
            data = out.getvalue()

        return [reply, msg, hdrs, data]


def httpPostUrl(url, data, mimeType, hdrs={},
                timeout=None, contDisp=None, auth=None):
    """
    Like `httpPost` but specifies a HTTP url instead of a combination of
    host, port and command.
    """
    url = urlparse.urlparse(url)
    pars = [] if not url.query else urlparse.parse_qsl(url.query)
    return httpPost(url.hostname, url.port, url.path, data, mimeType,
                    pars=pars, hdrs=hdrs, timeout=timeout,
                    contDisp=contDisp, auth=auth)


def httpGet(host, port, cmd, pars=[], hdrs={},
            timeout=None, auth=None):
    """
    Performs an HTTP GET request to http://host:port/cmd and
    returns an HTTP response object from which the response can be read.
    It is the callers' responsibility to close the response object,
    which in turn will close the HTTP connection.
    """
    hdrs = dict(hdrs) if hdrs else {}
    if auth:
        hdrs['Authorization'] = auth.strip()
    return _http_response(host, port, 'GET', cmd,
                          pars=pars, hdrs=hdrs, timeout=timeout)


def httpGetUrl(url, pars=[], hdrs={}, timeout=None, auth=None):
    """
    Like `httpGet`, but specifies a HTTP url instead of a combination of
    host, port and command.
    """
    url = urlparse.urlparse(url)
    pars = [] if not url.query else urlparse.parse_qsl(url.query)
    return httpGet(url.hostname, url.port, url.path,
                   pars=pars, hdrs=hdrs, timeout=timeout)

class sizeaware(object):
    """
    Small utility class that wraps a file object that doesn't have a __len__
    method and makes it aware of its size. Useful to present the body of an HTTP
    request or response as a file object with a length.
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