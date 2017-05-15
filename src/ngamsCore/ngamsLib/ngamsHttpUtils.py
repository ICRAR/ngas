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

import cStringIO
import contextlib
import httplib
import logging
import socket
import time
import urllib
import urlparse

from .ngamsCore import getHostName, NGAMS_HTTP_POST, NGAMS_HTTP_GET


logger = logging.getLogger(__name__)

_http_fmt = "%a, %d %b %Y %H:%M:%S GMT"
def httpTimeStamp():
    """
    Generate a time stamp in the 'HTTP format', e.g.:

        'Mon, 17 Sep 2001 09:21:38 GMT'

    Returns:  Timestamp (string).
    """
    return time.strftime(_http_fmt, time.gmtime(time.time()))


def _http_response(host, port, method, cmd,
                 data=None, timeout=None,
                 pars=[], hdrs={}):

    # Prepare all headers that need to be sent
    hdrs = dict(hdrs)
    hdrs["Host"] = getHostName()

    url = cmd
    if pars:
        # urlib.urlencode expects tuple elements (if pars is a list)
        if not hasattr(pars, 'items'):
            pars = [(p[0], p[1]) for p in pars]
        pars = urllib.urlencode(pars)
        url += '?' + pars

    # Go, go, go!
    logger.info("About to %s to %s:%d/%s", method, host, port, url)
    conn = httplib.HTTPConnection(host, port, timeout = timeout)
    try:
        conn.request(method, url, body=data, headers=hdrs)
        logger.debug("%s request sent to, waiting for a response", method)
    except socket.error:
        try:
            conn.close()
        except:
            pass
        raise

    return conn.getresponse()


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

    resp = _http_response(host, port, NGAMS_HTTP_POST, cmd, data, timeout, pars, hdrs)
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
        if "content-length" in hdrs:
            size = int(hdrs["content-length"])

        # Accumulate the incoming stream and return it whole in `data`
        bs = 65536
        with contextlib.closing(cStringIO.StringIO()) as out:
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
    hdrs = dict(hdrs)
    if auth:
        hdrs['Authorization'] = auth.strip()
    return _http_response(host, port, NGAMS_HTTP_GET, cmd,
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