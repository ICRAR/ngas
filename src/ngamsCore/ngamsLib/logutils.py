#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2016
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
Contains several logging utilities, such as NGAS log definition loading logic
and file rotation with renaming.
"""
import collections
import logging.handlers
import os
import re
import shutil
import time
import xml.dom.minidom

import six


# A single log definition
log_def = collections.namedtuple('log_def', 'id number text type description')

class LogDefHolder(object):
    """
    Object to hold the information from an XML Log Definition File.
    It is possible to load, Log Definition Files, and to generate
    the logs.
    """

    def __init__(self, f):
        """
        Constructor method. Loads the log definitions from f
        """
        self.logdefs = {}
        self.unpackXmlDoc(f)

    def unpackXmlDoc(self, f):
        """
        Unpack the Log Definition XML Document and set the members
        of the class accordingly. The XML document must be loaded
        into a string buffer.

        doc:          Log File Defintion XML Document (string).

        Returns:      Reference to object itself (PccLogDef).
        """
        dom = xml.dom.minidom.parse(f)
        nodeList = dom.getElementsByTagName("LogDef")
        if (len(nodeList) == 0):
            errMsg = "Log Definition XML Document, does not have the " +\
                     "proper root element: LogDef!"
            raise Exception(errMsg)

        # Unpack the document.
        logDefList = nodeList[0].getElementsByTagName("LogDefEl")
        for node in logDefList:

            lid = node.getAttribute("LogId")
            lnumber = node.getAttribute("LogNumber")
            ltype = node.getAttribute("LogType")

            # Get the Log Text
            tmpNodeList = node.getElementsByTagName("LogText")
            text = []
            for nd in tmpNodeList[0].childNodes:
                if (nd.nodeType == node.TEXT_NODE):
                    text.append(nd.data.strip(" \n"))
            # Remove newline characters and ensure that there is no
            # sequence of blanks.
            text = ' '.join(text)
            text = text.replace("\n", "")
            text = re.sub(r"\s *", " ", text)
            ltext = text

            # Get the Log Description
            tmpNodeList = node.getElementsByTagName("Description")
            text = ""
            for nd in tmpNodeList[0].childNodes:
                if (nd.nodeType == node.TEXT_NODE):
                    text = text + nd.data.strip()
            ldesc = text.strip(" \n")

            if lid in self.logdefs:
                raise ValueError("Repeated log definition %s" % (lid,))
            self.logdefs[lid] = log_def(lid, lnumber, ltext, ltype, ldesc)


    def generate_log(self, logId, *log_pars):
        """
        Get a log line by filling in the parameters giving in a list
        (if any). As a prefix to the log line, the following information
        is added. The format of the generated log line is as follows:

          <Log ID>:<Log Number>:<Log Type>: <Log Messge>

        """
        logdef = self.logdefs[logId]
        text = "%s:%s:%s: %s" % (logdef.id, logdef.number, logdef.type, logdef.text)
        if log_pars:
            sanitized = []
            for p in log_pars:
                if isinstance(p, six.string_types):
                    p = p.replace("\n", " ")
                sanitized.append(p)
            return text % tuple(sanitized)
        return text

class RenamedRotatingFileHandler(logging.handlers.BaseRotatingHandler):
    """
    Logging handler that rotates periodically a logfile using a new name.
    At close() time it also makes sure the current logfile is also rotated,
    whatever its size.

    This class is basically a strip-down version of TimedRotatingFileHandler,
    without all the complexities of different when/interval combinations, etc.
    """

    def __init__(self, fname, interval, rotated_fname_fmt):
        logging.handlers.BaseRotatingHandler.__init__(self, fname, mode='a')
        self.rotated_fname_fmt = rotated_fname_fmt
        self.interval = interval
        self.rolloverAt = self.interval + time.time()

    def shouldRollover(self, record):
        return time.time() >= self.rolloverAt

    def _rollover(self):
        if not os.path.exists(self.baseFilename):
            return
        if self.stream:
            self.stream.close()
        # We import ngamsCore here to avoid a top-level circural dependency
        # In the future that circular dependency would disappear if genLog lived
        # in this module
        from . import ngamsCore
        # It's time to rotate the current Local Log File.
        dirname = os.path.dirname(self.baseFilename)
        rotated_name = self.rotated_fname_fmt % (ngamsCore.toiso8601(),)
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

def get_formatter(fmt=None, include_thread_name=False, include_pid=False):
    """Get a "standard" NGAS log formatter, so logs produced by different tools
    look similar"""
    if fmt and (include_thread_name or include_pid):
        raise ValueError('no other argument allowed when fmt is given')
    if not fmt:
        fmt = '%(asctime)-15s.%(msecs)03d '
        if include_pid:
            fmt += '[%(process)5d] '
        if include_thread_name:
            fmt += '[%(threadName)10.10s] '
        fmt += '[%(levelname)6.6s] %(name)s#%(funcName)s:%(lineno)s %(message)s'
    datefmt = '%Y-%m-%dT%H:%M:%S'
    formatter = logging.Formatter(fmt, datefmt=datefmt)
    formatter.converter = time.gmtime
    return formatter

class ForwarderHandler(logging.Handler):
    """A handler that formats log records for forwarding to different processes.
    The actual forwarding function is given by the user"""

    def __init__(self, fwd):
        super(ForwarderHandler, self).__init__()
        self.fwd = fwd

    def _format_record(self, record):
        # ensure that exc_info and args
        # have been stringified. Removes any chance of
        # unpickleable things inside and possibly reduces
        # message size sent over the pipe.
        if record.args:
            record.msg = record.msg % record.args
            record.args = None
        if record.exc_info:
            self.format(record)
            record.exc_info = None
        return record

    def emit(self, record):
        try:
            self.fwd(self._format_record(record))
        except:
            self.handleError(record)