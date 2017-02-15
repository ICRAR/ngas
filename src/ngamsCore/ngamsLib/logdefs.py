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
Contains the Log Definition handling classes.

These were initially part of the "pcc" python package, but after the big pcc
cleanup that happened in NGAS these were the only pcc-related classes handling
around and thus were incorporated into NGAS and simplified a bit
"""

import collections
import re
import xml.dom.minidom

#: A single log definition
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
            text = re.sub("\s *", " ", text)
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
                if isinstance(p, basestring):
                    p = p.replace("\n", " ")
                sanitized.append(p)
            return text % tuple(sanitized)
        return text