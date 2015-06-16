import sys, time, urllib

from ngamsLib.ngamsCore import info, NGAMS_SUCCESS, setLogCond
from ngamsPClient import ngamsPClient
from ngasUtils.src.ngasUtilsLib import NGAS_OPT_MAN, NGAS_OPT_OPT, \
    genOptDicAndDoc, NGAS_OPT_VAL, parseCmdLine


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
# "@(#) $Id: ngasQuery.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  06/03/2007  Created
#
_doc =\
"""
The ngasQuery Tool is used to query the associated DB. The query result is
returned.

%s

"""



# Definition of predefined command line parameters.
_options = [\
    ["query", [], None, NGAS_OPT_MAN, "=<SQL Query>",
     "SQL query to execute"],
    ["server", [], None, NGAS_OPT_MAN, "=<Server:Port>",
     "NGAS Server to contact"],
    ["format", [], None, NGAS_OPT_OPT, "=<Format>",
     "Format output before returning it"],
    ["reload", [], None, NGAS_OPT_OPT, "",
     "Reload the command module."]]
_optDic, _optDoc = genOptDicAndDoc(_options)
__doc__ = _doc % _optDoc


def getOptDic():
    """
    Return reference to command line options dictionary.

    Returns:  Reference to dictionary containing the command line options
              (dictionary).
    """
    return _optDic


def correctUsage():
    """
    Return the usage/online documentation in a string buffer.

    Returns:  Man-page (string).
    """
    return __doc__


def execute(optDic):
    """
    Carry out the tool execution.

    optDic:    Dictionary containing the options (dictionary).

    Returns:   Void.
    """
    info(4,"Entering execute() ...")
    if (optDic["help"][NGAS_OPT_VAL]):
        print correctUsage()
        sys.exit(0)

    # Submit the query.
    client = ngamsPClient.ngamsPClient()
    client.parseSrvList(optDic["SERVER"][NGAS_OPT_VAL])
    parameters = [["query", urllib.quote(optDic["QUERY"][NGAS_OPT_VAL])]]
    if (optDic["format"][NGAS_OPT_VAL]):
        parameters.append(["format", optDic["FORMAT"][NGAS_OPT_VAL]])
    if (optDic["reload"][NGAS_OPT_VAL]):
        parameters.append(["reload", "1"])
    startTime = time.time()
    stat = client.sendCmdGen("", -1, "QUERY", pars=parameters)
    stopTime = time.time()
    print "# Status: %s" % stat.getStatus()
    print "# Query:  %s" % optDic["QUERY"][NGAS_OPT_VAL]
    print "# Time:   %.6fs\n" % (stopTime - startTime)

    # Handle the response, dump the result to stdout in case of success.
    if (stat.getStatus() == NGAMS_SUCCESS):
        print stat.getData()
    else:
        msg = "Error occurred executing query: %s" % stat.getMessage()
        raise Exception, msg
    
    info(4,"Leaving execute()")


if __name__ == '__main__':
    """
    Main function to execute the tool.
    """
    try:
        optDic = parseCmdLine(sys.argv, getOptDic())
    except Exception, e:
        print "\nProblem executing the tool:\n\n%s\n" % str(e)
        sys.exit(1)
    setLogCond(0, "", 0, "", 1)
    execute(optDic)

# EOF
