

#
#    ALMA - Atacama Large Millimiter Array
#    (c) European Southern Observatory, 2002
#    Copyright by ESO (in the framework of the ALMA collaboration),
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
# "@(#) $Id: ngasDumpGdbm.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  13/10/2005  Created
#

_doc =\
"""
Tool to dump the contents of a GDBM DBM.

The defined input parameters to the tool are:

%s

"""

import sys, os, time, getpass

import pcc, PccUtTime

from ngams import *
import ngamsDbm, ngamsLib, ngamsFileInfo
import ngamsPClient
import ngasUtils
from ngasUtilsLib import *

# Constants.
NGAS_TOOL_ID = "NGAS_DUMP_GDBM"

# Definition of predefined command line parameters.
_options = [\
    ["dbm", [], 0, NGAS_OPT_MAN, "",
     "Name of DBM to dump."]]
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
    T = TRACE()

    if (optDic["help"][NGAS_OPT_VAL]):
        print correctUsage()
        sys.exit(0)

    dbm = ngamsDbm.ngamsDbm2(optDic["dbm"][NGAS_OPT_VAL])
    dbm.initKeyPtr()
    print "DUMPING CONTENTS OF GDBM DBM: %s\n" % optDic["dbm"][NGAS_OPT_VAL]
    while (True):
        key, obj = dbm.getNext()
        if (not key): break
        print "%s: %s" % (str(key), str(obj))
        

if __name__ == '__main__':
    """
    Main function to execute the tool.
    """
    optDic = parseCmdLine(sys.argv, getOptDic())
    execute(optDic)
                  

# EOF
