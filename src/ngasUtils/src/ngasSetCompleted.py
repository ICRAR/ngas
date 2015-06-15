

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
# "@(#) $Id: ngasSetCompleted.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  23/11/2006  Created
#

_doc =\
"""
The ngasSetCompleted Tool is used to control the 'completed flag' of NGAS 
Disks. It is possible to mark a disk explictly as completed if needed.
This is useful when it is desirable to treat a disk as completed, although
in reality it is not completed.

It is also possible to reset the completed flag.

The defined input parameters to the tool are:

%s

"""

import sys, os, time, getpass

import pcc, PccUtTime

from ngams import *
import ngamsDb
import ngamsLib
import ngamsPClient
import ngasUtils
from ngasUtilsLib import *
import ngasUtilsLib


# Definition of predefined command line parameters.
_options = [\
    ["disk-id", [], None, NGAS_OPT_MAN, "=<Disk ID>",
     "Disk ID of disk to consider."],
    ["set-uncompleted", [], 0, NGAS_OPT_OPT, "",
     "Set the Completed Flag to uncompleted."]]
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

    # Update the Completed Flag for the disk in question.
    if (optDic["set-uncompleted"][2]):
        completed = 0
    else:
        completed = 1
    server, db, user, password = ngasUtilsLib.getDbPars()
    dbCon = ngamsDb.ngamsDb(server, db, user, password, 0)
    sqlQuery = "UPDATE ngas_disks SET completed=%d WHERE disk_id='%s'" %\
               (completed, optDic["disk-id"][2])
    dbCon.query(sqlQuery)
    
#    # Restart server to update NgasDiskInfo file.
#    host = ngasUtilsLib.getParNgasRcFile(ngasUtilsLib.NGAS_RC_PAR_HOST)
#    port = int(ngasUtilsLib.getParNgasRcFile(ngasUtilsLib.NGAS_RC_PAR_PORT))
#    # IMPL: Check that the NGAS Server specified is running locally.
#    msg = "Reinitialize the NGAS Node (%s:%d) (y/n) [n]?" % (host, port)
#    restart = ngasUtilsLib.input(msg)
#    if ((restart.upper() == "Y") or (restart.upper() == "YES")):
#        info(1,"Reinitializing NGAS Server ...")
#        ngasClient = ngamsPClient.ngamsPClient(host, port)
#        stat = ngasClient.init()
#        if (stat.getStatus() == NGAMS_SUCCESS):
#            info(1,"NGAS Server successfully reinitialized")
#        else:
#            info(1,"NGAS Server could not be reinitialized. Error: %s" %\
#		 stat.getMessage())
#    else:
#        info(1,"NOTE: Update of disk status first complete when NGAS Server "+\
#	     "brought to OFFLINE!")

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
