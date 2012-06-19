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
# "@(#) $Id: ngamsTestExecTillFailure.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  12/07/2004  Created
#

"""
Execute a command on the sheel until a failure is encountered. This is used
to execute a Test Case/Test Suite until a problem occurrs, which is not
reproduceable unless the command is repeatitively executed.

A failure is detected by the following:

  - The output on stdout/stderr contains the word 'FAILURE' or 'FAILED'.
  - The return value of the command is different from 0.

When an error is detected, the utility bails out.

An exemple of executing this tool is:

% python ngamsTestExecTillFailure.py -cmd \
  'python ngamsCloneCmdTest.py -nocleanup -tests test_CloneCmd_2'
% Executing command for the 1th time (Time difference: 0.0s/Total time: 0.0s
% Executing command for the 2th time (Time difference: 20.7s/Total time: 20.7s 
"""

import os, sys, time, commands
from   ngams import *
from   ngamsTestLib import *


def execTillFailure(cmd):
    """
    Execute the command given on the command line repeatedly until a failure
    occurrs.

    cmd:       Shell command to execute (string).

    Returns:   Void.
    """
    startTime = prevTime = time.time()
    count = 0
    logLine1 = "Executing command for the %dth time (Time difference: " +\
               "%.1fs/Total time: %.1fs"
    while (1):
        count += 1
        timeNow = time.time()
        print logLine1 % (count, (timeNow - prevTime), (timeNow - startTime))
        prevTime = timeNow
        stat, out = commands.getstatusoutput(cmd)
        if (stat or (out.find("FAILED") != -1) or (out.find("FAILURE") != -1)):
            print "Failure occurred after %.1fs. Output:\n\n%s\n\n" %\
                  ((time.time() - startTime), out)
            break


def correctUsage():
    """
    """
    print "Correct usage is:\n\n" +\
          "% ngamsTestExecTillFailure -cmd <Command>\n\n"


if __name__ == '__main__':
    """
    Main program to execte the given command until a failure is detected.
    """
    execTillFailure(sys.argv[2])


# EOF
