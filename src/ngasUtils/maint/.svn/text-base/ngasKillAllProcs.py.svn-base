

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
# "@(#) $Id: ngasKillAllProcs.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  07/10/2003  Created
#

"""
Kill all NG/AMS processes found for this user on the system.
"""

import sys, commands


if __name__ == '__main__':
    """
    Main program.
    """
    if (len(sys.argv) > 1):
        pattern = sys.argv[1]
    else:
        pattern = ""

    stat, out1 = commands.\
                 getstatusoutput("ps -ef |grep python|grep -v ngasKill")
    stat, out2 = commands.\
                 getstatusoutput("ps -ef |grep ngams|grep -v ngasKill")
    for grepLine in (out1 + out2).split("\n"):
        grepLine = grepLine.strip()
        print grepLine
        if ((grepLine != "") and (grepLine.find(pattern) != -1)):
            print "Killing process: " + grepLine
            grepLineEls = grepLine.split(" ")
            idx = 1
            while idx < len(grepLineEls):
                if (grepLineEls[idx] != ""):
                    pid = grepLineEls[idx]
                    break
                idx += 1
            killCmd = "kill -9 %s" % pid
            print "Executing: %s" % killCmd
            commands.getstatusoutput(killCmd)


# EOF
