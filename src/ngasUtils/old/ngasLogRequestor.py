

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
# "@(#) $Id: ngasLogRequestor.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  04/02/2003  Created
#

"""
Extract part of an NG/AMS Log File.
"""

import sys, os


def getFileSize(filename):
    """
    Get size of file referred.

    filename:   Filename - complete path (string).

    Returns:    File size (integer).
    """
    return int(os.stat(filename)[6])


def correctUsage():
    """
    """
    print "Correct usage is:"
    print ""
    print "ngasLogRequestor(.py) <index> | INIT [<log file>]"
    print ""


if __name__ == '__main__':
    """
    Main function.
    """
    # Expect: ngasLogRequestor <index> | INIT [<log file>]
    if (len(sys.argv) < 2):
        correctUsage()
        sys.exit(1)

    index = sys.argv[1]
    if (len(sys.argv) > 2):
        logFile = sys.argv[2]
    else:
        logFile = "/NGAS/ngams_staging/log/LogFile.nglog"

    # If the index is INIT, we simply return the size of the log file.
    logFileSize = getFileSize(logFile)
    if (index == "INIT"):
        print str(logFileSize)
        sys.exit(0)
    else:
        index = int(index)

    # If the index is larger than the present file size, we set the index
    # to 0 (the log file was re-initialized), and start again.
    if (logFileSize < index): index = 0

    # Read the difference between the index and the present size of the log
    # file into a buffer, and write this together with the new index
    # (size of log file on stdout).
    fo = None
    try:
        fo = open(logFile)
        fo.seek(index)
        buf = fo.read(logFileSize - index)
        fo.close()
        print str(logFileSize) + "\n" + buf
    except Exception, e:
        if (fo): fo.close()
        sys.stderr.write("ERROR: " + str(e))
        sys.exit(1)

    sys.exit(0)


# EOF
