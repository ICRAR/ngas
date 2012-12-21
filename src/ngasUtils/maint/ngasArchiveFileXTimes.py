

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
# "@(#) $Id: ngasArchiveFileXTimes.py,v 1.6 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  16/07/2003  Created
#

"""
The script is used to archive a file a specified number of times.
"""

import sys

import pcfitsio
import pcc, PccUtTime

from ngams import *
import ngamsPClient


def correctUsage():
    """
    Print correct tool usage on stdout.

    Returns:   Void.
    """
    print "\nCorrect usage:\n"
    print "% ngasArchiveFileXTimes.py -f <filename> -n <# of times> " +\
          "-p <port #> [-h <host name>] | " +\
          "-servers '<Host>:<Port>, ...' | -targetDir <Directory>\n " +\
          "[-inc] [-auth <Code>]\n" 


def archiveXTimes(filename,
                  number,
                  hostName,
                  port,
                  targetDir,
                  ouputFile,
                  increment,
                  auth):
    """
    Archive the given FITS file X times. Each time the file is archived the
    value of the ARCFILE keyword can be incremented to enforce a new
    File ID.
    
    filename:      FITS file to archive (string).
    
    number:        Number of times to archive the FITS file (integer).
    
    hostName:      Host name of remote NG/AMS Server. Can also be
                   a list of servers (string).

    port:          Port number of remote NG/AMS Server (integer).

    targetDir:     If specified, the created files are merely dumped in the
                   given directory (string).
   
    ouputFile:     Output log file (string).
    
    increment:     If set to 1 the ARCFILE keyword is incremented 1ms for
                   each archiving to create a new File ID (integer/0|1).

    auth:          Authentication code (string).

    Returns:       Void.
    """
    if (not targetDir):
        # Carry out the archive session.
        client = ngamsPClient.ngamsPClient(hostName, port).\
                 setAuthorization(auth)
    
        # Host list given?
        if (hostName.find(":") != -1): client.parseSrvList(hostName)

    reqTimer = PccUtTime.Timer()
    totTime  = 0.0
    if (ouputFile): fo = open(ouputFile, "w")
    format = "Status of Archive Request number: %d: %s. Time: %.3fs. " +\
             "Total time: %.3fs."

    if (increment):
        tmpFilename = os.path.normpath("/tmp/tmp__" +
                                       os.path.basename(filename))
        os.system("cp " + filename + " " + tmpFilename)
        commands.getstatusoutput("add_chksum " + tmpFilename)
    else:
        tmpFilename = filename

    ts = None
    for n in range(1, (number + 1)):
        reqTimer.start()
        if (targetDir):
            baseName = os.path.basename(tmpFilename)[4:]
            tmpTargetFilename = os.path.join(targetDir, "." + str(n) + "_" +\
                                             baseName)
            targetFilename = os.path.join(targetDir, str(n) + "_" + baseName)
            print "Creating file: %s" % targetFilename
            os.system("cp %s %s" % (tmpFilename, tmpTargetFilename))
            mvFile(tmpTargetFilename, targetFilename)
            reqTime = reqTimer.stop()
            totTime += reqTime            
        else:
            print "Archiving file: %s for the time number: %d." %\
                  (tmpFilename, (n+1))
            status = client.archive(tmpFilename)
            reqTime = reqTimer.stop()
            totTime += reqTime
            if (ouputFile):
                fo.write("%-4d %.3f\n" % ((n + 1), reqTime))
                fo.flush()
            print format % ((n+1), status.getStatus(), reqTime, totTime)

        # Increment the ARCFILE keyword.
        if (increment):
            fptr = pcfitsio.fits_open_file(tmpFilename, 1)
            arcFile = pcfitsio.fits_read_keyword(fptr, "ARCFILE")[0][1:-1]
            idx = arcFile.find(".")
            insId = arcFile[0:idx]
            ts = arcFile[(idx + 1):]
            newMjd = PccUtTime.TimeStamp().initFromTimeStamp(ts).getMjd() +\
                     (0.001 / 86400)
            newArcFile = insId + "." +\
                         PccUtTime.TimeStamp(newMjd).getTimeStamp()
            pcfitsio.fits_modify_key_str(fptr, "ARCFILE", newArcFile, "")
            pcfitsio.fits_close_file(fptr)
            commands.getstatusoutput("add_chksum " + tmpFilename)


    if (ouputFile): fo.close()
    format = "\nFinished archiving file: %s, %d times. Total time: %.3fs. "+\
             "Average time per frame: %.3fs.\n"
    print format % (filename, number, totTime, float(totTime / float(number)))

    #if (tmpFilename != filename): os.system("rm -f " + tmpFilename)


if __name__ == '__main__':
    """
    Main program.
    """    
    if (len(sys.argv) < 3):
        correctUsage()
        sys.exit(1)
        
    # Parse input parameters.
    filename  = ""
    number    = 0
    port      = 7777
    hostName  = getHostName()
    targetDir = ""
    ouputFile = ""
    increment = 0
    auth      = None
    
    idx = 1
    while (idx < len(sys.argv)):
        par = sys.argv[idx].upper()
        if (par == "-F"):
            idx += 1
            filename = sys.argv[idx]
        elif (par == "-N"):
            idx += 1
            number = int(sys.argv[idx])
        elif (par == "-P"):
            idx += 1
            port = int(sys.argv[idx])
        elif (par == "-H"):
            idx += 1
            hostName = sys.argv[idx]
        elif (par == "-O"):
            idx += 1
            ouputFile = sys.argv[idx]
        elif (par == "-INC"):
            increment = 1
        elif (par == "-AUTH"):
            idx += 1
            auth = sys.argv[idx]
        elif (par == "-SERVERS"):
            idx += 1
            hostName = sys.argv[idx]
        elif (par == "-TARGETDIR"):
            idx += 1
            targetDir = sys.argv[idx]
        else:
            correctUsage()
            sys.exit(1)
        idx += 1
    archiveXTimes(filename, number, hostName, port, targetDir,
                  ouputFile, increment, auth)


# EOF
