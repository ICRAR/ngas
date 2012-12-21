

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
# "@(#) $Id: ngasMonMemUsage.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  12/07/2005  Created
#

_doc =\
"""
The tool is used to monitor the memory usage of a process referred to by its
PID. The memory occupied by the process and the time for obtain the value is
written in a output file specified.

%s
"""

import sys, os, time


from ngams import *
import pcc, PccUtTime


# Definition of predefined command line parameters.
_pars = [["PID",      0,    "PID of process to monitor"],
         ["OUTFILE",  None, "Output where the memory usage will be dumped."],
         ["INTERVAL", 1,    "Sampling interval in seconds."]]
_parDic = {}
_parFormat = "%s [%s]:\n"
_parDoc = ""
for parInfo in _pars:
    _parDic[parInfo[0]] = parInfo[1]
    if (parInfo[0][0] != "_"):
        _parDoc += _parFormat % (parInfo[0], str(parInfo[1]))
        _parDoc += parInfo[2] + "\n\n"
__doc__ = _doc % _parDoc


def parDic():
    """
    Return reference to parameter dictionary.

    Returns:  Reference to parameters dictionary (dictionary).
    """
    return _parDic


def monitorMemUsage(parDic):
    """
    """
    rmFile(parDic["OUTFILE"])
    fo = open(parDic["OUTFILE"], "w")
    memUsageCmd = "/usr/bin/pmap %d" % parDic["PID"]
    while (1):
        stat, out = commands.getstatusoutput(memUsageCmd)
        memUsage = out.split("\n")[-1].split(" ")[-1][0:-1]
        logLine = "%s %s\n" % (PccUtTime.TimeStamp().getTimeStamp(), memUsage)
        fo.write(logLine)
        print logLine[:-1]
        fo.flush()
        time.sleep(parDic["INTERVAL"])
    fo.close()
    

def correctUsage():
    """
    Return the usage/online documentation in a string buffer.

    Returns:  Man-page (string).
    """
    return __doc__

  
if __name__ == '__main__':
    """
    Main function to execute the tool.
    """
    setDebug(1)
    
    # Parse input parameters.
    parDic = parDic()
    idx = 1
    while idx < len(sys.argv):
        parOrg = sys.argv[idx]
        par    = parOrg.upper()
        try:
            if (par.find("--PID") == 0):
                parDic["PID"] = int(parOrg.split("=")[-1])
            elif (par.find("--OUTFILE") == 0):
                parDic["OUTFILE"] = parOrg.split("=")[-1]
            elif (par.find("--INTERVAL") == 0):
                parDic["INTERVAL"] = int(parOrg.split("=")[-1])
            else:
                raise Exception, "Unknown parameter: %s" % parOrg
            idx += 1
        except Exception, e:
            print "\nProblem executing the tool: %s\n" % str(e)
            print correctUsage()  
            sys.exit(1)
    try:
        monitorMemUsage(parDic)
    except Exception, e:
        if (str(e) != ""):
            print "Problem encountered handling request:\n\n%s\n" % str(e)
        sys.exit(1)

# EOF
