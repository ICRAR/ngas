#******************************************************************************
# ESO/DFS
#
# "@(#) $Id: PccLogFtuLogConverter.py,v 1.1 2008/07/02 21:48:10 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  04/05/2000  Created
#

"""
Small utility to convert logs in the FTU (OLAS) format into a list of
PccLogInfo objects.
"""

import os.path, string
import PccLogInfo, PccUtString

class PccLogFtuLogConverter:
    """
    Class to convert log in the 'FTU style' (OLAS style) to a list of
    PccLogInfo log objects.
    """

    def ftuLog2LogInfoObjs(self,
                           logFileRaw):
        """
        Read in the log lines from a log file in the OLAS/FTU style, and
        convert it to a list of PccLogInfo objects.

        **logFileRaw:**   Complete name of log file.

        **Returns:**      Reference to list containing the PccLogInfo objects.
        """
        complFileName = os.path.expandvars(logFileRaw)
        fd = open(complFileName, "a+")
        logInfoLines = fd.readlines()
        fd.close()

        # Each line in the 'FTU" type of log is formatted as follows:
        # "<time stamp> [<type>] <log message>".
        logInfoObjList = {}
        for logNo in range(len(logInfoLines)):
            # Get Log Date.
            idx1 = string.find(logInfoLines[logNo], " ")
            date = logInfoLines[logNo][0:idx1]

            # Get Log Type.
            idx2 = string.find(logInfoLines[logNo], "[", idx1)
            idx3 = string.find(logInfoLines[logNo], "]", idx2)
            type = logInfoLines[logNo][(idx2 + 1):idx3]

            # Get Log Message.
            msg = logInfoLines[logNo][(idx3 + 2):len(logInfoLines[logNo])]
            msg = PccUtString.trimString(msg, " \n\t")

            # Build up Log Info Object.
            logObj = PccLogInfo.PccLogInfo()
            logObj.setDate(date).setType(type).setMessage(msg)
            logInfoObjList[logNo] = logObj
        
        return logInfoObjList 

#
# ___oOo___
