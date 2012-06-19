#******************************************************************************
# ESO/DFS
#
# "@(#) $Id: PccLogInfo.py,v 1.1 2008/07/02 21:48:10 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  04/05/2000  Created
#

"""
Class to handle the information for one log.
"""


class PccLogInfo:
    """
    Class to handle the information in connection with one log entry.
    """


    def __init__(self):
        """
        Constructor method to initialize class member variables.
        """
        # Members to keep the logging information.
        # - Basic log info attributes.
        self.__date       = ""
        self.__type       = -1
        self.__host       = ""
        self.__prio       = ""
        self.__ctxt       = ""
        self.__mod        = ""
        self.__proc       = ""
        self.__msg        = ""
        
        # - Extended attributes.
        self.__locObj     = ""
        self.__locObjIdx  = ""
        self.__logNo      = ""
        self.__logId      = ""


    def setDate(self,
                date):
        """
        Set the date reference of the log entry.

        date:     Date in a string format. The format can be free but it
                  is suggested to use the ISO-8601 format.

        Returns:  Reference to the object itself.
        """
        self.__date = date
        return self


    def getDate(self):
        """
        Return the date reference of the object.

        Returns:  Date contained in the object.
        """
        return self.__date


    def setType(self,
                type):
        """
        Set the type of the log.

        type:     Number indicating the type of log (from syslog).

        Returns:  Reference to the object itself.
        """
        self.__type = type
        return self


    def getType(self):
        """
        Return the type specifier of the object.

        Returns:   The type of log contained in the object.
        """
        return self.__type


    def setHost(self,
                host):
        """
        Set the host reference of the object.

        host:       The host name for the log entry.

        Returns:    Reference to the object itself.
        """
        self.__host = host
        return self


    def getHost(self):
        """
        Returns the host name set in connection with this log entry.

        Returns:   Name of the host in connection with the log entry.
        """
        return self.__host


    def setPrio(self,
                prio):
        """
        Sets the priority specified for this log entry.

        prio:      Priority of the log entry in free format.

        Returns:   Reference to the object itself.
        """        
        self.__prio = prio
        return self


    def getPrio(self):
        """
        Returns the priority specifier for this log entry.

        Returns:    Priority for this log entry.
        """
        return self.__prio

    
    def setContext(self,
                   context):
        """
        Sets the context specifier for this log entry.

        context:   Free format string indicating the context
                       of the log entry.

        Returns:   Reference to the object itself.
        """        
        self.__ctxt = context
        return self


    def getContext(self):
        """
        Return the context specifier of this log entry.

        Returns:   The context specifier of this log entry.
        """
        return self.__ctxt


    def setModule(self,
                  module):
        """
        Sets the module specifier of this log entry.

        module:    Free format identifier for this log entry.

        Returns:   Reference to the object itself.
        """
        self.__module = module
        return self


    def getModule(self):
        """
        Returns the module identifier for this log entry.

        Returns:   Module specifier for this log entry.
        """
        return self.__module


    def setProcess(self,
                   process):
        """
        Sets the process identifier for this log entry.

        process:   Free format identifier for the process name of this
                       log entry.

        Returns:   Reference to the object itself.
        """
        self.__proc = process
        return self


    def getProcess(self):
        """
        Return the process specifier for this log entry.

        Returns:   Process specifier for this log entry.
        """
        return self.__proc


    def setMessage(self,
                   message):
        """
        Sets the log message of this log entry.

        message:    Free format log message of this log entry.

        Returns:    Reference to the object itself.
        """
        self.__msg = message
        return self


    def getMessage(self):
        """
        Return the message in connection with this log entry.

        Returns:   Log message stored for this log entry.
        """
        return self.__msg


    def setLocObj(self,
                  locObj):
        """
        Set the location object. This could e.g. be the file in which
        the log occurred.

        logObj:   Free format string indicating the place from where this
                  log entry was logged.

        Returns:  Reference to the object itself.
        """
        self.__locObj = locObj 
        return self


    def getLocObj(self):
        """
        Return the location object identifier for this log entry.

        Returns:   The location object identifier for this log entry.
        """
        return self.__locObj


    def setLocObjIdx(self,
                     locObjIdx):
        """
        Set the location object index for this log entry. This could e.g.
        be the line number in which this error was logged.

        locObjIdx:   Reference to the location in connection with this
                     log entry.

        Returns:     Reference to the object itself.
        """
        self.__locObjIdx = locObjIdx
        return self


    def getLocObjIdx(self):
        """
        Return the location object index for this log entry.

        Returns:     The location object index for this log entry.
        """
        return self.__locObjIdx


    def setLogNumber(self,
                     logNumber):
        """
        Set the log number for this log entry for this log entry.

        logNumber:   Number of the log contained in this log entry.

        Returns:     Reference to the object itself.
        """
        self.__logNo = logNumber
        return self


    def getLogNumber(self):
        """
        Return the log number set for this log entry.

        Returns:  The log number stored in connection with this log entry.
        """
        return self.__logNo


    def setLogId(self,
                 logId):
        """
        Sets the log ID for this log entry.

        logId:     ID for the log stored in this log entry.

        Returns:   Reference to the object itself.
        """
        self.__logId = logId
        return self


    def getLogId(self):
        """
        Return the Log ID stored in connection with this log entry.

        Returns:   Log ID stored in this log entry.
        """
        return self.__logId


#
# ___oOo___
