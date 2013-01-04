#******************************************************************************
# ESO/DFS
#
# "@(#) $Id: PccLogMgr.py,v 1.1 2008/07/02 21:48:10 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  04/05/2000  Created
#

"""
Module providing a logging facility that can log information on stdout
and in log files. The logging services are provided via the
class PccLogMgr.

See also documentation for class PccLogMgr and module PccLog.
"""

import sys, os, threading, string

##################################################################
# Remove all this handling when syslog is supported (from v2.1)
##################################################################
ver = string.split(sys.version, " ")[0]
if (string.find(ver, "1.5") == 0):
    sysLog_ = 0
else:
    sysLog_ = 1

def sysLog():
    global sysLog_
    return sysLog_

if sysLog():
    import syslog
else:
    class syslog:
        LOG_EMERG   = 0
        LOG_ALERT   = 1
        LOG_CRIT    = 2
        LOG_ERR     = 3
        LOG_WARNING = 4
        LOG_NOTICE  = 5
        LOG_INFO    = 6
        LOG_DEBUG   = 7
##################################################################

import PccLogInfo, PccUtTime

# Log Levels.
LOG0 = 0
LOG1 = 1
LOG2 = 2
LOG3 = 3
LOG4 = 4
LOG5 = 5
LOG_OFF = -1

# UNIX syslog types --------------- OLAS Name    DFS Log Name
_logMapping = {-1:                 ['',          ''],
               syslog.LOG_EMERG:   ['EMERGENCY', 'Emergency'],
               syslog.LOG_ALERT:   ['ALERT',     'Alert'],
               syslog.LOG_CRIT:    ['CRITICAL',  'Critical'],
               syslog.LOG_ERR:     ['ERROR',     'Error'],
               syslog.LOG_WARNING: ['WARNING',   'Warning'],
               syslog.LOG_NOTICE:  ['NOTICE',    'Notice'],
               syslog.LOG_INFO:    ['INFO',      'Info'],
               syslog.LOG_DEBUG:   ['DEBUG',     'Debug']}

def logNo2Name(no):
    global _logMapping
    return _logMapping[no][0]

def logNo2DfsName(no):
    global _logMapping
    return _logMapping[no][1]


# 3 types of log files can be generated.
SIMPLE_LOG_FORMAT = 1
EXT_LOG_FORMAT = 2
XML_LOG_FORMAT = 3

# Global list to keep the log caches (for some reason we cannot store
# these as class variables???).
_logCacheList = {}

# Used to protect the access e.g. to log files, when flushing out
# cached logs into the log file associated to the Log Manager.
_logSemList = {}

class PccLogMgr:
    """
    The PccLogMgr (Log Manager) class is to handle logging information.
    It can produce logs on stdout and in log files. When logging into
    a file, a file must be associated with the Log Manager.

    The Log Manager decides whether to log an entry or not according to the
    Log Level set (for log which goes into a log file), and the Verbose
    Level set (for logs written on stdout). These parameters should be
    adjusted for an instance of the Log Manager. The scheme works in the
    following way:

    When a log entry is logged with e.g. the method "log()" the log is
    tagged with a certain Log Level. This Log Level is compared to the
    current Log Level and Verbose Level set internally in the Log Manager.

    The Log Manager caches log entries into an internal cache which size
    can be adjusted with the method "setLogCacheFlushSize()". When the
    internal log cache reaches this size, the logs stored will be flushed
    automatically into the log file associated to this Log Manager.
    Note that syslog entries are not cached.

    CAUTIONS
    For performance reasons the Log Manager does not enforce any
    proper time sequence of the logs. The logs could be sorted off-line
    with some tool.    
    """
    

    def __init__(self,
                 logFile = ""):
        """
        Destructor method for the Log Manager Object which associates
        the log manager with a log file.
        """
        # Variables to handle the logging.
        self.__logLevel          = 0
        self.__verboseLevel      = 0
        self.__logFile           = ""
        self.__sysLogPrio        = -1
        self.__sysLogLevel       = -1
        self.__sysLogPrefix      = ""
        self.__logCacheFlushSize = 100
        self.__localHost         = "<define>"
        self.__logFileFormat     = SIMPLE_LOG_FORMAT
        self.__logLocation       = 0
     
        self.setLogFile(logFile)


    def __del__(self):
        """
        Destructor method for the Log Manager Object which performs a
        flush of the logs currently stored in the log cache.
        """
        try:
            self.flush()
        except Exception, e:
            pass


    def getLogLocation(self):
        """
        Return the flag indicating if the location of the log (from
        where it was generated) should be added in the log output
        in the log files.

        Returns:    Value of Log Location Flag (integer/0|1).
        """
        return self.__logLocation


    def setLogLocation(self,
                       logLoc):
        """
        Set the flag indicating if the location of the log (from
        where it was generated) should be added in the log output
        in the log files.

        logLoc:     New value of the Log Location Flag (integer/0|1).

        Returns:    Reference to object itself (PccLogMgr).
        """
        self.__logLocation = int(logLoc)


    def getLogLocation(self):
        """
        Return the flag indicating if the location of the log (from
        where it was generated) should be added in the log output
        in the log files.

        Returns:    Value of Log Location Flag (integer/0|1).
        """
        return self.__logLocation


    def setLogLevel(self,
                    level):
        """
        Set the Log Level used within this Log Manager. The Log Level
        indicates for the Log Manager which logs it should accept.

        level:    Integer number indicating the Log Level of this
                  Log Manager. The constants LOG0, ..., LOG5 should be
                  used (integer).

        Returns:  Reference to the object itself PccLogMgr).
        """
        self.__logLevel = level
        return self


    def getLogLevel(self):
        """
        Return the Log Level set for this object.

        Returns:  The Log Level (integer number) set for this Log Manager
                  (integer).
        """
        return self.__logLevel

    
    def setVerboseLevel(self,
                        level):
        """
        Set the Verbose Level for this Log Manager. The Verbose Level
        indicates at which level verbose logs should be written to
        stdout. The Verbose Level is an integer number (use the constants
        LOG0, ..., LOG5).

        level:         Verbose Level to use for this Log Manager (integer).

        Returns:       Reference to object itself (PccLogMgr).
        """
        self.__verboseLevel = level
        return self


    def getVerboseLevel(self):
        """
        Return the Verbose Level set for
        this Log Manager.

        Returns:   The Verbose Level defined for this Log Manager (integer).
        """
        return self.__verboseLevel


    def setSysLogPrio(self,
                      prio):
        """
        Specify which type of syslogs that should be produced. Types
        are: syslog.LOG_EMERG, syslog.LOG_ALERT, syslog.LOG_CRIT,
        syslog.LOG_ERR, syslog.LOG_WARNING, syslog.LOG_NOTICE,
        syslog.LOG_INFO, syslog.LOG_DEBUG. 

        prio:      Specifies the SysLog Priority. A value of "-1"
                   means off (integer).

        Returns:   Reference to object itself (PccLogMgr).
        """
        self.__sysLogPrio = prio
        return self


    def getSysLogPrio(self):
        """
        Return the SysLog Priority set for
        this Log Manager.

        Returns:   The SysLog Priority (integer).
        """
        return self.__sysLogPrio


    def setSysLogLevel(self,
                       level):
        """
        Set the SysLog Level for this Log Manager. The Verbose Level
        indicates at which level verbose logs should be written to
        stdout. The Verbose Level is an integer number (use the constants
        LOG0, ..., LOG5).

        level:     SysLog Level to use for this Log Manager (integer).

        Returns:   Reference to object itself (PccLogMgr).
        """
        self.__sysLogLevel = level
        return self


    def getSysLogLevel(self):
        """
        Return the SysLog Level set for
        this Log Manager.

        Returns:   The SysLog Level defined for this Log Manager (integer).
        """
        return self.__sysLogLevel


    def setSysLogPrefix(self,
                        prefix):
        """
        Set the SysLog Prefix for this Log Manager. The prefix is written
        in the log line to make it possible to filter out logs from a
        specific source/system, from the log line in the syslog file.

        prefix:    Prefix (string).

        Returns:   Reference to object itself (PccLogMgr).
        """
        self.__sysLogPrefix = str(prefix)
        return self


    def getSysLogPrefix(self):
        """
        Return the SysLog Prefix set for this Log Manager.

        Returns:   The SysLog Prefix defined for this Log Manager (string).
        """
        return self.__sysLogPrefix


    def verboseLog(self,
                   level,
                   date,
                   location,
                   type,
                   logMessage):
        """
        Produce a Verbose Log on stdout according to the Verbose Level
        defined.

        level:       The Verbose Level of the message to be logged (integer).

        date:        (Optional). Time stamp to print out in connection with
                     the Verbose Log. If not specified (if given in as "")
                     a time stamp will be generated internally (current
                     time) (string).

        location:    Location identifier indicating from where this log
                     is issued (string).

        type:        Type according to syslog (integer).

        logMessage:  The log message to print to stdout (string).

        Returns:     Void.
        """
        if (level <= self.getVerboseLevel()):
            if (location != ""):
                print date + ":" + location + ":" +\
                      logNo2Name(type) + "> " + logMessage
            else:
                print date + ":" + logNo2Name(type) + "> " + logMessage


    def log(self, level, type, location, logMessage):
        """
        Method to generate a log entry whereby only a subset of the
        complete set of logging parameters are defined. The time stamp
        is automatically generated as the current time.
        
        level:        Level indicator for this log entry (integer).
 
        type:         The type of log (integer).

        location:     Place in code (file/class/method) from where the
                      log is generated (string).

        logMessage:   Message to log (string).

        Returns:      Void.
        """
        self.logAll(level, "", logMessage, type, self.__localHost, location)


    def sysLog(self, level, type, location, logMessage):
        """
        Same as log() but logs only into the UNIX syslog if conditions
        are fulfilled.
        
        level:        Level indicator for this log entry.
     
        type:         The type of log.
    
        location:     Place in code (file/class/method) from where the
                      log is generated.
    
        logMessage:   Message to log.
    
        Returns:      Void.
        """
        if ((level <= self.getSysLogLevel()) and sysLog()):
            date = PccUtTime.getIsoTime(0, 3)
            logInfoObj = PccLogInfo.PccLogInfo()
            logInfoObj.\
                         setDate(date).\
                         setType(type).\
                         setMessage(logMessage).\
                         setLocObj(location)
            syslog.syslog(logInfoObj.getType(), self.genSysLogLine(logInfoObj))


    def logAll(self,
               level,
               date,
               msg,
               type = "",
               host = "",
               locObj = "",
               locObjIdx = "",
               process = "",
               context = "",
               module = "",
               logId = "",
               logNo = 0,
               priority = ""):
        """
        Method to log all the log fields defined. This log method calls
        internally the 'verboseLog()' method to produce also a Verbose Log
        according to the conditions.

        level:        Level indicator for this log entry (integer).

        date:         The time stamp for this log entry (string).

        msg:          The message to log (string).
 
        type:         The type of log (from syslog) (integer).

        host:         The host from where this log is produced (string).

        locObj:       Reference to the location from where this log was
                      produced. Could be e.g. the filename (string).

        locObjIdx:    Reference to an index in the location from where the
                      log was produced. Could be e.g. a line number in a
                      source file (string).

        process:      The process from which the log entry was produced
                      (string).

        context:      The context in which the process which produced the
                      log is running (string).

        module:       The module (project) to which the process logging
                      this entry belongs (string).

        logId:        The ID of the log. Could be e.g. a mnemonic
                      for the log (string).

        logNo:        The number assigned to the log (if relevant) (integer).

        priority:     The priority of the log (integer).

        Returns:      Void.
        """
        if (date == ""): date = PccUtTime.getIsoTime(0, 3)
        self.verboseLog(level, date, locObj, type, msg)

        # Generate the log for the log file.
        if (level <= self.getLogLevel()):
            logInFile = 1
        else:
            logInFile = 0
    
        if ((type <= self.getSysLogPrio()) and \
            (level <= self.getSysLogLevel())):
            logInSysLog = 1
        else:
            logInSysLog = 0
        
        if (logInFile or logInSysLog):
            logInfoObj = PccLogInfo.PccLogInfo()
            logInfoObj.\
                         setDate(date).\
                         setType(type).\
                         setHost(host).\
                         setPrio(priority).\
                         setContext(context).\
                         setModule(module).\
                         setProcess(process).\
                         setMessage(msg).\
                         setLocObj(locObj).\
                         setLocObjIdx(locObjIdx).\
                         setLogNumber(logNo).\
                         setLogId(logId)
            logCache = self.__getLogCache()
            logCache[len(logCache)] = logInfoObj
            self.autoFlush()

            if (logInSysLog and sysLog()):
                syslog.syslog(logInfoObj.getType(),
                              self.genSysLogLine(logInfoObj))

                
    def __getLogCache(self):
        """
        Return reference to the log cache used to cache logs for this
        Log Manager.

        Returns:    Reference to the log cache (list).
        """    
        global _logCacheList
        if (_logCacheList.has_key(self.getLogFile()) == 0):
            _logCacheList[self.getLogFile()] = {}
        return _logCacheList[self.getLogFile()]


    def autoFlush(self):
        """
        Checks if the logs stored in the log cache should be auto flushed
        into the log file specified.

        This will be done when the size of the log cache reaches the
        Log Cache Flush Size specified in the Log Manager.

        Returns:     Void.
        """
        if (len(self.__getLogCache()) >= self.getLogCacheFlushSize()):
            self.flush()


    def flush(self):
        """
        Flush the log stored in the log cache into the log file associated
        with the Log Manager.

        Returns:    Void.
        """
        self.__takeSem()
        logCache = self.__getLogCache()
        fd = None
        if (self.getLogFile() != ""):                            
            if (self.getLogFormat() == XML_LOG_FORMAT):
                print "IMPL.: Logging in XML"
            else:
                if (os.path.exists(self.getLogFile())):
                    fd = open(self.getLogFile(), "a+")
                else:
                    path = os.path.dirname(self.getLogFile())
                    if (not os.path.exists(path)): os.makedirs(path)
                    fd = open(self.getLogFile(), "w")
                for logNo in range(len(logCache)):
                    logLine = self.genLogLine(logCache[logNo])
                    fd.write("%s\n" % logLine)
        self.__giveSem()
        global _logCacheList
        _logCacheList[self.getLogFile()] = {}

        if (fd != None): fd.close()
        

    def genLogLine(self,
                   logInfoObj):
        """
        Geneate a log line the format defined
        for log files.

        logInfoObj:    PccLogInfo object containing the information to be
                       used to build up the log line (PccLogInfo).

        Returns:       The log line created (string).
        """
        if (self.getLogFormat() == SIMPLE_LOG_FORMAT):
            # Format: <time stamp> [<type>] <log message> ([<location>])
            logEntry = logInfoObj.getDate() + " [" +\
                       logNo2Name(logInfoObj.getType()) + "] " +\
                       logInfoObj.getMessage()
            if (self.getLogLocation()):
                logEntry += " [" + logInfoObj.getLocObj() + "]"
        elif (self.getLogFormat() == EXT_LOG_FORMAT):
            print "IMPL.: Generate Extended Format"
        else:
            print "IMPL.: Generate XML logs!"
            
        return logEntry


    def genSysLogLine(self,
                      logInfoObj):
        """
        Geneate a syslog line the format defined for log files.

        logInfoObj:    PccLogInfo object containing the information to be
                       used to build up the log line (PccLogInfo).

        Returns:       The log line created (string).
        """
        # Format:
        # [<prefix>:]<date> <prio> <host> <location> <msg>
        if (self.getSysLogPrefix() == ""):
            prefix = ""
        else:
            prefix = self.getSysLogPrefix() + ":"
        hostName = os.uname()[1]
        logEntry = prefix + logInfoObj.getDate() + " " +\
                   logNo2DfsName(logInfoObj.getType()) + " " +\
                   hostName + " " + logInfoObj.getLocObj() + " " +\
                   logInfoObj.getMessage()
        return logEntry

    
    def setLogFormat(self,
                     format):
        """
        Set the format to be used when generating log file. Three formats
        are defined.

        format:  Indicates the format. The following constants should be
                 used to set this (integer):

                     SIMPLE_LOG_FORMAT   Logs in a format containing limited
                                         set of information. This is a plain
                                         file. One log per log entry.
        
                     EXT_LOG_FORMAT      Log the information in a file
                                         containing all the fields specified.
                                         The format is a simple ASCII file,
                                         one line per log entry.
        
                     XML_LOG_FORMAT      Log the information in the log entries
                                        in an XML file in the LOGML format.
                      
        Returns: Reference to the object itself (PccLogMgr).
        """
        self.__logFileFormat = format
        return self


    def getLogFormat(self):
        """
        Return the format to be used when producing log files.

        Returns:    The log format. See method 'setLogFormat()' for
                    explanations of the various log formats supported (string).
        """
        return self.__logFileFormat


    def setLogFile(self,
                   logFile):
        """
        Associate a log file to this
        Log Manager.

        logFile:    Log File associated to this Log Manager (string).

        Returns:    Reference to the object itself (PccLogMgr).
        """
        global _logCacheList, _logSemList
        self.__logFile = os.path.expandvars(logFile)
        _logCacheList[logFile] = {}
        _logSemList[logFile] = threading.Semaphore()
        return self


    def getLogFile(self):
        """
        Return the name of the Log File associated to this Log Manager.

        Returns:   The Log File associated to this Log Manager (string).
        """
        return self.__logFile


    def setLogCacheFlushSize(self,
                             size):
        """
        Set the size of the internal Log Cache. This is defined as the
        number of log entries the should be cached before flushing the
        logs into the log file associated to the Log Manager.

        Returns:   Reference to the object itself (PccLogMgr).
        """
        self.__logCacheFlushSize = size
        return self


    def getLogCacheFlushSize(self):
        """
        Get the size of the internal log cache.

        Returns:   Size in logs of the internal log cache (integer).
        """        
        return self.__logCacheFlushSize
    

    def __takeSem(self):
        """
        Internal method used to protect critical operation done by the
        Log Manager. This is typical writing of log entries into the log
        file. In case the Log Manager is used in a multi-threaded application,
        the logs could be mixed up if not protecting the flushing.

        The semaphore operation is related to the log file associated with
        the Log Manager. I.e., one semaphore per log file which is used.

        Returns:    Void.
        """
        if (threading.activeCount() > 1):
            global _logSemList
            _logSemList[self.getLogFile()].acquire()


    def __giveSem(self):
        """
        Internal method used to release the semaphore protecting a critical
        operation of this Log Manager.

        Returns:   Void.
        """
        if (threading.activeCount() > 1):
            global _logSemList
            _logSemList[self.getLogFile()].release()


    def logObj(self,
               level,
               logInfoObj):
        """
        Log the information stored in a
        PccLogInfo object.

        level:       Level tag for this log entry (integer).
 
        logInfoObj:  PccLogInfo object containing the log information in
                     connection with this log entry (PccLogInfo).

        Returns:     Void.
        """
        self.verboseLog(level, logInfoObj.getDate(),
                        logInfoObj.getLocObj(), logInfoObj.getMessage())
        if (level <= self.getLogLevel()):
            logCache = self.__getLogCache()
            logCache[len(logCache)] = logInfoObj
            self.autoFlush()

#
# ___oOo___
