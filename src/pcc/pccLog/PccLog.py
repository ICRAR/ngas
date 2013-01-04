#******************************************************************************
# ESO/DFS
#
# "@(#) $Id: PccLog.py,v 1.1 2008/07/02 21:48:10 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  28/09/2000  Created
#

"""
Module that provides a convenient API to the PCC Log Manager.
"""

import types, threading
import PccLogMgr, PccUtUtils

# Remove this when going to v2.1 and import syslog only.
if (PccLogMgr.sysLog()):
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

   
# Global instance of the Log Manager.
__logMgr = PccLogMgr.PccLogMgr()

# Provide mutual exclusion for logging.
__logSem  = threading.Semaphore(1)


def setLogCond(logLevel,
               logFile,
               verboseLevel,
               sysLogProps = [-1, -1, ""],
               bufferSize = 100,
               logLocation = 0):
    
    """
    Method used to set the basic logging conditions. This is the
    level of information to write into the log file, the name of the
    log file, and the level (intensity) for writing logs to stdout. "0"
    means that no logs are produced to the target.
    
    logLevel:          The level with which there should be logged into the
                       specified log file. Level '0' means that no logs
                       are written into the log file (integer).

    logFile:           The name of the log file in which to generate the logs.
                       If '' is given, nothing will be logged into a log file
                       (string).

    verboseLevel:      Sets the level with which there should be generated
                       logs on stdout. Level "0" means that no logs will be
                       produced (integer).

    sysLogProps:       Tuple containing the properties for the logging in
                       the UNIX syslog file. The elements are as follows
                       (string):

                       [<prio>, <level>, <prefix>]

                       The fields are as follows:

                       <prio>:
                       Indicates which kind of logs should be logged into the
                       UNIX syslog. The priotities for syslog are defined
                       as:

                       syslog.LOG_EMERG   - Emergency: 0
                       syslog.LOG_ALERT   - Alert:     1
                       syslog.LOG_CRIT    - Critical:  2
                       syslog.LOG_ERR     - Error:     3
                       syslog.LOG_WARNING - Warning:   4
                       syslog.LOG_NOTICE  - Notice:    5
                       syslog.LOG_INFO    - Info:      6
                       syslog.LOG_DEBUG   - Debug:     7

                       Level '-1' means no logging into syslog.

                       <level>:
                       This is used in a similar way as the verboseLevel
                       and logLevel to specify the intensity with which
                       there is logged. This is only relevant for
                       logging functions having the level parameter
                       as input parameter, e.g. info().

                       <prefix>:
                       Prefix added in the begining of the log line to
                       indicate that log lines belong to a special system.
                       This can be used to filter out the logs from the
                       syslog file.

    bufferSize:        Indicates the size of the buffer used for storing
                       logs before flushing these out to the log file(s)
                       (integer).

    logLocation:       Flag indicating if the location where the log was
                       generated should be logged in the log file
                       (integer/0|1).

    Returns:           Void.
    """
    # Check types of input parameters.
    PccUtUtils.checkType("logLevel", logLevel,"PccLog.setLogCond()",
                         types.IntType)
    PccUtUtils.checkType("logFile", logFile, "PccLog.setLogCond()",
                         types.StringType)
    PccUtUtils.checkType("verboseLevel", verboseLevel, "PccLog.setLogCond()",
                         types.IntType)
    PccUtUtils.checkType("bufferSize", bufferSize, "PccLog.setLogCond()",
                         types.IntType)

    global __logMgr
    __logMgr.setLogLevel(logLevel)
    __logMgr.setLogFile(logFile)
    __logMgr.setVerboseLevel(verboseLevel)
    __logMgr.setSysLogPrio(sysLogProps[0])
    __logMgr.setSysLogLevel(sysLogProps[1])
    __logMgr.setSysLogPrefix(sysLogProps[2])
    __logMgr.setLogCacheFlushSize(bufferSize)
    __logMgr.setLogLocation(logLocation)

def takeLogSem():
    """
    Acquire the State Semaphore to request for permission to change it.
    
    Returns:        Void.
    """
    global __logSem
    __logSem.acquire()


def relLogSem():
    """
    Release the State Semaphore acquired with takeStateSem().
    
    Returns:        Void.
    """
    global __logSem
    __logSem.release()


def logGen_(fct,
            logType,
            level,
            msg,
            location):
    """
    Generic log function which checks types of input parameters.
    Used internally.

    fct:            Function from where the logGen_() function is called
                    (string).
    
    logType:        Type of log. See UNIX syslog types in PccLogMgr
                    (integer).

    level:          See description of PccLog.info() (integer).
    
    msg:            Message to log (string).
    
    location:       Location ID where the log was generated (string).

    Returns:        Log line (string).
    """
    global __logMgr
    if ((__logMgr.getLogLevel() < level) and
        (__logMgr.getVerboseLevel() < level) and
        (__logMgr.getSysLogLevel() < level)):
        return
    
    takeLogSem()
    try:
        msg = str(msg)
        PccUtUtils.checkType("level", level, fct, types.IntType)
        PccUtUtils.checkType("location", location, fct, types.StringType)
                 
        __logMgr.log(level, logType, location, msg)
        relLogSem()
    except Exception, e:
        relLogSem()
        raise e


def emergency(msg,
              location = ""):
    """
    Add an Emergency Log entry to the logging targets.

    Parameters:      See description of PccLog.info().

    Returns:         Always '1' (integer/FAILURE).
    """
    logGen_("PccLog.emergency()", syslog.LOG_EMERG, 0, msg, location)
    return 1


def alert(msg,
          location = ""):
    """
    Add an Alert Log entry to the logging targets.

    Parameters:      See description of PccLog.info().

    Returns:         Always '1' (integer/FAILURE).    
    """
    logGen_("PccLog.alert()", syslog.LOG_ALERT, 0, msg, location)
    return 1


def critical(msg,
             location = ""):
    """
    Add an Critical Log entry to the logging targets.

    Parameters:      See description of PccLog.info().

    Returns:         Always '1' (integer/FAILURE).
    """
    logGen_("PccLog.critical()", syslog.LOG_CRIT, 0, msg, location)
    return 1


def error(msg,
          location = ""):
    """
    Add an Error Log entry to the logging targets.

    Parameters:      See description of PccLog.info().

    Returns:         Always '1' (integer/FAILURE).    
    """
    logGen_("PccLog.error()", syslog.LOG_ERR, 0, msg, location)
    return 1


def warning(msg,
            location = ""):
    """
    Add an Warning Log entry to the logging targets.

    Parameters:      See description of PccLog.info().

    Returns:         Always '1' (integer/FAILURE).    
    """
    logGen_("PccLog.warning()", syslog.LOG_WARNING, 0, msg, location)
    return 1


def notice(msg,
           location = ""):
    """
    Add an Notice Log entry to the logging targets.

    Parameters:      See description of PccLog.info().

    Returns:         Always "0" (SUCCESS).    
    """
    logGen_("PccLog.notice()", syslog.LOG_NOTICE, 0, msg, location)
    return 0


def info(level,
         msg,
         location = ""):
    """
    Generate an info log into the logging targets.

    level:          The level tag for this log. This is compared to the
                    overall log levels defined in the logging properties
                    for the log file and stdout. If the level defined here
                    is lower or equal the overall all logging level the log
                    will be produced (integer).

    msg:            The message to be logged (string).

    location:       The location from where the log is produced. This could
                    typically be given as: '<module name>:<method>'. If
                    left undefined, it will not be contained in the log
                    output (string).

    Returns:        Always '0' (integer/SUCCESS).
    """
    logGen_("PccLog.info()", syslog.LOG_INFO, level, msg, location)
    return 0


def sysLogInfo(level,
               msg,
               location = ""):
    """
    Generate an INFO log entry in the UNIX syslog.

    level:        Level allocated to this log (integer).

    msg:          Message to log (string).
    
    location:     Optional location specifier (string).

    Returns:      Void.
    """ 
    global __logMgr
    __logMgr.sysLog(level, syslog.LOG_INFO, location, msg)

    
def debug(level,
          msg,
          location = ""):
    """
    Add an Debug Log entry to the logging targets.

    Parameters:      See description of PccLog.info().

    Returns:         Always '0' (integer/SUCCESS).    
    """
    logGen_("PccLog.debug()", syslog.LOG_DEBUG, level, msg, location)
    return 0


def getSysLogPrio():
    """
    Get the syslog priority.

    Returns:      Syslog priority (integer).
    """
    global __logMgr
    return __logMgr.getSysLogPrio()


def getSysLogLogLevel():
    """
    Get the syslog level.

    Returns:      Syslog level (integer).
    """
    global __logMgr
    return __logMgr.getSysLogLevel()


def getSysLogPrefix():
    """
    Get the syslog prefix.

    Returns:       Syslog prefix (string).
    """
    global __logMgr
    return __logMgr.getSysLogPrefix()


def getVerboseLevel():
    """
    Return the current Verbose Level used.

    Returns:         Verbose Level (integer).
    """
    global __logMgr
    return __logMgr.getVerboseLevel()


def getLogLevel():
    """
    Return the current Log Level used.

    Returns:         Log Level (integer).
    """
    global __logMgr
    return __logMgr.getLogLevel()


def getLogFile():
    """
    Return the name of the Log File into which there is logged.

    Returns:         Name of Log File (string).
    """
    global __logMgr
    return __logMgr.getLogFile()


def logFlush():
    """
    Flush the log cache.

    Returns:     Void.
    """
    global __logMgr
    __logMgr.flush()
    

#
# ___oOo___
