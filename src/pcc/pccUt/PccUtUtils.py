#******************************************************************************
# ESO/DFS
#
# "@(#) $Id: PccUtUtils.py,v 1.1 2008/07/02 21:48:10 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  06/02/2001  Created
#

"""
Module that provide various utilities.
"""

import exceptions, time, popen2, os, select
from PccLog import *


def checkType(parameter,
              value,
              method,
              type):
    """
    Function to check if the type of a parameter corresponds to an expected
    type.
                            
    parameter:   Name of parameter to check (string).

    value:       Value referred to by variable.
    
    method:      Name of the method where this check is invoked from
                 (string: "<module>.<method>").

    type:        The supposed Python type of the instance.
    
    Returns:     Void.
    """
    if (isinstance(value, type) == 0):
        raiseTypeException(parameter, method, type)


def raiseTypeException(parameter,
                       method,
                       type):
    """
    Raise an "exceptions.Exception" indicating that the parameter given
    by "parameter" and called by the method/function "method" is not of
    the expected type given by \"type\" (reference to Python type).
    
    parameter:    Name of parameter to test.
    
    method:       Name of method/function to which the parameter was
                  given as input.
    
    type:         Expected type of parameter ([<module>.]<class>).

    Returns:     Void.
    """
    raise exceptions.Exception, \
          "Parameter " + parameter + " given to method/function "+method +\
          " is not of type " + str(type) + "."


def execCmd(cmd,
            timeOut = -1):
    """
    Execute sthe command given on the UNIX command line and returns a
    list with the cmd exit code and the output written on stdout and stderr.

    timeOut:     Timeout waiting for the command in seconds. A timeout of
                 -1 means that no timeout is applied (float).

    Returns:     List with the exit code and output on stdout and stderr:

                     [<exit code>, <stdout>, <stderr>]  (list).
    """
    stdOut = ""
    stdErr = ""
    exitCode = 0
    startTime = time.time()
    pollTime = 0.100
    p = popen2.Popen3(cmd, 1)
    while (1):
        exitCode = p.poll()
        if (exitCode == -1):
            deltaTime = (time.time() - startTime)
            if ((timeOut != -1) and (deltaTime > timeOut)):
                raise exceptions.Exception, \
                      "Executing command " + cmd + " timed out after " +\
                      str(deltaTime) + " s."
        else:
            break
        rdFds, wrFds, exFds = select.select([p.fromchild, p.childerr], [],
                                            [], 0)
        for rdFd in rdFds:
            if (rdFd == p.fromchild):
                stdOut = stdOut + rdFd.read()
            else:
                stdErr = stdErr + rdFd.readline()
        time.sleep(pollTime)

    p.childerr.close()
    p.fromchild.close()
    del p
    
    return [exitCode, stdOut, stdErr]


def checkInList(value,
                refList,
                parameter = "",
                moduleClass = ""):
    """
    Check that a string value is
    contained in a list.

    value:        String value.

    refList:      List containing the defined/accepted values (as strings).
    
    parameter:    Name of the input parameter to a method.
    
    moduleClass:  Name of module+class.

    Returns:      Void.
    """
    try:
        refList.index(value)
    except exceptions.Exception, e:
        err = "Value \"" + str(value) + "\" not found in list."
        if (parameter != ""):
            err = err + " Parameter: \"" + parameter + "\"."
        if (moduleClass != ""):
            err = err + " Module/Class: \"" + moduleClass + "\"."
        raise exceptions.Exception, err


def getDbPwd(dbSrv,
             user):
    """
    Retrieve the password defined for a given combination of DB server +
    DB user from the ~/.dbrc file. If not possible, None is returned.

    dbSrv:      Name of DB server (string).
    
    user:       Db user (string).

    Returns:    Password or None (string|None).
    """
    complPath = os.path.expanduser("~/.dbrc")
    pwd = None
    if (os.path.exists(complPath)):
        fo = open(complPath)
        dbrc = fo.readlines()
        fo.close()
        for line in dbrc:
            if ((line.find(dbSrv) != -1) and (line.find(user) != -1)):
                pwd = line.split(" ")[-1]
    return pwd

#
# ___oOo___
