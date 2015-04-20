#******************************************************************************
# ESO/DFS
#
# "@(#) $Id: PccUtDefs.py,v 1.1 2008/07/02 21:48:10 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  04/05/2000  Created
#

"""
This module contains definitions of variables (constants) used
for handling errors, timeouts etc.

The following constants are defined:

**Undefined Property:**

o UNDEFINED


**Status of a process/method/function:** 

o SUCCESS

o FAILURE

o IDLE

o EXECUTING


**To signal that no timeout should be applied:** 

o NO_TIMEOUT

**Log Levels:**

o LOG0: For information that must always be logged.

o LOG1: For high level information.

o LOG2: For more detailed information.

o LOG3: For quite detailed information, mostly used for debugging/error
        tracing.

o LOG4: For very detailed information (often at bit level).

o LOG5: For very detailed, very repetitive information (at bit level).

o LOG_OFF: No logging.
"""

# Undefined property.
UNDEFINED = -1


"""
Status of a process/method/function.
"""
SUCCESS = 0
FAILURE = 1
IDLE = 2
EXECUTING = 3

SUCCESS_STR   = "SUCCESS"
FAILURE_STR   = "FAILURE"
IDLE_STR      = "IDLE"
EXECUTING_STR = "EXECUTING"

def procStatus2Str(statusNo):
    """
    Converts a Process Status ID given as number into the corresponding
    ID (string).

    statusNo:   Number of the status.

    Returns:    Status name.
    """
    if (statusNo == SUCCESS):
        return SUCCESS_STR
    elif (statusNo == FAILURE):
        return FAILURE_STR
    elif (statusNo == IDLE):
        return IDLE_STR
    elif (statusNo == EXECUTING):
        return EXECUTING_STR
    else:
        raise exceptions.Exception, \
              "Illegal Process Status number given in to "+\
              "PccUtDefs.procStatus2Str(): " + str(statusNo) +"."

def procStatus2No(statusStr):
    """
    Converts a Process Status ID given as string into the corresponding
    ID number.

    statusStr:   Number of the status as string.

    Returns:     Status ID number.
    """
    if (statusNo == SUCCESS_STR):
        return SUCCESS
    elif (statusNo == FAILURE_STR):
        return FAILURE
    elif (statusNo == IDLE_STR):
        return IDLE
    elif (statusNo == EXECUTING_STR):
        return EXECUTING
    else:
        raise exceptions.Exception, \
              "Illegal Process Status ID given in to "+\
              "PccUtDefs.procStatus2No(): " + statusStr +"."
         

# Time-Out.
# Does something like INT_MAX exist?
NO_TIMEOUT = 10000000

# Log Levels. We store them here in order to make all global constants
# available from within this file.
LOG0 = 0
LOG1 = 1
LOG2 = 2
LOG3 = 3
LOG4 = 4
LOG5 = 5
LOG_OFF = -1

# --- oOo ---
