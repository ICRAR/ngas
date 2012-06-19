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
# "@(#) $Id: ngamsThreadGroup.py,v 1.3 2008/12/15 22:00:49 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  18/06/2008  Created
#

"""
Class implementing a manager service to control a number of threads running
in parallel, carrying out the same task.

Note, after object creation the threads will be paused and will first start
execution when the method start() is called.

It is possible to request the threads to suspend themselves by calling the
pause() method.

The function which will be invoked as a thread, should have the following
interface:

def myThread(threadGroupObj)

The body of the thread function should typically be of structured as follows:

    while (True):
        threadGroupObj.checkPauseStop()

        # ... business logic ...

        threadGroupObj.suspend()

    
If the business logic of the thread can be split into logical blocks, with
a 'significant' execution time, it should be considered to call the
checkPauseStop() method between such blocks.

The suspend() method makes the thread suspend itself for a period of time
according to the following rules:

  - If the loop period parameter is specified the thread suspends itself
    for a period of time such that the total execution time one iteration,
    is constant. I.e., specifying this parameter correctly, it is possible to
    obtain a certain real-time behavior of the thread execution.

  - If the loop suspension parameter is specified, the execution of the loop
    will be suspended for this period of time. This suepension time is counted
    as part of the total loop execution time.


General mutual exclusion is obtained in the thread function business logic
code by calling the methods takeGenMux()/releaseGenMux(). Note, care should
be taken not to exagerate the usage of this semaphore, as if used too
frequently, the threads may block eachother unnecessarily.

When the 
"""

import sys, os, time, threading

from ngams import *
import ngamsLib


# Constants.
# - Used to signal that the thread has terminated execution. This will be
# thrown as part of an exception.
NGAMS_THR_GROUP_STOP_NORMAL = "__NGAMS_THR_GROUP_STOP_NORMAL__"
NGAMS_THR_GROUP_STOP_ERROR  = "__NGAMS_THR_GROUP_STOP_ERROR__"



class ngamsThreadGroup:
    """
    Class implmenting a manager service to control a number of threads running
    in parallel, carrying out the same task.
    """

    def __init__(self,
                 id,
                 function,
                 instances,
                 parameters = [],
                 loopSuspension = None,
                 loopPeriod = None):
        """
        Constructor method initializing the object and starting the threads

        id:              ID of this group of threads (string).
        
        function:        The function containing the code that will be executed
                         as the N threads within the context of the class
                         (Python function reference).
        
        instances:       Number of instances (threads) to run internally
                         (integer). 
        
        parameters:      Parameters to hand over to the thread call-back
                         function (list).

        loopSuspension:  Loop suspension in seconds to carry out during each
                         iteration (float).
                      
        loopPeriod:      The minimum time each iteration should take in
                         seconds. If an iteration takes shorter time to execute
                         than the loopPeriod specified, the execution is
                         suspended for a period of time to make the total
                         execution time of the iteration equal to the specified
                         loopPeriod. Specifying this parameter, makes it
                         possible to achieve a certain 'real-time behavior'
                         of the execution of each loop, although this is
                         only the case when the loopPeriod is larger than the
                         time it takes to execute the business logical of the
                         thread (float).
        """
        T = TRACE()

        self.__id             = id
        self.__function       = function
        self.__instances      = int(instances)
        self.__parameters     = parameters
        self.__loopSuspension = loopSuspension
        self.__loopPeriod     = loopPeriod

        self.__execute        = True
        self.__pauseEvent     = threading.Event()
        self.__generalMux     = threading.Semaphore(1)
        self.__threadHandles  = []
        self.__threadTiming   = {}

        # Create the thread handles as requested.
        for n in range(1, (self.__instances + 1)):
            args = (self, None)
            thrId = "%s-%d" % (id, n)
            thrObj = threading.Thread(None, self.__threadEncapsulator,
                                      thrId, args)
            thrObj.setDaemon(0)
            thrObj.start()
            self.__threadHandles.append(thrObj)

            
    def __del__(self):
        """
        Destructor method. Unblocks possible blocked (paused) threads.
        """
        T = TRACE()
        
        self.__pauseEvent.set()


    def takeGenMux(self,
                   timeout = None):
        """
        Take the general mutual exclusion semaphore.

        Returns:  Reference to object itself.
        """
        T = TRACE(5)
        
        self.__generalMux.acquire()
        return self
    

    def releaseGenMux(self):
        """
        Release the general mutual exclusion semaphore.

        Returns:  Reference to object itself.
        """
        T = TRACE(5)
        
        self.__generalMux.release()
        return self


    def getNumberOfActiveThreads(self):
        """
        Return the number of threads (apparently) running in the group.

        Returns:   Number of active threads (integer).        
        """
        T = TRACE(5)
        
        activeThreads = 0
        for threadHandle in self.__threadHandles:
            if (threadHandle.isAlive()): activeThreads += 1
        return activeThreads
            

    def getThreadId(self):
        """
        Return thread ID of this thread.

        Returns:   Thread ID (string).
        """
        return threading.currentThread().getName()


    def getThreadNo(self):
        """
        Return thread number of this thread.

        Returns:   Thread number (integer).
        """
        thrId = threading.currentThread().getName()
        # The thread ID looks like this: <id>-<#>, extract the number.
        return int(thrId.split("-")[-1])


    def getParameters(self):
        """
        Return reference to list containing the parameters registered for the
        thread group.
        
        Returns:   Reference to list of parameters (list).
        """
        return self.__parameters
        

    def __threadEncapsulator(self,
                             dummy1,
                             dummy2):
        """
        Method in which the actual thread function specified, is executing.
        This is actually the thread started by the class.

        Returns:     Void.
        """
        T = TRACE()
        
        self.checkPauseStop()
        try:
            self.__threadTiming[self.getThreadId()] = time.time()
            self.__function(self)
        except Exception, e:
            if (str(e).find(NGAMS_THR_GROUP_STOP_NORMAL) != -1):
                # Execution stopped 'normally'.
                return
            else:
                # An error ocurred, raise the exception again.
                raise e


    def start(self,
              wait = True,
              timeout = None):
        """
        Starts the execution of the threads and waits for termination of all
        threads, if requested.

        If a timeout is specified, there will be waited for maximum the
        timeout period of time, before forcing the threads to terminate.

        wait:      Wait until all threads finishes execution (boolean).

        timeout:   Timeout in seconds to wait for thread termination (float).
        
        Returns:   Reference to object itself.
        """
        T = TRACE()
        
        self.__pauseEvent.set()
        if (wait): self.wait(timeout)
        return self


    def stop(self):
        """
        Request the threads in the group to stop execution.

        Returns:    Reference to object itself.
        """
        T = TRACE()

        self.__execute = False
        self.__pauseEvent.set()
        return self
    
                
    def checkExecute(self):
        """
        Return value of Execution Flag.

        Returns:   Value of Execution Flag (boolean).
        """
        return self.__execute
    

    def checkPauseStop(self):
        """
        Check if the thread execution should be paused or stopped.

        Returns:   Reference to object itself.
        """
        T = TRACE(5)

        if (not self.__execute): self.terminateNormal()
        self.__pauseEvent.wait()
        if (not self.__execute): self.terminateNormal()
        return self


    def suspend(self):
        """
        Suspend the thread functions execution for a period of time to obtain
        a constant loop execution time, if the loopPeriod parameter is
        specified or to suspend the loop execution a fixed amount of time if
        the loopSuspension is specified.

        Returns:   Reference to object itself.
        """
        T = TRACE()
        
        # Make a constant suspension if requested.
        if (self.__loopSuspension): time.sleep(self.__loopSuspension)

        # If it is requested to implement a 'real-time' loop, by specifying a
        # loop period, calculate the time to wait to accomplish the loop
        # period.
        if (self.__loopPeriod):
            suspTime = (self.__loopPeriod -
                        (time.time() -
                         self.__threadTiming[self.getThreadId()]))
            if (suspTime > 0): time.sleep(suspTime)
            self.__threadTiming[self.getThreadId()] = time.time()
        return self


    def wait(self,
             timeout = None):
        """
        Wait for all threads tp terminate execute, then return.

        timeout:   Timeout in seconds to apply before returning (float).

        Returns:   Reference to object itself.
        """
        T = TRACE()
        
        startTime = time.time()
        thrWaitingList = []
        for thrHandle in self.__threadHandles:
            thrWaitingList.append(thrHandle)
        curTimeout = None
        while (True):
            if (timeout):
                curTimeout = (timeout - (time.time() - startTime))
                if (curTimeout < 0):
                    msg = "Timeout encountered while waiting for threads " +\
                          "to terminate"
                    raise Exception, msg
            thrWaitingList[0].join(curTimeout)
            if (not thrWaitingList[0].isAlive()): del thrWaitingList[0]
            if (thrWaitingList == []):
                return self
        

    def terminateNormal(self):
        """
        Makes the thread stop execution, indicating a normal termination,
        with no error condition.

        Returns:   Void.
        """
        T = TRACE()
        
        self.__pauseEvent.set()
        raise Exception, NGAMS_THR_GROUP_STOP_NORMAL

    
    def terminateError(self,
                       error):
        """
        Makes the thread stop execution, indicating an abnormal termination,
        with an error condition.

        Returns:   Void.
        """
        T = TRACE()
        
        self.__pauseEvent.set()
        raise Exception, "%s: %s" % (NGAMS_THR_GROUP_STOP_ERROR, str(error))


def test1_thread_function(threadGroupObj):
    """
    Test thread function for the test1() test case.

    threadGroupObj:   Thread Group Object instance (ngamsThreadGroup
                                                    (or child of)).

    thrId:            Thread ID allocated to this thread (string).

    Returns:          Void.                                                    
    """
    for n in range(1, 21):
        info(1, "%s/%d" % (threadGroupObj.getThreadId(), n))
        threadGroupObj.checkPauseStop().suspend().checkPauseStop()
       

def test1(timeout):
    """
    Small test that creates a Thread Group and runs a simple set of threads
    counting from 1 to 100 and printing out the counts on stdout.
    """
    thrGroupObj = ngamsThreadGroup("test1", test1_thread_function, 5,
                                   loopSuspension = 0.010, loopPeriod = 0.200)
    try:
        thrGroupObj.start(wait = True, timeout = timeout)
    except Exception, e:
        warning("Exception occurred waiting for threads to terminate: %s" %\
                str(e))

        
if __name__ == '__main__':
    """
    Main function.
    """
    setLogCond(False, "", 0, "", 4)
    # Start threads, wait until all terminate (no timeout).
    test1(None)
    # Start threads, provoke a timeout waiting for the threads to terminate.
    test1(1)

    
# EOF
