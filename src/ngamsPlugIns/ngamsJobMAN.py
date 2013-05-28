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
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu      20/May/2013  Created

"""
JobMAN manages NGAS MapReduce jobs by providing the following features:
1. Maintain a job queue
2. Receive job submission 
3. Construct each job using MapReduce tasks (in a job-type-specific manner), and schedule each task to a different NGAS node
4. Receive job monitor request
5. Monitor job progress, including error handling

For event: 
1. receive event requests and turn them into event object
2. notify job/tasks related to these events 

This module relies on two external libs - Paste and Bottle
To deploy (install) them:

1. Activate Python Virtual Env
2. pip install bottle
3. pip install Paste

"""
import time, threading, signal

from bottle import route, run

import ngamsJobMWALib

staging_run = 1
DEBUG = True
web_host = 'macbook46.icrar.org' # or 'localhost'

@route('/hello')
def hello():
    return getHello()

def getHello():
    return "Hello World 001"

def _scheduleStageThread(dummy):
    global staging_run
    retries = 0
    while (staging_run):
        ret = ngamsJobMWALib.scheduleForStaging(retries)
        if (ret):
            retries += ret
        else:
            retries = 0
        time.sleep(ngamsJobMWALib.ST_INTVL_STAGE)

def startStagingThread():
    #signal.signal(signal.SIGTERM, exitHandler)
    #signal.signal(signal.SIGINT, exitHandler)
    args = (1, )
    thrd = threading.Thread(None, _scheduleStageThread, 'STAGING_THRD', args) 
    thrd.setDaemon(1) # it will exit immediately
    thrd.start()

def exitHandler(signalNo, stackFrame):
    global staging_run
    staging_run = 0
    print 'Received terminating signal, shutting down the server'

startStagingThread()
run(host = web_host, server = 'paste', port = 7778, debug = DEBUG)

