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
import time, threading, signal, json, decimal
from datetime import datetime

from bottle import route, run, request, get, post, static_file, template

import ngamsJobMWALib
from ngamsJob_MWA_RTS import *

staging_run = 1
DEBUG = True
web_host = 'macbook46.icrar.org' # or 'localhost'
jobDic = {} # key - jobId, val - job obj

def invalidParam(param):
    if (None == param or len(str(param)) == 0):
        return 1
    else:
        return 0

@route('/hello')
def hello():
    return getHello()

@route('/ingest')
def ingest():
    fileId = request.query.get('file_id')
    if (fileId == None):
        return 'No file id is provided'
    else:
        ngamsJobMWALib.fileIngested(fileId)
        return 'File %s is just ingested' % fileId

#TODO - use template soon!
@get('/job/submit')
def submit_job_get():
    return '''<form method="POST" action="/job/submit">
                Your name: <input name="name" type="text" /> <br/>
                Job  type: <select name="type">
                <option value="MWA_RTS" selected>MWA RTS</option>
                <option value="MWA_CASA">MWA CASA</option>
                </select> <br/>
                Observation numbers (comma separated) <br/>
                <textarea name="observations" rows="4" cols="50">1052803816,1053182656,1052749752</textarea><br/>
                <input type="submit" />
              </form>'''

@post('/job/submit')
def submit_job_post():
    name = request.forms.get('name')
    if invalidParam(name):
        return 'Invalid user name'
    observations = request.forms.get('observations')
    if invalidParam(observations):
        return 'Invalid observation numbers'
    jtype = request.forms.get('type')
    if ('MWA_RTS' != jtype):
        return 'Sorry we currently only accept MWA RTS jobs, %s jobs will be supported soon' % jtype
    
    obsNums = observations.split(',')
    
    dt = datetime.now()
    jobId = name + '_' + dt.strftime('%Y%m%dT%H%M%S') + '.' + str(dt.microsecond / 1000)
    
    job = None
    try:        
        if (jtype == 'MWA_RTS'):
            params = RTSJobParam()
            params.obsList = obsNums
            job = RTSJob(jobId, params)
        
        if (job == None):
            raise Exception ('Cannot initialise the job.')
    except Exception, e:
        return 'Failed to submit your job due to Exception: %s' % str(e)
    
    jobDic[jobId] = job
    return 'Job %s has been submitted. <a href="/job/status?job_id=%s">View its status (JSON) </a>. Or <a href="/job/monitor?job_id=%s">Monitor its progress</a>' % (jobId, jobId, jobId)

def encode_decimal(obj):
    """
    Just a little helper function for JSON serialisation
    """
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError(repr(obj) + " is not JSON serializable")

@get('/job/status')
def getJobStatus():
    jobId = request.query.get('job_id')
    if (invalidParam(jobId) or (not jobDic.has_key(jobId))):
        return 'Please provide an valid job_id as the parameter'
    mrJob = jobDic[jobId]
    return json.dumps(mrJob.toJSONObj(), default=encode_decimal)

@get('/job/monitor')
def monitorJob():
    """
    Create a job tree for monitoring
    """
    jobId = request.query.get('job_id')
    if (invalidParam(jobId) or (not jobDic.has_key(jobId))):
        return 'Please provide an valid job_id as the parameter'
    return template('ngamsJobMonitor.html', job_id = jobId) 

@route('/static/<filepath:path>')
def server_static(filepath):
    return static_file(filepath, root='./')

def getHello():
    return "Hello World 001"

def _jobThread(mrTaskJob):
    mrTaskJob.start()

def _scheduleStageThread(dummy):
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

