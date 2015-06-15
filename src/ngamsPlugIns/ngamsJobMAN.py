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
#staging_run = 1

import cPickle as pickle
import datetime
import logging
from optparse import OptionParser
import os, ConfigParser, threading, json, decimal, traceback
from urlparse import urlparse

from bottle import route, run, request, get, post, static_file, template, redirect
from ngamsPlugIns import ngamsJobMWALib
from ngamsPlugIns.ngamsJobProtocol import STATUS_COMPLETE, STATUS_EXCEPTION
from ngamsPlugIns.ngamsJob_MWA_RTS import RTSJobParam, RTSJob


jobDic = {} # key - jobId, val - job obj
predef_tpls = ['drift','FnxA','gencal','regrid','simplecal','stokes','usecal']

logger = logging.getLogger(__name__)

def invalidParam(param):
    if (None == param or len(str(param)) == 0):
        return 1
    else:
        return 0

@route('/hello')
def hello():
    return getHello()

@route('/failtodeliverfile')
def failToDeliver():
    fileId = request.query.get('file_id')
    toUrl = request.query.get('to_url')
    errMsg = request.query.get('err_msg')
    if (fileId == None):
        return 'No file id is provided'
    else:
        try:
            ngamsJobMWALib.fileFailToDeliver(fileId, toUrl, errMsg)
            msg = 'File %s fail to be delivered to %s: %s' % (fileId, toUrl, errMsg)
            logger.info(msg)
            return msg
        except Exception, err:
            logger.error(traceback.format_exc())
            return 'Exception (%s) when doing - File %s failed to be deliverred on %s' % (str(err), fileId, toUrl)

@route('/ingest')
def ingest():
    fileId = request.query.get('file_id')
    filePath = request.query.get('file_path')
    toHost = request.query.get('to_host')
    ingestRate = request.query.get('ingest_rate')
    if (fileId == None):
        return 'No file id is provided'
    else:
        try:
            ngamsJobMWALib.fileIngested(fileId, filePath, toHost, ingestRate)
            msg = 'File %s is just ingested at %s on %s with a rate %s' % (fileId, filePath, toHost, ingestRate)
            logger.info(msg)
            return msg
        except Exception, err:
            logger.error(traceback.format_exc())
            return 'Exception (%s) when doing - File %s is just ingested at %s on %s' % (str(err), fileId, filePath, toHost)

@route('/report/hostdown')
def reportHostError():
    fileId = request.query.get('file_id')
    nexturl = request.query.get('nexturl')
    if (nexturl and fileId):
        o = urlparse(nexturl)
        try:
            ngamsJobMWALib.reportHostDown(fileId, '%s:%d' % (o.hostname, o.port))
        except Exception, err:
            logger.error(traceback.format_exc())
        finally:
            return 'Thanks for letting me know that %s:%d is down' % (o.hostname, o.port)

@route('/')
def redictToSubmit():
    redirect('/job/submit')

@get('/job/submit')
def submit_job_get():
    return template('ngamsJobMAN_submit.html')

@get('/rts/docs')
def showDocs():
    return template('ngamsJobMAN_docs.html')

def _responseMsg(msg):
    """
    show the msg as a response HTML page
    """
    return template('ngamsJobMAN_response.html', ret_msg = msg)    

@post('/job/submit')
def submit_job_post():
    name = request.forms.get('name')
    if invalidParam(name):
        return _responseMsg('Invalid user name')
    name = name.strip()
    
    observations = request.forms.get('observations')
    if invalidParam(observations):
        return _responseMsg('Invalid observation numbers')
    jtype = request.forms.get('type')
    if ('MWA_RTS' != jtype):
        return _responseMsg('Sorry we currently only accept MWA RTS jobs, %s jobs will be supported soon' % jtype)
    
    params = RTSJobParam()
    if ('GPU' == request.forms.get('processor')):
        params.use_gpu = True
    else:
        params.use_gpu = False
    
    use_mytpl =  int(request.forms.get('use_mytpl'))
    all_tpls = ''    
    if (not use_mytpl):
        for i in range(len(predef_tpls)):
            tt = request.forms.get('template_%d' % i)
            if (tt):
                all_tpls += ',%s' % tt            
        if (len(all_tpls) < 1):
            return _responseMsg('No templates are selected.')
        else:
            params.rts_tpl_name = all_tpls[1:] #remove the first comma
    else:
        tmp_prefix = request.forms.get('tpl_prefix').strip()
        tpl_path = os.path.dirname(tmp_prefix)
        if (not os.path.exists(tpl_path)):
            return _responseMsg('It appears that template path %s does not exist on Fornax.' % tpl_path)
        tpl_suffix = request.forms.get('tpl_suffix').strip()
        if (not tpl_suffix):
            tpl_suffix = ''
        tpl_files = request.forms.get('tpl_name').strip()
        if invalidParam(tpl_files):
            return _responseMsg('Please specify template name')
        for tpl_file in tpl_files.split(','):
            tpl_full_file = '%s%s%s' % (tmp_prefix, tpl_file, tpl_suffix)
            if (not os.path.exists(tpl_full_file)):
                return _responseMsg('Template file %s does not exist on Fornax' % tpl_full_file)
        params.rts_tpl_name = tpl_files
        params.rts_tplpf = tmp_prefix
        params.rts_tplsf = tpl_suffix
    
    lttimeout = request.forms.get('LT_timeout')
    if invalidParam(lttimeout):
        return _responseMsg('invalid Task execution timeout')
    try:
        params.LT_timeout = int(lttimeout)
    except Exception, err:
        return _responseMsg('invalid Task execution timeout')
    
    fitimeout = request.forms.get('FI_timeout')
    if invalidParam(fitimeout):
        return _responseMsg('invalid File ingestion timeout')
    try:
        params.FI_timeout = int(fitimeout)
    except Exception, err:
        return _responseMsg('invalid File ingestion timeout')
        
    obsNums = observations.replace(' ', '').split(',')
    try:
        for obsNum in obsNums:
            if (not ngamsJobMWALib.isValidObsNum(obsNum)):
                return _responseMsg('Observation %s does not appear to be valid.' % obsNum)
        for obsNum in obsNums:
            if (not ngamsJobMWALib.hasAllFilesInLTA(obsNum)):
                return _responseMsg('Observation %s does not have ALL files archived on Cortex yet.' % obsNum)
    except Exception, err:
        logger.error(traceback.format_exc())
        return _responseMsg('Fail to validate observation numbers, Exception: %s' % str(err))
    
    dt = datetime.datetime.now()
    jobId = name + '_' + dt.strftime('%Y%m%dT%H%M%S') + '.' + str(dt.microsecond / 1000)
    params.obsList = obsNums
    
    # launch thread to create and execute the job
    args = (jobId, params, jobDic)
    thrd = threading.Thread(None, _jobThread, 'MR_THRD_%s' % jobId, args) 
    thrd.setDaemon(1) # it will exit immediately should the server down
    thrd.start()
    return _responseMsg('Job %s has been submitted. <br/><ul>'  % jobId +\
        '<li> <a href="/job/monitor?job_id=%s">Monitor job progress</a></li>' % jobId +\
        '<li> <a href="/job/result?job_id=%s">Check job result</a></li></ul>' % jobId)
        #'<a href="/job/status?job_id=%s">View its status (JSON). </a> <br>' % jobId +\
    #"""

@post('/localtask/result')
def reportLocalTask():
    """
    Report the result of LocalTask to JobMAN
    """ 
    try:
        localTaskResult = pickle.loads(request.body.read())
    except Exception, err:
        msg = 'Invalid MRLocalTask pickle content: %s' % str(err)
        logger.error(msg)
        return msg
    taskId = localTaskResult._taskId
    if (localTaskResult.getErrCode()):
        msg = 'Task %s has an error: %s' % (taskId, localTaskResult.getInfo())
        logger.error(msg)
    else:
        if (localTaskResult.isResultAsFile()):
            msg = 'Got local task result for taskId: %s, url = %s' % (taskId, localTaskResult.getResultURL())
            logger.info(msg)
        else:
            msg = 'Got local task result for taskId: %s, info = %s' % (taskId, localTaskResult.getInfo())
            logger.info(msg)
    try:
        ngamsJobMWALib.localTaskCompleted(localTaskResult)
    except Exception, err:
        logger.error(traceback.format_exc())
    finally:
        if (msg):
            return msg
        else:
            return 'locatask result report with some exceptions'

@get('/localtask/dequeue')
def dequeueLocalTask():
    """
    Report to JobMAN that this task is just dequeued and starts running
    """
    taskId = request.query.get('task_id')
    if (invalidParam(taskId)):
        return 'Invalid task id'
    try:
        ngamsJobMWALib.localTaskDequeued(taskId)
        return 'OK'
    except Exception, err:
        logger.error(traceback.format_exc())
        return 'Fail to notify jobman of task dequeue'   
        
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
        return _responseMsg('Please provide an valid job_id as the parameter')
    return template('ngamsJobMonitor.html', job_id = jobId) 

@get('/job/testmonitor')
def testmonitorjob():
    """
    """
    return template('ngamsJobMonitor.html', job_id = '001')

@get('/job/result')
def getJobResult():
    """
    Retrieve the job result
    """
    jobId = request.query.get('job_id')
    if (invalidParam(jobId) or (not jobDic.has_key(jobId))):
        return _responseMsg('Please provide an valid job_id as the parameter')
    mrJob = jobDic[jobId]
    sta = mrJob.getStatus()
    if (sta < STATUS_COMPLETE):
        return _responseMsg('Job is still running or not yet started. Check the result later.')
    if (sta == STATUS_EXCEPTION and mrJob.getFinalJobResult() == None):
        return _responseMsg('Job encountered exceptions, result is not available')
    return _responseMsg(str(mrJob.getFinalJobResult()))

@get('/job/list')
def listJobs():
    """
    Show a list of jobs (running and completed) whose references
    reside in memory. Do not yet support job persistency
    """
    #job0 = ('001', 'Running', '35min')
    #job1 = ('002', 'Complete', '30min')
    #job2 = ('003', 'Complete', '3min')
    #job3 = ('004', 'Suspended', '44min')
    #job4 = ('005', 'Running', '1hour')
    jobList = []
    #jobList.append(job0)
    #jobList.append(job1)
    #jobList.append(job2)
    #jobList.append(job3)
    #jobList.append(job4)
    for job in jobDic.values():
        obs_nums = str(job.getParams().obsList).replace("'", "").replace('[', '').replace(']', '')
        tpl_names = job.getParams().rts_tpl_name
        file_timeout = str(job.getParams().FI_timeout)
        task_timeout = str(job.getParams().LT_timeout)
        
        jobList.append((job.getId(), job.getStatusString(), job.getWallTime(), obs_nums, tpl_names, file_timeout, task_timeout))
    jobList.sort(key = _jobSortFunc, reverse = True) # most recent first
    return template('ngamsJobMAN_listjob.html', jobList = jobList)
 
def _jobSortFunc(job):   
    return job[0].split('_')[1]

@route('/static/<filepath:path>')
def server_static(filepath):
    return static_file(filepath, root='./')

def getHello():
    return "Hello World 001"

def _jobThread(jobId, params, myjobDic):   
    job = RTSJob(jobId, params)
    myjobDic[jobId] = job
    if (job):
        try:
            job.buildRTSTasks()
            job.start()
        except Exception, err:
            job.setStatus(STATUS_EXCEPTION)
            job.setFinalJobResult('Fail to start the Job %s, Exception: %s' % (jobId, str(traceback.format_exc())))
            logger.error(traceback.format_exc())
    else:
        logger.error('Cannot initialise the job %s' % jobId)   

"""
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
"""
gconfig = None
def getConfig():
    """
    Maitain a singleton for configuration
    """
    global gconfig
    if (gconfig):
        return gconfig
    parser = OptionParser()
    parser.add_option("-c", "--cfg", dest = "config_fname", help = "The path to the configuration file (Mandatory)")
    (options, args) = parser.parse_args()
    if (None == options.config_fname):
        parser.print_help()
        return None
    if (not os.path.isfile(options.config_fname)):
        logger.warning('\nCannot access configuration file %s' % options.config_fname)
        return None
    
    config = ConfigParser.ConfigParser()
    config.readfp(open(options.config_fname))
    
    gconfig = config
    return config

def main():
    #FORMAT = "%(asctime)-15s %(message)s"
    FORMAT = "%(asctime)-15s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(filename='/tmp/NGAS_MWA/log/ngamsJobMAN.log', level=logging.DEBUG, format = FORMAT)
    logger.info('ngamsJobMAN Started.......')
    
    config = getConfig()
    if (not config):
        exit(1)
    
    # start the web server supported by bottle and paste
    run(host = config.get('Web Server', 'IpAddress'), 
        server = 'paste', 
        port = config.getint('Web Server', 'Port'), 
        debug = config.getboolean('Web Server', 'Debug'))
    
    logger.info('ngamsJobMAN Shutdown.......')

if __name__ == "__main__":
    main()

