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
# Who                   When             What
# -----------------   ----------      ------------
# chen.wu@icrar.org  26/May/2013        Created

"""
This module builds the NGAS MapReduce job for the MWA RTS system running
on a cluster of NGAS servers (e.g. iVEC Fornax)

To implement this module, knowledge of MWA and RTS is hardcoded. So if you want
to have another pipeline (e.g. CASA for VLA), write your own based on the generic MRTask 
framework in ngamsJobProtocol.py
"""

import os, threading, commands, time, urllib2, logging
from Queue import Queue, Empty
import cPickle as pickle
from urlparse import urlparse

from ngamsJobProtocol import *
import ngamsJobMWALib

DEBUG = 1
debug_url = 'http://192.168.1.1:7777'

FSTATUS_NOT_STARTED = 0 # white
FSTATUS_STAGING = 1 # green
FSTATUS_ONLINE = 2 # blue
FSTATUS_OFFLINE = 3 # red

logger = logging.getLogger(__name__)

def dprint(s):
    if (DEBUG):
        logger.info(s)

class RTSJob(MapReduceTask):
    """
    A job submitted by an MWA scientist to do the RTS imaging pipeline
    It is the top level of the RTS MRTask tree
    """
    def __init__(self, jobId, rtsParam):
        """
        Constructor
        
        JobId:    (string) identifier uniquely identify this RTSJob (e.g. RTS-cwu-2013-05-29H05:04:03.456)
        rtsParam:    RTS job parameters (class RTSJobParam)
        """
        MapReduceTask.__init__(self, jobId)
        self.__completedObsTasks = 0
        self.__hostAllocDict = {} # key: hostId that has been allocated, value: place_holder (e.g. CorrId)
        self.__obsRespQ = Queue()
        self._rtsParam = rtsParam
        self.__buildRTSTasks(rtsParam)
        
    
    def __buildRTSTasks(self, rtsParam):
        if (rtsParam.obsList == None or len(rtsParam.obsList) == 0):
            errMsg = 'No observation numbers found in this RTS Job'
            raise Exception, errMsg
        
        num_obs = len(rtsParam.obsList)
        
        for j in range(num_obs):
            obs_num = rtsParam.obsList[j]
            obsTask = ObsTask(obs_num, rtsParam, self)
            self.addMapper(obsTask)
            
            fileIds = ngamsJobMWALib.getFileIdsByObsNum(obs_num)
            if (len(fileIds.keys()) == 0):
                raise Exception('Obs number %s does not appear to be valid. We require exact observation numbers rather than GPS ranges.' % obs_num)
            for k in range(rtsParam.num_subband):
                if (fileIds.has_key(k + 1)): # it is possible that some correlators were not on
                    exeHost, fileLocaDic = self._allocateHost(fileIds[k + 1])
                    if (not exeHost):
                        errMsg = 'There are no online NGAS servers available'
                        raise Exception, errMsg
                    corrTask = CorrTask(str(k + 1), fileIds[k + 1], rtsParam, obsTask, exeHost, fileLocDict = fileLocaDic)
                    obsTask.addMapper(corrTask)  
                    if (self.__hostAllocDict.has_key(exeHost)):    
                        self.__hostAllocDict[exeHost] += 1
                    else:
                        self.__hostAllocDict[exeHost] = 1   
        
    def _allocateHost(self, corrFileList):
        """
        Allocate host to a correlator based on two criteria:
        1. maximum of parallel processing (i.e. declustering)
        2. data locality
        
        corrFileList:    A list of file ids belonging to a correlator (a List of strings)
        return:          A tuple, 0 - a host (hostId:port) (string), 1 - a dict, # key - fileId, value - FileLocation
        
        This function is not thread-safe, it must be sequentially called by a single thread,    
        """
        fileLocDict = ngamsJobMWALib.getBestHost(corrFileList, self.__hostAllocDict.keys())
        if (not fileLocDict or len(fileLocDict.keys()) == 0):
            nextHost = ngamsJobMWALib.getNextOnlineHost(self.__hostAllocDict.keys())
            if (not nextHost): 
                logger.warning('Cannot find a host that is different from what have been allocated for files %s' % str(corrFileList))
                # try a random one that might have been allocated, thus compromising the maximum parallel processing
                return (ngamsJobMWALib.getNextOnlineHost(), {}) 
            else:
                return (nextHost, {})
        else:
            return (fileLocDict.values()[0]._svrHost, fileLocDict)
    
    def getParams(self):
        return self._rtsParam
    
    def combine(self, mapOutput):
        """
        Hold each observation's result, thread safe
        """
        dprint('Job %s is combined' % self.getId())
        if (mapOutput):
            self.__obsRespQ.put(mapOutput) # this is thread safe
    
    def reduce(self):
        """
        Return urls of each tar file, each of which corresponds to an observation's images
        """
        dprint('Job %s is reduced' % self.getId())
        jobRe = JobResult(self.getId())
        
        while (1):
            obsTaskRe = None
            try:
                obsTaskRe = self.__obsRespQ.get_nowait()
            except Empty, e:
                break
            jobRe.merge(obsTaskRe)
        
        self.setFinalJobResult(jobRe)
        return str(jobRe)
    
    def setFinalJobResult(self, jobRe):
        self._jobRe = jobRe
        
    def getFinalJobResult(self):
        return self._jobRe

class ObsTask(MapReduceTask):
    """
    MWA Observation, the second level of the RTS MRTask tree
    Thus, a RTSJob consists of multiple ObsTasks
    """
    def __init__(self, obsNum, rtsParam, jobParent):
        """
        Constructor
        
        obsNum:    (string) observation number/id, i.e. the GPS time of each MWA observation
        rtsParam:    RTS job parameters (class RTSJobParam)
        """
        MapReduceTask.__init__(self, str(obsNum), parent = jobParent) #in case caller still passes in integer
        self.__completedCorrTasks = 0
        self.__rtsParam = rtsParam
        self.__corrRespQ = Queue()
        self._taskExeHost = ''
        self._ltComltEvent = threading.Event() # local task complete event
        self._ltDequeueEvent = threading.Event() # local task dequeue event
        self._localTaskResult = None
        self._timeOut4LT = 60 * 10 # 10 min
        self._progress = -1
    
    def combine(self, mapOutput):
        """
        Hold each correlator's result, thread safe
        """
        dprint('Observation %s is combined' % self.getId())
        if (mapOutput):
            self.__corrRespQ.put(mapOutput) # this is thread safe
    
    def reduce(self):
        """
        Return results of each correlator, each of which corresponds to images of a subband
        """
        dprint('Observation %s is reduced' % self.getId())
        obsTaskRe = ObsTaskResult(self.getId())
        timeout_retry = 0
        
        imgurlList = []
        while (1):
            corrTaskRe = None
            try:
                corrTaskRe = self.__corrRespQ.get_nowait()
                if (corrTaskRe._errcode == 0):
                    imgurl = corrTaskRe._imgUrl
                    imgurlList.append(imgurl)                                       
                else:
                    pass
            except Empty, e:
                break
            obsTaskRe.merge(corrTaskRe)
        
        if (len(imgurlList) < 1):
            errmsg = 'Fail to find any image urls from correlators'
            obsTaskRe._errmsg = errmsg
            obsTaskRe._errcode = 4
            self.setStatus(STATUS_EXCEPTION)
            dprint(obsTaskRe._errmsg)
            return obsTaskRe
        
        # 1. decide an exeHost to do the local reduction task    
        self._progress = 0
        while (self._progress == 0):    
            self._progress = 1
            host = None        
            
            # TODO
            # for loop the imgurllist until find one that is online
            # if cannot find, just get random one that is online
            urlError = 0
            for imgurl in imgurlList:
                try:
                    host = urlparse(imgurl)
                    if (not host):
                        urlError = 1
                        continue
                        #obsTaskRe._errcode = 1 
                        #obsTaskRe._errmsg = 'Image url is not valid %s' % imgurl
                    else:
                        ret = ngamsJobMWALib.pingHost('http://%s:%d/STATUS' % (host.hostname, host.port))
                        if (ret):
                            urlError = 1
                            continue
                        else:
                            urlError = 0
                            break
                except Exception, err:                                       
                    urlError = 1
                    continue
            
            if (urlError):
                #try another random site
                host = ngamsJobMWALib.getNextOnlineHost()
                if (host):
                    self._taskExeHost = host
                else:
                    obsTaskRe._errcode = 1
                    obsTaskRe._errmsg = 'Failed to find any host to execute local job'
                    self.setStatus(STATUS_EXCEPTION)
                    break
            else:    
                self._taskExeHost = '%s:%d' % (host.hostname, host.port)
            
            #  2. construct the local TaskId (jobId__obsNum)
            taskId = '%s__%s' % (self.getParent().getId(), self.getId())
            obsLT = ObsLocalTask(taskId, imgurlList, self.__rtsParam)
            
            # 3. register the local task
            ngamsJobMWALib.registerLocalTask(taskId, self)
            
            # 4. - do the real reduction work (i.e. combine all images from correlators into a single one) at a remote node
            strLT = pickle.dumps(obsLT)
            try:
                strRes = urllib2.urlopen('http://%s/RUNTASK' % self._taskExeHost, data = strLT, timeout = 15).read()
                logger.debug('Submit local task, acknowledgement received: %s'     % strRes)
            except urllib2.URLError, urlerr:
                if (str(urlerr).find('Connection refused') > -1): # the host is down
                    #TODO - make it a log!
                    logger.info('The original host %s is down, changing to another host to download all image files...' % self._taskExeHost)
                    self._progress = 0
                    self._taskExeHost = None
                    self._localTaskResult = None
                    continue # the current host is down, change to another host, and redo file staging
                else:
                    errmsg = 'Fail to schedule obs reduction task on %s: %s' % (self._taskExeHost, str(urlerr))
                    obsTaskRe._errmsg = errmsg
                    obsTaskRe._errcode = 2
                    self.setStatus(STATUS_EXCEPTION)
                    dprint(obsTaskRe._errmsg)
                    break       
            
            self._progress = 4.5
            self.setStatus(STATUS_NOT_STARTED)
            self._ltDequeueEvent.wait() # no timeout            
            
            # 5. - wait until result comes back
            self.setStatus(STATUS_RUNNING)
            self._ltComltEvent.wait(self._timeOut4LT)
            if (not self._localTaskResult):
                timeout_retry += 1
                if (timeout_retry > 2):
                    errmsg = 'Timeout when running obs reduction task on %s' % (self._taskExeHost)
                    obsTaskRe._errmsg = errmsg
                    obsTaskRe._errcode = 3
                    self.setStatus(STATUS_EXCEPTION)
                    dprint(obsTaskRe._errmsg)
                    break
                else:
                    logger.info('The local task %s on node %s has timed out, try another host' % (taskId, self._taskExeHost))
                    self._progress = 0
                    self._taskExeHost = None
                    self._ltComltEvent.clear()
                    self._localTaskResult = None
                    continue
            else:
                obsTaskRe._errcode = 0
                obsTaskRe.setImgUrl(self._localTaskResult.getResultURL())
        
        return obsTaskRe
    
    def localTaskCompleted(self, localTaskResult):
        self._localTaskResult = localTaskResult
        self._ltDequeueEvent.set()
        self._ltComltEvent.set()
    
    def localTaskDequeued(self, taskId):
        self._ltDequeueEvent.set()
        

class CorrTask(MapReduceTask):
    """
    MWA Correlator task, the third level of the RTS MRTask tree
    It is where actual execution happens, and an ObsTask consists of 
    multiple CorrTasks
    Each CorrTask processes all files generated by that
    correlator 
    """
    def __init__(self, corrId, fileIds, rtsParam, obsParent, taskExeHost, fileLocDict = {}):
        """
        Constructor
        
        corrId:    (string) Correlator identifier (e.g. 1, 2, ... , 24)
        fileIds:   (list) A list of file ids (string) to be processed by this correlator task
        rtsParam:    RTS job parameters (class RTSJobParam)
        
        """
        MapReduceTask.__init__(self, str(corrId), parent = obsParent)
        self.__fileIds = fileIds
        self.__rtsParam = rtsParam
        self._progress = -1
        self._fileIngEvent = threading.Event() # file ingestion event
        self._ltComltEvent = threading.Event() # local task complete event
        self._ltDequeueEvent = threading.Event() # local task dequeue event
        self._timeOut4FileIng = rtsParam.FI_timeout # maximum wait for 30 min during file staging
        self._timeOut4LT = rtsParam.LT_timeout # maximum wait for an hour during RTS local task execution
        self._numIngested = 0 # at start, assume all files are ingested
        self._numIngSem = threading.Semaphore()
        self._hostErrSem = threading.Semaphore()
        self._taskExeHost = taskExeHost # this will become the NextURL for PARCHIVE during file staging from Cortex/other hosts (if any)
        self._fileLocDict = fileLocDict # key - fileId, value - FileLocation (may not be the final one)
        self._localTaskResult = None
        self._failedLTExec = 0 # the number of failed local task executions
        self._blackList = []
    
    def _stageFiles(self, cre):
        """
        cre - CorrTaskResult
        """
        # 1 Check all files' locations, and determines the best host 
        self._numIngested = len(self._fileLocDict.keys())  
        if (not self._taskExeHost):  # this is re-try
            try:
                self._fileLocDict = ngamsJobMWALib.getBestHost(self.__fileIds, self._blackList)
            except Exception, e:
                cre._errcode = 4
                cre._errmsg = 'Fail to get the best host for file list %s: %s' % (str(self.__fileIds), str(e))
                self.setStatus(STATUS_EXCEPTION)
                dprint(cre._errmsg)
                return cre
            
            self._numIngested = len(self._fileLocDict.keys())
            if (self._numIngested > 0):
                self._taskExeHost = self._fileLocDict.values()[0]._svrHost
            else:
                self._taskExeHost = ngamsJobMWALib.getNextOnlineHost(self._blackList)
            
            if (not self._taskExeHost):
                cre._errcode = 7
                cre._errmsg = 'There are no online NGAS servers available'   
                self.setStatus(STATUS_EXCEPTION)
                dprint(cre._errmsg)
                return cre     
        
        # 2. For those files that are not on the best host, check if they are inside the cluster
        #    If so, stage them from an cluster node, otherwise, stage them from the external archive
        frmExtList = []
        for fid in self.__fileIds:
            if (not self._fileLocDict.has_key(fid)):                
                try:
                    fileLoc = ngamsJobMWALib.getFileLocations(fid)           
                except Exception, e:
                    cre._errmsg = "Fail to get location for file '%s': %s" % (fid, str(e))
                    cre._errcode = 2
                    dprint(cre._errmsg)
                    # most likely a DB error                
                if (len(fileLoc) == 0 or cre._errcode == 2):
                    # not in the cluster/or some db error , stage from outside
                    frmExtList.append(fid)
                    cre._errcode = 0 # reset the error code
                else:
                    stageerr = 0
                    for i in range(len(fileLoc)):
                        # record its actual location inside the cluster
                        self._fileLocDict[fid] = fileLoc[i] # get the host
                        # stage from that host within the cluster
                        stageerr = ngamsJobMWALib.stageFile([fid], self, self._taskExeHost, fileLoc[0]._svrHost)
                        if (0 == stageerr):
                            break
                    if (stageerr):
                        # if all cluster nodes failed, try the external archive
                        frmExtList.append(fid)
                    
        if (len(frmExtList) > 0):
            stageerr = ngamsJobMWALib.stageFile(frmExtList, self, self._taskExeHost)
            if (stageerr):
                cre._errmsg = "Fail to stage files %s from the external archive to %s. Stage errorcode = %d" % (frmExtList, self._taskExeHost, stageerr)
                cre._errcode = 5
                self.setStatus(STATUS_EXCEPTION)
                dprint(cre._errmsg)
                return cre
        
        if (self._numIngested == len(self.__fileIds)): # all files are there
            self._fileIngEvent.set() # so do not block
        else:
            self._fileIngEvent.clear()
            
        return cre
    
    def map(self, mapInput = None):
        """
        Actual work for Correlator's file processing
        """
        cre = CorrTaskResult(self.getId(), self.__fileIds)
        if (self.__fileIds == None or len(self.__fileIds) == 0):
            cre._errcode = 1
            cre._errmsg = 'No correlator files in the input'
            self.setStatus(STATUS_EXCEPTION)
            return cre # should raise exception here
        #TODO - deal with timeout!
        dprint('Correlator %s is being mapped' % self.getId())
        self._progress = 0     
                
        while (self._progress == 0):
            self._progress = 1
            cre = self._stageFiles(cre)
            if (cre._errcode):
                #return cre
                break
            # block if / while files are being ingested
            self._fileIngEvent.wait(timeout = self._timeOut4FileIng)
            
            ret = ngamsJobMWALib.pingHost('http://%s/STATUS' % (self._taskExeHost))
            if (ret):
                # host was down
                logger.info('The task host %d was down, retry another host ...' % self._taskExeHost)
                self._taskExeHost = None
                self._fileIngEvent.clear()
                self._progress = 0
                if (self._localTaskResult):
                    self._localTaskResult = None
                continue
            
            """
            if (self._progress == 0):
                # host was down (i.e. the function reportHostDown was called)
                logger.info('The task host %d was down, retry another host ...' % self._taskExeHost)
                self._taskExeHost = None
                self._fileIngEvent.clear()
                continue
            """
            
            if (self._numIngested < len(self.__fileIds)):
                
                cre._errmsg = "Timeout when waiting for file ingestion. Ingested %d out of %d." % (self._numIngested, len(self.__fileIds))
                cre._errcode = 3
                self.setStatus(STATUS_EXCEPTION)
                logger.error(cre._errmsg)
                # this could be caused by the entire network issue so no point to retry
                #return cre
                break
            
            self._progress = 2
            # create local task and send them to remote servers
            #  1. construct the local TaskId (jobId__obsNum__corrId)
            taskId = '%s__%s__%s' % (self.getParent().getParent().getId(), self.getParent().getId(), self.getId())
            #  2. construct the file list
            fileLocList = []
            for flocs in self._fileLocDict.values():
                fileLocList.append(flocs._filePath)
            
            corrLT = CorrLocalTask(taskId, fileLocList, self.__rtsParam)
            
            # 3. submit the local task to a remote host to run
            # but register itself to receive task complete event first
            ngamsJobMWALib.registerLocalTask(taskId, self)
            strLT = pickle.dumps(corrLT)
            try:
                strRes = urllib2.urlopen('http://%s/RUNTASK' % self._taskExeHost, data = strLT, timeout = 15).read()
                logger.debug('Submit localtask, acknowledgement received: %s' % strRes)
            except urllib2.URLError, urlerr:
                if (str(urlerr).find('Connection refused') > -1): # the host is down
                    logger.info('The original host %s is down, changing to another host and re-staging all files...' % self._taskExeHost)
                    self._blackList.append(self._taskExeHost)
                    self._progress = 0
                    self._taskExeHost = None
                    self._fileIngEvent.clear()
                    if (self._localTaskResult):
                        self._localTaskResult = None
                    continue # the current host is down, change to another host, and redo file staging
                else:
                    errmsg = 'Fail to schedule correlator task on %s: %s' % (self._taskExeHost, str(urlerr))
                    cre._errmsg = errmsg
                    cre._errcode = 6
                    self.setStatus(STATUS_EXCEPTION)
                    dprint(cre._errmsg)
                    #return cre
                    break
                
            # 3.5 Waiting for the task dequeue
            self._progress = 2.5
            self.setStatus(STATUS_NOT_STARTED)
            self._ltDequeueEvent.wait(timeout = self._timeOut4LT * 3) # dequeue timeout is 3 times of the task timeout
            if (not self._ltDequeueEvent.isSet()):
                #queue timeout, need to change to another host
                logger.info('The local task %s on node %s has timed out in the queue, try another host' % (taskId, self._taskExeHost))
                self._blackList.append(self._taskExeHost)
                self._progress = 0
                self._taskExeHost = None
                self._fileIngEvent.clear()
                self._ltDequeueEvent.clear()
                self._ltComltEvent.clear()
                self._localTaskResult = None
                continue
            
            # 4. Waiting for the result
            self._progress = 3
            self.setStatus(STATUS_RUNNING)
            if (not self._localTaskResult): # just in case previously abandoned tasks got back some result so no need to wait again               
                logger.debug('Preparing for waiting on correlator %s' % self.getId())
                self._ltComltEvent.wait(timeout = self._timeOut4LT)
                logger.debug('Woke up on correlator %s' % self.getId())
                if (not self._localTaskResult):
                    # timeout, consider re-run the task on another node
                    self._failedLTExec += 1
                    if (self._failedLTExec > 2): # maximum attempts 1 times, no - retry!!!!
                        errmsg = 'Timeout when waiting for the local task to complete on node %s' % self._taskExeHost
                        cre._errmsg = errmsg
                        cre._errcode = 7
                        self.setStatus(STATUS_EXCEPTION)
                        logger.error(errmsg)
                        break
                    else:
                        logger.info('The local task %s on node %s has timed out, try another host' % (taskId, self._taskExeHost))
                        self._blackList.append(self._taskExeHost)
                        self._progress = 0
                        self._taskExeHost = None
                        self._fileIngEvent.clear()
                        self._ltDequeueEvent.clear()
                        self._ltComltEvent.clear()
                        self._localTaskResult = None
                        continue
            logger.debug('Continue on correlator %s' % self.getId())
            # 5. analyse the result value
            if (self._localTaskResult.getErrCode()):
                errmsg = 'Correlator local task returned error: %d - %s' % (self._localTaskResult.getErrCode(), self._localTaskResult.getInfo())
                cre._errcode = 8
                cre._errmsg = errmsg
                self.setStatus(STATUS_EXCEPTION)
                logger.error(errmsg)
                break
            elif (not self._localTaskResult.getResultURL()):
                self._failedLTExec += 1
                if (self._failedLTExec > 2):
                    errmsg = 'The local task %s on node %s did not produce any img_url, and retry exhausted' % (taskId, self._taskExeHost)
                    cre._errmsg = errmsg
                    cre._errcode = 10
                    self.setStatus(STATUS_EXCEPTION)
                    logger.error(errmsg)
                    break
                else:
                    logger.info('The local task %s on node %s did not produce any img_url, try another host' % (taskId, self._taskExeHost))
                    self._blackList.append(self._taskExeHost)
                    self._progress = 0
                    self._taskExeHost = None
                    self._fileIngEvent.clear()
                    self._ltDequeueEvent.clear()
                    self._ltComltEvent.clear()
                    self._localTaskResult = None
                    continue
            else:
                logger.debug('Got correct result on correlator %s, length of _blacklist = %d' % (self.getId(), len(self._blackList)))
                cre._errcode = 0
                cre._imgUrl = self._localTaskResult.getResultURL()
                #TODO - cancel any pending local tasks since the final result is obtained
                if (len(self._blackList)):
                    for failHost in self._blackList:
                        if (failHost != self._taskExeHost):
                            try:
                                logger.debug('Before calling taskcancel on correlator %s' % self.getId())
                                strRes = urllib2.urlopen('http://%s/RUNTASK?action=cancel&task_id=%s' % (failHost, taskId), timeout = 15).read()
                                logger.debug('Submit task cancel request, acknowledgement received: %s' % strRes)
                            except urllib2.URLError, urlerr:
                                logger.error('Fail to submit task cancel request for task: %s, Exception: %s', (taskId, str(urlerr)))
        return cre 
    
    def fileIngested(self, fileId, filePath, ingestRate):
        logger.info('Obs %s corr %s received file %s' % (self.getParent().getId(), self.getId(), fileId))
        self._numIngSem.acquire()
        self._numIngested += 1
        self._numIngSem.release()
        floc = ngamsJobMWALib.FileLocation(self._taskExeHost, filePath, fileId)
        floc._ingestRate = ingestRate
        self._fileLocDict[fileId] = floc
        if (self._numIngested == len(self.__fileIds)):
            self._fileIngEvent.set()
    
    def localTaskCompleted(self, localTaskResult):
        logger.debug('Received task result with an URL: %s on correlator %s, progress = %s, _ltComltEvent isset = %s' % (localTaskResult.getResultURL(), self.getId(), str(self._progress), str(self._ltComltEvent.isSet())))
        self._localTaskResult = localTaskResult
        self._ltDequeueEvent.set()
        self._ltComltEvent.set()
        
        
    def localTaskDequeued(self, taskId):
        self._ltDequeueEvent.set()
    
    def reportHostDown(self, fileId, errorHost):
        """
        Notify the task of a failed host, indicating that re-staging is necessary
        
        this function deals with duplicated notifications of the same 
        failed hosts on the same correlator (for different files)
        
        errorHost:    the host that is down (string)
        fileId:       the file which causes the detection of this error during ingestion
            
        """
        # reschedule staging the file to another host
        logger.info('Obs %s corr %s received host down msg on %s' % (self.getParent().getId(), self.getId(), errorHost))
        self._hostErrSem.acquire()
        if (self._taskExeHost == errorHost and  # make sure the failed host is the current host
            self._progress == 1): # and make sure it is currently staging some files
            
            self._progress = 0
            self._fileIngEvent.set() # wake up in case it is still waiting
            
        self._hostErrSem.release()
    
    def getHostNameFromHostId(self, hostId):
        """
        This function is cluster-specific
        The following is based on Fornax
        """
        hostName = hostId.split(':')[0] # get rid of port if any
        hostName = hostName.split('.')[-1]
        if (len(hostName) == 1):
            return 'f00%s' % hostName
        elif (len(hostName) == 2):
            return 'f0%s' % hostName
        else:
            return hostName
    
    def toJSONObj(self):
        """
        Override the default impl
        to jsonise file ids
        """
        
        jsobj = {}
        jsobj['name'] = self.getId() + '-' + self.getHostNameFromHostId(self._taskExeHost) + '-' + statusDic[self.getStatus()]
        jsobj['status'] = self.getStatus()
        #moredic = self.getMoreJSONAttr()
        if (self.__fileIds == None or len(self.__fileIds) == 0):
            return jsobj
        children = []
        for fileId in self.__fileIds:
            fjsobj = {}
            if (self._fileLocDict.has_key(fileId)):
                floc = self._fileLocDict[fileId]._svrHost
                ingR = self._fileLocDict[fileId]._ingestRate
                if (not ingR):
                    ingR = 0
                if (floc == self._taskExeHost):
                    fjsobj['status'] = FSTATUS_ONLINE
                else:
                    fjsobj['status'] = FSTATUS_STAGING
                floc = self.getHostNameFromHostId(floc)
            else:
                floc = 'Offline'
                fjsobj['status'] = FSTATUS_OFFLINE # 1 online, 0 offline
                ingR = 0
            #
            try:
                fjsobj['name'] = '%s@%s-%.0fMB/s' % (fileId, floc, (float(ingR) / 1024.0 ** 2)) # convert to MB/s
            except Exception, jerr:
                logger.info('Ingestion rate = %s. Exception: %s' % (str(ingR), str(jerr)))
                fjsobj['name'] = '%s@%s-%.0fMB/s' % (fileId, floc, float(0.0))
            
            children.append(fjsobj)
        jsobj['children'] = children
        return jsobj
        
        
    """
    def getMoreJSONAttr(self):
        moredic = {}
        name = self.getId()
        
        for fileId in self.__fileIds:
            name += ',' + fileId[26:]
        
        moredic['name'] = name  
        return moredic 
    """ 

class CorrTaskResult:
    """
    A class hold all the results values produced by the correlator task
    """    
    def __init__(self, corrId, fileIds, imgUrl = None):
        """
        Constructor
        
        corrId:    correlator id (string)
        fileIds:   fileIds belong to this correlator (a list of string)
        imgUrl:    (optional) If successful, the url of the generated image zip file (string)
        """
        self.__corrId = corrId
        self._subbandId = int(corrId)
        self._fileIds = fileIds
        # imgUrl will be used by the ObsTask reducer, which 
        # sends a job to imgUrl to aggregate all correlator zip files into one zip file
        self._imgUrl = imgUrl # e.g. 192.168.1.1:7777/RETRIEVE?file_id=job001_obs002_gpubox02.zip
        self._errcode = 0
        self._errmsg = ''

class ObsTaskResult:
    """
    """
    def __init__(self, obsNum):
        """
        """
        self._obsNum = obsNum
        self._imgUrl = None
        self.good_list = [] # a list of tuples (subbandId, fileIds separated by comma)
        self.bad_list = [] # a list of tuples (subbandId, errMsg)
        self._errcode = 0
        self._errmsg = ''
    
    def merge(self, corrTaskResult):
        """
        Merge a correlator's result into this observation result
        This is not thread safe, so call it in the "reduce" but not "combine"
        """
        if (not corrTaskResult):
            return
        
        if (corrTaskResult._errcode): # error occured
            re = (corrTaskResult._subbandId, 'errorcode:%d, errmsg:%s' % 
                  (corrTaskResult._errcode, corrTaskResult._errmsg))
            self.bad_list.append(re)
            self.bad_list.sort()
        else:
            #fileList = corrTaskResult._fileIds
            #nf = len(fileList)
            #if (nf < 1):
            #    return
            #fids = fileList[0]
            #if (nf > 1):
            #    for fileId in fileList[1:]:
            #        fids += ',%s' % fileId
            re = (corrTaskResult._subbandId, corrTaskResult._imgUrl) # at least print out successful img urls
            self.good_list.append(re)
            self.good_list.sort()                    
    
    def setImgUrl(self, imgUrl):
        """
        Set the final url to get all images of this observation
        """
        self._imgUrl = imgUrl
        
    def __str__(self):
        sl = len(self.good_list)
        fl = len(self.bad_list)
        tt = sl + fl
        
        if (fl):
            re = '# of successful subbands:\t\t%d\n' % sl # no image url available for incomplete observations
            for subtuple in self.good_list:
                re += '\t subbandId: %d, image urls (for this sub-band only): %s\n' % (subtuple[0], subtuple[1])
            re += '# of failed subbands:\t\t%d\n' % fl
            for subtuple in self.bad_list:
                re += '\t subbandId: %d, vis files failed: %s\n' % (subtuple[0], subtuple[1])
        else:
            re = 'image_url = %s (accessible from within Fornax) \n # of completed subbands: \t\t%d\n\n' % (self._imgUrl, tt)
        
        
        return re + '\n'   
        

class JobResult:
    """
    """
    def __init__(self, jobId):
        """
        constructor
        """
        self.__jobId = jobId
        self.__obsGoodList = []
        self.__obsBadList = []
        self.__pureGood = 0
        self.__totalObs = 0
    
    def merge(self, obsTaskResult):
        """
        """
        if (not obsTaskResult):
            return
        self.__totalObs += 1
        if (len(obsTaskResult.bad_list) == 0):
            self.__pureGood += 1        
            self.__obsGoodList.append(obsTaskResult)
            self.__obsGoodList.sort()
        else:
            self.__obsBadList.append(obsTaskResult)
            self.__obsBadList.sort()
    
    def __str__(self):
        
        re = '# of successful observations: \t\t%d\n\n' % self.__pureGood
        for obsRT in self.__obsGoodList:
            re += 'Observation - %s\n%s\n' % (obsRT._obsNum, '-' * len('Observation - ' + obsRT._obsNum))
            re += str(obsRT)
        if (self.__pureGood != self.__totalObs):
            re += '# of failed observations: \t\t%d\n\n' % (self.__totalObs - self.__pureGood)
            for obsRT in self.__obsBadList:
                re += 'Observation - %s\n%s\n' % (obsRT._obsNum, '-' * len('Observation - ' + obsRT._obsNum))
                re += str(obsRT)
        
        re = re.replace('\t', '&nbsp;&nbsp;&nbsp;&nbsp;')
        re = re.replace('\n', '<br/>')
        #header = '<html><head><style>p.ex1{font:12px arial,sans-serif;}</style></head><body><p class="ex1">'
        #tail = '</p></body></html>'
        return re 
        #'%s%s%s' % (header, re, tail)
    
    def toJSON(self):
        pass
    
    def toText(self):
        return self.__str__()


class RTSJobParam:
    """
    A class contains essential/optional parameters to 
    run the RTS pipeline
    """
    def __init__(self):
        """
        Constructor
        Set default values to all parameters
        """
        self.obsList = [] # a list of observation numbers
        self.time_resolution = 0.5
        self.fine_channel = 40 # KHz
        self.num_subband = 24 # should be the same as coarse channel
        self.tile = '128T'
        
        self.mwa_path = '/scratch/astronomy556/MWA'
        #RTS template prefix (optional, string)
        self.rts_tplpf = '%s/RTS/utils/templates/RTS_template_' % self.mwa_path
        #RTS template suffix
        self.rts_tplsf = '.in'
        self.ngas_src = '%s/ngas_rt' % self.mwa_path
        self.ngas_processing_path = '/tmp/NGAS_MWA/processing'
        self.LT_timeout = 3600
        self.FI_timeout = 3600
        
        
        #RTS template names for this processing (optional, string - comma separated aliases, e.g.
        #drift,regrid,snapshots, default = 'regrid'). Each name will be concatenated with 
        #rts-tpl-pf and rts-tpl-sf to form the complete template file path, 
        #e.g. /scratch/astronomy556/MWA/RTS/utils/templates/RTS_template_regrid.in
        
        # this should get it from the job submission form
        self.rts_tpl_name = 'regrid,simplecal'
        self.use_gpu = False

class DummyLocalTask(MRLocalTask):
    """
    This class is used to simulate some local tasks
    during tests
    """
    def __init__(self, taskId):
        """
        Constructor
        
        taskId      uniquely identify this CorrLocalTask
        """
        MRLocalTask.__init__(self, taskId)                
    
    def execute(self):
        ret = MRLocalTaskResult(self._taskId, 0, 'everything is fine')
        time.sleep(10) #simulate work
        return ret

class ObsLocalTask(MRLocalTask):
    
    def __init__(self, taskId, imgurl_list, params):
        """
        Constructor
        
        taksId        uniquely identify this ObsLocalTask (string)
        imgurl_list   a list of img urls (List) 
        params        RTSJobParam
        """
        MRLocalTask.__init__(self, taskId)
        self._imgurl_list = imgurl_list
        self._params = params
    
    def execute(self):
        """
        Task manager calls this function to
        execute the task.
        
        Return:    MRLocalTaskResult
        """
        # create the working directory
        ids = self._taskId.split('__')
        job_id = ids[0]
        obs_num = int(ids[1])
        work_dir = '%s/%s/%s/%d' % (self._params.ngas_processing_path, self._taskId, job_id, obs_num)
        if (os.path.exists(work_dir)):
            cmd = 'rm -rf %s' % work_dir
            ret = self._runBashCmd(cmd)
            if (ret):
                return ret
        cmd = 'mkdir -p %s' % work_dir
        ret = self._runBashCmd(cmd)
        if (ret):
            return ret
        
        # cd working directory and download images
        os.chdir(work_dir)
        for imgurl in self._imgurl_list:
            cmd = 'wget -O tmp.tar.gz %s' % imgurl
            self._runBashCmd(cmd, False)
            cmd = 'tar -xzf tmp.tar.gz'
            self._runBashCmd(cmd, False)
        
        cmd = 'rm tmp.tar.gz'
        self._runBashCmd(cmd, False)
        
        # go to the upper level directory, and pack all image files together
        upp_dir = '%s/%s' % (self._params.ngas_processing_path, self._taskId)
        os.chdir(upp_dir)
        cmd = 'tar -czf %s.tar.gz %s/' % (self._taskId, job_id)
        ret = self._runBashCmd(cmd)
        if (ret):
            return ret
        
        # record the image path so that cmd_RUNTASK can use it to archive it on the 
        # same host
        complete_img_path = '%s/%s.tar.gz' % (upp_dir, self._taskId)
        ret = MRLocalTaskResult(self._taskId, 0, complete_img_path, True)
        return ret
    
    def _runBashCmd(self, cmd, failonerr = True):
        re = commands.getstatusoutput(cmd)
        if (failonerr and re[0]):
            ret = MRLocalTaskResult(self._taskId, re[0], 'Fail to %s due to %s' % (cmd, re[1]))
            return ret
        else:
            return None
        
        
        # wget -O file_id imgurl
    
class CorrLocalTask(MRLocalTask):
    
    def __init__(self, taskId, filelist, params):
        """
        Constructor
        
        taskId      uniquely identify this CorrLocalTask (string)
        filelist    a list of file path (List)
        params      RTSJobParam  
        """
        MRLocalTask.__init__(self, taskId)
        self._filelist = filelist
        self._params = params
        
    
    def execute(self):
        """
        Task manager calls this function to
        execute the task.
        
        Return:    MRLocalTaskResult
        """
        #TODO - this cmd should be a member of the RTSJobParam class
        #       which is read from the configuration file
        cmd = '/scratch/astronomy556/MWA/ngas_rt/src/ngamsPlugIns/ngamsJob_MWA_RTS_Task.sh'
        args = self.paramsToArgs()
        re = commands.getstatusoutput('%s %s' % (cmd, args))
        ret = MRLocalTaskResult(self._taskId, re[0], re[1], True)
        return ret
    
    def paramsToArgs(self):
        """
        Convert self._params to command line arguments
        
        Return:    command line arguments (string)
        """
        #TODO - add tempalte customisation!
        ids = self._taskId.split('__')
        job_id = ids[0]
        obs_num = int(ids[1])
        corr_id = int(ids[2])
        if (self._params.use_gpu):
            gpu_flag = 'Y'
        else:
            gpu_flag = 'N'
        args = '-j %s -o %d -c %d -t %s -f %s -g %s' % (job_id, obs_num, corr_id, self._params.rts_tpl_name, self._fileListToString(self._filelist), gpu_flag)
        return args
    
    def _fileListToString(self, filelist):
        re = filelist[0]
        if (len(filelist) > 1):
            for file in filelist[1:]:
                re += ',%s' % file
        return re

class RTSJobLocalTask(MRLocalTask):
    """
    Reduce all observations' output into a single RTSJob's output
    """
    pass

def test():
    params = RTSJobParam()
    params.obsList = ['1052803816', '1053182656', '1052749752']
    rts_job = RTSJob('job001', params)
    print rts_job.start()

if __name__=="__main__":
    test()