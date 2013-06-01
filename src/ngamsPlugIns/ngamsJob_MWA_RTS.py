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

import threading
from Queue import Queue, Empty

from ngamsJobProtocol import *
import ngamsJobMWALib

DEBUG = 0
debug_url = 'http://192.168.1.1:7777'

FSTATUS_NOT_STARTED = 0 # white
FSTATUS_STAGING = 1 # green
FSTATUS_ONLINE = 2 # blue
FSTATUS_OFFLINE = 3 # red

def dprint(s):
    if (DEBUG):
        print s

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
        self.__buildRTSTasks(rtsParam)
        self.__obsRespQ = Queue()
    
    def __buildRTSTasks(self, rtsParam):
        if (rtsParam.obsList == None or len(rtsParam.obsList) == 0):
            errMsg = 'No observation numbers found in this RTS Job'
            raise Exception, errMsg
        
        num_obs = len(rtsParam.obsList)
        
        for j in range(num_obs):
            obs_num = rtsParam.obsList[j]
            obsTask = ObsTask(obs_num, rtsParam)
            self.addMapper(obsTask)
            
            fileIds = ngamsJobMWALib.getFileIdsByObsNum(obs_num)
            if (len(fileIds.keys()) == 0):
                raise Exception('Obs number %s does not appear to be valid. We require exact observation numbers rather than GPS ranges.' % obs_num)
            for k in range(rtsParam.num_subband):
                if (fileIds.has_key(k + 1)): # it is possible that some correlators were not on
                    corrTask = CorrTask(str(k + 1), fileIds[k + 1], rtsParam)
                    obsTask.addMapper(corrTask)         
        
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
        
        return str(jobRe)

class ObsTask(MapReduceTask):
    """
    MWA Observation, the second level of the RTS MRTask tree
    Thus, a RTSJob consists of multiple ObsTasks
    """
    def __init__(self, obsNum, rtsParam):
        """
        Constructor
        
        obsNum:    (string) observation number/id, i.e. the GPS time of each MWA observation
        rtsParam:    RTS job parameters (class RTSJobParam)
        """
        MapReduceTask.__init__(self, str(obsNum)) #in case caller still passes in integer
        self.__completedCorrTasks = 0
        self.__rtsParam = rtsParam
        self.__corrRespQ = Queue()
    
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
        
        while (1):
            corrTaskRe = None
            try:
                corrTaskRe = self.__corrRespQ.get_nowait()
            except Empty, e:
                break
            obsTaskRe.merge(corrTaskRe)
        
        # TODO - do the real reduction work (i.e. combine all images from correlators into a single one)
        obsTaskRe.setImgUrl(debug_url)
        return obsTaskRe
        

class CorrTask(MapReduceTask):
    """
    MWA Correlator task, the third level of the RTS MRTask tree
    It is where actual execution happens, and an ObsTask consists of 
    multiple CorrTasks
    Each CorrTask processes all files generated by that
    correlator 
    """
    def __init__(self, corrId, fileIds, rtsParam):
        """
        Constructor
        
        corrId:    (string) Correlator identifier (e.g. 1, 2, ... , 24)
        fileIds:   (list) A list of file ids (string) to be processed by this correlator task
        rtsParam:    RTS job parameters (class RTSJobParam)
        
        """
        MapReduceTask.__init__(self, str(corrId))
        self.__fileIds = fileIds
        self.__rtsParam = rtsParam
        self._progress = -1
        self._fileIngEvent = threading.Event()
        self._timeOut4FileIng = 60 * 15 # maximum wait for 15 min
        self._numIngested = 0 # at start, assume all files are ingested
        self._numIngSem = threading.Semaphore()
        self._taskExeHost = '' # this will become the NextURL for PARCHIVE during file staging from Cortex/other hosts (if any)
        self._fileLocDict = {} # key - fileId, value - FileLocation (may not be the final one)
    
    def map(self, mapInput = None):
        """
        Actual work for Correlator's file processing
        """
        cre = CorrTaskResult(self.getId(), self.__fileIds, debug_url)
        if (self.__fileIds == None or len(self.__fileIds) == 0):
            cre._errcode = 1
            cre._errmsg = 'No correlator files in the input'
            self.setStatus(STATUS_EXCEPTION)
            return cre # should raise exception here
        #TODO - deal with timeout!
        dprint('Correlator %s is being mapped' % self.getId())
        self._progress = 0
        
        # 1 Check all files' locations
        # TODO - what if the best host is down? 
        try:
            self._fileLocDict = ngamsJobMWALib.getBestHost(self.__fileIds)
        except Exception, e:
            cre._errcode = 4
            cre._errmsg = 'Fail to get the best host for file list %s: %s' % (str(self.__fileIds), str(e))
            self.setStatus(STATUS_EXCEPTION)
            return cre
        
        self._numIngested = len(self._fileLocDict.keys())
        if (self._numIngested > 0):
            self._taskExeHost = self._fileLocDict.values()[0]
        else:
            self._taskExeHost = ngamsJobMWALib.getNextOnlineHostUrl()        
        
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
                    # most likely a DB error                
                if (len(fileLoc) == 0 or cre._errcode == 2):
                    # not in the cluster/or some db error , stage from outside
                    frmExtList.append(fid)
                else:
                    stageerr = 0
                    for i in range(len(fileLoc)):
                        # record its actual location inside the cluster
                        self._fileLocDict[fid] = fileLoc[i] # get the host
                        # stage from that host within the cluster
                        stageerr = ngamsJobMWALib.stageFile([fid], self, self._taskExeHost, fileLoc[0])
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
                return cre
                    
        if (self._numIngested == len(self.__fileIds)): # all files are there
            self._fileIngEvent.set() # so do not block
        
        # 3. block if / while files are being ingested
        self._fileIngEvent.wait(self._timeOut4FileIng)
   
        if (self._numIngested < len(self.__fileIds)):
            #TODO need to print which files are timed out
            cre._errmsg = "Timeout when waiting for file ingestion. Ingested %d out of %d." % (self._numIngested, len(self.__fileIds))
            cre._errcode = 3
            self.setStatus(STATUS_EXCEPTION)
            return cre
        
        # 4. Run RTS executable and archive images back to an NGAS server
        #    This is running on remote servers, which should be asynchronously invoked                
        """
        # this is for test
        if (self.getId() == '11' or self.getId() == '23'):
            if (self.__fileIds[0].split('_')[0] == '1053182656'):
                cre._errcode = 12
                cre._errmsg = 'Files not found!'
        """
        return cre 
    
    def fileIngested(self, fileId, filePath):
        self._numIngSem.acquire()
        self._numIngested += 1
        self._numIngSem.release()
        floc = ngamsJobMWALib.FileLocation(self._taskExeHost, filePath, fileId)
        self._fileLocDict[fileId] = floc
        if (self._numIngested == len(self.__fileIds)):
            self._fileIngEvent.set()
    
    def toJSONObj(self):
        """
        Override the default impl
        to jsonise file ids
        """
        
        jsobj = {}
        jsobj['name'] = self.getId() + '-' + statusDic[self.getStatus()]
        jsobj['status'] = self.getStatus()
        #moredic = self.getMoreJSONAttr()
        if (self.__fileIds == None or len(self.__fileIds) == 0):
            return jsobj
        children = []
        for fileId in self.__fileIds:
            fjsobj = {}
            if (self._fileLocDict.has_key(fileId)):
                floc = self._fileLocDict[fileId]._svrUrl
                if (floc == self._taskExeHost):
                    fjsobj['status'] = FSTATUS_ONLINE
                else:
                    fjsobj['status'] = FSTATUS_STAGING
            else:
                floc = 'Offline'
                fjsobj['status'] = FSTATUS_OFFLINE # 1 online, 0 offline
            fjsobj['name'] = '%s:%s' % (fileId, floc)
            
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
        self.__imgUrl = imgUrl # e.g. 192.168.1.1:7777/RETRIEVE?file_id=job001_obs002_gpubox02.zip
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
            fileList = corrTaskResult._fileIds
            nf = len(fileList)
            if (nf < 1):
                return
            fids = fileList[0]
            if (nf > 1):
                for fileId in fileList[1:]:
                    fids += ',%s' % fileId
            re = (corrTaskResult._subbandId, fids)
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
        else:
            re = '# of completed subbands: \t\t%d, image_url = %s\n.' % (tt, self._imgUrl)
        for subtuple in self.good_list:
            re += '\t subbandId: %d, vis files: %s\n' % (subtuple[0], subtuple[1])
        if (fl):
            re += '# of failed subbands:\t\t%d\n' % fl
            for subtuple in self.bad_list:
                re += '\t subbandId: %d, vis files failed: %s\n' % (subtuple[0], subtuple[1])
        
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
        
        return re
    
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
        #RTS template prefix (optional, string)
        self.rts_tplpf = '/scratch/astronomy556/MWA/RTS/utils/templates/RTS_template_'
        #RTS template suffix
        self.rts_tplsf = '.in'
        
        #RTS template names for this processing (optional, string - comma separated aliases, e.g.
        #drift,regrid,snapshots, default = 'regrid'). Each name will be concatenated with 
        #rts-tpl-pf and rts-tpl-sf to form the complete template file path, 
        #e.g. /scratch/astronomy556/MWA/RTS/utils/templates/RTS_template_regrid.in
        
        self.rts_tpl_name = 'regrid'

def test():
    params = RTSJobParam()
    params.obsList = ['1052803816', '1053182656', '1052749752']
    rts_job = RTSJob('job001', params)
    print rts_job.start()

if __name__=="__main__":
    test()