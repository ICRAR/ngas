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
This module provides an MapReduce framework for running NGAS jobs, task and other related entities
A particular job type (i.e. RTS) should implement these interfaces
"""

import threading


class MapReduceTask:
    """
    i.e. the MRTask, the building block of NGAS job processing
    Compared to Google/Hadoop mapreduce, a major difference is
    that reducer keys are statically pre-determined at design time.
    This difference is due to the fact that 
    
    1. In many cases we are working directly at the file level (simply because existing code written by astronomers all work on files)
       Big data are already split in an application-specific manner that makes sense.
       An advantage is that we can easily establish a files-to-key mapping (e.g. [file1, file2, file3] ---> Correlator1) during the workflow design time. 
       
       This is in contrast to Google/Hadoop where files are split by a generic filesystem, 
       which "artificially" created the need for dynamic reducer key generation 
       since a big dataset may have multiple mis-aligned 'stuff' -- observations, correlators, etc. -- that need to be shuffled to different places to get reduced.  
    
    2. As a consequence of 1, each task is often assigned files belong to the same reducer key
    
    So in a sense, this is a file-level MapReduce framework rather than an algorithmic one
    """            
    def __init__(self, Id):
        """
        Constructor
        
        Each MRTask has a list of "children" MRTasks, and a single reducer
        
        Id:    the task id (String)
        """
        self.__mapList = [] # a list of MRTask, composite design pattern
        self.__mapDic = {} # key - mrTaskId, val - mrTask. findMRTaskById
        self.__reducer = None
        if (None == Id or '' == Id):
            errStr = 'Invalid task id: %s' % Id
            raise Exception, errStr
        self.__id = Id 
    
    def _mapTaskThread(self, mrTask):        
        out = mrTask.__map()
        out = self.map(out) # optional augmentation before passing it onto reducer
        self.__reducer.__combine(out) 
    
    def __map(self):
        """
        TODO - fault tolerance (e.g. some tasks may fail)
        """               
        if (len(self.__mapList) > 0):
            if (self.__reducer == None):
                errStr = 'Reducer missing for the MRtask %s, which has at least one child mapper.' % str(self.getId())
                raise Exception, errStr
            mrThreads = []
            for mrTask in self.__mapList:
                #parallelise this in a separate thread
                args = (mrTask,)
                mrthrd = threading.Thread(None, self._mapTaskThread, 'MRThrd_' + str(self.getId()), args)
                mrthrd.setDaemon(0)                
                mrthrd.start()
                mrThreads.append(mrthrd)
                
            for mrt in mrThreads:
                mrt.join()
            return self.__reducer.__reduce()
        else:
            # no children MRTasks at this level
            # Thus no reducer either, so return to the upper level
            return self.map()
    
    def __combine(self, mapOutput):
        """
        Accummulate results from each child-map task
        
        mapOutput    the output from the child-map task (object)
        """
        self.combine(mapOutput)
    
    def __reduce(self):
        """
        system reduce task called internally
        """
        return self.reduce()
    
    def getId(self):
        return self.__id
    
    def start(self):
        """
        Execute the map reduce tasks on this and all children MRTasks
        """
        # get count
        return self.__map()
    
    def addMapper(self, mpT):
        if (mpT):
            self.__mapList.append(mpT)
            self.__mapDic[mpT.getId()] = mpT
    
    def getMapper(self, taskId):
        return self.__mapDic[taskId]
    
    def setReducer(self, rdT):
        if (rdT):
            self.__reducer = rdT
    
    def getReducer(self):
        return self.__reducer
    
    def map(self, mapInput = None):
        """
        Dummy implementation: data flow through without augmentation
        """        
        if (mapInput):
            return mapInput
        #else:
            #print '\n%s is mapped with an input none **** \n' % (self.getId())            
    
    def combine(self, mapOutput):
        """
        Real work for combiner
        Please implement this
        """    
        pass
    
    def reduce(self):
        """
        Real work for reducer
        Please implement this
        """
        pass

class TestMRTask(MapReduceTask):    
    def __init__(self, id):
        """
        Constructor
        """
        MapReduceTask.__init__(self, id)
        self.__rednum = 0
    
    def combine(self, mapOutput):
        self.__rednum += mapOutput
        print '%s is combined: %s' % (self.getId(), str(self.__rednum))
    
    def reduce(self):
        print '%s is reduced: %s' % (self.getId(), str(self.__rednum))
        return self.__rednum
    
class TestMRLeafTask(TestMRTask):
    
    def __init__(self, mapnum, id):
        TestMRTask.__init__(self, id)
        self.__mapnum = mapnum
        
    def map(self, mapInput = None):
        print '%s is mapped' % self.getId()
        return self.__mapnum
        

def buildTestMRTask():
    """
    This is for testing the MapReduce framework in NGAS
    It sums over a list of float points by building a two-level
    MRTask tree
    """
    # build the highest (outer) level of the tree first
    mrtk0 = TestMRTask('mrtk0')
    num_mid = 4
    for j in range(num_mid):
        mrtk1 = TestMRTask('mrtk1_%s' % str(j))
        mrtk0.addMapper(mrtk1)        
        
        for k in range(num_mid):
            mrtk2 = TestMRLeafTask(1.32, 'mrtk2_%s' % str(k))
            mrtk1.addMapper(mrtk2)
        
        rt1 = TestMRTask('mrtk1_%s_reduce' % str(j))
        mrtk1.setReducer(rt1)
                    
    rt0 = TestMRTask('mrtk0_reduce')
    mrtk0.setReducer(rt0)
    
    result = mrtk0.start()
    print 'Final result = %s' % str(result)

class MRTaskFactory:
    
    def __init__(self, name = 'RTS'):
        pass
    
def main():
    buildTestMRTask()


            
######################################################################

if __name__=="__main__":
    main()