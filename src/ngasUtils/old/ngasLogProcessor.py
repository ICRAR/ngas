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
# "@(#) $Id: ngasLogProcessor.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# awicenec  09/07/2001  Created
#
"""
Tool to analyze the NG/AMS Log File and to generate statistics from this.
"""


import sys, os, time, exceptions, commands, re, types, pylab, numpy
from mx import DateTime

_TYPES = ['[ERROR]','[INFO]','[NOTICE]','[ALERT]','[WARNING]']

class TraceEntry():
    def __init__(self, traceString):
        self.TraceString = traceString
        (self.File, self.Method, self.Line, \
            self.Instance, self.Thread) = self.parseTrace(traceString)
    
    def parseTrace(self, traceString):
        try:
            (traceFile, traceMethod, traceLine, traceInstance,\
             traceThread) = traceString.split(':',4)
        except Exception, e:
            return (None, None, None, None, None)
        return (traceFile, traceMethod, traceLine, traceInstance, traceThread)
    
    def __repr__(self):
        return self.TraceString

class LogEntry():
    """
    Class defining a log entry
    """
    def __init__(self,logString):
        self.LogString = logString
        self.LogTypes = ['ERROR','INFO','NOTICE','ALERT','WARNING']
        try:
            (self.Time, self.Type, self.Msg, self.Trace) = \
                self.parseLog(log=logString.strip())
            self.Trace = TraceEntry(self.Trace)
        except Exception,e:
            msg = "ERROR: Unable to parse logString: %s\n%s" % (e,logString.strip())
            raise exceptions.Exception, msg
    
    def parseLog(self, log=''):
        rexp = re.compile('[\[\]]')
        (logTime, logType, logMsg, logTrace) = rexp.split(log,3)
        if logType not in self.LogTypes:
            raise exceptions.Exception, "unknown log type %s" % logType
#        lT = DateTime.ISO.ParseDateTime(logTime.strip())
        return (DateTime.DateTimeFrom(logTime.strip()), logType.strip(), logMsg.strip(), \
                logTrace.strip()[:-1])


    def getLogType(self):
        return self.LogType
    
    def getLogTime(self):
        return self.LogTime
    
    def getLogMsg(self):
        return self.LogMsg
    
    def getLogTrace(self):
        return self.LogTrace
    
    def __repr__(self):
        return self.LogString
    
    def filterTrace(self, filter):
        return (lambda x:eval(filter))(self.Trace)


class ThreadEntry(dict):
    """
    Class representing all messages of a single thread
    """
    def __init__(self, log=None, name=None, request='unknown', entries=[]):
        if not log and not name:
            pass
        elif log and log.Trace.Thread:
            name = log.Trace.Thread
            entries = [log]
            self.update({'request':request, 'start': None, 'end': None, 
            'duration':None,
                        'entries':entries,
            'client':None})
            self.name = self.keys()[0]
            self.request = self['request']
            self.start = self['start']
            self.end = self['end']
            self.duration = self['duration']
            self.entries = self['entries']
        elif name:
            self.update({'request':request, 'duration':None, \
                           'entries':entries, 'client':None})
            self.name = self.keys()[0]
            self.request = self['request']
            self.start = self['start']
            self.end = self['end']
            self.duration = self['duration']
            self.entries = self['entries']
            
    def appendEntry(self,logEntry):
        self.entries.append(logEntry)
        self.setTimes()
        if logEntry.Msg.startswith('Received command:'):
            self.request = logEntry.Msg.split(':')[-1].strip()
        return
 
    def setTimes(self):
        if len(self.entries) == 0:
            self['duration'] = None
        else:
            et = self.entries[-1].Time
            st = self.entries[0].Time
            self['start'] = st
            self['end'] = et
            self['duration'] = et - st

        self.start = self['start']
        self.end = self['end']
        self.duration = self['duration']
        return
            
    def setRequest(self):
        if len(self.entries) == 0:
            self.request = 'unknown'
        else:
            ii = 0
            while ii < len(self.entries) and not self.entries[ii].Msg.startswith('Received command:'): ii+=1
            if ii >= len(self.entries):
                req = 'unknown'
            else:
                req = self.entries[ii].Msg.split(':')[-1]
            self.request = req.strip()
        self['request'] = self.request
        return



    
def buildLogDict(logList):
    """
    Function constructs a dictionary of logs using the
    classes above.
    
    Input:
        logList:  list of strings containing log entries
    Output:
        threadDict:    log dictionary
    """
    threadDict = {}
    threads = []
    for log in logList:
        logEntry = LogEntry(log)
        key = logEntry.Trace.Thread 
        if key and (key not in threads):
            threadDict.update({key:ThreadEntry(logEntry)})
        if logEntry.Msg.startswith('Handling HTTP request:'):
                threadDict[key]['client'] = \
                      logEntry.Msg.split(':')[1].split("'")[1]
                threads.append(key)
        elif key:
            threadDict[key].appendEntry(logEntry)
#            threadDict[key].setTimes()
    return threadDict


def getThreadSet(logEntryList):
    """
    """
    threads=filter(lambda x:(x.Trace.Thread!=None and x.Trace.Thread != 'JANITOR-THREAD'),logEntryList)
    return set(map(lambda x:x.Trace.Thread,threads))


def readLog(fObj, nlines=None):
    """
    Routine reads a log file <fnm> and returns an array of strings.

    SYNOPSIS: logArr = readLog(<fnm>)

    INPUT:
       fObj:    fileObject or string: file name (including path)
       nlines:  int: number of lines to return

    OUTPUT:
       stringarr containing the log entries   
    """
    _FACTOR = 150
    oflag = 0
    if type(fObj) == types.StringType:
        fObj = open(fObj)
        oflag = 1
    try:
        if nlines:
            logArr = []
            nl = nlines
            while nl > 0:
                logArr.extend(fObj.readlines(nl * _FACTOR))
                nl = nl - len(logArr)
            if len(logArr) > nlines:
                ll = sum(map(lambda x:len(x),logArr[nlines+1:]))
                fObj.seek(-ll,1)
            logArr = logArr[:nlines]
        else:
            logArr = fObj.readlines()
        if oflag == 1: fObj.close()
        return logArr
    except exceptions.Exception, e:
        errMsg = "Problems reading file (" + str(e) + ") "
        raise exceptions.Exception, errMsg

def _executeSysCmd(cmd):
    """
    Helper function to execute a system command
    
    Input:
       cmd:  string, the command to be executed
    
    Output:
       plain string output of the result of the command as
       received from STDOUT.
    """
    try:
        status,output = commands.getstatusoutput(cmd)
    except Exception,e:
        raise e
    if status == 256:
        return ''
    return output

def getArchiveStartFromFile(fnm, command='ARCHIVE'):
    """
    Function uses grep command on a LogFile (fnm) to find
    the start of ARCHIVE requests.
    
    Input:
        fnm:    string, file name of the log-file
    Output:
        logs:    list of strings
    """
    cmd = "grep 'path=|%s|' %s" % (command, fnm)
    output = _executeSysCmd(cmd)
    if output == '':
        return []
    logs = output.split('\n')
    return logs

def getSizeTimeRate(logarr):
    """
    Extract the size,rate and time from an array of ARCHIVE log entries
    related to a single ARCHIVE request.
    """
    saveLine = filter(lambda x:x.find('Saved data in file') > -1,logarr)[0]
    splitLine = saveLine.split()
    (size, time, rate) = (float(splitLine[-8]), float(splitLine[-6]), float(splitLine[-3]))
    tend = DateTime.DateTimeFrom(splitLine[0]) # end transfer
    saveLine = filter(lambda x:x.find('Saving data in file') > -1,logarr)[0]
    tstart = DateTime.DateTimeFrom(saveLine.split()[0]) # start of transfer
    
    return (size, time, rate, tstart, tend)


def getArchiveThreadsFromFile(fnm, dict=1, command='ARCHIVE', verbose=0, nthreads=0):
    """
    """
    tlogs = []
    archStats = {}
    alogs = getArchiveStartFromFile(fnm, command=command)
    atd = buildLogDict(alogs)
    atk = atd.keys()
    if verbose: print "Number of threads found: %d" % len(atk)
    ii =0
    for key in atk:
        ii += 1
        if verbose and float(ii/100) == ii/100.: print ii
        tlog = getThreadFromFile(fnm,key)
        if len(filter(lambda x:x.find('Saved data in file') > -1,tlog)) == 0: continue #fix the index out_of_bound issue
        archStats.update({key:getSizeTimeRate(tlog)})
        tlogs.extend(tlog)
    if dict:
        tdict = buildLogDict(tlogs)
        return tdict, archStats
    else:
        return tlogs
                

def getLogList(fnm, logType="INFO"):
    """
    Function uses grep command on a LogFile (fnm) to find
    the <logType> logs.
    
    Input:
        fnm:    string, file name of the log-file
    Output:
        logs:    list of strings
    """
    cmd = "grep %s %s" % (logType, fnm)
    output = _executeSysCmd(cmd)
    if output == '':
        return []
    logs = output.split('\n')
    return logs


def getErrorsFromFile(fnm):
    """
    Function uses grep command on a LogFile (fnm) to find
    the ERROR logs.
    
    Input:
        fnm:    string, file name of the log-file
    Output:
        logs:    list of strings
    """
    cmd = "grep ERROR %s" % fnm
    output = _executeSysCmd(cmd)
    if output == '':
        return []
    logs = output.split('\n')
    return logs


def getWarningsFromFile(fnm):
    """
    Function uses grep command on a LogFile (fnm) to find
    the WARNING logs.
    
    Input:
        fnm:    string, file name of the log-file
    Output:
        logs:    list of strings
    """
    cmd = "grep WARNING %s" % fnm
    output = _executeSysCmd(cmd)
    if output == '':
        return []
    logs = output.split('\n')
    return logs


def getThreadFromFile(fnm,thread):
    """
    Function uses grep command on a LogFile (fnm) to find
    the logs belonging to one thread.
    
    Input:
        fnm:    string, file name of the log-file
        thread: string, thread to be searched for (e.g. Thread-123)
    Output:
        logs:    list of strings
    """
    cmd = "grep '%s]' %s" % (thread,fnm)
    output = _executeSysCmd(cmd)
    if output == '':
        return []
    logs = output.split('\n')
    return logs




def test():
    logs=readLog('/Users/awicenec/Work/ALMA/data/LogFile.nglog')
    l=LogEntry(logs[54000])
    return logs, l    
                                   

#=====

if __name__ == "__main__":
    """
    The stuff below is just an example of the usage of the classes and functions
    in this file. It is best to call it in the following way:
    
    python -i ngasLogProcessor.py LogFile.nglog
    
    Like this it is possible to access all the variables and functions
    afterwards from the command line.
    """
    if len(sys.argv) == 1:
        print "Usage: ngamsLogProcessor <LogFile> [<type>]"
        sys.exit()
    else:
        fnm = sys.argv[1]

    if len(sys.argv) == 3:
        req = getLogList(fnm,logType=sys.argv[2])
        for r in req:
            print r
        sys.exit()
    
    adict = getArchiveThreadsFromFile(fnm, dict=1, command='QARCHIVE')
    astart = numpy.array(map(lambda x:adict[0][x].start,adict[1].keys()))
    ind = numpy.argsort(astart)
    astart.sort()
    aend = numpy.array(map(lambda x:adict[0][x].end,adict[1].keys()))
    aend = aend[ind]
    tstart = numpy.array(map(lambda x:adict[1][x][-2],adict[1].keys()))
    tstart = tstart[ind]
    tend = numpy.array(map(lambda x:adict[1][x][-1],adict[1].keys()))
    tend = tend[ind]
    bytes = numpy.array(map(lambda x:adict[1][x][0],adict[1].keys()))
    bytes = bytes[ind]
    std = numpy.array(map(lambda x:adict[1][x][2],adict[1].keys())).std()/1024**2

    print "SUMMARY:"
    print "--------"
    print "Number of archive requests: %d" % len(adict[0])
    print "Total elapsed time: %9.2f seconds" % (aend.max()-astart.min()).seconds
    print "Total volume: %5.2f GB" % (bytes.sum()/1024**3)
    print "Overall rate: %5.2f +/- %3.2f MB/s" % \
    (bytes.sum()/1024**2/(aend.max()-astart.min()).seconds, std)

    adur = (aend-astart).astype(float)
    
    tdur = (tend-tstart).astype(float)
    rate = bytes/1024**2/adur

    
    step = 6
    bins = len(range(0,len(astart),step))
    dt = astart[1:] - astart[:-1]
    dt = adur
    md = pylab.zeros([bins])  # array for median request interval
    mt = pylab.zeros([bins])  # array for median total time
    smt = pylab.zeros([bins,2]) # standard deviation of mt elements
    fd = pylab.zeros([bins])  # array for frequency of requests
    mrr = pylab.zeros([bins]) # array for median transfer rate
    ff = pylab.zeros([bins]) # multi-dim array holding various values
    ft = pylab.zeros([bins]) # array holding timestamp of midpoint of interval
    dur = pylab.zeros([bins]) # array holding archive durations
    err = pylab.zeros([bins])
    mtr = pylab.zeros([bins]) # array holding total rate
    rr = bytes/(aend-astart).astype(float)/1024**2
    di = map(lambda x,y:float(x-astart.min())+y/2.,astart,(aend-astart).astype(float))
    di.sort()


    for ii in range(0,len(astart),step):
        li = ii+step-1
        md[ii/step] = (pylab.median(dt[ii:li]))
        if li > len(dt): li = len(dt)-1
        if li != ii:
            tmpdur = (aend[ii:li].max()-astart[ii:li].min())
            dur[ii/step]=tmpdur+0.0001
            err[ii/step]=(tmpdur+0.0001)/2.
            fd[ii/step]=((li-ii)/(dur[ii/step]))
            ft[ii/step]=(astart[ii]+dur[ii/step]/(2.*86400)-astart[0])
        #           ff[ii/step]=([(li-ii),astart[ii],aend[li],fd[-1],adur[-1]])
            mrr[ii/step]=(sum(rr[ii:li])/dur[ii/step])
            mtr[ii/step]=(sum(bytes[ii:li])/dur[ii/step]/1024**2)
        else:
            fd[ii/step]=(0)
            mrr[ii/step]=(rr[li])
        mt[ii/step]=(pylab.median(adur[ii:li]))
#       smt[0][ii/step]=((min(mt[-1],pylab.std(dur[-1])/2)))
#       smt[1][ii/step]=(pylab.std(dur[-1])/2)


    pylab.subplot(2,1,1)
    pylab.errorbar(ft,mtr,xerr=err,fmt='b.')
    pylab.plot([0,di[-1]+20],[mtr.mean(),mtr.mean()], 'r-')
    pylab.errorbar([di[-1]+15], [mtr.mean()], yerr=mtr.std(), fmt='r.')
    pylab.xlim(xmin=-0.01, xmax=di[-1]+20)
    pylab.xlabel('Time since %s [s]' % (astart[0]))
    pylab.ylabel('Accumulated transfer speed [MB/s]')

    pylab.subplot(2,1,2)
    pylab.errorbar(tstart-tstart.min(), rate, \
          xerr=[pylab.zeros(len(tstart)),tdur], fmt='b.')
    pylab.errorbar(astart-astart.min(), rate-0.005, 
          xerr=[pylab.zeros(len(astart)),adur], fmt='r.')
    pylab.xlabel('Time since %s [s]' % (astart[0]))
    pylab.ylabel('Transfer rate (single file) [MB/s]')
    pylab.xlim(xmin=-0.01,xmax=di[-1]+20)
    
#    logfnm = fnm + '.png'
#    pylab.savefig(logfnm, dpi=200)
    
