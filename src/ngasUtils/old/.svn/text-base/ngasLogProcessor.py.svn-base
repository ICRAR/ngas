

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
# "@(#) $Id: ngasLogProcessor.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# awicenec  09/07/2001  Created
#
"""
Tool to analyze the NG/AMS Log File and to generate statistics from this.
"""


import sys, os, time, exceptions, commands, re, types
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
            msg = "ERROR: Unable to parse logString: %s" % e
            raise msg
    
    def parseLog(self, log=''):
        rexp = re.compile('[\[\]]')
        (logTime, logType, logMsg, logTrace) = rexp.split(log,3)
        if logType not in self.LogTypes:
            raise "unknown log type %s" % logType
#        lT = DateTime.ISO.ParseDateTime(logTime.strip())
        return (logTime.strip(), logType.strip(), logMsg.strip(), logTrace.strip()[:-1])


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
            self.update({'request':request, 'start': None, 'end': None, 'duration':None, \
                           'entries':entries})
            self.name = self.keys()[0]
            self.request = self['request']
            self.start = self['start']
            self.end = self['end']
            self.duration = self['duration']
            self.entries = self['entries']
        elif name:
            self.update({'request':request, 'duration':None, \
                           'entries':entries})
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
            et = DateTime.ISO.ParseDateTime(self.entries[-1].Time)
            st = DateTime.ISO.ParseDateTime(self.entries[0].Time)
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
    logs = output.split('\n')
    return logs


def getArchiveThreads(fnm, dict=0):
    """
    """
    tlogs = []
    alogs = getArchiveStartFromFile(fnm)
    atd = buildLogDict(alogs)
    atk = atd.keys()
    for key in atk:
        tlogs.extend(getThreadFromFile(fnm,key))
    if dict:
        tdict = buildLogDict(tlogs)
        return tdict
    else:
        return tlogs
                

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
    logs = output.split('\n')
    return logs




def test():
    logs=readLog('/Users/awicenec/Work/ALMA/data/LogFile.nglog')
    l=LogEntry(logs[54000])
    return logs, l    
                                   

#=====

if __name__ == "__main__":
    """
    """
    if len(sys.argv) == 1:
        print "Usage: ngamsLogProcessor <LogFile> [<type>]"
        sys.exit()
    else:
        fnm = sys.argv[1]

    if len(sys.argv) == 3:
        req = getLogList(fnm,type=sys.argv[2])
        for r in req:
            print r
        sys.exit()
    
    infos = getLogList(fnm,type="info")
    entr = getLogEntries(infos,'with mime-type: ngas/fits')
    diffs = getTimeDiffs(infos,'with mime-type: ngas/fits',\
                         'POST,ARCHIVE,ngas/archive-push')
    kk = diffs.keys()
    kk.sort()

    #
    # get additional information from log-file
    #
    # disks used:
    (stat,main_disks) = commands.getstatusoutput(\
        """grep "Saving data in file" """ + fnm + \
        """| awk -F"/" '{print "/"$2"/"$3}' | sort | uniq""")
    main_disks = main_disks.split("\n")
    if len(main_disks) == 1:
        main_disks = main_disks[0]

    # output from ngamsFitsPlugIn
    #
    #  old search entry
    #    search = """Data returned from plug-in: ngamsFitsPlugIn:"""
    #
    search = """Data returned from"""
    lplug = getLogEntries(infos,search)
    if len(lplug) == 0:
        print "[WARNING] No FITS archive requests found in file: " + fnm
        print "bailing out..."
        sys.exit()
    tot_ratio = 0.
    max_orig = 0.
    llogs = []
    for entry in lplug:
        edict = {}
        plist = entry[2][len(search)-1:]
        for e in plist.split(","):
            ee = e.split(':')
            edict.update({ee[0].strip():ee[1].strip()})
        llogs.append(edict)
        (comp,orig) = (float(edict['File Size']),\
                       float(edict['Uncompressed File Size']))
        if max_orig <  orig/1024./1024.:
            max_orig = orig/1024./1024.
        tot_ratio = tot_ratio + float(orig)/comp

    mean_ratio = tot_ratio/len(lplug)    
        
    #
    # Now produce the output
    #        
    print "Status of main archive disk(s) used:"
    if type(main_disks) == type([]):
        for disk in main_disks:
            (stat,df) = commands.getstatusoutput('df ' + disk)
            print disk + ":"
            (head,df) = df.split("\n")
            (dev,tot,used,ava,perc,mntpt) = df.split()
            (dev,tot,used,ava,perc,mntpt) = df.split()
            print "There is still space for %5.1f files" % \
                  (float(ava)/(max_orig*1024./mean_ratio))
            print "maximum file size:\t%7.2f MB" % max_orig
            print "mean compression ratio:\t%3.2f" % mean_ratio
            print ""
    else:
        (stat,df) = commands.getstatusoutput('df ' + main_disks)
        print main_disks + ":"
        (head,df) = df.split("\n")
        (dev,tot,used,ava,perc,mntpt) = df.split()
        print "There is still space for %5.1f files" % \
              (float(ava)/(max_orig*1024./mean_ratio))
        print "maximum file size:\t%7.2f MB" % max_orig
        print "mean compression ratio:\t%3.2f" % mean_ratio
        print ""
        
    j = 0
    tot_time = 0
    last_end = 0.
    idle = 0.
    tot_idle = 0.
    min_idle = 86400.
    max_idle = 0.
    min_archive = 86400.
    max_archive = 0.
    tot_through = 0.
    min_through = 1000.
    max_through = 0.
    if len(lplug) != len(kk):
        print "Throughput statistics not available..."
        print len(lplug),len(kk)
        stat = 0
    else:
        stat = 1
    for i in kk:
        if stat:
            # plug = eval(lplug[j])
            through = float(llogs[j]['File Size'])/(float(diffs[i].seconds) \
                                             * 1024. * 1024.) 
            tot_through = tot_through + through
            if min_through > through:
                min_through = through
            if max_through < through:
                max_through = through
        if min_archive > diffs[i].seconds:
            min_archive = diffs[i].seconds
        if max_archive < diffs[i].seconds:
            max_archive = diffs[i].seconds
        if j > 0:
            idle = (i * 86400.) - last_end
            if min_idle >= idle:
                min_idle = idle
        if max_idle < idle:
            max_idle = idle
        tot_time = tot_time + diffs[i].seconds
        tot_idle = tot_idle + idle
        last_end = (i * 86400.) + diffs[i].seconds
        j=j+1

    errors = getLogList(fnm,type="error")
    alerts = getLogList(fnm,type="alert")
    warnings = getLogList(fnm,type="warning")
    print ""
    print "Number of FITS ARCHIVE requests: ",len(diffs)
    print "Mean/Min./Max elapsed ARCHIVE time: %5.2f/%5.2f/%5.2f s" % \
          ((tot_time/j),min_archive,max_archive)
    print "Mean/Min./Max idle time between ARCHIVE: %5.2f/%5.2f/%5.2f s" % \
          ((tot_idle/j),min_idle,max_idle)
    if stat:
        print  "Mean/Min./Max throughput:  %5.2f/%5.2f/%5.2f MB/s" % \
              ((tot_through/j),min_through,max_through)
    print ""
    print "Number of ALERT entries: ",len(alerts)
    print "Number of ERROR entries: ",len(errors)
    print "Number of WARNING entries: ",len(warnings)

    
# EOF
