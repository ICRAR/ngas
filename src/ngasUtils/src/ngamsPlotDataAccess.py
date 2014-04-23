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
# cwu      17/April/2014  Created
#
# time.gmtime(1067868000 + 315964800)
import os, commands
from os import walk
from collections import namedtuple
import numpy as np
import datetime as dt
from collections import defaultdict
import pylab as pl
from optparse import OptionParser
import urlparse

# retrieval access (date, observation id, was the file offline?)
RA = namedtuple('RA', 'date obsId offline')


def execCmd(cmd, failonerror = True, okErr = []):
    re = commands.getstatusoutput(cmd)
    if (re[0] != 0 and not (re[0] in okErr)):
        errMsg = 'Fail to execute command: "%s". Exception: %s' % (cmd, re[1])
        if (failonerror):
            raise Exception(errMsg)
        else:
            print errMsg
    return re

def unzipLogFiles(dir):
    """
    unzip all files whose names end with ".gz" in a given directory
    """
    f = []
    for (dirpath, dirnames, filenames) in walk(dir):
        f.extend(filenames)
        break
    
    for fn in f:
        if fn.endswith('.nglog.gz.gz'):
            # extract
            cmd = 'gzip -d %s/%s' % (dir, fn)
            re = execCmd(cmd, failonerror = False)
            # then change name
            if (re[0] == 0):
                # remove the ".gz" from the end of the file name
                cmd = 'mv %s/%s %s/%s' % (dir, fn[0:-3], dir, fn[0:-6])
                re = execCmd(cmd, failonerror = False)
        elif fn.endswith('.nglog.gz'):
            # extract only
            cmd = 'gzip -d %s/%s' % (dir, fn)
            re = execCmd(cmd, failonerror = False)

def _raListToNumArray(al):
    """
    Convert a list of RA tuples to the following num arrays:
    
    1st    date stamp (x1)
    2nd    online access obsId (y1)
    3rd    date stamp (x2)
    4th    offline access obsId (y2)
    5th    date stamp (x3)
    6th    number of offline access (y3)
    7th    date stamp (x4)
    8th    number of online access (y4)
    """
    x1 = []
    y1 = []
    x2 = []
    y2 = []
    x3 = []
    y3 = []
    x4 = []
    y4 = []
    
    xy3 = defaultdict(int)
    xy4 = defaultdict(int)
 
    d0 = dt.datetime.strptime(al[0].date,'%Y-%m-%d').date()
    for i in range(len(al)):
        a = al[i]
        di = dt.datetime.strptime(a.date,'%Y-%m-%d').date()
        # if no retrievals on a particular day, that day will show nothing
        ax = int((di - d0).days)
        if (a.offline):
            x2.append(ax)
            y2.append(int(a.obsId)) # miss
            xy3[ax] += 1
        else:
            x1.append(ax)
            y1.append(int(a.obsId)) # hit   
            xy4[ax] += 1 
    
    for k, v in xy3.items():
        x3.append(k)
        y3.append(v) 
    
    for k, v in xy4.items():
        x4.append(k)
        y4.append(v)
    
    return (np.array(x1), np.array(y1), np.array(x2), np.array(y2), np.array(x3), np.array(y3), np.array(x4), np.array(y4))

def processLogs(dirs, fgname, stgline = 'to stage file:'):
    """
    process all logs from a list of directories
    
    dirs:    a list of directories (list)
    fgname:    the name of the plot figure (including the full path)
    """
    accessList = []
    for dir in dirs:
        f = []
        for (dirpath, dirnames, filenames) in walk(dir):
            f.extend(filenames)
            break
        for fn in f:
            if fn.endswith('.nglog'):
                fullfn = '%s/%s' % (dir, fn)
                parseLogFile(fullfn, accessList, stgline)
    
    if (len(accessList) == 0):
        print 'There are no retrieval entries found in the logs'
        return
    
    accessList.sort() # automatically sort based on date
    x1, y1, x2, y2, x3, y3, x4, y4 = _raListToNumArray(accessList)
    
    fig = pl.figure()
    if (len(x3) or len(x4)):
        ax = fig.add_subplot(211)
    else:
        ax = fig.add_subplot(111)
    ax.set_xlabel('Time (days)', fontsize = 9)
    ax.set_ylabel('Obs number (GPS time)', fontsize = 9)
    ax.set_title('Observation access from %s to %s' % (accessList[0].date,accessList[-1].date), fontsize=10)
    ax.tick_params(axis='both', which='major', labelsize=8)
    ax.tick_params(axis='both', which='minor', labelsize=6)
    
    ax.plot(x1, y1, color = 'b', marker = 'x', linestyle = '', 
                        label = 'online', markersize = 4)
    if (len(x2)):
        ax.plot(x2, y2, color = 'r', marker = '+', linestyle = '', 
                            label = 'offline', markersize = 5)
        left = min(min(x1), min(x2))
        right = max(max(x1), max(x2)) + 2
    else:
        left = min(x1)
        right = max(x1) + 2
    
    ax.set_xlim([left, right])    
    legend = ax.legend(loc = 'upper left', shadow=True, prop={'size':8})
    
    if (len(x3) or len(x4)):
        ax1 = fig.add_subplot(212)
        ax1.set_xlabel('Time (days)', fontsize = 9)
        ax1.set_ylabel('Number of offline accesses', fontsize = 9)
        #ax1.set_title('Number of offline acc')
        
        if (len(x4)):
            ax1.plot(x4, y4, color = 'b', linestyle = '-', marker = 'x', label = 'online', markersize = 4)   
        if (len(x3)):
            ax1.plot(x3, y3, color = 'r', linestyle = '--', marker = '+', label = 'offline', markersize = 5)
        
        ax1.set_xlim([left, right])
        ax1.tick_params(axis='both', which='major', labelsize=8)
        ax1.tick_params(axis='both', which='minor', labelsize=6)
        
        legend1 = ax1.legend(loc = 'upper left', shadow=True, prop={'size':8})
    
    pl.tight_layout()
    fig.savefig(fgname)
    pl.close(fig)
    return accessList

def _getTidFrmLine(line):
    """
    Obtain the thread id from an NGAS log line
    
    e.g. '....:httpRedirReply:1783:24459:Thread-225]' --> '225'
    """
    tp = line.rfind(':Thread-')
    if (tp == -1):
        return None
    return line[tp:].split('-')[1][0:-1]

def _buildRA(access, isOffline):
    """
    Construct the retrieval access tuple from the line
    """
    tokens = access.split(' ')
    timestamp = tokens[0]
    date = timestamp.split('T')[0]
    #time = timestamp.split('T')[1]        
    
    #clientaddress = tokens[5].split('=')[1].split('\'')[1]
    obsNum = None 
    atts = access.split(' - ')                
    for att in atts:
        atttokens = att.split('=')
        attnm = atttokens[0]
        if (attnm == 'path'):
            path = att.replace('|', '').split('?')[1]
            tt =  urlparse.parse_qs(path)
            if (not tt.has_key('file_id')):
                continue
            fileId = tt['file_id'][0] #atttokens[2].split('|')[0]
            obsNum = fileId.split('_')[0] 
            try:
                int(obsNum)
            except ValueError, ve:
                obsNum = fileId.split('.')[0]
                try:
                    int(obsNum)
                except ValueError, ve1:
                    obsNum = None
                
            #obsDate = fileId.split('_')[1][0:8] 
        #elif (attnm == 'user-agent'):
            #userAgent = atttokens[1].split(' ')[0]
    if (obsNum):
        re = RA(date, obsNum, isOffline)
    else:
        re = None
    return re

def parseLogFile(fn, accessList, stgline = 'to stage file:'):
    """
    parse out a list of RA tuples from a single NGAS log file
    add them to the accessList
    """
    if (not os.path.exists(fn) or accessList == None):
        return
    
    # need to skip the redirect
    # cmd = 'grep -e RETRIEVE\? -e "Reading data block-wise" -e "to stage file:" -e NGAMS_INFO_REDIRECT %s' % fn
    cmd = 'grep -e \|RETRIEVE\? -e "%s" -e NGAMS_INFO_REDIRECT %s' % (stgline, fn)
    re = execCmd(cmd, failonerror = False, okErr = [256])
    if (re[0] != 0 and re[0] != 256):
        print 'Fail to parse log file %s' % fn
        return
    
    redrct = []
    stg = {}
    raDict = {}
    lines = re[1].split('\n')
    
    for li in lines:
        tid = _getTidFrmLine(li)
        if (not tid):
            continue
        
        if (li.find('|RETRIEVE?') > -1):
            raDict[tid] = li
        
        # check redirect
        if (li.find('NGAMS_INFO_REDIRECT') > -1):
            redrct.append(tid)
        
        # check staging
        if (li.find(stgline) > -1):
            stg[tid] = None
        
    for tid in redrct:
        if (raDict.has_key(tid)):
            #print "removing %s from %s" % (tid, fn)
            raDict.pop(tid) # remove all redirect requests
    
    for k, v in raDict.items():
        ra = _buildRA(v, stg.has_key(k))
        if (ra):
            accessList.append(ra)
        else:
            print 'none RA for %s in file %s' % (k, fn)
if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-d", "--dir", action="store", type="string", dest="dir", help="directories separated by comma")
    parser.add_option("-o", "--output", action="store", type="string", dest="output", help="output figure name (path)")
    parser.add_option("-s", "--stgline", action="store", type="string", dest="stgline", help="a line representing staging activity")
    
    (options, args) = parser.parse_args()
    if (None == options.dir or None == options.output):
        parser.print_help()
        sys.exit(1)
    
    print 'Checking directories....'
    dirs = options.dir.split(':')
    for d in dirs:
        unzipLogFiles(d)
    
    print 'Processing logs...'
    #options.stgline = "staging it for"
    acl = None
    if (None == options.stgline):
        acl = processLogs(dirs, options.output)
    else:
        acl = processLogs(dirs, options.output, stgline = options.stgline)
    
        