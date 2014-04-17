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
import os, commands
from os import walk
from collections import namedtuple

# retrieval access (date, observation id, was the file offline?)
RA = namedtuple('RA', 'obsId date offline')


def execCmd(cmd, failonerror = True):
    re = commands.getstatusoutput(cmd)
    if (re[0] != 0):
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

def processLogs(dirs):
    """
    process all logs from a list of directories
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
                parseLogFile(fullfn, accessList)
    
    accessList.sort() # automatically sort based on obsId
        

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
    obsName = None 
    atts = access.split(' - ')                
    for att in atts:
        atttokens = att.split('=')
        attnm = atttokens[0]
        if (attnm == 'path'):
            fileId = atttokens[2].split('|')[0]
            obsNum = fileId.split('_')[0] 
            #obsDate = fileId.split('_')[1][0:8] 
        #elif (attnm == 'user-agent'):
            #userAgent = atttokens[1].split(' ')[0]
    if (obsNum):
        re = RA(obsNum, date, isOffline)
    else:
        re = None
    return re

def parseLogFile(fn, accessList):
    """
    parse out a list of RA tuples from a single NGAS log file
    add them to the accessList
    """
    if (not os.path.exists(fn) or accessList == None):
        return
    
    # need to skip the redirect
    # cmd = 'grep -e RETRIEVE\? -e "Reading data block-wise" -e "to stage file:" -e NGAMS_INFO_REDIRECT %s' % fn
    cmd = 'grep -e RETRIEVE\? -e "to stage file:" -e NGAMS_INFO_REDIRECT %s' % fn
    re = execCmd(cmd, failonerror = False)
    if (re[0] != 0):
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
        if (li.find('Staging rate =') > -1):
            stg[tid] = None
        
    for tid in redrct:
        if (raDict.has_key(tid)):
            raDict.pop(tid) # remove all redirect requests
    
    for k, v in raDict.items():
        ra = _buildRA(v, stg.has_key(k))
        if (ra):
            accessList.append(ra)
    
    
    
        