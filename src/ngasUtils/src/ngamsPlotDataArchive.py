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
# cwu      28/April/2014  Created

"""
Support multiple logs 
convert all time stamps into virtual relative time stamps
"""

import os, commands, gc, sys, time
import re as regx
import datetime as dt
import numpy as np

# quick archive (date, start_time, end_time, transfer_rate, file_size)
# time is "virtual", starting from 0 within the scope of each log file
QA = namedtuple('QA', 'stt edt rate size')

def execCmd(cmd, failonerror = True, okErr = []):
    re = commands.getstatusoutput(cmd)
    if (re[0] != 0 and not (re[0] in okErr)):
        errMsg = 'Fail to execute command: "%s". Exception: %s' % (cmd, re[1])
        if (failonerror):
            raise Exception(errMsg)
        else:
            print errMsg
    return re

def _buildQA(a_start, a_end, rate):
    """
    Construct the retrieval access tuple from the line
    """
    stt = a_start.split(' ')[0]
    edt = a_end.split(' ')[0]
    #date = timestamp.split('T')[0]
    #time = timestamp.split('T')[1]        
    
    atts = regx.split(' - |; ', a_start)   
    ingSize = 0 # for ingestion only           
    for att in atts:
        atttokens = att.split('=')
        attnm = atttokens[0]
        if (attnm == 'content-length'):
            """
            e.g. 
            method=POST - path=|QARCHIVE| - host=146.118.87.251 - content-length=1015030080 - content-type=application/octet-stream 
            - authorization=Basic bmdhcy1pbnQ6bmdhcyRkYmE= - content-disposition=attachment; 
            filename="1077377712_20140225153458_gpubox05_00.fits"; no_versioning=1 [ngamsServer.py:handleHttpRequest:1537:86486:Thread-208369]
            """
            try:
                ingSize = int(atttokens[1])
            except:
                pass

    if (ingSize):
        re = QA(stt, edt, float(rate), ingSize)
    else:
        re = None
    return re

def _normQAList(qaList):
    """
    sort then normalise the time
    """
    if (len(qaList) < 2):
        return
    
    qaList.sort() # sort based on start time
    
    strd0 = qaList[0].stt
    d0 = dt.datetime.strptime(strd0,'%Y-%m-%dT%H:%M:%S.%f')
    
    for qa in qaList:
        dst = dt.datetime.strptime(qa.stt,'%Y-%m-%dT%H:%M:%S.%f') - d0 # delta start time
        ded = dt.datetime.strptime(qa.edt,'%Y-%m-%dT%H:%M:%S.%f') - d0 # delta end time
                
        qa.stt = dst.seconds + (dst.microseconds)/1e6
        qa.edt = ded.seconds + (ded.microseconds)/1e6

def parseLogFile(fn):
    """
    parse an NGAS log and return a list of QAs
    """
    archiveList = []
    cmd = 'grep -e \|QARCHIVE\| -e "Successfully handled Archive" -e "Saved data in file" %s' % (fn)
    re = execCmd(cmd, failonerror = False, okErr = [256])
    if (re[0] != 0 and re[0] != 256):
        print 'Fail to parse log file %s' % fn
        return
    
    goodarch = {}
    archDict = {}
    tmpDict = {}
    rateDict = {}
    fsize = defaultdict(int) # k - tid, v - size
    lines = re[1].split('\n')
    
    for li in lines:
        tid = _getTidFrmLine(li)
        if (not tid):
            continue
        if (li.find('|QARCHIVE|') > -1):
            tmpDict[tid] = li 
        elif (li.find('Saved data in file') > -1)    :
            rateDict[tid] = li.split(' ')[14]     # this is highly fragile  
        elif (li.find('Successfully handled Archive') > -1): # get ingestion volume
            goodarch[tid] = li        
        
    for tid in goodarch.keys():
        if (tmpDict.has_key(tid)):
            archDict[tid] = tmpDict[tid] # only successful archives are counted.
    
    for k, v in archDict.items():
        ra = _buildQA(v, goodarch[k], rateDict[k])
        if (ra):
            archiveList.append(ra)
        else:
            print 'none RA for %s in file %s' % (k, fn)
    
    _normQAList(archiveList)
    return archiveList

def computeThruput(QALists):
    """
    QALists:    a list of qaLists (all qaLists belong to the same experiment setting)
    
    Return:     a tuple (overall_throughput, mean_thruput, error_mean_thruput, mean_iat, error_iat)
    """
    max = 0
    size = 0
    mrate = []
    iat = []
    for qaList in QALists:
        if (qaList[-1].edt > max):
            max = qaList[-1].edt
        c = 0
        for qa in qaList:
            size += qa.size
            mrate.append(qa.rate)
            if (c > 0):
                iat.append(qa.edt - qaList[c - 1].stt)
            c += 1
            
    ot = float(size) / max
    
    arate = np.array(mrate)
    aiat = np.array(iat)
    
    return (ot, np.mean(arate), np.std(arate), np.mean(aiat), np.std(aiat))
    
    
    
    


    