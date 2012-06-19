#!/usr/bin/env python
"""
    Name:
       scsiDeviceMapping
    Purpose:
        Retrieve the mapping of the SCSI device files to hardware controllers, channels and luns.
    Calling Sequence:
	scsiDeviceMapping		
    Inputs:
	None
    Common blocks:
    Side effects:
	This is using information of the /proc filesystem and certain files there. Thus
	it only works on standard Linux and only if the /proc filesystem is enabled..    
    History:
	2008-02-05: Created, Andreas Wicenec, ESO
"""
import sys
from commands import getstatusoutput

_supportedOS_ = ['Linux', 'SunOS']


def checkOS(expOs):
   """
   check whether this machine is running the correct OS
   """
   cmd0 = "uname -s"
   (stat,os) = getstatusoutput(cmd0)
   if os not in expOs:
      sys.exit("uname -s returned '%s', expecting on of [%s]" % (os, ','.join(expOs)))

def getPartitions():
   """
   """
   cmd1 = "grep 'sd[a-z]$' /proc/partitions"
   # execute grep..
   (stat,part) = getstatusoutput(cmd1)
   parts = part.split('\n')
   return parts

def getSCSI():
   """
   """
   cmd1 = "grep -A 1 scsi[0-9]* /proc/scsi/scsi"
   (stat,scsi) = getstatusoutput(cmd1)
   scsi = scsi.split('--\n')
   scsi = map(lambda x:x.split('\n'),scsi)
   return scsi

def formatOutput(part, scsi):
   """
   arrange the return list
   """
   full = map(lambda x,y:x.split(' ')[-1]+' --> '+y[0].strip()+' --> ' \
       + y[1].strip(),part,scsi)
   return full

if __name__ == '__main__':
   checkOS(_supportedOS_)
   parts = getPartitions()
   scsis = getSCSI()
   full = formatOutput(parts, scsis)
# print the return list
   for f in full:
      print f

