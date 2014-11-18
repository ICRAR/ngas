#
#    (c) University of Western Australia
#    International Centre of Radio Astronomy Research
#    M468/35 Stirling Hwy
#    Perth WA 6009
#    Australia
#
#    Copyright by UWA,
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
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu      23/Sep/2014  Created

"""
Fix phase2 image DEC job plugin that will be called
by the SubscriptionThread._deliveryThread

1. check the DEC from the observation date
2. copy the file to an tmp directory (is it needed?)
3. add the new keyword
4. re-calculate the checksum
5. re-measure the size
6. copy the file back (involves changing file permission)
7. update the NGAS DB (checksum, size, etc.)
"""
import os, commands, binascii
import pccFits.PccSimpleFitsReader as fitsapi
import pyfits

work_dir = '/home/ngas/NGAS/volume1/processing/phase2fix'

dict_dec = {'2013-08-03':-55.0,'2013-08-05':-26.7,'2013-08-06':-13.0,'2013-08-07':-40.0,
'2013-08-08':1.6,'2013-08-09':-55.0,'2013-08-10':-26.7,'2013-08-12':18.6,
'2013-08-13':-72.0,'2013-08-17':18.6,'2013-08-18':-72.0,'2013-08-22':-13.0,
'2013-08-25':-40.0,'2013-11-04':-27.0,'2013-11-05':-13.0,'2013-11-06':-40.0,
'2013-11-07':1.6,'2013-11-08':-55.0,'2013-11-11':-18.0,'2013-11-12':-72.0,
'2013-11-25':-27.0,'2014-03-03':-27.0,'2014-03-04':-13.0,'2014-03-05':-40.0,
'2014-03-06':1.6,'2014-03-07':-55.0,'2014-03-08':18.0,'2014-03-09':-72.0,
'2014-03-16':-40.0,'2014-03-17':-55.0,'2014-06-09':-27.0,'2014-06-10':-40.0,
'2014-06-11':1.6,'2014-06-12':-55.0,'2014-06-13':-13.0,'2014-06-14':-72.0,
'2014-06-15':18.0,'2014-06-16':-13.0,'2014-06-18':-55.0,'2014-08-04':-27.0,
'2014-08-05':-40.0,'2014-08-06':-55.0,'2014-08-07':-72.0,'2014-08-08':-13.0,
'2014-08-09':1.6,'2014-08-10':18.0,'2014-09-15':-27.0,'2014-09-16':-40.0,
'2014-09-17':-55.0,'2014-09-18':-72.0,'2014-09-19':-13.0,'2014-09-20':1.6,
'2014-09-21':18.0,'2014-10-27':-27.0,'2014-10-28':-40.0,'2014-10-29':-55.0,
'2014-10-30':-72.0,'2014-10-31':-13.0,'2014-11-01':1.6,'2014-11-02':18.0} # key - obs_date, value - dec (float)

def execCmd(cmd, failonerror = True):
    re = commands.getstatusoutput(cmd)
    if (failonerror and (not os.WIFEXITED(re[0]))):
        errMsg = 'Fail to execute command: "%s". Exception: %s' % (cmd, re[1])
        raise Exception(errMsg)    
    return re

def getFileCRC(filename):
    block = "-"
    crc = 0
    blockSize = 1048576 # 1M block size
    fdIn = open(filename)
    while (block != ""):
        block = fdIn.read(blockSize)
        crc = binascii.crc32(block, crc)
    fdIn.close()
    return crc

def ngamsGLEAM_Fix_Phase2_JobPI(srvObj,
                          plugInPars,
                          filename,
                          fileId,
                          fileVersion,
                          diskId):
    """
    srvObj:        Reference to NG/AMS Server Object (ngamsServer).

    plugInPars:    Parameters to take into account for the plug-in
                   execution (string).(e.g. scale_factor=4,threshold=1E-5)
   
    fileId:        File ID for file to test (string).

    filename:      Filename of (complete) (string).

    fileVersion:   Version of file to test (integer).
 
    Returns:       the return code of the compression plugin (integer).
    """
    
    hdrs = fitsapi.getFitsHdrs(filename)
    date_obs = hdrs[0]['DATE-OBS'][0][1].replace("'", "").split('T')[0]
    if (not dict_dec.has_key(date_obs)):
        return (1, 'no date for %s' % filename)
    # copy it to some tmp directory
    fndir = os.path.dirname(filename)
    bname = os.path.basename(filename)
    fn = '%s/%s' % (work_dir, bname)
    execCmd('mv %s %s/' % (filename, work_dir))
    os.chmod(fn, 644) # owner writable
    hdulist = pyfits.open(fn, mode='update')
    prihdr = hdulist[0].header
    prihdr['DEC_PNT'] = dict_dec[date_obs]
    hdulist.close()
    
    # calculate the checksum, new filesize
    csum = getFileCRC(fn)
    fsize = getFileSize(fn) 
    
    # move the original file to a different name
    os.chmod(filename, 644)
    execCmd('mv %s %s/%s_origin' % (filename, fndir, bname))
    
    # move the new file back under the original name
    execCmd('mv %s %s/' % (fn, fndir))
    os.chmod(filename, 444) # make it readonly
    
    # if all fine, remove the original file (under the different name)
    # otherwise, remove the new file, and move the original file back to the original name
    os.remove('%s/%s_origin' % (fndir, bname))
    
    # update database with new crc and file size
    #