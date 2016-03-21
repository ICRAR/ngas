#    ICRAR - International Centre for Radio Astronomy Research
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
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu      2014-10-15  Created
"""
Compression, CRC, and transfer to MIT job plugin that will be called
by the SubscriptionThread._deliveryThread
"""
"""
curl 146.118.84.67:7792/SUBSCRIBE?priority=1\&url=ngasjob://ngamsJob_MITDeliveryPlugin%3Fredo_on_fail%3D0\&subscr_id=COMPRESS_TO_MIT_SYNC\&filter_plug_in=ngamsJob_MITDeliveryFI\&start_date=2014-10-01T05:16:05.660
nohup /home/ngas/ngas_rt/bin/python /home/ngas/ngas_rt/src/ngamsServer/ngamsDataMoverServer.py -cfg /home/ngas/ngas_rt/cfg/NgamsCfg.PostgreSQL.fe04_dm_mit_sync02.xml -autoOnline -force -multiplesrvs > /dev/null&

"""

import commands, os, binascii, urllib2

from ngamsLib import ngamsPlugInApi
from ngamsLib.ngamsCore import error, info, warning


uvcompress = '/home/ngas/ngas_rt/bin/uvcompress'

def execCmd(cmd, failonerror = False):
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

def isMWAVisFile(fileId):
    fileName, fileExtension = os.path.splitext(fileId)
    if ('.fits' != fileExtension.lower()):
        return False # only FITS file is considered

    if (fileName.find('_gpubox') == -1):
        return False

    ss = fileName.split('_')
    if (len(ss) != 4):
        return False

    return True

def ngamsJob_MITDeliveryPlugin(srvObj,
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
    debug = 0
    pars = ""
    sf = 4 # scaling factor
    th = 1E-5 # threshold
    bins = 0
    send_crc = 1
    to_host = 'eor-10.mit.edu'
    to_port = '7777'
    if ((plugInPars != "") and (plugInPars != None)):
        pars = plugInPars

    parDic = ngamsPlugInApi.parseRawPlugInPars(pars)

    if (parDic.has_key('scale_factor')):
        sf = int(parDic['scale_factor'])

    if (parDic.has_key('threshold')):
        th = float(parDic['threshold'])

    if (parDic.has_key('bins')):
        bins = int(parDic['bins'])

    if (parDic.has_key('send_crc')):
        send_crc = int(parDic['send_crc'])

    if (parDic.has_key('to_host')):
        to_host = parDic['to_host']

    if (parDic.has_key('to_port')):
        to_port = parDic['to_port']


    if (bins):
        binstr = '-h %d' % bins
    else:
        binstr = ''
    work_dir = srvObj.getCfg().getRootDirectory() + '/tmp/'
    newfn = '%s/%s' % (work_dir, os.path.basename(filename))

    cmd = "%s -d %d %s %s %s" % (uvcompress, sf, binstr, filename, newfn)
    cmd3 = "rm %s" % newfn

    if (os.path.exists(newfn)):
        execCmd(cmd3)


    mwaFits = isMWAVisFile(fileId)
    sendRawFile = False

    if (mwaFits):
        re = execCmd(cmd)
        #if (0 == re[0]):
        if (os.WIFEXITED(re[0])): # if the child process exit normally (see http://linux.die.net/man/2/wait)
            if (not bins):
                retstr = re[1].split('\n')[-1] # just get the elapsed time
            else:
                retstr = re[1].split('------ Histogram ------\n')[1]

            #bbcpurl = "curl --connect-timeout 7200 eor-12.mit.edu:7777/BBCPARC?fileUri=ngas%40146.118.84.67%3A/mnt/mwa01fs/MWA/testfs/output_320M_001.dat\&bport=7790\&bwinsize=%3D32m\&bnum_streams=12\&mimeType=application/octet-stream"
            bbcpurl = "http://%s:%s/BBCPARC?fileUri=ngas%%40146.118.84.67%%3A%s&bport=7790&bwinsize=%%3D32m&bnum_streams=12&mimeType=application/octet-stream" % (to_host, to_port, newfn)

            if (send_crc):
                try:
                    crc32 = getFileCRC(newfn)
                    bbcpurl += "&bchecksum=%s" % str(crc32)
                except Exception, crcexp:
                    error('Fail to calculate the file CRC %s: %s' % (newfn, str(crcexp)))
                    return (500, str(crcexp).replace("'",""))

            # send it thru bbcp
            try:
                resp = urllib2.urlopen(bbcpurl, timeout = 7200)
                retstr = resp.read()
                if (retstr.find("Successfully handled Archive") > -1):
                    info(3, 'Successfully compressed and sent the file %s' % newfn)
                else:
                    error('Fail to sent the file %s: %s' % (newfn, retstr))
                    return (500, retstr.replace("'",""))
            except Exception, exp:
                warn_msg = ''
                if (type(exp) is urllib2.HTTPError):
                    errors = exp.readlines()
                    for ee in errors:
                        warn_msg += ee
                    warn_msg = 'HTTP error: %s' % (warn_msg)
                elif (type(exp) is urllib2.URLError):
                    warn_msg = 'Target NGAS server %s is down: %s' % (to_host, str(exp))
                else:
                    warn_msg = 'Unexpected error: %s' % (str(exp))

                warning('Fail to sent the file %s: %s' % (newfn, warn_msg))
                return (500, warn_msg.replace("'",""))
            finally:
                # remove the temp file THIS IS DANGEROUS!!
                re = execCmd(cmd3) #be cautious when copying this line!!
                if (0 != re[0]):
                    warning('Fail to remove the temp compressed file %s' % newfn)

            return (0, retstr.replace("'",""))
        else:
            error('Fail to compress file %s: %s' % (filename, re[1]))
            re = execCmd(cmd3)
            if (0 != re[0]):
                warning('Fail to remove the temp compressed file %s' % newfn)
            sendRawFile = True

    if ((not mwaFits) or sendRawFile):
        #send uncompressed version
        bbcpurl = "http://%s:%s/BBCPARC?fileUri=ngas%%40146.118.84.67%%3A%s&bport=7790&bwinsize=%%3D32m&bnum_streams=12&mimeType=application/octet-stream" % (to_host, to_port, filename)

        if (send_crc):
            try:
                crc32 = getFileCRC(filename)
                bbcpurl += "&bchecksum=%s" % str(crc32)
            except Exception, crcexp:
                error('Fail to calculate the file CRC %s: %s' % (filename, str(crcexp)))
                return (500, str(crcexp).replace("'",""))

        # send it thru bbcp
        try:
            resp = urllib2.urlopen(bbcpurl, timeout = 7200)
            retstr = resp.read()
            if (retstr.find("Successfully handled Archive") > -1):
                info(3, 'Successfully sent the uncompressed file %s' % filename)
                return (0, retstr.replace("'",""))
            else:
                error('Fail to sent the file %s: %s' % (filename, retstr))
                return (500, retstr.replace("'",""))
        except Exception, exp:
            warn_msg = ''
            if (type(exp) is urllib2.HTTPError):
                errors = exp.readlines()
                for ee in errors:
                    warn_msg += ee
                warn_msg = 'HTTP error: %s' % (warn_msg)
            elif (type(exp) is urllib2.URLError):
                warn_msg = 'Target NGAS server %s is down: %s' % (to_host, str(exp))
            else:
                warn_msg = 'Unexpected error: %s' % (str(exp))

            warning('Fail to sent the file %s: %s' % (filename, warn_msg))
            return (500, warn_msg.replace("'",""))
