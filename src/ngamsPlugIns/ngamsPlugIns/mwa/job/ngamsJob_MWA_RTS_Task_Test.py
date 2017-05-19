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

# Fornax module for MWA RTS
#
# Who                   When          What
# -----------------   ----------    ---------
# chen.wu@icrar.org   10-June-2013    created

"""
This module is a test module for the function

ngamsJob_MWA_RTS.CorrLocalTask.execute() and paramsToArgs()
ngamsCmd_RUNTASK

"""

import commands, urllib2
import cPickle as pickle
from ngamsJob_MWA_RTS import CorrLocalTask, RTSJobParam, DummyLocalTask
from optparse import OptionParser

"""
file_list = '/tmp/NGAS_MWA/volume1/afa/2013-06-05/2/1052803816_20130517053000_gpubox20_00.fits,' +\
                '/tmp/NGAS_MWA/volume1/afa/2013-06-05/2/1052803816_20130517053104_gpubox20_01.fits,' +\
                '/tmp/NGAS_MWA/volume1/afa/2013-06-05/2/1052803816_20130517053208_gpubox20_02.fits,' +\
                '/tmp/NGAS_MWA/volume1/afa/2013-06-05/2/1052803816_20130517053312_gpubox20_03.fits,' +\
                '/tmp/NGAS_MWA/volume1/afa/2013-06-05/2/1052803816_20130517053416_gpubox20_04.fits'
"""
file_list = '/tmp/NGAS_MWA/volume1/afa/2013-06-05/2/1052803816_20130517052959_gpubox04_00.fits,' +\
                '/tmp/NGAS_MWA/volume1/afa/2013-06-05/2/1052803816_20130517053103_gpubox04_01.fits,' +\
                '/tmp/NGAS_MWA/volume1/afa/2013-06-05/2/1052803816_20130517053207_gpubox04_02.fits,' +\
                '/tmp/NGAS_MWA/volume1/afa/2013-06-05/2/1052803816_20130517053311_gpubox04_03.fits,' +\
                '/tmp/NGAS_MWA/volume1/afa/2013-06-05/2/1052803816_20130517053415_gpubox04_04.fits'


def testDummyLocalTaskOnNGAS():
    ngas_host = '192.168.222.96:7777'
    localTask = DummyLocalTask('123456')
    strLT = pickle.dumps(localTask)
    strRes = urllib2.urlopen('http://%s/RUNTASK' % ngas_host, data = strLT, timeout = 10).read()
    print 'Submit localtask, acknowledgement received: %s' % strRes

def testCorrTaskOnNGAS():
    """
    To run autoTest, need to start two servers
    1. an NGAS running on a Fornax compute node
    2. an NGAS running on a Fornax I/O node (JobMAN)

    construct a localtask
    send this local task to an Fornax compute node running NGAS
    monitor if the JobMAN on Fornax I/O node has any responses
    """
    job_id = 'cwu_20130605T171913.236'
    obs_num = 1052803816
    corr_id = 4
    ngas_host = '192.168.222.96:7777'

    taskId = '%s__%d__%d' % (job_id, obs_num, corr_id)
    fileList = file_list.split(',')
    params = RTSJobParam()

    localTask = CorrLocalTask(taskId, fileList, params)
    strLT = pickle.dumps(localTask)
    strRes = urllib2.urlopen('http://%s/RUNTASK' % ngas_host, data = strLT, timeout = 10).read()
    print 'Submit localtask, acknowledgement received: %s' % strRes

def manualTest():
    job_id = 'cwu_20130605T171913.236'
    obs_num = 1052803816
    corr_id = 20



    rts_tpl = 'regrid,simplecal'

    ngas_host = 'macbook46.icrar.org:7779'

    cmd = '/home/cwu/ngas_rt/src/ngamsPlugIns/ngamsJob_MWA_RTS_Task.sh' +\
        ' -j %s -o %d -c %d -t %s -f %s -g N' % (job_id, obs_num, corr_id, rts_tpl, file_list)
    re = commands.getstatusoutput(cmd)
    print re[0]
    print re[1]

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-t", "--task", dest = "test_task", help = "dummy | corr")
    (options, args) = parser.parse_args()
    if (not options.test_task or options.test_task == 'dummy'):
        testDummyLocalTaskOnNGAS()
    elif (options.test_task == 'corr'):
        testCorrTaskOnNGAS()
    else:
        parser.print_help()
    #

