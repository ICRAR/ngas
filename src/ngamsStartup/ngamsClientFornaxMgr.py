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
# cwu      20/Sep/2013  Created
#
"""
Launch and manage NGAS simulators on the Fornax GPU Cluster
The work was supported by iVEC through the use of advanced computing resources
located at iVEC@UWA.
"""
import commands, os, sys, random, shutil
import psycopg2

from optparse import OptionParser

from mpi4py import MPI

import ngamsFornaxMgr

dc_root = '/home/cwu/DataCapture'
dc_exec = '%s/testobj/dcexample' % dc_root
work_dir = '/tmp/NGAS_Client'
work_conf = '%s/dcconf' % work_dir
tgtDir = '/scratch/astronomy564/cwu/ngas_logs'



def launchSimulator(obsId, data_rate = 16, num_obs = 1, max_num_hdus = 42, crc_enabled = True, global_fs = False, num_server = None):
    comm = MPI.COMM_WORLD
    # get currernt node id/ip
    nodeId = ngamsFornaxMgr.getNGASNodeName()
    print 'I am %s' % nodeId
    # calculate host_id and port number of the corresponding ngas server based on the node_id
    if (global_fs):
        localFS = 0
    else:
        localFS = 1
    (rank, listSrvs) = getMyNGASHost(comm, num_server, localfs = localFS)
    num_servers = len(listSrvs)
    ngas_host = listSrvs[rank % num_servers]
    
    # create working directory of the simulator based on the node id
    if (not os.path.exists(work_dir)):
        os.makedirs(work_dir)    
    
    """
    # copy dcconf file to the working directory 
    shutil.copy('%s/dcconf_tpl' % dc_root, work_conf)
    
    # change hostId and port within the dcconf file
    ngamsFornaxMgr.replacePathInFile('\${root_dir}', work_dir, work_conf) # '\' is the escape for '$'
    ngamsFornaxMgr.replaceTextInFile('\${max_num_hdus}', str(max_num_hdus), work_conf)
    ngamsFornaxMgr.replaceTextInFile('\${server_and_port}', ngas_host, work_conf)
    ngamsFornaxMgr.replaceTextInFile('\${data_rate}', str(data_rate), work_conf)
    ngamsFornaxMgr.replaceTextInFile('\${num_obs}', str(num_obs), work_conf)   
    cmd = "bash %s/rundc_tpl.sh %s %s %s" % (dc_root, nodeId, obsId, work_conf)
    """
    # launch the simulator
    if (crc_enabled):
        archive_cmd = 'QARCHIVE'
    else:
        archive_cmd = 'QARCHIVENOCRC'
    speedFile = '%s/speed_%s.pkl' % (work_dir, nodeId)
    cmd = "python %s/src/ngasUtils/src/diskTest.py -d http://%s/%s -b 1048576 -w -t 5 -r %d -f %s -e %s" % \
               (ngamsFornaxMgr.ngas_src_root, ngas_host, archive_cmd, data_rate, speedFile, obsId)
    ngamsFornaxMgr.execCmd(cmd)
    
    comm.Barrier() #wait until all clients finishing archiving
    
    if (rank == 0):
        # if rank0, I will get all server logs, analyse them, and record the final result
        num_clients = comm.Get_size()
        if (crc_enabled):
            crc_comment = 'CRC'
        else:
            crc_comment = 'NoCRC'
        if (global_fs):
            fs_comment = 'Lu'
        else:
            fs_comment = 'Lo'
            
        comment = '%s-%ds-%s-%dc-%dm' % (fs_comment, num_servers, crc_comment, num_clients, data_rate)
        tgtLogDir = ngamsFornaxMgr.getServerLogs(tgtDir, comment, analyse = True, serverList = listSrvs, localfs = localFS)
    else:
        tgtLogDir = None
    
    comm.Barrier() #wait until rank 0 finishes copying all the server logs
    tgtLogDir = comm.bcast(tgtLogDir, root = 0)
    
    if (tgtLogDir):
        shutil.copy(speedFile, tgtLogDir + '/')
    
    #cmd = 'rm -rf %s/*' % work_dir
    #ngamsFornaxMgr.execCmd(cmd, failonerror = False)

def getMyNGASHost(comm, num_server, localfs = 1):
    """
    clients_per_server    how many clients share the 
                          same server (integer)
                          
    calculate host_id and port number of 
    the corresponding ngas server based on the node_id
    """
    rank = comm.Get_rank()        
    
    if (rank == 0):
        
        listSrvs = ngamsFornaxMgr.getAvailableArchiveServers()
        if (num_server and num_server < len(listSrvs)):
            listSrvs = listSrvs[0:num_server]
        ngamsFornaxMgr.cleanServerLogs(listSrvs, localFS = localfs)
    else:
        listSrvs = None
        
    comm.Barrier() #wait until rank 0 finishes getting serverlist and cleaning the logs
    listSrvs = comm.bcast(listSrvs, root = 0)
    print 'List of available NGAS server = %s' % str(listSrvs)
    
    return (rank, listSrvs)

def main():
    """
    """
    parser = OptionParser()
    parser.add_option("-n", "--nocrc",
                  action="store_false", dest="crc_enabled", default = True,
                  help="CRC on server is disabled")
    
    parser.add_option("-g", "--globalfs",
                  action="store_true", dest="global_fs", default = False,
                  help="NGAS uses the lustre global file system")
    
    parser.add_option("-d", "--datarate", type="int", dest="data_rate", default=96, 
                      help="Data rate per client (MB/s)")
    
    parser.add_option("-o", "--obsId",
                  action="store", type="string", dest="obs_id")
    
    parser.add_option("-s", "--numservers", type="int", dest="num_server", 
                      help="Number of servers used")
    
    (options, args) = parser.parse_args()
    if (None == options.obs_id):
        parser.print_help()
        exit(1)
            
    speed_step = 128
    for i in range(7):
        dataRate = (i + 1) * speed_step
        #print dataRate
        launchSimulator(options.obs_id + "_" + str(i), data_rate = dataRate, 
                   crc_enabled = True, global_fs = options.global_fs, num_server = options.num_server)
        launchSimulator(options.obs_id + "_" + str(i + 100), data_rate = dataRate, 
                   crc_enabled = False, global_fs = options.global_fs, num_server = options.num_server)
    
    """
    launchSimulator(options.obs_id, data_rate = 128, 
                    crc_enabled = True, global_fs = False, num_server = options.num_server)
    
    launchSimulator(options.obs_id, data_rate = 128, 
                    crc_enabled = False, global_fs = False, num_server = options.num_server)
    
    launchSimulator(options.obs_id, data_rate = 256, 
                    crc_enabled = True, global_fs = False, num_server = options.num_server)
    
    launchSimulator(options.obs_id, data_rate = 256, 
                    crc_enabled = False, global_fs = False, num_server = options.num_server)
    
    launchSimulator(options.obs_id, data_rate = 384, 
                    crc_enabled = True, global_fs = False, num_server = options.num_server)
    
    launchSimulator(options.obs_id, data_rate = 384, 
                    crc_enabled = False, global_fs = False, num_server = options.num_server)
    
    launchSimulator(options.obs_id, data_rate = 512, 
                    crc_enabled = True, global_fs = False, num_server = options.num_server)
    
    launchSimulator(options.obs_id, data_rate = 512, 
                    crc_enabled = False, global_fs = False, num_server = options.num_server)
    
    launchSimulator(options.obs_id, data_rate = 640, 
                    crc_enabled = True, global_fs = False, num_server = options.num_server)
    
    launchSimulator(options.obs_id, data_rate = 640, 
                    crc_enabled = False, global_fs = False, num_server = options.num_server)
    
    launchSimulator(options.obs_id, data_rate = 768, 
                    crc_enabled = True, global_fs = False, num_server = options.num_server)
    
    launchSimulator(options.obs_id, data_rate = 768, 
                    crc_enabled = False, global_fs = False, num_server = options.num_server)
    
    launchSimulator(options.obs_id, data_rate = 896, 
                    crc_enabled = True, global_fs = False, num_server = options.num_server)
    
    launchSimulator(options.obs_id, data_rate = 896, 
                    crc_enabled = False, global_fs = False, num_server = options.num_server)
    """
if __name__=="__main__":
    main()
    
    




