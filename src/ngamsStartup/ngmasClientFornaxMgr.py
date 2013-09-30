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

from mpi4py import MPI

import ngamsFornaxMgr

dc_root = '/home/cwu/DataCapture'
dc_exec = '%s/testobj/dcexample' % dc_root
work_dir = '/tmp/NGAS_Client'
work_conf = '%s/dcconf' % work_dir

Non_Archive_Ngas = ['202.8.39.136', '202.8.39.137', '192.168.212.5', '192.168.212.6']

def launchSimulator(obsId, data_rate = 16, num_obs = 1, max_num_hdus = 42):
    # get currernt node id/ip
    nodeId = ngamsFornaxMgr.getNGASNodeName()
    # calculate host_id and port number of the corresponding ngas server based on the node_id
    ngas_host = getMyNGASHost()
    # create working directory of the simulator based on the node id
    if (not os.path.exists(work_dir)):
        os.makedirs(work_dir)    
    
    # copy dcconf file to the working directory 
    shutil.copy('%s/dcconf_tpl' % dc_root, work_conf)
    
    # change hostId and port within the dcconf file
    ngamsFornaxMgr.replaceTextInFile('${root_dir}', work_dir, work_conf)
    ngamsFornaxMgr.replaceTextInFile('${max_num_hdus}', str(max_num_hdus), work_conf)
    ngamsFornaxMgr.replaceTextInFile('${server_and_port}', ngas_host, work_conf)
    ngamsFornaxMgr.replaceTextInFile('${data_rate}', str(data_rate), work_conf)
    ngamsFornaxMgr.replaceTextInFile('${num_obs}', str(num_obs), work_conf)   
    
    # launch the simulator
    cmd = "bash %s/rundc_tpl.sh %s %s %s" % (dc_root, nodeId, obsId, work_conf)
    ngamsFornaxMgr.execCmd(cmd)

def getAvailableArchiveServers():
    """
    Return a List of servers (string) (host:port)
    
    This function is called by MPI-Rank 0
    """
    sqlQuery = "select host_id from ngas_hosts where srv_state = 'online'"
    conn = psycopg2.connect(database = 'ngas', user='ngas', 
                            password = 'bmdhcyRkYmE=\n'.decode('base64'), 
                            host = ngamsFornaxMgr.db_host)
    ret = []
    try:
        cur = conn.cursor()         
        cur.execute(sqlQuery)
        res = cur.fetchall()
        for ho in res:
            #print 'Ping host %s ...' % ho[0]
            if (ho[0].split(':')[0] in Non_Archive_Ngas):
                continue
            if (not ngamsFornaxMgr.pingHost('http://%s/STATUS' % ho[0])):
                ret.append(ho[0])
    finally:
        if (cur):
            del cur
        if (conn):            
            del conn
    
    return ret

def getMyNGASHost():
    """
    clients_per_server    how many clients share the 
                          same server (integer)
                          
    calculate host_id and port number of 
    the corresponding ngas server based on the node_id
    """
    comm = MPI.COMM_WORLD
    comm.Barrier()
    rank = comm.Get_rank()
    if (rank == 0):
        listSrvs = getAvailableArchiveServers()
    else:
        listSrvs = None
    
    listSrvs = comm.bcast(listSrvs, root = 0)
    
    return listSrvs[rank % len(listSrvs)]

def main():
    """
    """
    launchSimulator('12345', data_rate = 32)
    
    




