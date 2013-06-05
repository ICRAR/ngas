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
# cwu      06/May/2013  Created
#

"""
Launch and manage NGAS instances on the Fornax GPU Cluster
The work was supported by iVEC through the use of advanced computing resources
located at iVEC@UWA.
"""
import commands, os, sys, random, threading
import psycopg2

import ngamsCmd_QUERY

ipovib_prefix = '192.168.222.'
io_ex_ip = {'io1':'202.8.39.136', 'io2':'202.8.39.137'}  # the two Copy Nodes external ip

# the two Copy Nodes ib ip (this may not be useful since we run NGAS on external IPs to receive data from the outside world)
io_ipovib = {'io1':'192.168.212.5', 'io2':'192.168.212.6'} 
ngas_src_root = '/scratch/astronomy556/MWA/ngas_rt'
ngas_vol_tool = ngas_src_root + '/src/ngasUtils/src/ngasPrepareVolumeNoRoot.py'
ngas_cache_server = ngas_src_root + '/src/ngamsServer/ngamsCacheServer.py'
ngas_pclient = ngas_src_root + '/bin/ngamsPClient'
ngas_runtime_root = '/tmp/NGAS_MWA'
python_exec = ngas_src_root + '/bin/python'
tplCfgFile = ngas_src_root + '/cfg/NgamsCfg.PostgreSQL.fornax.xml'
host_status = {'online':'ONLINE', 'offline':'OFFLINE', 'down':'NOT-RUNNING'}
db_host = 'fornaxspare'

def execCmd(cmd, failonerror = True):
    re = commands.getstatusoutput(cmd)
    if (failonerror and re[0] != 0):
        raise Exception('Fail to execute command: "%s". Exception: %s' % (cmd, re[1]))
    return re

def replaceTextInFile(fr, to, file):
    cmd = 'sed -i s/%s/%s/g %s' % (fr, to, file)
    execCmd(cmd)
    
def isHeadNode():
    """
     to check if I am the head node
    """
    pass

def getNGASNodeName():
    """
    Get the node where this NGAS is running
    e.g. 'f001', 'f023', 'io1'  
    """
    cmd = "uname -a |awk '{print $2}'"
    re = execCmd(cmd)
    return re[1]

def getNGASBindingIp(node):
    """
    Get the IP Address NGAS will bind to
    
    node --  e.g. 'fornax101', 'fornax023', 'io1'
    return -- 192.168.222.101, 192.168.222.23, 202.8.39.136
   
    """
    if (io_ex_ip.has_key(node)):
            return io_ex_ip[node]
    for i in range(len(node)):        
        if (node[i].isdigit()):
            return ipovib_prefix + str(int(node[i:]))
    raise Exception("IP address cannot be parsed out from %s" % node)

def createConfigFile(overwrite = False):
    """
    Create the ngas configuration file
    based on the tempalte, but change to
    the binding IpAddress
    
    Return    the path of the configuration file just created
    """    
    # fornax.server.id, fornax.server.archivename,fornax.server.ipaddress
    node = getNGASNodeName()    
    cfgFile = ngas_src_root + '/cfg/fornax/' + node + '.xml'
    if (os.path.exists(cfgFile)):
        if (overwrite):
            cmd = 'rm %s' % cfgFile
            execCmd(cmd, failonerror = False)
        else:
            return cfgFile
    cmd = 'cp %s %s' % (tplCfgFile, cfgFile)
    execCmd(cmd)
    ipAdd = getNGASBindingIp(node)
    srvId = 'Fornax-' + node
    archName = 'MWA-Fornax-CacheArchive-' + node

    replaceTextInFile('fornax.server.id', srvId, cfgFile)
    replaceTextInFile('fornax.server.archivename', archName, cfgFile)
    replaceTextInFile('fornax.server.ipaddress', ipAdd, cfgFile)
    
    return cfgFile

def createDiskVolumes(overwrite = False, num_volume = 1):
    """
    Create local disk volumes on each Fornax compute node
    create the volume id if it is not there
    
    overwrite       whether or not to overwrite (Boolean)
                    an existing volume directory (if already there)    
    """
    for i in range(num_volume):
        vol_path = ngas_runtime_root + '/volume' + str(i + 1)
        if (os.path.exists(vol_path)):
            if (overwrite):
                cmd = 'rm -rf %s' % vol_path
                re = execCmd(cmd, failonerror = False)
            else:
                continue
        cmd = 'mkdir -p %s' % vol_path
        execCmd(cmd)
        cmd = '%s %s --path=%s --silent' % (python_exec, ngas_vol_tool, vol_path) 
        execCmd(cmd)
    
def startServer(overwriteCfg = False, overwriteDisks = False):
    """
    Start the server, need to check
    1. config file (IpAddress, etc.)
    2. running as a cache mode?
    3. prepare the disks
    """
    cfgFile = createConfigFile(overwrite = overwriteCfg)
    if (not io_ex_ip.has_key(getNGASNodeName())): # for copy/proxy archive nodes, do not create local disks
        createDiskVolumes(overwrite = overwriteDisks)
    
    cmd = '%s %s -cfg %s -autoOnline -force -multipleSrvs' % (python_exec, ngas_cache_server,cfgFile)
    #2>&1>/dev/null
    execCmd(cmd)

def _sshStartServerThread(serverId):
    """
    serverId:    server identifier(int)
    """
    if (serverId > 10):
        nodename = 'f0%s' % str(serverId)
    else:
        nodename = 'f00%s' % str(serverId)
    
    cmd = 'ssh %s "/scratch/astronomy556/MWA/ngas_rt/bin/python /scratch/astronomy556/MWA/ngas_rt/src/ngamsStartup/ngamsFornaxMgr.py"' % nodename
    execCmd(cmd)

def sshStartServers(num = 24):
    """
    Use SSH to start NGAS server
    """
    serverList = random.sample(range(1, 97), num)
    for sid in serverList:
        args = (sid,)
        thrd = threading.Thread(None, _sshStartServerThread, 'NGAS_%d' % sid, args) 
        thrd.setDaemon(1) # it will exit immediately should the command exit
        thrd.start()
        
    
def monitorServers(status = 'online', printRes = True):
    """
    Monitor NGAS servers on Fornax filtered by their status
    status:        one of ['down', 'offline', 'online']
    """
    if (not host_status.has_key(status)):
        print 'Unknown monitor status: %s, valid status are: %s' % (status, host_status.keys())
        return
    sqlQuery = "select host_id, srv_state, installation_date from ngas_hosts where srv_state = '%s'" % host_status[status]
    conn = psycopg2.connect(database = 'ngas', user='ngas', 
                            password = 'bmdhcyRkYmE=\n'.decode('base64'), 
                            host = db_host)    
    try:   
        cur = conn.cursor()         
        cur.execute(sqlQuery)
        res = cur.fetchall()    
        header = ()
        for i in range(len(cur.description)):
            header += (cur.description[i].name,)
    finally:
        if (cur):
            del cur
        if (conn):
            del conn
    if (printRes):
        print ngamsCmd_QUERY.formatAsList([res], header)
    return res

def stopSingleServer(host_id):
    """
    Shut down a single server
    host_id   ip address:port of the server (string)
    """
    if ('' == host_id or None == host_id):
        return
    print 'Stopping server %s ...' % host_id
    ip = host_id.split(':')[0]
    port = host_id.split(':')[1]
    cmd = '%s -port %s -host %s -status -cmd OFFLINE -force' % (ngas_pclient, port, ip)
    execCmd(cmd, failonerror = False)
    cmd = '%s -port %s -host %s -status -cmd EXIT' % (ngas_pclient, port, ip)
    execCmd(cmd)

def stopServers(serverList = None):
    """
    shut down a list of NGAS servers
    
    serverList:    comma separated host_id's (string)
                    e.g. server1:port, server2:port
    """
    if (not serverList):
        #create the serverList
        serverList = ''
        reList = monitorServers(status = 'online', printRes = False)
        c = 0
        for re in reList:
            if (0 == c):
                serverList = re[0]
            else:
                serverList += ',%s' % re[0]
            c += 1
    
    for host_id in serverList.split(','):
        try:
            stopSingleServer(host_id.strip())
        except Exception, e:
            print 'Fail to shut down server %s, Exception: %s' % (host_id, str(e))
            continue

if __name__ == '__main__':
    #createConfigFile(overwrite = True)
    #createDiskVolumes(overwrite = True)
    leng = len(sys.argv)
    if (leng == 1):
        startServer()
    else:
        action = sys.argv[1].lower()
        if ('start' == action):
            overwriteCfg = 0
            overwriteDisks = 0
            if (leng > 2): # get the start parameter (1 - overwriteCfg, 0 - otherwise)
                overwriteCfg = int(sys.argv[2])
                if (leng > 3): # get the second start parameter (1 - overwrite volumes, 0 - otherwise)
                    overwriteDisks = int(sys.argv[3])                
            startServer(overwriteCfg, overwriteDisks)
        elif ('stop' == action):
            if (leng > 2):
                stopServers(sys.argv[2]) # get a list of servers separated by comma, e.g. 192.168.222.2:7777,192.168.222.34:7777
            else:
                stopServers()
        elif ('monitor' == action):
            if (leng > 2):
                monitorServers(status = sys.argv[2])
            else:
                monitorServers()
        elif ('ssh' == action):
            if (leng > 2):
                sshStartServers(num = int(sys.argv[2]))
            else:
                sshStartServers()
        else:
            print 'usage: %s ngamsFornaxMgr.py [start[1|0 [1|0]] | stop [server1:port, server2:port] | monitor [online|offline|down]]' % python_exec
        