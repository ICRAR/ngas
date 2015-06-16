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

import commands, os, sys, random, threading, datetime

from ngamsPlugIns import ngamsCmd_QUERY
import psycopg2


ipovib_prefix = '192.168.222.'
io_ex_ip = {'io1':'202.8.39.136', 'io2':'202.8.39.137'}  # the two Copy Nodes external ip

# the two Copy Nodes ib ip (this may not be useful since we run NGAS on external IPs to receive data from the outside world)
io_ipovib = {'io1':'192.168.212.5', 'io2':'192.168.212.6'} 
Non_Archive_Ngas = ['202.8.39.136', '202.8.39.137', '192.168.212.5', 
                    '192.168.212.6', '192.168.222.97', '192.168.222.98', 
                    '192.168.222.99', '192.168.222.100']
ngas_src_root = '/home/cwu/ngas_rt'
ngas_vol_tool = ngas_src_root + '/src/ngasUtils/src/ngasPrepareVolumeNoRoot.py'
ngas_cache_server = ngas_src_root + '/src/ngamsServer/ngamsServer.py'
ngas_pclient = ngas_src_root + '/bin/ngamsPClient'
ngas_runtime_root = '/tmp/NGAS_MWA'
python_exec = ngas_src_root + '/bin/python'
tplCfgFile = ngas_src_root + '/cfg/NgamsCfg.PostgreSQL.fornax.xml'
#NgamsCfg.PostgreSQL.fornax_lustre.xml
host_status = {'online':'ONLINE', 'offline':'OFFLINE', 'down':'NOT-RUNNING'}
db_host = 'fornaxspare'
ngas_lustre_root = '/scratch/partner766/cwu/NGAS_roots'

def execCmd(cmd, failonerror = True):
    re = commands.getstatusoutput(cmd)
    if (failonerror and re[0] != 0):
        raise Exception('Fail to execute command: "%s". Exception: %s' % (cmd, re[1]))
    return re

def replaceTextInFile(fr, to, file):
    cmd = 'sed -i s/%s/%s/g %s' % (fr, to, file)
    execCmd(cmd)

def replacePathInFile(fr, to, file):
    # see http://alldunne.org/2011/01/escaping-the-forward-slash-character-with-sed/
    cmd = 'sed -i s=%s=%s=g %s' % (fr, to, file)
    execCmd(cmd)

def pingHost(url, timeout = 5):
    """
    To check if a host is successfully running
    
    Return:
    0        Success
    1        Failure
    """
    cmd = 'curl %s --connect-timeout %d' % (url, timeout)
    try:
        return execCmd(cmd)[0]
    except Exception, err:
        return 1
    
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

def createConfigFile(overwrite = False, localfs = True):
    """
    Create the ngas configuration file
    based on the tempalte, but change to
    the binding IpAddress
    
    Return    the path of the configuration file just created
    """    
    # fornax.server.id, fornax.server.archivename,fornax.server.ipaddress
    node = getNGASNodeName()    
    foraxCfgDir = ngas_src_root + '/cfg/fornax'
    if (not os.path.exists(foraxCfgDir)):
        cmd = 'mkdir -p %s' % foraxCfgDir
        execCmd(cmd)
        
    cfgFile = ngas_src_root + '/cfg/fornax/' + node + '.xml'
    if (os.path.exists(cfgFile)):
        if (overwrite):
            cmd = 'rm %s' % cfgFile
            execCmd(cmd, failonerror = False)
        else:
            return cfgFile
    if (not localfs):
        global tplCfgFile
        tplCfgFile = ngas_src_root + '/cfg/NgamsCfg.PostgreSQL.fornax_lustre.xml'
    cmd = 'cp %s %s' % (tplCfgFile, cfgFile)
    execCmd(cmd)
    """
    if (not localfs): #running on lustre
        global ipovib_prefix
        ipovib_prefix = '192.168.22.' # binding to a non-storage network ip address
    """
    ipAdd = getNGASBindingIp(node)
    srvId = 'Fornax-' + node
    archName = 'MWA-Fornax-CacheArchive-' + node

    replaceTextInFile('fornax.server.id', srvId, cfgFile)
    replaceTextInFile('fornax.server.archivename', archName, cfgFile)
    replaceTextInFile('fornax.server.ipaddress', ipAdd, cfgFile)
    
    if (not localfs):
        replaceTextInFile('fornax.server.node', getNGASNodeName(), cfgFile)
    
    
    return cfgFile

def createDiskVolumes(overwrite = False, num_volume = 1, localfs = 1):
    """
    Create local disk volumes on each Fornax compute node
    create the volume id if it is not there
    
    overwrite       whether or not to overwrite (Boolean)
                    an existing volume directory (if already there)    
    """
    for i in range(num_volume):
        if (localfs):
            vol_path = ngas_runtime_root + '/volume' + str(i + 1)
        else:
            vol_path = ngas_lustre_root + '/' + getNGASNodeName() + '/volume' + str(i + 1)
            
        if (os.path.exists(vol_path)):
            if (overwrite):
                cmd = 'rm -rf %s' % vol_path
                execCmd(cmd, failonerror = False)
            else:
                continue
                              
        cmd = 'mkdir -p %s' % vol_path
        execCmd(cmd)
        cmd = '%s %s --path=%s --silent' % (python_exec, ngas_vol_tool, vol_path) 
        execCmd(cmd)
    """
    else:
        create_new = False        
        vol_path = ngas_runtime_root + '/volume1'
        if (os.path.exists(vol_path)): 
            if os.path.islink(vol_path):# was link for lustre
                if (overwrite):
                    cmd = 'rm %s' % vol_path
                    execCmd(cmd, failonerror = False)
                    create_new = True
            else:#was directory for local
                # rename it first (dont want to remove it all!)
                cmd = 'mv %s %s' % (vol_path, vol_path + '_dir_bak')
                execCmd(cmd)
                if (os.path.exists(vol_path + '_link_bak')):
                    cmd = 'mv %s %s' % (vol_path + '_link_bak', vol_path)
                else:
                    create_new = True
        else:
            create_new = True
        
        if (create_new):
            node = getNGASNodeName()
            real_vol_path = ngas_lustre_root + '/' + node + '/volume1'
            cmd = 'mkdir -p %s' % real_vol_path
            execCmd(cmd)
            cmd = '%s %s --path=%s --silent' % (python_exec, ngas_vol_tool, real_vol_path)
            execCmd(cmd)
            #cmd = 'rm %' % (vol_path)
            #execCmd(cmd, False)
            cmd = 'ln -s %s %s' % (real_vol_path, vol_path)
            execCmd(cmd)
    """

def cleanTaskQueue():
    """
    Run on a host to kill all pending RTS jobs.
    """
    cmd = 'ps xa|grep rts_node'
    re = execCmd(cmd)
    lines = re[1].split('\n')
    for line in lines:
        if (line.find('grep rts_node')):
            continue
        cmd = 'kill -9 %s' % line.split()[0]
        execCmd(cmd)
    
def startServer(overwriteCfg = False, overwriteDisks = False, localFS = True):
    """
    Start the server, need to check
    1. config file (IpAddress, etc.)
    2. running as a cache mode?
    3. prepare the disks
    """
    cfgFile = createConfigFile(overwrite = overwriteCfg, localfs = localFS)
    if (not io_ex_ip.has_key(getNGASNodeName())): # for copy/proxy archive nodes, do not create local disks
        createDiskVolumes(overwrite = overwriteDisks, localfs = localFS)
    
    cleanTaskQueue()
    cmd = '%s %s -cfg %s -autoOnline -force -multipleSrvs' % (python_exec, ngas_cache_server,cfgFile)
    #2>&1>/dev/null
    execCmd(cmd)

def _sshStartServerThread(serverId, localFS):
    """
    serverId:    server identifier(int)
    """
    if (serverId > 10):
        nodename = 'f0%s' % str(serverId)
    else:
        nodename = 'f00%s' % str(serverId)
    
    cmd = 'ssh %s "/home/cwu/ngas_rt/bin/python /home/cwu/ngas_rt/src/ngamsStartup/ngamsFornaxMgr.py start 1 0 %d"' % (nodename, localFS)
    execCmd(cmd)

def sshStartServers(num = 24, localFS = 1):
    """
    Use SSH to start NGAS server
    """
    nowList = getAvailableArchiveServers()
    nowLen = len(nowList)
    if (nowLen >= num):
        print "There are already %d servers running." % nowLen
        return
    toAdd = num - nowLen
    totalList = range(1, 97)
    for ser in nowList:
        servId = int(ser.split(':')[0].split('.')[-1]) # 192.168.222.13:7777 --> 13
        totalList.remove(servId)
        
    serverList = random.sample(totalList, toAdd)
    for sid in serverList:
        print 'SSH Starting Server %s' % sid
        args = (sid,localFS)
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
    sqlUpdate = "update ngas_hosts set srv_state = 'NOT-RUNNING' where host_id = '%s'"
    conn = psycopg2.connect(database = 'ngas', user='ngas', 
                            password = 'bmdhcyRkYmE=\n'.decode('base64'), 
                            host = db_host)    
    try:   
        cur = conn.cursor()         
        cur.execute(sqlQuery)
        res = cur.fetchall()    
        
        if ('online' == status):
            needToRefresh = 0
            for ho in res:
                print 'Ping host %s ...' % ho[0]
                if (pingHost('http://%s/STATUS' % ho[0])):
                    print 'Host %s is not reachable' % ho[0]
                    needToRefresh = 1
                    cur_1 = None
                    try:
                        cur_1 = conn.cursor()
                        cur_1.execute(sqlUpdate % ho[0])
                        conn.commit()
                    finally:
                        if (cur_1):
                            del cur_1
            if (needToRefresh):
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
            host_name = host_id.split(':')[0]
            if (host_name in Non_Archive_Ngas):
                continue
            #if (host_id.split(':')[0] == io_ex_ip['io1'] or host_id.split(':')[0] == io_ex_ip['io2']):
            #    continue
            stopSingleServer(host_id.strip())
        except Exception, e:
            print 'Fail to shut down server %s, Exception: %s' % (host_id, str(e))
            continue

    
def getAvailableArchiveServers():
    """
    Return a List of servers (string) (host:port)
    
    This function is called by MPI-Rank 0
    """
    sqlQuery = "select host_id from ngas_hosts where srv_state = 'ONLINE'"
    conn = psycopg2.connect(database = 'ngas', user='ngas', 
                            password = 'bmdhcyRkYmE=\n'.decode('base64'), 
                            host = db_host)
    ret = []
    try:
        cur = conn.cursor()         
        cur.execute(sqlQuery)
        res = cur.fetchall()
        for ho in res:
            #print 'Ping host %s ...' % ho[0]
            if (ho[0].split(':')[0] in Non_Archive_Ngas):
                continue
            if (not pingHost('http://%s/STATUS' % ho[0])):
                ret.append(ho[0])
    finally:
        if (cur):
            del cur
        if (conn):            
            del conn   
    return ret  

def cleanServerLogs(serverList = None, localFS = 1):
    """
    Remove logs for all servers in the serverList
    Current implemented as SSH and remove
    
    serverList    [server1:port,server2:port,...](List)
    """
    listServer = None
    if (not serverList):
        listServer = getAvailableArchiveServers()
    else:
        listServer = serverList
    
    for server in listServer:
        ip = server.split(':')[0]
        if (localFS):
            logFile = '%s/log/LogFile.nglog' % ngas_runtime_root
        else:
            node = int(ip.split('.')[-1])
            node_name = 'f%03d' % (node) #39 --> 'f039', 3 --> 'f003'
            logFile = '%s/%s/log/LogFile.nglog' % (ngas_lustre_root, node_name)
            
        cmd = 'ssh %s rm %s' % (ip, logFile)
        print 'Cleaning log for host %s' % ip
        execCmd(cmd, failonerror = False)

def getServerLogs(tgtDir, comment, analyse = True, serverList = None, localfs = 1):
    """
    This will create a directory like this:
    
    tgtDir/comment/datetime/
    
    It will then copy LogFile.nglog to the above directory with a new log name like this
    
    f099.log
    
    tgtDir        globally accessible directory on the Lustre file system (string)
    serverList    [server1:port,server2:port,...](List)
    comment       e.g. lu-8s-CRC-24c-32m (lustre file system, 8 servers, 24 clients, 32MB/s per client)
                       lo-4s-NoCRC-12c-64m (local file system, 4 servers, 12 clients, 64MB/s per client)
    
    """
    if (not tgtDir or len(tgtDir) == 0):
        raise Exception('Invalid target directory')
    
    listServer = None
    if (not serverList):
        listServer = getAvailableArchiveServers()
    else:
        listServer = serverList
    
    allLogs = []
    
    dt = datetime.datetime.now()
    tgtLogDir = '%s/%s/%s' % (tgtDir, comment, dt.strftime('%Y%m%dT%H%M%S'))
    cmd = 'mkdir -p %s' % tgtLogDir
    execCmd(cmd)
    
    for server in listServer:
        ip = server.split(':')[0]
        node = int(ip.split('.')[-1])
        node_name = 'f%03d' % (node) #39 --> 'f039', 3 --> 'f003'
        if (localfs):
            srcLog = '%s:%s/log/LogFile.nglog' % (ip, ngas_runtime_root)
        else:
            srcLog = '%s/%s/log/LogFile.nglog' % (ngas_lustre_root, node_name)
        tgtLog = '%s/%s.log' % (tgtLogDir, node_name)
        cmd = 'scp %s %s' % (srcLog, tgtLog)
        print 'Copying log from %s to %s' % (srcLog, tgtLog)
        ret = execCmd(cmd, failonerror = False)
        if (ret[0] == 0):
            allLogs.append(tgtLog)
    
    if (analyse):
        print 'Analysing logs....'
        cmd = '%s/src/ngasUtils/src/analyseLog.sh' % ngas_src_root
        for tlog in allLogs:
            cmd += ' %s' % tlog
        cmd += ' > %s/perf_%s.txt' % (tgtLogDir, comment)
        execCmd(cmd)
    
    return tgtLogDir
        
    

if __name__ == '__main__':
    #createConfigFile(overwrite = True)
    #createDiskVolumes(overwrite = True)
    leng = len(sys.argv)
    if (leng == 1):
        startServer()
    else:
        action = sys.argv[1].lower()
        if ('start' == action):
            overwriteCfg = 1 # by default, always re-generate configuration file 
            overwriteDisks = 0
            localFS = 1
            if (leng > 2): # get the start parameter (1 - overwriteCfg, 0 - otherwise)
                overwriteCfg = int(sys.argv[2])
                if (leng > 3): # get the second start parameter (1 - overwrite volumes, 0 - otherwise)
                    overwriteDisks = int(sys.argv[3])    
                    if (leng > 4):
                        localFS = int(sys.argv[4])            
            startServer(overwriteCfg, overwriteDisks, localFS)
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
            if (leng == 3):
                sshStartServers(num = int(sys.argv[2]))
            elif (leng == 4):
                sshStartServers(num = int(sys.argv[2]), localFS = int(sys.argv[3]))
            else:
                sshStartServers()
        else:
            usage = 'usage: %s ngamsFornaxMgr.py [start[1|0 [1|0 [1|0]]] | stop [server1:port, server2:port] | monitor [online|offline|down]] ' % python_exec
            print '%s | ssh [num_server[1|0]]| cleanLog [server1:port,server2:port,] | getLog target comment [ analyse (1/0) [server1:port, server2:port]]' % usage
        