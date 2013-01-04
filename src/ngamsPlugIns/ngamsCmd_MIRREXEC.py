#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) European Southern Observatory, 2009
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
# "@(#) $Id: ngamsCmd_MIRREXEC.py,v 1.7 2010/06/22 18:55:03 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jagonzal  2009/12/15  Created
#

"""
NGAS Command Plug-In, implementing a command to actually perform mirroring tasks

NOTES:
	By default it performs pending mirroring tasks assigned to the NGAS server
	handling the command, but when mirror_cluster is specified (=1), default (=0),
	all pending mirroring tasks assigned to the local cluster are processed.

PARAMETERS:
	-mirror_cluster [optional]	(=0), process all pending mirroring tasks assigned to the NGAS server handling the command
					(=1), process all pending mirroring tasks assigned to the local cluster
					      (centralizing the process from the NGAS server handling the command)
					(=2), process all pending mirroring tasks assigned to the local cluster
					      (distributing the process to the active nodes in the local cluster)
	-order				(=0), Start mirroring sequence order with small files
					(=1), Start mirroring sequence order with big files

EXAMPLES:
	- Carry out pending mirroring tasks for this NGAS server using 4 threads per source node
	http://ngas05.hq.eso.org:7778/MIRREXEC?n_threads=4
	- Carry out all pending mirroring tasks assigned to the local cluster using 2 threads per source node
	http://ngas05.hq.eso.org:7778/MIRREXEC?mirror_cluster=1&n_threads=2
    
"""

from ngams import *
import ngamsLib, ngamsStatus, ngamsDbm
import urllib,httplib
from time import *
from threading import *

def handleCmd(srvObj,
              reqPropsObj,
              httpRef):
    """
    Handle Command MIRRTABLE to populate bookkeeping table in target cluster
    
    INPUT:
    	srvObj:         ngamsServer, Reference to NG/AMS server class object
    
    	reqPropsObj:	ngamsReqProps, Request Property object to keep track
                    	of actions done during the request handling 
        
   	httpRef:        ngamsHttpRequestHandler, Reference to the HTTP request
                    	handler object
        
    RETURNS:		Void.
    """
    T = TRACE()

    # Get command parameters.
    mirror_cluster = 0
    n_threads = 2
    order = 1 
    if (reqPropsObj.hasHttpPar("mirror_cluster")):
        mirror_cluster = int(reqPropsObj.getHttpPar("mirror_cluster")) 
    if (reqPropsObj.hasHttpPar("n_threads")):
       n_threads = int(reqPropsObj.getHttpPar("n_threads"))
    if (reqPropsObj.hasHttpPar("order")):
       order = int(reqPropsObj.getHttpPar("order"))

    # Centralized cluster mirroring
    if (mirror_cluster == 1):
        # Get cluster name
        local_cluster_name = get_cluster_name(srvObj)
        # Get active target nodes
        active_target_nodes = get_active_target_nodes(local_cluster_name,srvObj)
        # Get active source nodes
        active_source_nodes = get_active_source_nodes(srvObj,cluster_name=local_cluster_name)
    # Distributed cluster mirroring
    if (mirror_cluster == 2):
        # Get cluster name
        local_cluster_name = get_cluster_name(srvObj)
        # Get active target nodes
        active_target_nodes = get_active_target_nodes(local_cluster_name,srvObj)
        # Start mirroring
        distributed_mirroring(active_target_nodes,n_threads)
    else:
        # Get full qualified name of this server
        local_server_full_qualified_name = get_full_qualified_name(srvObj)
        # Format full qualified name as a list
        active_target_nodes = [local_server_full_qualified_name]
        # Get active source nodes
        active_source_nodes = get_active_source_nodes(srvObj,full_qualified_name=local_server_full_qualified_name)

    if (mirror_cluster != 2):
        # Start mirroring
        info(3,"Performing mirroring tasks from (%s) to (%s) using %s threads per source node and target node" \
        % (str(active_source_nodes),str(active_target_nodes),str(n_threads)))
        multithreading_mirroring(active_source_nodes,active_target_nodes,n_threads,order,srvObj)
        

    # Return Void 
    return


def get_cluster_name(srvObj):
    """
    Get cluster name corresponding to the processing NGAMS server
    
    INPUT:
        srvObj          ngamsServer, Reference to NG/AMS server class object
    
    RETURNS:
        cluster_name    string, name of the cluster corresponding to the input host_id
    """
    # Construct query
    query = "select cluster_name from ngas_hosts where host_id='" + getHostName() + "'"

    # Execute query
    info(4, "Executing SQL query to get local cluster name: %s" % query)
    cluster_name = srvObj.getDb().query(query, maxRetries=1, retryWait=0)
    cluster_name = str(cluster_name[0][0][0])
    info(3, "Local cluster name: %s" % cluster_name)

    # Return cluster_name
    return cluster_name


def get_full_qualified_name(srvObj):
    """
    Get full qualified server name for the input NGAS server object
    
    INPUT:
        srvObj  ngamsServer, Reference to NG/AMS server class object 
    
    RETURNS:
        fqdn    string, full qualified host name (host name + domain + port)
    """

    # Get hots_id, domain and port using ngamsLib functions
    host_id = getHostName()
    domain = ngamsLib.getDomain()
    port = str(srvObj.getCfg().getPortNo())
    # Concatenate all elements to construct full qualified name
    # Notice that host_id may contain port number
    fqdn = (host_id.rsplit(":"))[0] + "." + domain + ":" + port

    # Return full qualified server name
    return fqdn


def get_active_source_nodes(srvObj,cluster_name="none",full_qualified_name="none"):
    """
    Get active source nodes containing files to mirror
    for input cluster name or full qualified server name

    INPUT:
	cluster_name		string, Name of the cluster to process mirroring tasks
        full_qualified_name	string, Full qualified name of ngams server to process mirroring tasks
	srvObj          	ngamsServer, Reference to NG/AMS server class object
    
    RETURNS:
	active_source_nodes	list[string], List of active source nodes with files to mirror
    """

    # Construct query
    if (full_qualified_name == "none"):
       query = "select source_host from ngas_mirroring_bookkeeping where status='READY' and target_cluster='" + cluster_name +"' group by source_host"
       info(4, "Executing SQL query to get active nodes with files to mirror for cluster %s: %s" % (cluster_name,query))
    else:
       query = "select source_host from ngas_mirroring_bookkeeping where status='READY' and target_host='" + full_qualified_name +"' group by source_host"
       info(4, "Executing SQL query to get active nodes with files to mirror for local server %s: %s" % (full_qualified_name,query))

    # Execute query
    source_nodes = srvObj.getDb().query(query, maxRetries=1, retryWait=0)

    # Re-dimension query results array and check status
    source_nodes = source_nodes[0]
    active_source_nodes = []
    for node in source_nodes:
       if ngams_server_status(node[0]): active_source_nodes.append(node[0]) 
    
    # Return result
    return active_source_nodes
    
    
def get_active_target_nodes(cluster_name,srvObj):
    """
    Get active target nodes ready to process mirroring tasks

    INPUT:
        cluster_name		string, Name of the cluster to process mirroring tasks
	srvObj          	ngamsServer, Reference to NG/AMS server class object
    
    RETURNS:
        active_target_nodes	list[string], List of active target nodes with files to mirror
    """

    # Construct query
    query = "select target_host from ngas_mirroring_bookkeeping where status='READY' and target_cluster='" + cluster_name + "' group by target_host"
    info(4, "Executing SQL query to get active nodes with files to mirror for cluster %s: %s" % (cluster_name,query))

    # Execute query
    target_nodes = srvObj.getDb().query(query, maxRetries=1, retryWait=0)

    # Re-dimension query results array and check status
    target_nodes = target_nodes[0]
    active_target_nodes = []
    for node in target_nodes:
       if ngams_server_status(node[0]): active_target_nodes.append(node[0]) 

    # Log info
    info(3, "Active nodes found in cluster %s: %s" % (cluster_name,str(active_target_nodes)))

    # Return result
    return active_target_nodes


def ngams_server_status(ngams_server):
    """
    Check NGAMS server status

    INPUT:
        ngams_server    string, Full qualified name of ngams_server
    
    RETURNS:
	status 		bool, True if active False if unactive
    """

    server_conn = httplib.HTTPConnection(ngams_server)
    server_conn.request("GET","/STATUS?")
    status = server_conn.getresponse().read().find("ONLINE") >= 0
    return status

def cutoff_file_size(target_node,srvObj):
    """
    INPUT:
        target_node     string, Node target of the files to be mirrored 
        srvObj          ngamsServer, Reference to NG/AMS server class object
    
    RETURNS:
        cutoff_fs	float, Cutoff file_size to determin small-files threshold
    """
   
    # Get file_size list
    query = "select file_size/(1024.0*1024.0) from ngas_mirroring_bookkeeping where target_host='" + target_node + "' order by file_size"
    info(4, "Executing SQL query to get sorted list of files to be mirrored to target_node=%s : %s" % (target_node,query))
    file_size_list = srvObj.getDb().query(query, maxRetries=1, retryWait=0)
    file_size_list = file_size_list[0]

    # Get total load in mb
    query = "select sum(file_size/(1024.0*1024.0)) from ngas_mirroring_bookkeeping where target_host='" + target_node + "'"
    info(4, "Executing SQL query to get total load to be mirrored to target_node=%s : %s" % (target_node,query))
    total_load_mb = srvObj.getDb().query(query, maxRetries=1, retryWait=0)
    total_load_mb = float(total_load_mb[0][0][0])

    # Find cutoff file_size
    cumsum = 0
    for file_size in file_size_list:
        cutoff_fs = float(file_size[0])
        cumsum += cutoff_fs
        if (cumsum>=0.25*total_load_mb):
            break

    # Log info
    info(3, "Target node %s cut-off file_size: %s" % (target_node,str(cutoff_fs)))

    # Return cutoff file_size
    return cutoff_fs


def get_list_mirroring_tasks(source_node,target_node,srvObj):
    """
    Check pending mirroring tasks in the ngas_bookkeeping
    table assigned to the input host name

    INPUT:
        source_node     string, Node source of the files to be mirrored
        target_node     string, Node target of the files to be mirrored 
        srvObj          ngamsServer, Reference to NG/AMS server class object
    
    RETURNS:
        mirroring_tasks list[string], List of files to be mirrored from the source_node to the target_node
    """

    # Construct query
    query = "select rowid,'/' || archive_command || '?mime_type=' || format || "
    query += "'&' || 'filename=http://' || source_host || '/' || retrieve_command || '?disk_id=' || disk_id || '&' || 'host_id=' || host_id || "
    query += "'&' || 'quick_location=1' || '&' || 'file_version=' || file_version || '&' || 'file_id=' || file_id "
    query += "from ngas_mirroring_bookkeeping where source_host='" + source_node + "' and target_host='" + target_node
    query += "' and status='READY' order by file_size"

    # Execute query
    info(3, "Executing SQL query to get list of mirroring tasks from (%s) to (%s): %s" % (source_node,target_node,query))
    mirroring_tasks = srvObj.getDb().query(query, maxRetries=1, retryWait=0)
    mirroring_tasks = mirroring_tasks[0]

    # Return mirroring tasks list
    return mirroring_tasks


def get_sublist_mirroring_tasks(list,n_threads,ith_thread,reverse_flag):
    """
    Generate a sub-list containing the ith-element of
    every n elements. Reverse the list is specified.

    INPUT:
        list            list, Original list
        n_threads	int, Number of threads
        ith_thread	int, pos-th to be selected
        reverse_flag	bool, True if the list has to be reversed
    
    RETURNS:
        filtered_list   list, Filtered list
    """

    # Filter list loop
    i=0
    filtered_list = []
    for element in list:
        if ((i % n_threads) == ith_thread): filtered_list.append(list[i])
        i += 1

    # Reverse if specified
    if (reverse_flag): filtered_list.reverse()

    # Return filter list
    return filtered_list


def process_mirroring_tasks(mirroring_tasks,source_node,target_node,ith_thread,srvObj):
    """
    Process mirroring tasks described in the input mirroring_tasks list

    INPUT:
        mirroring_tasks	list[strings], List of the mirroring tasks assigned to the input server
        source_node	string, Full qualified name of the source node
	target_node	string, Full qualified name of the target node
	ith_thread	int, Thread number 
	srvObj          ngamsServer, Reference to NG/AMS server class object
    
    RETURNS:		Void
    """
    
    # Create target server connection
    target_node_conn = httplib.HTTPConnection(target_node)
    
    # Loop mirroring_tasks list
    n_tasks = str(len(mirroring_tasks))
    i = 1
    for item in mirroring_tasks:
        # Get rowid of the entry in the ngas_mirroring_bookkeeping table and http command to be sent
        rowid = str(item[0])
        http_cmd = str(item[1])
        # URL-encode RETRIEVE command (query to the source cluster)
        http_cmd_urlcoded = (http_cmd.split("http://"))[0] + "http://" + urllib.pathname2url((http_cmd.split("http://"))[1])
        full_http_cmd = "http://" + target_node + http_cmd_urlcoded
        info(3,"Processing %s/%s mirroring task [thread %s] http command: %s" % (str(i),n_tasks,str(ith_thread),full_http_cmd))
        # Start clock
        start = time()
        # Send request to target node
        target_node_conn.request("GET",http_cmd_urlcoded)
        # Get response from target node
        response = target_node_conn.getresponse()
        # Get status
        status = "SUCCESS"
        if (response.read().find("FAILURE") >= 0): status="FAILURE"
        # Get time elapsed
        elapsed_time = (time() - start)
        # Construct query to update ingestion date, ingestion time and status
        query = "update ngas_mirroring_bookkeeping set status='" + status + "',"
        query += "ingestion_date='" + strftime("%Y-%m-%dT%H:%M:%S:000", gmtime()) + "',"
        query += "ingestion_time='" + str(elapsed_time) + "' "
        query += "where rowid='" + rowid + "'"
        # Execute query
        info(4, "Executing SQL query to update status of the mirroring task (%s): %s" % (full_http_cmd,query))
        srvObj.getDb().query(query, maxRetries=1, retryWait=0)
        # Log message for mirroring task processed
        info(3, "Mirroring task %s/%s (%s to %s [thread %s]) processed in %ss (%s)" % (str(i),n_tasks,source_node,target_node,str(ith_thread),str(elapsed_time),status))
        i += 1
        
    # Return Void
    return


def multithreading_mirroring(source_nodes_list,target_nodes_list,n_threads,total_sequence_order,srvObj):
    """
    Creates n threads per source node and target node to process the corresponding mirroring tasks
    Each thread starts from big files or small files alternating 

    INPUT:
	source_nodes_list	list[string], List of active source nodes in the source cluster
	target_nodes_list	list[string], List of the active target nodes in the target cluster
	n_threads		int, Number of threads per source-target connection
        total_sequence_order	int, Mirroring sequence order
        srvObj          	ngamsServer, Reference to NG/AMS server class object
    
    RETURNS:			Void
    """

    threads_range = range(n_threads)
    threads_list = []

    n_source_nodes = len(source_nodes_list)
    delta_percent = 1.0 / n_source_nodes

    # Check sequence order
    if (total_sequence_order==0):
        outer_sequence_reverse_flag = True
    else:
        outer_sequence_reverse_flag = False

    for target_node in target_nodes_list:
        # Get cut-off file_size
        cutoff_fs = cutoff_file_size(target_node,srvObj)
        source_i = 0
        # Flip outer sequence order so that we have a mixture
        # of big files/small files arriving to the target node
        if (outer_sequence_reverse_flag):
            outer_sequence_reverse_flag = False
        else:
            outer_sequence_reverse_flag = True
        # Propagete outer sequence order to inner sequence order
        inner_sequence_reverse_flag = outer_sequence_reverse_flag
        # Source nodes loop
        for source_node in source_nodes_list:
            # Get cut-off file_size for this source node
            cutoff_fs_source_node = (delta_percent*source_i)*cutoff_fs 
            # Get complete list of mirroring tasks
            mirroring_tasks_list = get_list_mirroring_tasks(source_node,target_node,srvObj)
            # n-threads loop
            for ith_thread in threads_range:
                # Get list of mirroring tasks for this node-thread
                ith_thread_mirroring_tasks_list = get_sublist_mirroring_tasks(mirroring_tasks_list,n_threads,ith_thread,inner_sequence_reverse_flag)
                # Initialize mirror_worker thread object
                mirror_worker_obj =  mirror_worker(ith_thread_mirroring_tasks_list,source_node,target_node,ith_thread+1,srvObj)
                # Add mirror_worker thread object to the list of threads
                threads_list.append(mirror_worker_obj)
                # Start mirror_worker thread object
                mirror_worker_obj.start()
                # Flip inner sequence order so that we have a mixture
                # of big files/small files arriving to the target node
                if (inner_sequence_reverse_flag):
                     inner_sequence_reverse_flag = False
                else:
                     inner_sequence_reverse_flag = True
                # Increase source_i counter (for cut-off file_size)
                source_i += 1
    
            
    # Join mirror_node threads
    for ith_thread in threads_list:
        ith_thread.join()

    # Return Void
    return


class mirror_worker(Thread):
    def __init__ (self,mirroring_tasks,source_node,target_node,ith_thread,srvObj):
        Thread.__init__(self)
        self.mirroring_tasks = mirroring_tasks
        self.source_node = source_node
        self.target_node = target_node
        self.ith_thread = ith_thread
        self.srvObj = srvObj
    def run(self):
        process_mirroring_tasks(self.mirroring_tasks,self.source_node,self.target_node,self.ith_thread,self.srvObj)

def sort_target_nodes(target_nodes_list):
    """
    Sort target_nodes_list to balance priority
    
    INPUT:
        target_nodes_list       list[string], List of active target nodes in the target cluster
    
    RETURNS:                    list[string], Sorted active target nodes list
    """

    # Get machines list
    machines_list = []
    for target_node in target_nodes_list:
        machine = target_node.split(".")[0]
        if (machines_list.count(machine)==0): machines_list.append(machine)

    sorted_target_nodes_list = []
    
    # Add lower port (machines sort-ascending)
    machines_list.sort()
    target_nodes_list.sort()
    for machine in machines_list:
        for target_node in target_nodes_list:
            node_machine = target_node.split(".")[0]
            if (node_machine==machine):
                sorted_target_nodes_list.append(target_node)
                break
    
    # Add higher port (machines sort-descending)
    machines_list.reverse()
    target_nodes_list.reverse()
    for machine in machines_list:
        for target_node in target_nodes_list:
            node_machine = target_node.split(".")[0]
            if ((node_machine==machine) and (sorted_target_nodes_list.count(target_node)==0)):
                sorted_target_nodes_list.append(target_node)
                break

    # Log info
    info(3, "Target nodes order to send MIRREXEC command: %s" % (str(sorted_target_nodes_list)))    # Add higher port (machines sort-descending)
    
    # Return sorted target nodes list
    return sorted_target_nodes_list


def distributed_mirroring(target_nodes_list,n_threads):
    """
    Send MIRREXEC command to each nodes in the target nodes
    list in order to have a distributed mirroring process
    
    INPUT:
        target_nodes_list	list[string], List of active target nodes in the target cluster
	n_threads		int, Number of threads per source-target connection
    
    RETURNS:            	Void
    """

    # Get sorted_target_nodes_list
    sorted_target_nodes_list = sort_target_nodes(target_nodes_list)
    
    
    threads_list = []
    sequence_order = 1
    machine_conf = []
    for target_node in sorted_target_nodes_list:
	# Get host machine sequence order info
        machine = target_node.split(".")[0]
        conf_1 = [machine,sequence_order]
        conf_2 = [machine,1*(sequence_order==0)]
        if (machine_conf.count(conf_1)>machine_conf.count(conf_2)):
            sequence_order = 1*(sequence_order==0)
            conf = conf_2
        else:
            conf = conf_1
        machine_conf.append(conf)
        # Initialize mirrexec_command_sender thread object
        mirrexec_command_sender_obj = mirrexec_command_sender(target_node,n_threads,sequence_order)
        # Add mirrexec_command_sender thread object to the list of threads
        threads_list.append(mirrexec_command_sender_obj)
        # Start mirrexec_command_sender thread object
        mirrexec_command_sender_obj.start()
        # Flip sequence order so that we have a mixture of big files/small files
        if (sequence_order == 0):
            sequence_order = 1
        else:
            sequence_order = 0 

    # Join mirror_node threads
    for ith_thread in threads_list:
        ith_thread.join()

    # Return Void
    return
 
def send_mirrexec_command(target_node,n_threads,sequence_order):
    """
    Send MIRREXEC command to the input source_node
        
    INPUT:
        source_node	string, Target node to send MIRREXEC
        n_threads       int, Number of threads per source-target connection
        sequence_order	int, Mirroring sequence order
    
    RETURNS:		Void
    """

    # Print log info
    info(3, "MIRREXEC command sent to %s with (n_threads=%s,sequence_order=%s)" % (target_node,str(n_threads),str(sequence_order)))

    # Create target server connection
    target_node_conn = httplib.HTTPConnection(target_node)
    # Start clock
    start = time()
    # Send request to target node
    target_node_conn.request("GET","MIRREXEC?n_threads="+str(n_threads)+"&order="+str(sequence_order))
    # Get response from target node
    response = target_node_conn.getresponse()
    # Get status
    status = "SUCCESS"
    if (response.read().find("FAILURE") >= 0): status="FAILURE"
    # Get time elapsed
    elapsed_time = (time() - start)

    # Print log info
    info(3, "MIRREXEC command sent to %s with (n_threads=%s,sequence_order=%s) was handled  with status %s in %ss" % (target_node,str(n_threads),str(sequence_order),status,str(elapsed_time)))

    # Return Void
    return


class mirrexec_command_sender(Thread):
    def __init__ (self,source_node,n_threads,sequence_order):
        Thread.__init__(self)
        self.source_node = source_node
        self.n_threads = n_threads
        self.sequence_order = sequence_order
    def run(self):
        send_mirrexec_command(self.source_node,self.n_threads,self.sequence_order)
       

# EOF
