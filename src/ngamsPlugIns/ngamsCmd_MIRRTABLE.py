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
# "@(#) $Id: ngamsCmd_MIRRTABLE.py,v 1.6 2010/06/22 18:55:14 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jagonzal  2009/12/14  Created
#

"""
NGAS Command Plug-In, implementing a command to fill the mirroring_bookkeping_table

NOTES: By default only the last version found in the source cluster is synchronized
       to the target cluster. To synchronize all existing versions specify all_versions=1.
       In case of remote data-base mirroring it is necessary to enable data-base links
       to the source/target data-bases in the local data-base. If the target data-base
       is the local one is not neccesary to specify a target data-base link, (by default 
       the local data base is the target data base)

PARAMETERS:
	-source_cluster	[mandatory] source cluster name
	-target_cluster	[mandatory] target cluster name
	-source_dbl	[optional] source archive data base link (remote data base mirroring)
	-target_dbl	[optional] target archive data base link (remote data base mirroring)    
	-all_versions	[optional] (=1) mirror all missing versions of each file, default (=0)
	-archive_cmd	[optional] custom archive command, default (=MIRRARCHIVE)
	-retriev_cmd	[optional] custom retrieve command, default (=RETRIEVE)

EXAMPLES:
	- Local data base mirroring with custom ARCHIVE command
		http://ngas05.hq.eso.org:7778/MIRRTABLE?target_cluster=ngas05:7778&source_cluster=ngas02:7778&archive_cmd=QARCHIVE?
    
"""

from ngams import *
import ngamsLib, ngamsStatus, ngamsDb


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
    target_cluster = ""
    if (reqPropsObj.hasHttpPar("target_cluster")):
        target_cluster = reqPropsObj.getHttpPar("target_cluster")
    source_cluster = ""
    if (reqPropsObj.hasHttpPar("source_cluster")):
        source_cluster = reqPropsObj.getHttpPar("source_cluster")    
    source_dbl = ""
    if (reqPropsObj.hasHttpPar("source_dbl")):
        source_dbl = "@" + reqPropsObj.getHttpPar("source_dbl")
    target_dbl = ""
    if (reqPropsObj.hasHttpPar("target_dbl")):
        target_dbl = "@" + reqPropsObj.getHttpPar("target_dbl")
    all_versions = 0
    if (reqPropsObj.hasHttpPar("all_versions")):
        all_versions = int(reqPropsObj.getHttpPar("all_versions"))        
    archive_cmd = "MIRRARCHIVE"
    if (reqPropsObj.hasHttpPar("archive_cmd")):
        archive_cmd = reqPropsObj.getHttpPar("archive_cmd")
    retrieve_cmd = "RETRIEVE"
    if (reqPropsObj.hasHttpPar("retrieve_cmd")):
        retrieve_cmd = reqPropsObj.getHttpPar("retrieve_cmd")
    
    # Construct sub-query for source cluster
    source_query = generate_extended_ngas_files_query(source_dbl,source_cluster,all_versions)
    info(4, "SQL sub-query to get source cluster files-hosts information: %s" % source_query)
    
    # Construct sub-query for target cluster 
    target_query = generate_extended_ngas_files_query(target_dbl,target_cluster,all_versions)
    info(4, "SQL sub-query to get target cluster files-hosts information: %s" % target_query)

    # Construct sub-query for diff table
    diff_query = generate_diff_ngas_files_query(source_query,target_query)
    info(4, "SQL sub-query to get diff between source and target files: %s" % diff_query)

    # Populate book keeping table
    info(3,"Populating ngas_mirroring_bookkeeping_table, source_cluster=%s , target_cluster=%s, all_versions=%s, archive_cmd=%s, retrieve_cmd=%s" \
    % (source_cluster+source_dbl,target_cluster+target_dbl,str(all_versions),archive_cmd,retrieve_cmd))
    populate_mirroring_bookkeeping_table(diff_query,archive_cmd,retrieve_cmd,target_dbl,target_cluster,srvObj)

    # Get target cluster active nodes
    target_active_nodes = get_cluster_active_nodes(target_dbl,target_cluster,srvObj)

    # Get source cluster active nodes
    source_active_nodes = get_cluster_active_nodes(source_dbl,source_cluster,srvObj) 

    # Get non-empty source cluster nodes
    working_source_nodes = remove_empty_source_nodes(source_active_nodes,target_cluster,srvObj)   
   
    # Assign book keeping table entries
    info(3,"Updating entries in ngas_mirroring_bookkeeping_table to assing target nodes")
    assign_mirroring_bookkeeping_entries(target_active_nodes,working_source_nodes,target_dbl,target_cluster,srvObj)

    return

def generate_extended_ngas_files_query(db_link,
                                       cluster_name,
                                       all_versions):
    """
    Specify an alias table that extends ngas_files 
    table including host_id/domain/srv_port information

    INPUT:
    	db_link		string, Name for the data base link hosting the cluster
	cluster_name	string, Name of the cluster involved in the operation
	all_versions	int, Parameter to determine if all-versions mode is desired
    
    RETURNS:
        query   	string, Sub-Query to be aliased in a whole query
    """    
    
    # Query create table statement (common)
    query = "(select nf.file_id file_id, nf.file_version file_version, nf.disk_id disk_id, "
    query += "min(format) format, min(nf.file_size) file_size, min(nh.host_id) host_id, min(nh.domain) domain, "
    query += "substr(min(nh.host_id || ':' || nh.srv_port),-4) srv_port from "
    # Depending on all_versions parameter we select only last version or not
    if all_versions:
        query += "ngas_files" + db_link + " nf, ngas_disks" + db_link + " nd, ngas_hosts" + db_link + " nh where "
    else:
        query += "(select file_id, max(file_version) file_version from ngas_files" + db_link + " group by file_id) nflv, "
        query += "ngas_files" + db_link + " nf, ngas_disks" + db_link + " nd, ngas_hosts" + db_link + " nh where "
        query += "nflv.file_id = nf.file_id and nflv.file_version=nf.file_version and "
    # Query join conditions to reach host_id (common) and check cluster name
    query += "INSTR(nf.file_id,'NGAS-ngas')=0 and INSTR(nf.file_id,'logOutput')=0 and nf.ignore=0 and nf.file_status=0 and "
    query += "nf.disk_id=nd.disk_id and nd.host_id=nh.host_id and nh.cluster_name='" + cluster_name
    query += "' group by nf.file_id,nf.file_version,nf.disk_id)"

    # Lof info
    info(4, "SQL sub-query to generate extended ngas_files table: %s" % query)

    # Return query
    return query


def generate_diff_ngas_files_query(source_ext_ngas_files_query,
                                   target_ext_ngas_files_query):
    """
    Specify an alias table to handle the result 
    of the left join (diff) query between source
    and target extended ngas files sub-queries

    INPUT:
        source_ext_ngas_files_query	string, Sub-Query defining ngas_files information in source cluster
        target_ext_ngas_files_query	string, Sub-Query defining ngas_files information in target cluster
    
    RETURNS:
        query   			string, Sub-Query to be aliased in a whole query
    """
    
    # Query create table statement (common)
    query = "(select source.file_id,source.file_version,source.disk_id,source.format,source.file_size,source.host_id,source.domain,source.srv_port from "
    # Query left join condition
    query += source_ext_ngas_files_query + " source left join " + target_ext_ngas_files_query + " target on "
    query += "target.file_id = source.file_id and target.file_version = source.file_version "
    # Get no-matched records
    query += "where target.file_id is null)"

    # Return query
    return query    


def populate_mirroring_bookkeeping_table(diff_ngas_files_query,
                                         archive_command,
                                         retrieve_command,
                                         dbLink,
                                         cluster_name,
                                         srvObj):
    """
    Populate mirroring book keeping table with the
    diff between the source and target tables.
    
    INPUT:
        diff_ngas_files_query	string, Sub-Query defining the diff between the ngas_files
					in the source and target clusters 
        archive_command		string, Archive command to be used by the target cluster
        retrieve_command	string, Retrieve command to be used by the source cluster
    	dbLink			string, Name for the data base link hosting the target cluster
	cluster_name		string, Name of the target cluster
	srvObj			ngamsServer, Reference to NG/AMS server class object
    
    RETURNS:		Void.
    """
    
    ## First dump information from diff_ngas_files into book keeping table
    
    # Insert query statement
    query = "insert into ngas_mirroring_bookkeeping" + dbLink + " "
    # Fields to be filled
    query += "(file_id,file_version,disk_id,host_id,file_size,format,status,target_cluster,archive_command,source_host,retrieve_command) select "
    # file_id: Direct from diff_ngas_files table
    query += "diff.file_id, "
    # file_version: Direct from diff_ngas_files table
    query += "diff.file_version, "
    # disk_id: Direct from diff_ngas_files table
    query += "diff.disk_id, "
    # host_id: Direct from diff_ngas_files table
    query += "diff.host_id, "
    # file_size: Direct from diff_ngas_files table
    query += "diff.file_size, "
    # format: Direct from diff_ngas_files table
    query += "diff.format, "
    # status: Must be filled with 0 in case of no-ready entry
    query += "'LOCKED'," 
    # target_host: We can temporary use the name of the target cluster 
    #              rather than the target host to lock the table entry
    query += "'" + cluster_name + "', "
    # archive_command: Direct from diff_ngas_files table
    query += "'" + archive_command + "', "
    # source_host: We concatenate host_id (without port)
    query += "substr(diff.host_id,0,instr(diff.host_id || ':',':')-1) || '.' || "
    #              With domain name and port number
    query += "diff.domain || ':' || diff.srv_port, "
    # retrieve_command: Direct from diff_ngas_files table
    query += "'" + retrieve_command + "'" + " from " + diff_ngas_files_query + " diff"
    
    # Execute query 
    info(4, "Executing SQL query to generate new entries in ngas_mirroring_bookkeeping table: %s" % query)
    res = srvObj.getDb().query(query, maxRetries=1, retryWait=0)
    
    # Return void
    return


def get_cluster_active_nodes(db_link,
                             cluster_name,
                             srvObj):
    """
    Return active nodes in a NG/AMS cluster

    INPUT:
        dbLink		string, Name for the data base link of the cluster
        cluster_name	string, Name of the cluster to check
        srvObj 		ngamsServer, Reference to NG/AMS server class object
    
    RETURNS:
    	active_nodes	list[strings], List of the active nodes in the cluster
    """
    
    # Construct query
    query = "select substr(host_id,0,instr(host_id || ':',':')-1) || '.' || domain || ':' || srv_port "
    query += "from ngas_hosts" + db_link + " where "
    query += "cluster_name='" + cluster_name + "' and srv_state='ONLINE' and srv_archive=1"
    
    # Execute query
    info(4, "Executing SQL query to get active nodes in target cluster: %s" % query)
    active_nodes_object = srvObj.getDb().query(query, maxRetries=1, retryWait=0)
    
    # Re-dimension query results array
    active_nodes_object = active_nodes_object[0]
    active_nodes = []
    for node in active_nodes_object:
       active_nodes.append(node[0])
    
    # Log info
    info(3, "Active nodes found in cluster %s: %s" % (cluster_name+db_link,str(active_nodes)))

    # Return active nodes list
    return active_nodes


def remove_empty_source_nodes(source_active_nodes,
                              cluster_name,
                              srvObj):
    """
    Remove source active nodes that don't contain any file to mirror

    INPUT:
        source_active_nodes	string, List of active nodes in the source cluster
        cluster_name    	string, Name of the target_cluster
        srvObj          	ngamsServer, Reference to NG/AMS server class object
    
    RETURNS:
        source_nodes    list[strings], List of the active source nodes
    """

    # Construct query
    query = "select source_host from ngas_mirroring_bookkeeping where target_cluster='" + cluster_name + "' "
    query += " and status='LOCKED' group by source_host"

    # Execute query
    info(4, "Executing SQL query to get source nodes: %s" % query )
    source_nodes_object = srvObj.getDb().query(query, maxRetries=1, retryWait=0)

    # Re-dimension query results array
    source_nodes_object = source_nodes_object[0]
    source_nodes = []
    for node in source_nodes_object:
       source_nodes.append(node[0])

    # Compute the intersection of both lists
    working_nodes = []
    for node in source_active_nodes:
       if (source_nodes.count(node)>0): working_nodes.append(node)
    
    # Return working nodes list
    return working_nodes


def assign_mirroring_bookkeeping_entries(target_cluster_active_nodes,
                                         source_cluster_active_nodes,
                                         db_link,
                                         cluster_name,
                                         srvObj):
    """
    Update target_cluster field in the book keeping 
    table in order to assign entries to each node.

    The entries are assigned to target cluster nodes
    making sure that the file-size distribution is 
    balanced. To do it so the entries to be assigned
    are first file-size sorted and then the 1st entry
    is assigned to the first node, the 2nd entry to
    the 2nd node and so on until all the nodes have
    been assigned one entry and then it starts again.
    
    At the end all the nodes should have been assigned
    the same number of files, the same total load in Mb
    and the same distribution of files in terms of file size.

    INPUT:
        target_cluster_active_nodes	list[strings], List of the active nodes in the target cluster	
	target_cluster_source_nodes     list[strings], List of the active nodes in the source cluster
        db_link				string, Name for the data base link hosting the target cluster			
        cluster_name			string, Name of the target cluster
	srvObj				ngamsServer, Reference to NG/AMS server class object			
    
    RETURNS:			Void.
    """
    
    n_source_nodes = len(source_cluster_active_nodes)
    n_target_nodes = len(target_cluster_active_nodes)

    # Source nodes loop
    for source_node in source_cluster_active_nodes:
        i = 0
        # Target nodes loop
        for target_node in target_cluster_active_nodes:
            # Construct query, for every update the nodes left are n_nodes-i
            query ="update ngas_mirroring_bookkeeping set status='READY',target_host='" + target_node + "' where rowid in "
            query +="(select rowid from ngas_mirroring_bookkeeping where (rowid,0) in "
            query +="(select rowid,mod(rownum," + str(n_target_nodes-i) +") from "
            query +="(select * from ngas_mirroring_bookkeeping where target_cluster='" + cluster_name + "' "
            query +="and target_host is null order by file_size)))"
            # Perform query
            info(4, "SQL to assing entries from source node %s to target node %s: %s" % (source_node,target_node,query))
            srvObj.getDb().query(query, maxRetries=1, retryWait=0)
            i += 1
            # Log info
            query = "select count(*),sum(file_size/(1024*1024)) from ngas_mirroring_bookkeeping where status='READY' " 
            query += "and source_host='" + source_node + "' and target_host='" + target_node + "' "
            res =  (srvObj.getDb().query(query, maxRetries=1, retryWait=0))[0]
            n_files = str(res[0][0])
            total_load = str(res[0][1])
            info(3, "Mirroring tasks from source host %s assigned to target host %s: %s tasks, %s Mb" % (source_node,target_node,n_files,total_load))

    # Return void
    return

  
# EOF

