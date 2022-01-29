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
# *****************************************************************************
#
# "@(#) $Id: ngamsCmd_MIRREXEC.py,v 1.7 2010/06/22 18:55:03 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jagonzal  2009/12/15  Created
#
"""
NGAS Command Plug-In, implementing a command to actually perform mirroring tasks

NOTES
-----
By default it performs pending mirroring tasks assigned to the NGAS server handling the command, but when
mirror_cluster is specified (=1), default (=0), all pending mirroring tasks assigned to the local cluster are
processed.

PARAMETERS
----------
* mirror_cluster [optional] (=0), process all pending mirroring tasks assigned to the NGAS server handling the command
                            (=1), process all pending mirroring tasks assigned to the local cluster
                                (centralizing the process from the NGAS server handling the command)
                            (=2), process all pending mirroring tasks assigned to the local cluster
                                (distributing the process to the active nodes in the local cluster)
* order                     (=0), Start mirroring sequence order with small files
                            (=1), Start mirroring sequence order with big files

EXAMPLES
--------
* Carry out pending mirroring tasks for this NGAS server using 4 threads per source node
    http://ngas05.hq.eso.org:7778/MIRREXEC?n_threads=4
* Carry out all pending mirroring tasks assigned to the local cluster using 2 threads per source node
    http://ngas05.hq.eso.org:7778/MIRREXEC?mirror_cluster=1&n_threads=2
"""

import contextlib
import collections
import logging
import os
import time
import threading

from ngamsLib import ngamsLib, ngamsHttpUtils, ngamsReqProps, ngamsDiskInfo, ngamsDbCore, utils
from ngamsLib.ngamsCore import genUniqueId, toiso8601,\
    FMT_DATETIME_NOMSEC, NGAMS_FAILURE, NGAMS_HTTP_SUCCESS, NGAMS_STAGING_DIR
from . import ngamsFailedDownloadException
from . import ngamsCmd_MIRRARCHIVE

logger = logging.getLogger(__name__)


def handleCmd(ngams_server, request_properties, http_reference=None):
    """
    Handle Command MIRRTABLE to populate bookkeeping table in target cluster
    :param ngams_server: ngamsServer, Reference to NG/AMS server class object
    :param request_properties: ngamsReqProps, Request Property object to keep track of actions done during the
    request handling
    :param http_reference: HTTP reference
    """
    # Get command parameters
    mirror_cluster = 0
    n_threads = 2
    rx_timeout = None
    if request_properties.hasHttpPar("mirror_cluster"):
        mirror_cluster = int(request_properties.getHttpPar("mirror_cluster"))
    if request_properties.hasHttpPar("n_threads"):
        n_threads = int(request_properties.getHttpPar("n_threads"))
    if request_properties.hasHttpPar("rx_timeout"):
        rx_timeout = int(request_properties.getHttpPar("rx_timeout"))
    current_iteration = int(request_properties.getHttpPar("iteration"))

    # Distributed cluster mirroring
    if mirror_cluster:
        local_cluster_name = get_cluster_name(ngams_server)
        active_target_nodes = get_active_target_nodes(local_cluster_name, current_iteration, ngams_server)
        # Start mirroring
        distributed_mirroring(active_target_nodes, n_threads, rx_timeout, current_iteration)
    else:
        local_server_full_qualified_name = get_fully_qualified_name(ngams_server)
        # Format full qualified name as a list
        active_target_nodes = [local_server_full_qualified_name]
        active_source_nodes = get_active_source_nodes(ngams_server, current_iteration,
                                                      fully_qualified_name=local_server_full_qualified_name)
        # Start mirroring process driven by this host
        logger.info("Performing mirroring tasks from (%s) to (%s) using %d threads per source node and target node",
                    str(active_source_nodes), str(active_target_nodes), n_threads)
        try:
            # Set mirroring running flag to avoid data check thread and janitor thread
            ngams_server.mirroring_running = True
            multithreading_mirroring(active_source_nodes, n_threads, current_iteration, ngams_server)
        finally:
            # Set mirroring running flag to trigger data check thread and janitor thread
            ngams_server.mirroring_running = False
    return


def get_cluster_name(ngams_server):
    """
    Get cluster name corresponding to the processing NGAMS server
    :param ngams_server: ngamsServer, Reference to NG/AMS server class object
    :return: string, name of the cluster corresponding to the input host_id
    """
    sql = "select cluster_name from ngas_hosts where host_id = {0}"
    logger.debug("Executing SQL query to get local cluster name: %s", sql)
    cluster_name = ngams_server.getDb().query2(sql, args=(ngams_server.getHostId(),))
    cluster_name = str(cluster_name[0][0])
    logger.debug("Local cluster name: %s", cluster_name)
    return cluster_name


def get_fully_qualified_name(ngams_server):
    """
    Get fully qualified server name for the input NGAS server object
    :param ngams_server: ngamsServer, Reference to NG/AMS server class object
    :return: string, full qualified host name (host name + domain + port)
    """

    # Get hots_id, domain and port using ngamsLib functions
    host_id = ngams_server.getHostId()
    simple_hostname = host_id.split('.')[0]
    domain = ngamsLib.getDomain()
    port = str(ngams_server.getCfg().getPortNo())
    # Concatenate all elements to construct full qualified name
    # Notice that host_id may contain port number
    fqdn = (simple_hostname.rsplit(":"))[0] + "." + domain + ":" + port
    return fqdn


def get_active_source_nodes(ngams_server, current_iteration, cluster_name="none", fully_qualified_name=None):
    """
    Get active source nodes containing files to mirror for input cluster name or full qualified server name
    :param ngams_server: ngamsServer, Reference to NG/AMS server class object
    :param current_iteration: Current iteration
    :param cluster_name: string, Name of the cluster to process mirroring tasks
    :param fully_qualified_name: string, Full qualified name of ngams server to process mirroring tasks
    :return: list[string], List of active source nodes with files to mirror
    """
    # Construct query
    if fully_qualified_name is None:
        sql = "select source_host from ngas_mirroring_bookkeeping " \
              "where status='READY' and target_cluster = {0} and iteration = {1} group by source_host"
        logger.debug("Executing SQL query to get active nodes with files to mirror for cluster %s: %s",
                     cluster_name, sql)
        args = (cluster_name, current_iteration)
    else:
        sql = "select source_host from ngas_mirroring_bookkeeping " \
              "where status='READY' and target_host = {0} and iteration = {1} group by source_host"
        args = (fully_qualified_name, current_iteration)
        logger.debug("Executing SQL query to get active nodes with files to mirror for local server %s: %s",
                     fully_qualified_name, sql)

    # Execute query
    source_nodes = ngams_server.getDb().query2(sql, args=args)

    # Re-dimension query results array and check status
    active_source_nodes = []
    for node in source_nodes:
        if ngams_server_status(node[0]):
            active_source_nodes.append(node[0])
        else:
            logger.debug("Source node %s is not ONLINE. Not considering for this mirroring iteration", node[0])

    return active_source_nodes


def get_active_target_nodes(cluster_name, current_iteration, ngams_server):
    """
    Get active target nodes ready to process mirroring tasks
    :param cluster_name: string, Name of the cluster to process mirroring tasks
    :param current_iteration: Current iteration
    :param ngams_server: ngamsServer, Reference to NG/AMS server class object
    :return: list[string], List of active target nodes with files to mirror
    """
    sql = "select target_host from ngas_mirroring_bookkeeping " \
          "where status = 'READY' and target_cluster = {0} and iteration = {1} group by target_host"
    logger.debug("Executing SQL query to get active nodes with files to mirror for cluster %s: %s", cluster_name, sql)

    target_nodes = ngams_server.getDb().query2(sql, args=(cluster_name, current_iteration))

    # Re-dimension query results array and check status
    active_target_nodes = []
    for node in target_nodes:
        if ngams_server_status(node[0]):
            active_target_nodes.append(node[0])
        else:
            remove_tasks(node[0], cluster_name, ngams_server)

    logger.debug("Active nodes found in cluster %s: %s", cluster_name, str(active_target_nodes))
    return active_target_nodes


def ngams_server_status(ngams_server):
    """
    Check NGAMS server status
    :param ngams_server: string, Full qualified name of ngams_server
    :return: bool, True if active False if inactive
    """
    try:
        host, port = ngams_server.split(":")
        response = ngamsHttpUtils.httpGet(host, int(port), 'STATUS')
        with contextlib.closing(response):
            return b'ONLINE' in response.read()
    except Exception:
        logger.info("Problem trying to reach %s, setting status to OFFLINE (0)", ngams_server)
        return False


def remove_tasks(target_host, cluster_name, ngams_server):
    logger.warning("Server %s has gone OFFLINE. Marking all of it's pending fetches as ABORTED so that other nodes " 
                   "can take over.", target_host)
    sql = "update ngas_mirroring_bookkeeping " \
          "set status = 'ABORTED' " \
          "where status in ('READY', 'FETCHING', 'TORESUME') and target_host = {0} and target_cluster = {1}"
    ngams_server.getDb().query2(sql, args=(target_host, cluster_name))


def get_list_mirroring_tasks(current_iteration, source_node, target_node, ngams_server):
    """
    Check pending mirroring tasks in the ngas_bookkeeping table assigned to the input host name
    :param current_iteration: Current iteration
    :param source_node: string, Node source of the files to be mirrored
    :param target_node: string, Node target of the files to be mirrored
    :param ngams_server: ngamsServer, Reference to NG/AMS server class object
    :return: list[string], List of files to be mirrored from the source_node to the target_node
    """
    # Pull out the fields from the mirroring bookkeeping table that we may need in order to fetch files.
    # The individual fields are passed around and eventually formed into a command in process_mirroring_tasks
    sql = "select file_size, staging_file, rowid, format, checksum, " \
          "checksum_plugin, source_host, disk_id, host_id, file_version, file_id " \
          "from ngas_mirroring_bookkeeping " \
          "where source_host = {0} and target_host = {1} and iteration = {2} and status = 'READY' order by file_size"

    # Execute query to get mirroring tasks list
    logger.debug("Executing SQL query to get list of mirroring tasks from (%s) to (%s): %s", source_node,
                 target_node, sql)
    args = (source_node, target_node, current_iteration)
    return ngams_server.getDb().query2(sql, args=args)


def get_num_download_threads_in_use(current_iteration, target_node, ngams_server):
    """
    Count the number of files which are currently being downloaded. This is not completely accurate of course - we can
    issue this select while we are between downloading files. This is especially likely if the size of the files are
    small.
    However it's an easy way to roughly gauge the number of files being downloaded, without major changes, and much
    better than the previous algorithm.
    :param current_iteration:
    :param target_node: string, Node target of the files to be mirrored
    :param ngams_server: ngamsServer, Reference to NG/AMS server class object
    :return: string, Backlog size
    """
    sql = "select count(file_id) from ngas_mirroring_bookkeeping " \
          "where target_host = {0}  and iteration < {1} and status = 'FETCHING'"

    # Execute query to get mirroring tasks list
    logger.debug("Executing SQL query to get number of download threads currently mirroring files to node %s",
                 target_node)
    backlog_size = ngams_server.getDb().query2(sql, args=(target_node, current_iteration))[0][0]
    logger.info('Backlog size: %s', backlog_size)
    # Return mirroring tasks list
    return backlog_size


def revert_mirroring_bookkeeping_entries(current_iteration, target_node, ngams_server):
    sql = "delete from ngas_mirroring_bookkeeping " \
          "where target_host = {0} and iteration = {1} and status = 'READY'"
    ngams_server.getDb().query2(sql, args=(target_node, current_iteration))


def reorder_list_of_mirroring_tasks_for_target(current_iteration, source_nodes_list, target_node,
                                               mirroring_tasks_list, ngams_server):
    # Construct query to get average file size and number of tasks
    args = [target_node]
    sql = "select count(*), sum(file_size/(1024*1024)) / count(*) " \
          "from ngas_mirroring_bookkeeping " \
          "where target_host = {} and (1 = 0"
    if source_nodes_list:
        for node in source_nodes_list:
            sql += " or source_host = {}"
            args.append(node)
    sql += ") and status='READY' and iteration = {}"
    args.append(current_iteration)

    # Execute query to get average file size and number of tasks
    logger.debug("Executing SQL query to get average file size and number of mirroring tasks to (%s): %s",
                 target_node, sql)
    result = ngams_server.getDb().query2(sql, args=args)
    total_tasks = int(result[0][0])
    target_average_file_size = 0
    # If we divide by zero in the oracle query then we get an empty result back for average file size
    if total_tasks > 0:
        target_average_file_size = float(result[0][1])
    logger.debug("Average file size and number of mirroring tasks to target node (%s): total tasks = %d, " 
                 "average[MB] = %f", target_node, total_tasks, target_average_file_size)

    num_sources = len(source_nodes_list)
    ascending_index = list(range(num_sources))
    descending_index = list(range(num_sources))
    source_total_tasks = list(range(num_sources))
    source_processed_tasks = list(range(num_sources))
    reordered_mirroring_tasks_list = collections.deque()
    next_source_ascending_index = 0
    next_source_descending_index = 0

    for ith_source in range(num_sources):
        ascending_index[ith_source] = 0
        descending_index[ith_source] = -1
        source_total_tasks[ith_source] = len(mirroring_tasks_list[ith_source])
        source_processed_tasks[ith_source] = 0

    average_file_size = 0
    processed_tasks = 0
    processed_size = 0

    logger.debug("Start reordering mirroring tasks for %s", target_node)

    # Process files one after the other
    iteration = 0
    # This a complex algorithm and I've seen it hang on occasion. This is to prevent an infinite loop occurring.
    # There are too many variables for me to reason about this and too little time.
    # We will deploy and watch out for iterations where not all files are mirrored.
    # The 2M iterations count has the side effect that only 1M files will be processed in a single iteration
    emergency_breakpoint = 2000000
    while processed_tasks < total_tasks:
        logger.debug("Outer loop iteration: %d", iteration)
        iteration += 1
        if iteration > emergency_breakpoint:
            logger.critical("Re-ordering is not exiting - performing an emergency exit")
            break

        # Add big file if the current average below the total average
        logger.debug("Average: %f/%f", average_file_size, target_average_file_size)
        small_file = average_file_size < target_average_file_size
        if small_file:
            next_source = next_source_descending_index
            next_source_descending_index = (next_source_descending_index + 1) % num_sources
            task_index = descending_index[next_source]
        else:
            next_source = next_source_ascending_index
            next_source_ascending_index = (next_source_ascending_index + 1) % num_sources
            task_index = ascending_index[next_source]

        logger.debug("    next_source: %d", next_source)
        processed = source_processed_tasks[next_source]
        total = source_total_tasks[next_source]
        if processed >= total:
            logger.info("All tasks for %s have been assigned already", str(source_nodes_list[next_source]))
        else:
            completion = calculate_percentage_done(processed, total)
            total_completion = calculate_percentage_done(processed_tasks + 1, total_tasks)
            logger.debug("If completion: %f > %f", completion, total_completion)
            # This will fail if there is a single source, or group of sources, where the completion is always
            # greater than the total projected completion
            if completion > total_completion:
                logger.debug("    Skipping next file from source %d -> Completion :%f%% Total Completion: %f%%",
                             next_source, completion, total_completion)
            else:
                mirroring_task = mirroring_tasks_list[next_source][task_index]
                logger.debug("    mirroring_task: %s", str(mirroring_task))
                reordered_mirroring_tasks_list.append(mirroring_task)

                file_size = float(mirroring_task[0])
                processed_size += file_size

                source_processed_tasks[next_source] += 1
                processed_tasks += 1
                average_file_size = processed_size / processed_tasks
                logger.debug("    Appending file: tasks=%d, source=%d, index=%d, processed=%f, total=%f, " 
                             "size=%f MB, average=%f MB", processed_tasks, next_source, task_index, processed, total,
                             file_size, average_file_size)
                if small_file:
                    descending_index[next_source] -= 1
                else:
                    ascending_index[next_source] += 1

        logger.debug("    1 total:     %s", str(source_total_tasks))
        logger.debug("    1 processed: %s", str(source_processed_tasks))
        logger.debug("    1 desc:      %s", str(descending_index))
        logger.debug("    1 asc:       %s", str(ascending_index))

    logger.info("Done reordering mirroring tasks for %s", target_node)
    return reordered_mirroring_tasks_list


def calculate_percentage_done(num_complete, total):
    completion = 1.0
    if float(total) > 0.0:
        completion = float(num_complete) / float(total)
    return completion


def get_sublist_mirroring_tasks(tasks, num_threads, ith_thread, reverse_flag):
    """
    Generate a sub-list containing the ith-element of every n elements. Reverse the list is specified.
    :param tasks: list, Original list
    :param num_threads: int, Number of threads
    :param ith_thread: int, pos-th to be selected
    :param reverse_flag: bool, True if the list has to be reversed
    :return: list, Filtered list
    """
    filtered_list = []
    for i, element in enumerate(tasks):
        if (i % num_threads) == ith_thread:
            filtered_list.append(element)
    if reverse_flag:
        filtered_list.reverse()
    return filtered_list


def process_mirroring_tasks(mirroring_tasks_queue, target_node, ith_thread, num_tasks, ngams_server):
    """
    Process mirroring tasks described in the input mirroring_tasks list
    :param mirroring_tasks_queue: Queue of the mirroring tasks assigned to the input server
    :param target_node: string, Full qualified name of the target node
    :param ith_thread: int, Thread number
    :param num_tasks: int, Initial size of the queue
    :param ngams_server: ngamsServer, Reference to NG/AMS server class object
    """
    logger.debug("Inside mirror worker worker %d to mirror files to %s", ith_thread, target_node)
    try:
        # Loop on the mirroring_tasks_queue
        while True:
            # Get tasks from queue
            if len(mirroring_tasks_queue):
                try:
                    if ith_thread % 2:
                        item = mirroring_tasks_queue.pop()
                    else:
                        item = mirroring_tasks_queue.popleft()
                except Exception:
                    break
            else:
                break

            # Get all the fields we are interested in and form them into a command to fetch the files.
            # These fields were extracted in get_list_mirroring_tasks()
            logger.debug("Next task: %r", item)
            staging_file = str(item[1])
            mime_type = str(item[3])
            checksum = str(item[4])
            checksum_plugin = str(item[5])
            rowid = str(item[2])
            file_id = str(item[10])
            file_size = str(item[0])
            file_info = {}
            file_info['sourceHost'] = str(item[6])
            file_info['diskId'] = str(item[7])
            file_info['hostId'] = str(item[8])
            file_info['fileVersion'] = str(item[9])
            file_info['fileId'] = file_id

            logger.info("Processing mirroring task (target node: %s, file size: %s, thread: %d) file info: %s",
                        target_node, file_size, ith_thread, str(file_info))
            # Initialize ngamsReqProps object by just specifying the file URI and the mime-type
            request_properties = ngamsReqProps.ngamsReqProps()
            request_properties.setMimeType(mime_type)
            request_properties.checksum = checksum
            request_properties.checksum_plugin = checksum_plugin
            request_properties.fileinfo = file_info
            request_properties.setSize(file_size)

            # Start clock
            start = time.time()
            (staging_filename, target_disk_info) = calculate_staging_name(ngams_server, file_id, staging_file)
            request_properties.setStagingFilename(staging_filename)
            request_properties.setTargDiskInfo(target_disk_info)
            try:
                # Construct query to update ingestion date, ingestion time and status
                sql = "update ngas_mirroring_bookkeeping " \
                      "set status = 'FETCHING', staging_file = {0}, attempt = nvl(attempt + 1, 1) " \
                      "where rowid = {1}"
                # Add query to the queue
                ngams_server.getDb().query2(sql, args=(request_properties.getStagingFilename(), rowid))
                logger.info("Mirroring file: %s", file_id)
                ngamsCmd_MIRRARCHIVE.handleCmd(ngams_server, request_properties)
                status = "SUCCESS"
            except ngamsFailedDownloadException.FailedDownloadException:
                # Something bad happened...
                logger.exception("Failed to fetch %s", file_id)
                status = "FAILURE"
            except ngamsFailedDownloadException.AbortedException:
                logger.warning("File fetch aborted: %s", file_id)
                status = "ABORTED"
            except ngamsFailedDownloadException.PostponeException:
                logger.exception("Failed to fetch %s - will try to resume on next iteration", file_id)
                status = "TORESUME"
            except Exception:
                # This clause should never be reached
                logger.exception("Fetch failed in an unexpected way")
                status = "FAILURE"

            # Get time elapsed
            elapsed_time = (time.time() - start)

            # Construct query to update ingestion date, ingestion time and status
            sql = "update ngas_mirroring_bookkeeping set status = {0},"
            if status != 'TORESUME':
                sql += "staging_file = null, "
            sql += "ingestion_date = {1}, ingestion_time = nvl(ingestion_time, 0.0) + {2} " \
                   "where rowid = {3}"
            args = (status, toiso8601(fmt=FMT_DATETIME_NOMSEC) + ":000", elapsed_time, rowid)
            ngams_server.getDb().query2(sql, args=args)

            # Log message for mirroring task processed
            completion = 100 * (num_tasks - len(mirroring_tasks_queue)) / float(num_tasks)
            logger.info("Mirroring task (Target node: %s Thread: %d) processed in %fs (%s), completion: %f%%: %s",
                        target_node, ith_thread, elapsed_time, status, completion, str(file_info))
        logger.info("Mirroring Worker complete")
    except Exception:
        logger.exception("Error while running mirroring worker")
    return


def calculate_staging_name(ngams_server, file_id, existing_staging_file):
    """
    Generate staging filename.
    As of v2016.06 we either have a staging filename or just the disk volume to which the file should be mirrored
    How can we tell which is which? staging filenames all include the character string "___"
    """
    staging_filename = None
    target_disk_info = None
    logger.debug('Existing staging file: |%s|', existing_staging_file)
    if existing_staging_file == "None":
        logger.error('Trying to mirror a file where the staging file has not been set. This should no longer happen')
    elif "___" in existing_staging_file:
        logger.info("An existing staging file was specified: %s", existing_staging_file)
        if os.path.exists(existing_staging_file):
            logger.info("The file still exists on disk")
            staging_filename = existing_staging_file
            mount_point = staging_filename.split(NGAMS_STAGING_DIR)[0][:-1]
            logger.info('Staging file mount point: %s', mount_point)
            target_disk_info = get_mounted_disk_info(ngams_server, mount_point)
            logger.info('Staging file disk info: %s', str(target_disk_info))
        else:
            logger.info("The staging file no longer exists on disk")
    else:
        logger.info('Staging file does not exist, only the mount directory: %s', existing_staging_file)
        base_name = os.path.basename(file_id)
        staging_filename = os.path.join("/", existing_staging_file, NGAMS_STAGING_DIR,
                                        genUniqueId() + "___" + base_name)
        target_disk_info = get_mounted_disk_info(ngams_server, existing_staging_file)

    # I do not expect this case to be used any more from v2016.06 onwards leave it here for the moment, but delete ASAP
    if staging_filename is None:
        logger.error('Reached some old code which we should not have reached')
        base_name = os.path.basename(file_id)
        target_disk_info = get_target_volume(ngams_server)
        staging_filename = os.path.join("/", target_disk_info.getMountPoint(), NGAMS_STAGING_DIR,
                                        genUniqueId() + "___" + base_name)
    return staging_filename, target_disk_info


# TODO: remove this global variables
_cached_disk_info = {}


def get_target_volume(ngams_server):
    """
    Get the volume with most space available
    :param ngams_server: Reference to NG/AMS server class object (ngamsServer)
    :return: Target volume object or None (ngamsDiskInfo | None)
    """
    sql = "select %s from ngas_disks nd where completed = 0 and host_id = {0} order by available_mb desc"\
          % ngamsDbCore.getNgasDisksCols()
    result = ngams_server.getDb().query2(sql, args=(ngams_server.getHostId(),))
    if not result:
        return None
    else:
        return ngamsDiskInfo.ngamsDiskInfo().unpackSqlResult(result[0])


def get_mounted_disk_info(ngams_server, mount_point):
    disk_info = _cached_disk_info.get((ngams_server.getHostId(), mount_point), None)
    if disk_info is None:
        sql = "select %s from ngas_disks nd where mount_point = {0} and host_id = {1}"\
              % ngamsDbCore.getNgasDisksCols()
        result = ngams_server.getDb().query2(sql, args=(mount_point, ngams_server.getHostId()))
        if not result:
            disk_info = None
        else:
            disk_info = ngamsDiskInfo.ngamsDiskInfo().unpackSqlResult(result[0])
            _cached_disk_info[(ngams_server.getHostId(), mount_point)] = disk_info
    return disk_info


def multithreading_mirroring(source_nodes_list, num_threads, current_iteration, ngams_server):
    """
    Creates multiple threads per source node and target node to process the corresponding mirroring tasks. Each thread
    starts from big files or small files alternating.
    :param source_nodes_list: list[string], List of active source nodes in the source cluster
    :param num_threads: int, Number of threads per source-target connection
    :param current_iteration: Current iteration
    :param ngams_server: ngamsServer, Reference to NG/AMS server class object
    """
    target_node = get_fully_qualified_name(ngams_server)

    # How many files do we still have to download from previous iterations?
    num_threads_in_use = get_num_download_threads_in_use(current_iteration, target_node, ngams_server)
    available_threads_for_mirroring = num_threads - num_threads_in_use
    if available_threads_for_mirroring <= 0:
        logger.info("There are no mirroring threads available for host %s (%d are allowed, %d are being used)",
                    target_node, num_threads, num_threads_in_use)
        # This is not strictly an error, merely a badly implemented feature:
        # * the mirroring master only sends commands to the other nodes if it knows they have threads available
        # * it works this out by looking at how many files are marked as 'FETCHING' in teh bookkeeping table
        # * but it could query that table while the the node is preparing to download, or has just finished
        # As a result the master could tell the node to mirror, but the node says "no, no threads available"
        # We don't want to mark the files as ERROR, or TORESUME. Let's just pretend that it didn't happen.
        # Otherwise our monitoring application becomes much more confusing to interpret.
        logger.info('Removing mirroring bookkeeping entries for node %s for iteration %d',
                    target_node, current_iteration)
        revert_mirroring_bookkeeping_entries(current_iteration, target_node, ngams_server)
        return

    # Get tasks from each target-source pair
    all_sources_mirroring_tasks_list = []
    # Get mirroring tasks from each source
    source_index = 0
    for source_node in source_nodes_list:
        # Get mirroring tasks list for this pair target-source
        ith_source_mirroring_tasks_list = get_list_mirroring_tasks(current_iteration, source_node,
                                                                   target_node, ngams_server)
        # Assign list to all sources mirroring tasks list
        all_sources_mirroring_tasks_list.append(ith_source_mirroring_tasks_list)
        source_index += 1

    # Reorder lists to mix big/small files and put in queue format
    mirroring_tasks_queue = reorder_list_of_mirroring_tasks_for_target(current_iteration, source_nodes_list,
                                                                       target_node, all_sources_mirroring_tasks_list,
                                                                       ngams_server)
    # Start threads iterator
    threads_range = range(available_threads_for_mirroring)
    threads_list = []

    # Start multi-threading mirroring
    for ith_thread in threads_range:
        logger.info("Initializing mirror worker %d to mirror files to %s", ith_thread + 1, target_node)
        ith_mirror_worker = MirrorWorker(mirroring_tasks_queue, target_node, ith_thread + 1, ngams_server)
        ith_mirror_worker.start()
        threads_list.append(ith_mirror_worker)

    # Block until there are not remaining mirroring tasks in the queue
    for ith_thread in threads_list:
        ith_thread.join()

    return


class MirrorWorker(threading.Thread):

    def __init__(self, mirroring_tasks_queue, target_node, ith_thread, ngams_server):
        threading.Thread.__init__(self)
        self.mirroring_tasks_queue = mirroring_tasks_queue
        self.total_tasks = len(mirroring_tasks_queue)
        self.target_node = target_node
        self.ith_thread = ith_thread
        self.ngams_server = ngams_server

    def run(self):
        process_mirroring_tasks(self.mirroring_tasks_queue, self.target_node, self.ith_thread,
                                self.total_tasks, self.ngams_server)


def sort_target_nodes(target_nodes_list):
    """
    Sort target_nodes_list to balance priority
    :pram target_nodes_list: list[string], List of active target nodes in the target cluster
    :return: list[string], Sorted active target nodes list
    """
    # Initialize machine/port dictionary
    machines_list = {}
    for target_node in target_nodes_list:
        machine = target_node.split(":")[0]
        machines_list[machine] = []

    # Fill port list in machine/port dictionary
    for target_node in target_nodes_list:
        machine = target_node.split(":")[0]
        port = target_node.split(":")[1]
        machines_list[machine].append(port)

    # Create sorted target nodes list
    found_one = True
    sorted_target_nodes_list = []
    while found_one:
        found_one = False
        for machine in machines_list:
            if len(machines_list[machine]) > 0:
                # Add higher port (machines sort-descending)
                sorted_target_nodes_list.append(machine + ':' + machines_list[machine].pop())
                found_one = True
    logger.debug("Target nodes order to send MIRREXEC command: %s", str(sorted_target_nodes_list))
    return sorted_target_nodes_list


def distributed_mirroring(target_nodes_list, num_threads, rx_timeout, iteration):
    """
    Send MIRREXEC command to each nodes in the target nodes list in order to have a distributed mirroring process
    :param target_nodes_list: list[string], List of active target nodes in the target cluster
    :param num_threads: int, Number of threads per source-target connection
    :param rx_timeout: Socket timeout in seconds
    :param iteration: Iteration
    """
    # Get sorted_target_nodes_list
    sorted_target_nodes_list = sort_target_nodes(target_nodes_list)

    # Main loop
    threads_list = []
    for target_node in sorted_target_nodes_list:
        # Initialize mirrexec_command_sender thread object
        mirrexec_command_sender_obj = MirrexecCommandSender(target_node, num_threads, rx_timeout, iteration)
        # Add mirrexec_command_sender thread object to the list of threads
        threads_list.append(mirrexec_command_sender_obj)
        # Start mirrexec_command_sender thread object
        mirrexec_command_sender_obj.start()

    # Join mirror_node threads
    for ith_thread in threads_list:
        ith_thread.join()

    return


class MirrexecCommandSender(threading.Thread):

    def __init__(self, target_node, num_threads, rx_timeout, iteration):
        """
        :param target_node: string, Target node to send MIRREXEC
        :param num_threads: int, Number of threads per source-target connection
        :param rx_timeout: int, Socket timeout time in seconds
        :param iteration: int, Iteration
        """
        threading.Thread.__init__(self)
        self.target_node = target_node
        self.num_threads = num_threads
        self.rx_timeout = rx_timeout
        self.iteration = iteration

    def run(self):
        try:
            self.send_mirrexec_command()
        except Exception:
            logger.exception("MIRREXEC command failed")

    def send_mirrexec_command(self):
        """
        Send MIRREXEC command to the input source_node
        """
        logger.info("Sending MIRREXEC command to %s with (n_threads=%d)", self.target_node, self.num_threads)
        try:
            host, port = self.target_node.split(":")
            pars = {
                'n_threads': str(self.num_threads),
                'rx_timeout': str(self.rx_timeout),
                'iteration': str(self.iteration)
            }

            start_time = time.time()
            # it is important to not let this operation time out. If it times out then the files being fetched will
            # be eligable for re-fetching even though the spawned threads may still be executing. Chaos ensues.
            response = ngamsHttpUtils.httpGet(host, int(port), 'MIRREXEC', pars=pars)
            with contextlib.closing(response):
                failed = response.status != NGAMS_HTTP_SUCCESS or NGAMS_FAILURE in utils.b2s(response.read())
            elapsed_time = time.time() - start_time

            if failed:
                logger.error("MIRREXEC command sent to %s with (n_threads=%d) was handled with status FAILURE " 
                             "in %f [s]", self.target_node, self.num_threads, elapsed_time)
            else:
                logger.info("MIRREXEC command sent to %s with (n_threads=%d) was handled with status SUCCESS in %f [s]",
                            self.target_node, self.num_threads, elapsed_time)
        except Exception:
            logger.exception("Problems sending MIRREXEC command to %s", self.target_node)

        return
