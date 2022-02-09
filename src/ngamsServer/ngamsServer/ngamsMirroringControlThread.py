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
# *****************************************************************************
#
# "@(#) $Id: ngamsMirroringControlThread.py,v 1.27 2010/06/18 12:03:55 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  27/03/2008  Created
#
"""
This module contains the code for the Mirroring Control Thread, which is used to coordinate the mirroring of the
local NGAS Cluster with other NGAS Clusters.

The NGAS Mirroring Service is running as a background service which does not consume so many resources for the
general command handling.
"""

# TODO: Detailed reporting not yet implemented

import base64
import contextlib
import copy
import errno
import functools
import logging
import os
import random
import socket
import threading
import time

from ngamsLib.ngamsCore import \
    FMT_TIME_ONLY_NOMSEC, \
    NGAMS_HTTP_HDR_FILE_INFO, \
    NGAMS_HTTP_HDR_CONTENT_TYPE,\
    NGAMS_HTTP_SUCCESS, \
    NGAMS_HTTP_PAR_FILE_LIST,\
    NGAMS_HTTP_PAR_FILE_LIST_ID, \
    NGAMS_HTTP_PAR_FILENAME,\
    NGAMS_HTTP_PAR_FROM_ING_DATE, \
    NGAMS_HTTP_PAR_MAX_ELS, \
    NGAMS_HTTP_PAR_UNIQUE,\
    NGAMS_MIR_CONTROL_THR, \
    NGAMS_REARCHIVE_CMD,\
    NGAMS_STATUS_CMD,  \
    decompressFile, get_contact_ip, rmFile, toiso8601
from ngamsLib import ngamsFileInfo, ngamsStatus, ngamsHighLevelLib, ngamsDbm, \
    ngamsMirroringRequest, ngamsLib, ngamsHttpUtils

logger = logging.getLogger(__name__)

# Various definitions used within this module.
# Definitions for internal DBM based queues used.
NGAMS_MIR_QUEUE_DBM = "MIR_QUEUE"
NGAMS_MIR_ERR_QUEUE_DBM = "MIR_ERROR_QUEUE"
NGAMS_MIR_COMPL_QUEUE_DBM = "MIR_COMPLETED_QUEUE"
NGAMS_MIR_DBM_COUNTER = "MIR_DBM_COUNTER"
NGAMS_MIR_DBM_POINTER = "MIR_DBM_POINTER"
NGAMS_MIR_FILE_LIST_RAW = "MIR_FILE_LIST_RAW"
NGAMS_MIR_CLUSTER_FILE_DBM = "MIR_CLUSTER_FILE_INFO"
NGAMS_MIR_DBM_MAX_LIMIT = 2**30
NGAMS_MIR_MIR_THREAD_TIMEOUT = 10.0
NGAMS_MIR_SRC_ARCH_INF_DBM = "MIR_SRC_ARCH_INFO"
NGAMS_MIR_ALL_LOCAL_SRVS = "ALL"

# NGAMS_MIR_CONTROL_THR_STOP = "_STOP_MIR_CONTROL_THREAD_"


# We use an exception to stop the mirroring thread (deliberately)
class MirroringStoppedException(Exception):
    pass


def _finish_thread():
    logger.info("Stopping the Mirroring Service")
    raise MirroringStoppedException


def check_stop_mirror_control_thread(stop_event):
    """
    Used to check if the Mirroring Control Thread should be stopped and in case yes, to stop it.
    :param stop_event: Stop event
    """
    if stop_event.is_set():
        _finish_thread()


def suspend(stop_event, thread):
    if stop_event.wait(thread):
        _finish_thread()


def add_entry_mirror_queue(ngams_server, mirror_request, update_db=True):
    """
    Add (schedule) a new Mirroring Request in the internal DBM Mirroring Queue
    :param ngams_server: Reference to server object (ngamsServer)
    :param mirror_request: Instance of Mirroring Request Object to schedule (ngamsMirroringRequest)
    :param update_db: If true, the status is updated in the DB (boolean)
    """
    try:
        ngams_server._mirQueueDbmSem.acquire()
        logger.debug("Adding entry in Mirroring Queue: %s/%d", mirror_request.getFileId(),
                     mirror_request.getFileVersion())
        new_key = (ngams_server._mirQueueDbm.get(NGAMS_MIR_DBM_COUNTER) + 1) % NGAMS_MIR_DBM_MAX_LIMIT
        ngams_server._mirQueueDbm.add(str(new_key), mirror_request).add(NGAMS_MIR_DBM_COUNTER, new_key).sync()
        if update_db:
            ngams_server.getDb().updateMirReq(mirror_request)
        ngams_server._mirQueueDbmSem.release()
    except Exception as e:
        ngams_server._mirQueueDbmSem.release()
        raise Exception("Error adding new element to DBM Mirroring Queue. Error: %s" % str(e))


def add_entry_error_queue(ngams_server, mirror_request, update_db=True):
    """
    Add (push) a Mirroring Request in the internal DBM Mirroring Error Queue
    :param ngams_server: Reference to server object (ngamsServer)
    :param mirror_request: Instance of Mirroring Request Object to schedule (ngamsMirroringRequest)
    :param update_db: If true, the status is updated in the DB (boolean)
    """
    try:
        ngams_server._errQueueDbmSem.acquire()
        logger.debug("Adding entry in Mirroring Error Queue: %s/%d", mirror_request.getFileId(),
                     mirror_request.getFileVersion())
        ngams_server._errQueueDbm.add(mirror_request.genFileKey(), mirror_request).sync()
        if update_db:
            ngams_server.getDb().updateMirReq(mirror_request)
        ngams_server._errQueueDbmSem.release()
    except Exception as e:
        ngams_server._errQueueDbmSem.release()
        raise Exception("Error adding new element to DBM Mirroring Error Queue. Error: %s" % str(e))


def pop_entry_queue(mirror_request, dbm, dbm_sem):
    """
    Get (pop) a Mirroring Request Object from the given DBM queue. The entry is removed from the queue. The entry to
    get is referenced by its Mirroring Request Object.
    :param mirror_request: Instance of Mirroring Request Object to schedule (ngamsMirroringRequest)
    :param dbm: DBM handle to that DBM queue (ngamsDbm)
    :param dbm_sem: Semaphore controlling access to that queue (threading.Semaphore)
    :return: Reference to the Mirroring Request Object removed from the Error Queue DBM (ngamsMirroringRequest)
    """
    try:
        dbm_sem.acquire()
        if not dbm.hasKey(mirror_request.genFileKey()):
            raise Exception("Mirroring Request: %s not found in DBM Queue: %s"
                            % (mirror_request.genSummary(), dbm.getDbmName()))
        mirror_request = dbm.get(mirror_request.genFileKey())
        dbm.rem(mirror_request.genFileKey())
        dbm_sem.release()
        return mirror_request
    except Exception as e:
        dbm_sem.release()
        msg = "Error retrieving element from DBM queue: %s. Error: %s"
        raise Exception(msg % (dbm.getDbmName(), str(e)))


def dump_keys_queue(dbm, dbm_sem, target_dbm_name):
    """
    Make a snapshot of all keys in the referenced DBM
    :param dbm: DBM from which to dump the keys (ngamsDbm)
    :param dbm_sem: Semaphore used to access that DBM (threading.Semaphore)
    :param target_dbm_name: Name of the DBM in which to dump the keys (string)
    :return: The final name of the resulting DBM (string)
    """
    rmFile("%s*" % target_dbm_name)
    key_dbm = ngamsDbm.ngamsDbm(target_dbm_name, cleanUpOnDestr=False, writePerm=True)
    try:
        dbm_sem.acquire()
        dbm.initKeyPtr()
        while True:
            key, data = dbm.getNext()
            if not key:
                break
            key_dbm.add(key, data)
        dbm_sem.release()
    except Exception as e:
        dbm_sem.release()
        raise Exception("Error dumping keys from DBM: %s. Error: %s" % (dbm.getDbmName(), str(e)))
    dbm_name = key_dbm.sync().getDbmName()
    return dbm_name


def add_entry_completed_queue(ngams_server, mirror_request, update_db=True):
    """
    Add a Mirroring Request in the DBM Mirroring Completed Queue
    :param ngams_server: Reference to server object (ngamsServer)
    :param mirror_request: Instance of Mirroring Request Object to put in the queue (ngamsMirroringRequest)
    :param update_db: If true, the status is updated in the DB (boolean)
    """
    try:
        ngams_server._complQueueDbmSem.acquire()
        logger.debug("Adding entry in Mirroring Completed Queue: %s/%d", mirror_request.getFileId(),
                     mirror_request.getFileVersion())
        ngams_server._complQueueDbm.add(mirror_request.genFileKey(), mirror_request).sync()
        if update_db:
            ngams_server.getDb().updateMirReq(mirror_request)
        ngams_server._complQueueDbmSem.release()
    except Exception as e:
        ngams_server._complQueueDbmSem.release()
        raise Exception("Error adding new element to DBM Mirroring Completed Queue. Error: %s" % str(e))


def schedule_mirror_request(ngams_server, instance_id, file_id, file_version, ingestion_date, server_list_id,
                            xml_file_info):
    """
    Schedule a new Mirroring Request in the DB Mirroring Queue and the Mirroring Queue DBM
    :param ngams_server: Reference to server object (ngamsServer)
    :param instance_id: ID for instance controlling the mirroring (string)
    :param file_id: NGAS file ID (string)
    :param file_version: NGAS file version (integer)
    :param ingestion_date: NGAS ingestion date reference for file (number)
    :param server_list_id: Server list ID for this request indicating the nodes to contact to obtain this file (string)
    :param xml_file_info: The XML file information for the file (string/XML)
    """
    mirror_request_obj = ngamsMirroringRequest.ngamsMirroringRequest().\
        setInstanceId(instance_id).\
        setFileId(file_id).\
        setFileVersion(file_version).\
        setIngestionDate(ingestion_date).\
        setSrvListId(server_list_id).\
        setStatus(ngamsMirroringRequest.NGAMS_MIR_REQ_STAT_SCHED).\
        setXmlFileInfo(xml_file_info)
    logger.debug("Scheduling data object for mirroring: %s", mirror_request_obj.genSummary())
    ngams_server.getDb().writeMirReq(mirror_request_obj)
    add_entry_mirror_queue(ngams_server, mirror_request_obj, update_db=False)


def get_mir_request_from_queue(ngams_server):
    """
    Get the next Mirroring Request from the Mirroring Request Queue. If there are no requests in the queue, None is
    returned. The entries are removed (popped) from the queue.
    :param ngams_server: Reference to server object (ngamsServer)
    :return: Next Mirroring Request Object or None (ngamsMirroringRequest | None)
    """
    try:
        ngams_server._mirQueueDbmSem.acquire()
        next_key = (ngams_server._mirQueueDbm.get(NGAMS_MIR_DBM_POINTER) + 1) % NGAMS_MIR_DBM_MAX_LIMIT
        if ngams_server._mirQueueDbm.hasKey(str(next_key)):
            mirror_request_obj = ngams_server._mirQueueDbm.get(str(next_key))
            ngams_server._mirQueueDbm.add(NGAMS_MIR_DBM_POINTER, next_key).rem(str(next_key)).sync()
        else:
            mirror_request_obj = None
        ngams_server._mirQueueDbmSem.release()
        return mirror_request_obj
    except Exception as e:
        ngams_server._mirQueueDbmSem.release()
        raise Exception("Error adding new element to DBM Mirroring Queue. Error: %s" % str(e))


def start_mirroring_threads(ngams_server, stop_event):
    """
    Start the Mirroring Threads according to the configuration
    :param ngams_server: Reference to server object (ngamsServer)
    :param stop_event Stop event
    """
    for thread_num in range(1, ngams_server.getCfg().getMirroringThreads() + 1):
        thread_id = NGAMS_MIR_CONTROL_THR + "-" + str(thread_num)
        args = (ngams_server, stop_event)
        logger.debug("Starting Mirroring Thread: %s", thread_id)
        thread_handle = threading.Thread(None, mirroring_thread, thread_id, args)
        thread_handle.setDaemon(False)
        thread_handle.start()


def pause_mirror_threads(ngams_server, stop_event):
    """
    Called by the Mirroring Control Thread to request the Mirroring Threads to pause themselves until asked to resume
    :param ngams_server: Reference to server object (ngamsServer)
    :param stop_event: Stop event
    """
    ngams_server._pauseMirThreads = True
    # Wait for all threads to enter pause mode
    num_mirror_threads = ngams_server.getCfg().getMirroringThreads()
    while True:
        if ngams_server._mirThreadsPauseCount == num_mirror_threads:
            logger.debug("All Mirroring Threads entered paused mode")
            return
        suspend(stop_event, 1)


def resume_mirror_threads(ngams_server, stop_event):
    """
    Called by the Mirroring Control Thread to request the Mirroring Threads to resume service after they have
    been paused
    :param ngams_server: Reference to server object (ngamsServer)
    :param stop_event: Stop event
    """
    ngams_server._pauseMirThreads = False
    # Wait for all threads to resume service
    while ngams_server._mirThreadsPauseCount > 0:
        suspend(stop_event, 1)
    logger.debug("All Mirroring Threads resumed service")


def pause_mirror_thread(ngams_server, stop_event):
    """
    Called by the Mirroring Threads to check if they should pause on request from the Mirroring Control Thread. If yes,
    they pause themselves until requested to resume or to exit the service.
    :param ngams_server: Reference to server object (ngamsServer)
    :param stop_event: Stop event
    """
    if ngams_server._pauseMirThreads:
        logger.debug("Mirroring Thread suspending itself ...")
        ngams_server._mirThreadsPauseCount += 1
        while ngams_server._pauseMirThreads:
            suspend(stop_event, 1)
        logger.debug("Mirroring Thread resuming service ...")
        ngams_server._mirThreadsPauseCount -= 1


def get_mirror_request(ngams_server, timeout):
    """
    Check if there is a Mirroring Request in the queue
    :param ngams_server: Reference to server object (ngamsServer)
    :param timeout: Max. timeout to wait for a new request (float)
    :return: Return Mirroring Request Object or None if no became available in the specified period of time
    (ngamsMirroringRequest | None)
    """
    ngams_server.waitMirTrigger(timeout)
    mirror_request_obj = get_mir_request_from_queue(ngams_server)
    return mirror_request_obj


# TODO: remove these global variables
# An internal list of local serves is kept, to avoid reading this information continuously from the DB
_local_server_list = []
_last_update_local_server_list = 0


def get_local_nau_list(ngams_server, local_server_list_cfg):
    """
    Render the list of local servers that can be contacted for handling the re-archiving of the files from the source
    archive.
    If 'local_server_list_cfg' is 'ALL', all the local servers with archiving capability are considered. If
    'localSrvListCfg' is specified, only these are considered. In both cases the resulting list is shuffled randomly
    to obtain some load balancing.
    :param ngams_server: Reference to server object (ngamsServer)
    :param local_server_list_cfg: List with 'server:port,...' to be contacted for re-archiving requests in the local
    cluster, or 'ALL' (string)
    :return: List of servers that can be contacted (list)
    """
    if local_server_list_cfg == NGAMS_MIR_ALL_LOCAL_SRVS:
        # All local servers should be considered
        # Read only the list of local servers about every minute
        global _local_server_list, _last_update_local_server_list
        if (time.time() - _last_update_local_server_list) > 60:
            cluster_name = ngams_server.getHostInfoObj().getClusterName()
            _local_server_list = ngams_server.getDb().getClusterReadyArchivingUnits(cluster_name)
            _last_update_local_server_list = time.time()
        tmp_server_list = _local_server_list
    else:
        # A specific list is given
        tmp_server_list = filter(None, local_server_list_cfg.split(","))
    # Create a copy of the list and shuffle the item order
    server_list = copy.deepcopy(tmp_server_list)
    random.shuffle(server_list)
    return server_list


def handle_mirror_request(ngams_server, mirror_request):
    """
    Handle a Mirroring Request: Attempt to mirror the data object associated to that Mirroring Request.
    :param ngams_server: Reference to server object (ngamsServer)
    :param mirror_request: Mirroring Request Object (ngamsMirroringRequest)
    """
    mirror_request.setStatus(ngamsMirroringRequest.NGAMS_MIR_REQ_STAT_ACTIVE)
    ngams_server.getDb().updateStatusMirReq(mirror_request.getFileId(), mirror_request.getFileVersion(),
                                            ngamsMirroringRequest.NGAMS_MIR_REQ_STAT_ACTIVE_NO)

    # Find a node to contact in the local cluster (try the whole list if necessary)
    server_list = ngams_server.getSrvListDic()[mirror_request.getSrvListId()]
    mirror_source_obj = ngams_server.getCfg().getMirroringSrcObjFromSrvList(server_list)
    local_nau_list = get_local_nau_list(ngams_server, mirror_source_obj.getTargetNodes())
    succeeded = False
    encoded_file_info = base64.b64encode(mirror_request.getXmlFileInfo())
    file_info = ngamsFileInfo.ngamsFileInfo().unpackXmlDoc(mirror_request.getXmlFileInfo())
    error_message = ""
    for next_nau in local_nau_list:
        next_local_server, next_local_port = next_nau.split(":")
        next_local_port = int(next_local_port)

        # Cycle over the specified remote nodes in the source archive, until one of them are successful

        # Get shuffled list of nodes in the source archive to contact to
        # get a copy of the file in question.
        source_node_list = copy.deepcopy(ngams_server.getSrvListDic()[mirror_source_obj.getId()])
        random.shuffle(source_node_list)
        for source_node_address in source_node_list:
            source_host_name, source_port_num = source_node_address.split(":")
            source_port_num = int(source_port_num)
            # Send REARCHIVE Command to the next, local contact node, asking it to try to collect the file from the
            # next node in the Mirroring Source Archive.
            # FIXME: we should try using https
            file_uri = "http://%s:%d/RETRIEVE?file_id=%s&file_version=%d&quick_location=1"
            file_uri = file_uri % (source_host_name, source_port_num, mirror_request.getFileId(),
                                   mirror_request.getFileVersion())
            pars = [[NGAMS_HTTP_PAR_FILENAME, file_uri]]
            hdrs = [[NGAMS_HTTP_HDR_FILE_INFO, encoded_file_info],
                    [NGAMS_HTTP_HDR_CONTENT_TYPE, file_info.getFormat()]]
            response = ngamsHttpUtils.httpGet(next_local_server, next_local_port, NGAMS_REARCHIVE_CMD, pars=pars,
                                              hdrs=hdrs, timeout=600)

            with contextlib.closing(response):
                if response.status == NGAMS_HTTP_SUCCESS:
                    succeeded = True
                    break
                else:
                    # An error occurred, log error notice and go to next (if there are more nodes)
                    tmp_status = ngamsStatus.ngamsStatus().unpackXmlDoc(response.read())
                    msg = "Error issuing REARCHIVE Command. Local node: %s:%d, source contact node: %s:%d. " \
                          "Error message: %s"
                    msg = msg % (next_local_server, next_local_port, source_host_name, source_port_num,
                                 tmp_status.getMessage())
                    logger.warning(msg)
                    error_message = "Last error encountered: %s" % msg
                    continue
        if succeeded:
            break

    if not succeeded:
        mirror_request.setStatus(ngamsMirroringRequest.NGAMS_MIR_REQ_STAT_ERR_RETRY_NO).\
            setMessage(error_message).setLastActivityTime(time.time())
        ngams_server.getDb().updateStatusMirReq(mirror_request.getFileId(), mirror_request.getFileVersion(),
                                                ngamsMirroringRequest.NGAMS_MIR_REQ_STAT_ERR_RETRY_NO)
        raise Exception("Error handling Mirroring Request: %s" % mirror_request.genSummary())
    else:
        logger.debug("Successfully handled Mirroring Request: %s", mirror_request.genSummary())


def mirroring_thread(ngams_server, stop_event):
    """
    A number of Mirroring Threads are executing when the NGAS Mirroring Service is enabled to handle the requesting of
    data and ingestion into the local cluster.
    :param ngams_server: Reference to server object (ngamsServer)
    :param stop_event: Stop event
    """
    while True:
        # Encapsulate this whole block to avoid that the thread dies in case a problem occurs,
        # like e.g. a problem with the DB connection
        try:
            check_stop_mirror_control_thread(stop_event)
            pause_mirror_thread(ngams_server, stop_event)
            logger.debug("Mirroring Thread starting next iteration ...")
            # Business logic of Mirroring Thread
            try:
                # Wait for the next Mirroring Request. A timeout is applied, if no request becomes available within
                # the given timeout an exception is thrown.
                mirror_request = get_mirror_request(ngams_server, NGAMS_MIR_MIR_THREAD_TIMEOUT)
                if mirror_request:
                    handle_mirror_request(ngams_server, mirror_request)
                    # The handling of the Mirroring Request succeeded (no exception was thrown). Put the handle in the
                    # Completed Queue.
                    mirror_request.setStatus(ngamsMirroringRequest.NGAMS_MIR_REQ_STAT_MIR)
                    file_version = mirror_request.getFileVersion()
                    mirroring_status = ngamsMirroringRequest.NGAMS_MIR_REQ_STAT_MIR_NO
                    ngams_server.getDb().updateStatusMirReq(mirror_request.getFileId(), file_version, mirroring_status)
                    add_entry_completed_queue(ngams_server, mirror_request)
            except MirroringStoppedException as e:
                raise e
            except Exception as e:
                logger.warning("Error handling Mirroring Request. Putting in Error Queue. Error: %s" % str(e))
                # Put the request in the Error Queue DBM
                stat_num = ngamsMirroringRequest.NGAMS_MIR_REQ_STAT_ERR_RETRY_NO
                mirror_request.setStatus(stat_num).setMessage(str(e))
                ngams_server.getDb().updateStatusMirReq(mirror_request.getFileId(), mirror_request.getFileVersion(),
                                                        stat_num)
                add_entry_error_queue(ngams_server, mirror_request)
        except MirroringStoppedException as e:
            return
        except Exception as e:
            logger.exception("Error occurred during execution of the Mirroring Control Thread")
            # We make a small wait here to avoid that the process tries too often to carry out the tasks that failed
            if stop_event.wait(5.0):
                return


def initialise_mirroring(ngams_server):
    """
    Initialize the NGAS Mirroring Service. If there are requests in the Mirroring Request Queue in the DB, these are
    read out and inserted in the local Mirroring Request DBM.
    :param ngams_server: Reference to server object (ngamsServer)
    """
    host_id = ngams_server.getHostId()
    # Build up the server list in the DB and the local repository kept in memory
    # The ID allocated to each Mirroring Source, is used as ID in the Server List
    for mirror_source_obj in ngams_server.getCfg().getMirroringSrcList():
        server_list_id = ngams_server.getDb().getSrvListIdFromSrvList(mirror_source_obj.getServerList())
        ngams_server.getSrvListDic()[server_list_id] = mirror_source_obj.getServerList()
        ngams_server.getSrvListDic()[mirror_source_obj.getServerList()] = server_list_id
        # Add compiled version of the list, which is easy to use when accessing the contact nodes
        ngams_server.getSrvListDic()[mirror_source_obj.getId()] = mirror_source_obj.getServerList().split(",")

    # Create the Mirroring DBM Queue
    mirror_queue_dbm_name = "%s/%s_%s" % (ngamsHighLevelLib.getNgasChacheDir(ngams_server.getCfg()),
                                          NGAMS_MIR_QUEUE_DBM, host_id)
    rmFile("%s*" % mirror_queue_dbm_name)
    ngams_server._mirQueueDbm = ngamsDbm.ngamsDbm(mirror_queue_dbm_name, cleanUpOnDestr=0, writePerm=1)
    ngams_server._mirQueueDbm.add(NGAMS_MIR_DBM_COUNTER, 0).add(NGAMS_MIR_DBM_POINTER, 0)

    # Create the Error DBM Queue
    error_queue_dbm_name = "%s/%s_%s" % (ngamsHighLevelLib.getNgasChacheDir(ngams_server.getCfg()),
                                         NGAMS_MIR_ERR_QUEUE_DBM, host_id)
    rmFile("%s*" % error_queue_dbm_name)
    ngams_server._errQueueDbm = ngamsDbm.ngamsDbm(error_queue_dbm_name, cleanUpOnDestr=0, writePerm=1)

    # Create the Completed DBM Queue
    completed_queue_dbm_name = "%s/%s_%s" % (ngamsHighLevelLib.getNgasChacheDir(ngams_server.getCfg()),
                                             NGAMS_MIR_COMPL_QUEUE_DBM, host_id)
    rmFile("%s*" % completed_queue_dbm_name)
    ngams_server._complQueueDbm = ngamsDbm.ngamsDbm(completed_queue_dbm_name, cleanUpOnDestr=0, writePerm=1)

    # Create the DBM to keep track of when synchronization was last done with the specified Source Archives. Note this
    # DBM is kept between sessions to avoid too frequent complete synchronization checks.
    source_archive_info_dbm = "%s/%s_%s" % (ngamsHighLevelLib.getNgasChacheDir(ngams_server.getCfg()),
                                            NGAMS_MIR_SRC_ARCH_INF_DBM, host_id)
    ngams_server._srcArchInfoDbm = ngamsDbm.ngamsDbm(source_archive_info_dbm, cleanUpOnDestr=0, writePerm=1)
    # Update the Mirroring Source Archive DBM
    for mirror_source_obj in ngams_server.getCfg().getMirroringSrcList():
        if not ngams_server._srcArchInfoDbm.hasKey(mirror_source_obj.getId()):
            ngams_server._srcArchInfoDbm.add(mirror_source_obj.getId(), mirror_source_obj)
        else:
            dbm_mirror_source_obj = ngams_server._srcArchInfoDbm.get(mirror_source_obj.getId())
            mirror_source_obj.setLastSyncTime(dbm_mirror_source_obj.getLastSyncTime())
            ngams_server._srcArchInfoDbm.add(mirror_source_obj.getId(), mirror_source_obj)

    # Restore the previous state of the mirroring from the DB Mirroring Queue (if the service was interrupted)
    for mirror_request_obj in ngams_server.getDb().dumpMirroringQueue(ngams_server.getHostId()):
        logger.debug("Restoring Mirroring Request: %s", mirror_request_obj.genSummary())
        # Add entry in the Mirroring DBM Queue?
        if mirror_request_obj.getStatusAsNo() in (ngamsMirroringRequest.NGAMS_MIR_REQ_STAT_SCHED_NO,
                                                  ngamsMirroringRequest.NGAMS_MIR_REQ_STAT_ACTIVE_NO):
            add_entry_mirror_queue(ngams_server, mirror_request_obj)
        # Add entry in the Error DBM Queue?
        elif mirror_request_obj.getStatusAsNo() == ngamsMirroringRequest.NGAMS_MIR_REQ_STAT_ERR_RETRY_NO:
            add_entry_error_queue(ngams_server, mirror_request_obj)
        # Add entry in the Completed DBM Queue?
        elif mirror_request_obj.getStatusAsNo() in (ngamsMirroringRequest.NGAMS_MIR_REQ_STAT_MIR_NO,
                                                    ngamsMirroringRequest.NGAMS_MIR_REQ_STAT_REP_NO,
                                                    ngamsMirroringRequest.NGAMS_MIR_REQ_STAT_ERR_ABANDON_NO):
            add_entry_completed_queue(ngams_server, mirror_request_obj, update_db=False)


def retrieve_file_list(ngams_server, mirror_source, node, port, status_cmd_pars, cluster_files_dbm_name):
    """
    Retrieve and handle the information in connection with the STATUS?file_list request
    :param ngams_server: Reference to server object (ngamsServer)
    :param mirror_source: Mirroring Source Object associated with the NGAS Cluster contacted (ngamsMirroringSource)
    :param node: NGAS host to contact (string)
    :param port: Port used by NGAS instance to contact (integer)
    :param status_cmd_pars: HTTP parameters for the STATUS Command (list)
    :param cluster_files_dbm_name: Name of the DBM containing a snapshot of all files stored in the name space of the
    local cluster (string)
    """
    host_id = ngams_server.getHostId()

    # Send the STATUS?file_list query. Receive the data into a temporary file
    raw_file_list_compressed = "%s/%s_%s.gz" % (ngamsHighLevelLib.getNgasChacheDir(ngams_server.getCfg()),
                                                NGAMS_MIR_FILE_LIST_RAW, host_id)
    file_list_id = None
    try:
        cluster_files_dbm = ngamsDbm.ngamsDbm(cluster_files_dbm_name, cleanUpOnDestr=0, writePerm=1)
        # Retrieve the file info from the specified contact nodes and schedule the files relevant
        remaining_elements = None
        while True:
            rmFile("%s*" % raw_file_list_compressed[:-3])
            response = ngamsHttpUtils.httpGet(node, port, NGAMS_STATUS_CMD, pars=status_cmd_pars, timeout=1800)

            with contextlib.closing(response):
                if response.status != NGAMS_HTTP_SUCCESS:
                    rmFile("%s*" % raw_file_list_compressed[:-3])
                    status_obj = ngamsStatus.ngamsStatus().unpackXmlDoc(response.read())
                    raise Exception("Error accessing NGAS Node: %s/%d. Error: %s"
                                    % (node, port, status_obj.getMessage()))

                with open(raw_file_list_compressed, 'wb') as raw_file_obj:
                    response_read_buffer = functools.partial(response.read, 65536)
                    for response_buffer in iter(response_read_buffer, ''):
                        raw_file_obj.write(response_buffer)

            # Decompress the file (it is always transferred compressed)
            file_list_raw = decompressFile(raw_file_list_compressed)

            # Get the File List ID in connection with this request if not already extracted.
            # Get the number of remaining items to retrieve info about. It is necessary to scan through the beginning
            # of the file to get the FileList Element, which contains this information.
            # The entry looks something like this:
            # <FileList Id="a8f2cbdb705899588468f72986c813ab" Status="REMAINING_DATA_OBJECTS: 1453">
            file_list_raw_obj = open(file_list_raw)
            count = 0
            while count < 100:
                next_line = file_list_raw_obj.readline()
                if next_line.find("FileList Id=") != -1:
                    line_elements = filter(None, next_line.strip().split(" "))
                    if not file_list_id:
                        file_list_id = line_elements[1].split("=")[1].strip('"')
                        status_cmd_pars.append([NGAMS_HTTP_PAR_FILE_LIST_ID, file_list_id])
                    remaining_elements = int(filter(None, line_elements)[-1].split('"')[0])
                    break
                count += 1
            logger.debug("Retrieving File List. File List ID: %s. Remaining Elements: %d",
                         file_list_id, remaining_elements)
            if count == 100:
                raise Exception("Illegal file list received as response to STATUS?file_list Request")
            file_list_raw_obj.seek(0)

            # Read out the file info and figure out whether to schedule it for mirroring or not (file referenced by
            # File ID + Version), if this file is:
            # * being mirrored already (if it is queued): Skip
            # * available in the local cluster name space: Skip
            # * not already available in local cluster name space: Schedule it
            while True:
                next_line = file_list_raw_obj.readline()
                if next_line == "":
                    break
                if next_line.find("FileStatus AccessDate=") != -1:
                    tmp_file = ngamsFileInfo.ngamsFileInfo().unpackXmlDoc(next_line)
                    file_key = ngamsLib.genFileKey(None, tmp_file.getFileId(), tmp_file.getFileVersion())
                    # Entry found in the
                    #   * Mirroring DBM Queue?
                    #   * Error DBM Queue?
                    #   * Completed DBM Queue?
                    # Are there enough local copies in the cluster name space?
                    logger.debug("Checking whether to schedule file: %s/%d for mirroring ...",
                                 tmp_file.getFileId(), tmp_file.getFileVersion())
                    if ngams_server._mirQueueDbm.hasKey(file_key):
                        continue
                    elif ngams_server._errQueueDbm.hasKey(file_key):
                        continue
                    elif ngams_server._complQueueDbm.hasKey(file_key):
                        continue
                    else:
                        # Check if the file is available in the name space of this cluster. If not, schedule it.
                        if not cluster_files_dbm.hasKey(file_key):
                            # The data object is not available, schedule it!
                            server_list_id_db = ngams_server.getSrvListDic()[mirror_source.getServerList()]
                            schedule_mirror_request(ngams_server, host_id, tmp_file.getFileId(),
                                                    tmp_file.getFileVersion(), tmp_file.getIngestionDate(),
                                                    server_list_id_db, next_line)
            # Stop if there are no more elements to read out
            if remaining_elements == 0:
                break
    except Exception as e:
        raise Exception("Error retrieving file list. Error: %s" % str(e))


def check_source_archives(ngams_server):
    """
    Check the source archives to see if data is available for mirroring
    :param ngams_server: Reference to server object (ngamsServer)
    """
    # Dump the information for all files managed by this cluster
    cluster_name = ngams_server.getDb().getClusterNameFromHostId(ngams_server.getHostId())
    cluster_files_dbm_name = "%s/%s_%s" % (ngamsHighLevelLib.getNgasChacheDir(ngams_server.getCfg()),
                                           NGAMS_MIR_CLUSTER_FILE_DBM, cluster_name)
    cluster_files_dbm_name = os.path.normpath(cluster_files_dbm_name)

    # Dump information about files registered in this cluster
    # Use the file key as key in the DBM, count occurrences of each file
    cluster_files_dbm_name = ngams_server.getDb().dumpFileInfoCluster(cluster_name, cluster_files_dbm_name,
                                                                      useFileKey=True, count=True)

    # Loop over the various Mirroring Source Archives specified in the configuration
    for mirror_source_obj in ngams_server.getCfg().getMirroringSrcList():
        dbm_mirror_source_obj = ngams_server._srcArchInfoDbm.get(mirror_source_obj.getId())

        # Figure out if a partial or complete sync should be done for this mirroring source
        time_now = time.time()
        do_partial_sync = False
        do_complete_sync = False
        if mirror_source_obj.getCompleteSyncList():
            # OK, it is specified to do complete sync's in the configuration
            time_now_tag = "%s_TIME_NOW" % toiso8601(time_now, fmt=FMT_TIME_ONLY_NOMSEC)
            # Find the last time stamp compared to now
            tmp_complete_sync_list = copy.deepcopy(mirror_source_obj.getCompleteSyncList())
            tmp_complete_sync_list.append(time_now_tag)
            tmp_complete_sync_list.sort()
            time_now_idx = tmp_complete_sync_list.index(time_now_tag)
            # Get the closest sync. time handle (from the configuration) compared to the present time
            relevant_sync_time = tmp_complete_sync_list[time_now_idx - 1]
            last_complete_sync = dbm_mirror_source_obj.getLastCompleteSyncDic()[relevant_sync_time]
            if not last_complete_sync:
                # The sync time for that cfg sync entry is None -> no sync yet done for that sync time, just do it
                do_complete_sync = True
            else:
                # Check if a complete sync for the relevant time was done within the last 24 hours
                date_now = time_now_tag.split("_")[0]
                date_last_sync = last_complete_sync.split("T")[0]
                if date_now > date_last_sync:
                    do_complete_sync = True

        # Figure out if a partial sync should be done if not a complete sync should be carried out
        if not do_complete_sync:
            last_partial_sync_secs = dbm_mirror_source_obj.getLastSyncTime()
            if (time_now - last_partial_sync_secs) >= dbm_mirror_source_obj.getPeriod():
                do_partial_sync = True

        # If no synchronization to be done for this source, continue to the next mirroring source
        if not do_partial_sync and not do_complete_sync:
            continue

        # Complete sync: Don't specify a lower limit ingestion date
        # Partial sync:  Specify lower limit ingestion date
        # TODO: For now only ingestion date is supported as selection criteria
        max_elements = 100000
        status_cmd_pars = [[NGAMS_HTTP_PAR_FILE_LIST, 1],
                           [NGAMS_HTTP_PAR_UNIQUE, 1],
                           [NGAMS_HTTP_PAR_MAX_ELS, max_elements]]
        if do_partial_sync:
            status_cmd_pars.append([NGAMS_HTTP_PAR_FROM_ING_DATE, toiso8601(dbm_mirror_source_obj.getLastSyncTime())])

        # Go through the list, we shuffle it to get some kind of load balancing
        server_list_indexes = range(len(ngams_server.getSrvListDic()[mirror_source_obj.getId()]))
        random.shuffle(server_list_indexes)
        for server_index in server_list_indexes:
            next_server, next_port = ngams_server.getSrvListDic()[mirror_source_obj.getId()][server_index].split(":")
            next_port = int(next_port)
            msg = "Sending STATUS/file_list request to Source Archive: %s. Node: %s/%d"
            if do_complete_sync:
                msg += ". Complete synchronization"
            else:
                msg += ". Partial synchronization from date: %s" % toiso8601(dbm_mirror_source_obj.getLastSyncTime())
            logger.debug(msg, mirror_source_obj.getId(), next_server, next_port)
            try:
                retrieve_file_list(ngams_server, mirror_source_obj, next_server, next_port, status_cmd_pars,
                                   cluster_files_dbm_name)
                # The retrieval of the file list was successful, we don't need to contacting others of the
                # specified contact nodes
                break
            except Exception as e:
                # Create log entry in case it was not possible to communicate to this Mirroring Source Archive.
                # Continue to the next Mirroring Source Archive in that case.
                logger.error("Error sending STATUS/file_list to Mirroring Source Archive with ID: %s (%s:%d). " 
                             "Error: %s", mirror_source_obj.getId(), next_server, next_port, str(e))
                # Try the next contact node specified in the configuration
                continue

        # Register the times for the last partial or complete sync
        if do_complete_sync:
            dbm_mirror_source_obj.getLastCompleteSyncDic()[relevant_sync_time] = toiso8601(time_now, local=True)
            # Fair enough to consider that a partial sync been done when a complete sync has been carried out
            dbm_mirror_source_obj.setLastSyncTime(time_now)
        elif do_partial_sync:
            dbm_mirror_source_obj.setLastSyncTime(time_now)

        # Store the updated Source Archive Object back into the Source Archive DBM
        ngams_server._srcArchInfoDbm.add(mirror_source_obj.getId(), dbm_mirror_source_obj).sync()

    # Signal to the Mirroring Threads that there might be new Mirroring Requests to handle
    ngams_server.triggerMirThreads()


def check_error_queue(ngams_server):
    """
    Check the Error Queue for failing Mirroring Requests to reschedule into the internal DB Mirroring Queue
    :param ngams_server: Reference to server object (ngamsServer)
    """
    # Go through the list of entries in the Error Queue. Handle as follows:
    # * For entries marked as Error/Reschedule:
    #   * If the time since last activity time is larger than the ErrorRetryPeriod specified in the configuration, put
    #     these back into the Mirroring Queue.
    #   * If the ErrorRetryTimeOut has expired, leave the entry in the queue for the reporting.
    # * For entries marked as Error/Abandon: Leave these in the queue to be handled later by the reporting.

    # We create a snapshot of the keys in the Error DBM, so that we can pass through these without interfering with
    # other activities, and to avoid creating a maybe huge list in memory.
    dbm_name = "%s/%s_%s" % (ngamsHighLevelLib.getNgasChacheDir(ngams_server.getCfg()), "NGAMS_MIR_ERR_QUEUE_KEYS_DBM",
                             ngams_server.getHostInfoObj().getClusterName())
    dbm_name = dump_keys_queue(ngams_server._errQueueDbm, ngams_server._errQueueDbmSem, dbm_name)
    error_queue_keys_dbm = ngamsDbm.ngamsDbm(dbm_name, cleanUpOnDestr=True)
    error_queue_keys_dbm.initKeyPtr()
    while True:
        next_key, data = error_queue_keys_dbm.getNext()
        if not next_key:
            break
        try:
            mirror_request_obj = ngams_server._errQueueDbm.get(next_key)
        except Exception:
            # Maybe this entry was removed by the reporting, ignore, continue to the next entry
            continue
        if mirror_request_obj.getStatusAsNo() == ngamsMirroringRequest.NGAMS_MIR_REQ_STAT_ERR_RETRY_NO:
            # If the time since last activity is longer than ErrorRetryPeriod reschedule the request into the
            # Mirroring Queue
            time_now = time.time()
            if (time_now - mirror_request_obj.getLastActivityTime()) > \
                    ngams_server.getCfg().getMirroringErrorRetryPeriod():
                try:
                    mirror_request_obj = pop_entry_queue(mirror_request_obj, ngams_server._errQueueDbm,
                                                         ngams_server._errQueueDbmSem)
                    add_entry_mirror_queue(ngams_server, mirror_request_obj)
                except Exception as e:
                    logger.error("Error moving Mirroring Request from Error DBM Queue to the Mirroring DBM Queue: %s",
                                 str(e))
        else:
            # NGAMS_MIR_REQ_STAT_ERR_ABANDON_NO: Do nothing
            pass
    ngams_server.triggerMirThreads()


def generate_report(ngams_server):
    """
    Check the queues and report the Mirroring Requests in the
        * Mirroring Queue
        * Completed Queue
        * Error Queue
    A summary report and a detailed report is generated.
    :param ngams_server: Reference to server object (ngamsServer)
    """
    report_header = "Date:         %s\n" +\
                    "Control Node: %s\n\n"
    report_header = report_header % (toiso8601(), ngams_server.getHostId())
    summary = "NGAS MIRRORING - SUMMARY REPORT\n\n" + report_header

    # Go through the various queue DBMs
    dbm_name = "%s/%s_%s" % (ngamsHighLevelLib.getNgasChacheDir(ngams_server.getCfg()),
                             "NGAMS_MIR_REPORINTG_QUEUE_KEYS_DBM",
                             ngams_server.getHostInfoObj().getClusterName())

    # Go through Completed Queue
    dbm_name = dump_keys_queue(ngams_server._complQueueDbm, ngams_server._complQueueDbmSem, dbm_name)
    completed_queue_keys_dbm = ngamsDbm.ngamsDbm(dbm_name, cleanUpOnDestr=True)
    completed_queue_keys_dbm.initKeyPtr()
    completed_count = 0

    while True:
        next_key, mirror_request = completed_queue_keys_dbm.getNext()
        if not next_key:
            break
        try:
            pop_entry_queue(mirror_request, ngams_server._complQueueDbm, ngams_server._complQueueDbmSem)
            completed_count += 1
        except Exception as e:
            logger.error("Error popping Mirroring Request from the Completed DBM Queue: %s", str(e))

    summary += "Completed Requests:    %d\n" % completed_count

    # Go through Error Queue
    dbm_name = dump_keys_queue(ngams_server._errQueueDbm, ngams_server._errQueueDbmSem, dbm_name)
    error_retry_count = 0
    error_timeout_count = 0
    error_abandon_count = 0
    error_queue_keys_dbm = ngamsDbm.ngamsDbm(dbm_name, cleanUpOnDestr=True)
    error_queue_keys_dbm.initKeyPtr()

    while True:
        next_key, mirror_request = error_queue_keys_dbm.getNext()
        if not next_key:
            break
        if mirror_request.getStatusAsNo() == ngamsMirroringRequest.NGAMS_MIR_REQ_STAT_ERR_ABANDON:
            try:
                pop_entry_queue(mirror_request, ngams_server._errQueueDbm, ngams_server._errQueueDbmSem)
                error_abandon_count += 1
            except Exception as e:
                logger.error("Error popping Mirroring Request from Error DBM Queue: %s", str(e))
        elif (time.time() - mirror_request.getSchedulingTime()) > ngams_server.getCfg().getMirroringErrorRetryPeriod():
            try:
                pop_entry_queue(mirror_request, ngams_server._errQueueDbm, ngams_server._errQueueDbmSem)
                error_timeout_count += 1
            except Exception as e:
                logger.error("Error popping Mirroring Request from Error DBM Queue: %s", str(e))
        else:
            error_retry_count += 1
    summary += "Error Request/Retry:   %d\n" % error_retry_count
    summary += "Error Request/Timeout: %d\n" % error_timeout_count
    summary += "Error Request/Abandon: %d\n" % error_abandon_count

    # Go through Mirroring Queue
    dbm_name = dump_keys_queue(ngams_server._mirQueueDbm, ngams_server._mirQueueDbmSem, dbm_name)
    mirror_queue_keys_dbm = ngamsDbm.ngamsDbm(dbm_name, cleanUpOnDestr=True)
    mirror_queue_keys_dbm.initKeyPtr()
    mirror_queue_count = 0

    while True:
        next_key, mirror_request = mirror_queue_keys_dbm.getNext()
        if not next_key:
            break
        mirror_queue_count += 1
    summary += "Mirroring Queue:    %d\n" % mirror_queue_count

    # Submit report to the specified recipients
    if ngams_server.getCfg().getMirroringReportRecipients():
        report_recipients = str(ngams_server.getCfg().getMirroringReportRecipients()).strip()
        if not report_recipients:
            return
        subject = "NGAS MIRRORING SERVICE STATUS REPORT"
        for recipient in report_recipients.split(","):
            ngamsHighLevelLib.sendEmail(ngams_server.getCfg(), subject, [recipient], ngams_server.getCfg().getSender(),
                                        summary, "text/plain")


def clean_up_mirroring(ngams_server):
    host = get_fully_qualified_domain_name(ngams_server)
    logger.debug("Cleaning up mirroring tasks for NGAS node: %s", host)
    # An ngams server may have been restarted (or killed / crashed / rebooted) while mirroring. We
    # need to clean up older files otherwise mirroring wil ignore them, thinking they are already
    # being processed. In the general case we should only clean up files which have been assigned 
    # to the server which is restarting. The other servers in the cluster may be happily mirroring.
    # In a highly unlikely case we could also have killed the mirroring master before files have 
    # been assigned to a target host. Any node can clean these up, but only for previous iterations.
    sql = """
      update ngas_mirroring_bookkeeping 
      set status = 'ABORTED', staging_file = null
      where status in ('READY', 'LOCKED') 
      and (
        target_host = {0}
        or (
          target_host is null 
          and iteration < (
            select max(iteration)
            from ngas_mirroring_bookkeeping
          )
        )
      )
    """
    ngams_server.getDb().query2(sql, args=(host,))
    sql = "update ngas_mirroring_bookkeeping set status = 'TORESUME' where status = 'FETCHING' and target_host = {0}"
    ngams_server.getDb().query2(sql, args=(host,))


def get_fully_qualified_domain_name(ngams_server):
    """
    Get full qualified server name for the input NGAS server object
    :param ngams_server: ngamsServer, Reference to NG/AMS server class object
    :return: string, full qualified host name (host name + domain + port)
    """
    # Get hots_id, domain and port using ngamsLib functions
    host_id = ngams_server.getHostId()
    domain = ngamsLib.getDomain()
    port = str(ngams_server.getCfg().getPortNo())
    # Concatenate all elements to construct full qualified name
    # Notice that host_id may contain port number
    fqdn = (host_id.rsplit(":"))[0] + "." + domain + ":" + port
    return fqdn


def get_mirroring_sleep_time(ngams_server):
    """
    Get the sleep time parameter value from ngas_cfg_pars DB table
    """
    sql = "select cfg_val from ngas_cfg_pars where cfg_par = 'sleepTime'"

    logger.debug("Executing SQL query to get mirroring sleep time: %s", sql)
    sleep_time = float(ngams_server.getDb().query2(sql)[0][0])

    logger.debug("Mirroring sleep time value is %.3f", sleep_time)
    return sleep_time


def mirror_control_thread(ngams_server, stop_event):
    """
    The Mirroring Control Thread runs periodically when the NG/AMS Server is Online (if enabled) to synchronize the
    data holding of the local NGAS Cluster against a set of remote NGAS Clusters.
    :param ngams_server: Reference to server object (ngamsServer)
    :param stop_event: Stop event
    """
    if ngams_server.getCfg().getVal("Mirroring[1].AlmaMirroring"):
        logger.info("ALMA mirroring control thread cleaning up from previous state")
        clean_up_mirroring(ngams_server)

        logger.info("ALMA mirroring control thread entering main server loop")
        while True:
            # Encapsulate this whole block to avoid that the thread dies in case a problem occurs,
            # like e.g. a problem with the DB connection
            try:
                check_stop_mirror_control_thread(stop_event)
                logger.debug("ALMA Mirroring Control Thread starting next iteration ...")

                # Update mirroring book keeping table
                # TODO: handle the response, there's no information whatsoever if things went fine or not
                logger.debug("ALMA Mirroring Control Thread updating book keeping table ...")
                local_server_contact_ip = get_contact_ip(ngams_server.getCfg())
                timeout = 6 * 3600
                ngamsHttpUtils.httpGet(local_server_contact_ip, ngams_server.portNo, 'MIRRTABLE', timeout=timeout)

                # Sleep to let Janitor Thread and Data Check do their tasks always reload from DB to allow for updates
                # without restarting the server
                sleep_time = get_mirroring_sleep_time(ngams_server)
                logger.info("ALMA mirroring control thread sleeping for %.3f [s]", sleep_time)
                suspend(stop_event, sleep_time)
            except MirroringStoppedException as e:
                return
            except socket.error as e:
                if e.errno == errno.ECONNREFUSED:
                    logger.warning("Server not up and running yet, postponing mirroring")
                    try:
                        sleep_time = get_mirroring_sleep_time(ngams_server)
                        logger.info("ALMA mirroring control thread sleeping for %.3f [s]", sleep_time)
                        suspend(stop_event, sleep_time)
                    except MirroringStoppedException as e:
                        return
            except Exception as e:
                logger.exception("Error occurred during execution of the ALMA mirroring control thread")
                # We make a small wait here to avoid that the process tries too often to carry out tasks that failed
                if stop_event.wait(5.0):
                    return
    else:
        # Generic Mirroring service
        initialise_mirroring(ngams_server)

        # Start the Mirroring Threads
        start_mirroring_threads(ngams_server, stop_event)

        # Render the suspension time as the minimum time for checking a remote archive
        period = 24 * 3600
        for mirror_source_obj in ngams_server.getCfg().getMirroringSrcList():
            tmp_period = float(mirror_source_obj.getPeriod())
            if tmp_period < period:
                period = tmp_period

        logger.debug("Mirroring Control Thread entering main server loop")
        while True:
            start_time = time.time()

            # Encapsulate this whole block to avoid that the thread dies in case a problem occurs,
            # like e.g. a problem with the DB connection
            try:
                check_stop_mirror_control_thread(stop_event)
                logger.debug("Mirroring Control Thread starting next iteration ...")

                # Check if there are new data objects in the specified source archives to mirror. While checking
                # this the Mirroring Threads should be paused, since otherwise, inconsistencies may occur
                # (e.g. a file scheduled several times).
                try:
                    pause_mirror_threads(ngams_server, stop_event)
                    check_source_archives(ngams_server)
                except MirroringStoppedException as e:
                    raise e

                resume_mirror_threads(ngams_server, stop_event)

                # Check if there are entries in Error State, which should be resumed
                check_error_queue(ngams_server)

                # Check if there entries to be reported in the queues and generate the report(s)
                generate_report(ngams_server)

                # Suspend the Mirroring Control Thread for a while
                suspend_time = period - (time.time() - start_time)
                if suspend_time < 1:
                    suspend_time = 1
                logger.debug("Mirroring control thread executed - suspending for %s [s]", str(suspend_time))
                suspend(stop_event, suspend_time)
            except MirroringStoppedException as e:
                return
            except socket.error as e:
                if e.errno == errno.ECONNREFUSED:
                    logger.warning("Server not up and running yet, postponing mirroring")
                    try:
                        suspend_time = period - (time.time() - start_time)
                        if suspend_time < 1:
                            suspend_time = 1
                        logger.debug("Mirroring control thread executed - suspending for %s [s]", str(suspend_time))
                        suspend(stop_event, suspend_time)
                    except MirroringStoppedException as e:
                        return
            except Exception as e:
                error_message = "Error occurred during execution of the mirroring control thread"
                logger.exception(error_message)
                # We insert a short delay here to help avoid that the process tries too often to carry out the task
                # that caused the mirroring process to fail
                if stop_event.wait(5.0):
                    return
