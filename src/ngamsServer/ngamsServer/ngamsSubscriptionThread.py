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
# "@(#) $Id: ngamsSubscriptionThread.py,v 1.10 2009/11/25 21:47:11 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  06/11/2002  Created
#
"""
This module contains the code for the Subscription Thread, which is
used to handle the delivery of data to Subscribers.
"""

import collections
import functools
import logging
import multiprocessing.pool
import time
import operator
import os
from Queue import Queue, Empty

from ngamsLib.ngamsCore import isoTime2Secs,\
    NGAMS_SUCCESS, getFileSize, loadPlugInEntryPoint,\
    NGAMS_HTTP_HDR_CHECKSUM, fromiso8601
from ngamsLib import ngamsStatus, ngamsHighLevelLib, ngamsHttpUtils
import ngamsCacheControlThread


logger = logging.getLogger(__name__)


# Types used throughout this module: delivery and delivery_ext
_fields = 'file_id file_version disk_id subscriber_id'
delivery = collections.namedtuple('delivery', _fields)
_fields = ('file_id file_version disk_id filename mount_point mime_type'
           ' checksum checksum_type subscriber_id priority url')
delivery_ext = collections.namedtuple('delivery_ext', _fields)
del _fields

class StopSubscriptionThread(Exception):
    """Thrown to signal the stop of the subscription thread logic"""
    pass

class DeliveryError(Exception):
    """Thrown if there is an error during delivery of a particular file"""
    pass

def check_stop(stop_evt):
    if stop_evt.is_set():
        raise StopSubscriptionThread()

def filter_with_plugin(srv, plugin, plugin_pars, file_info):
    file_id, file_name, file_version = file_info
    logger.debug('Invoking filter plug-in %r for file with name/id/version %s/%d/%s',
                 file_name, file_id, file_version)
    return plugin(srv, plugin_pars, file_name, file_id, file_version)

def add_deliveries_for_new_subscriber(srv, subscriber):
    """Adds all deliveries associated with this new subscriber to the persistent queue"""

    # Avoid any further processing if we know there will be no matches
    if subscriber.start_date > time.time():
        return

    # Get files that the new subscriber must deliver.
    to_deliver = srv.db.getFileSummary2(hostId=srv.getHostId(),
                                        ing_date=subscriber.start_date, fetch_size=100)

    # Now reduce by (file_id, file_version) to get unique rows even if there
    # are multiple copies of a given file. This is similar to how the old code
    # implemented the logic of augmenting a (file_id, file_version) pair to a
    # (file_id, file_version, disk_id) triplet without duplicates.
    to_deliver = {(x[0], x[3]): x for x in to_deliver}.values()

    # Filter the list further if a plug-in is given
    if subscriber.filter_plugin:
        plugin = loadPlugInEntryPoint(subscriber.filter_plugin)
        do_filtering = functools.partial(filter_with_plugin, srv, plugin, subscriber.filter_plugin_pars)
        to_deliver = filter(do_filtering, [(fid, fname, fversion) for fid,_,fname,fversion,_,_,_ in to_deliver])

    # Add all entries to the persistent delivery queue
    logger.info('Adding %d deliveries to queue for subscriber %s',
                len(to_deliver), subscriber.id)
    to_deliver = [delivery(fid, fversion, did, subscriber.id)
                  for fid,_,_,fversion,_,_,did in to_deliver]
    srv.db.add_to_delivery_queue(to_deliver)

def add_deliveries_for_new_file(srv, file_info):
    """Adds all deliveries associated with this new file to the persistent queue"""

    # Get all active subscribers for this server and see which ones
    # should receive a copy a this file
    to_deliver = []
    for subscriber in srv.db.get_subscriber(hostId=srv.getHostId(), active=True):

        # Each subscriber filers by start date and by plug-in
        if (subscriber.start_date is None and
            fromiso8601(subscriber.start_date, local=True) > file_info.ingestion_date):
            continue

        if subscriber.filter_plugin:
            plugin = loadPlugInEntryPoint(subscriber.filter_plugin)
            info = file_info.file_id, file_info.file_name, file_info.file_version
            if not filter_with_plugin(srv, plugin, subscriber.filter_plugin_pars, info):
                continue

        to_deliver.append(delivery(file_info.file_id, file_info.file_version,
                                   file_info.disk_id, subscriber.id))

    logger.info('Adding %d deliveries queue for file_id/file_version %s/%d',
                len(to_deliver), file_info.file_id, file_info.file_version)
    srv.db.add_to_delivery_queue(to_deliver)

def subscriptionThread(srv, run_evt, stop_evt):
    """Main routine of the subscription thread"""

    # TODO: we are hardcoding 10 threads here, make it configurable
    thread_pool = multiprocessing.pool.ThreadPool(processes=10)
    susp_time = isoTime2Secs(srv.cfg.getSubscrSuspTime())

    try:
        while True:
            subscription_major_cycle(srv, stop_evt, thread_pool)

            # React to full stop and "run now" events sent by the server
            # Otherwise, the thread will sleep for the configured time
            if run_evt.wait(susp_time):
                run_evt.clear()
            check_stop(stop_evt)

    except StopSubscriptionThread:
        logger.info('Exiting from subscription thread')
        return

def subscription_major_cycle(srv, stop_evt, thread_pool):

    # Find all file deliveries that are still pending
    # and filter out those that we know won't be reachable
    deliveries = [delivery_ext(*d) for d in srv.db.get_all_deliveries(srv.getHostId())]
    logger.info('Found %d deliveries to make from %s', len(deliveries), srv.getHostId())
    deliveries = filter_hosts_down(deliveries, thread_pool)
    logger.info('After filtering by host-pinging, %d deliveries are still to be carried out', len(deliveries))
    if not deliveries:
        return

    check_stop(stop_evt)

    # Sort by priority so high-priority deliveries get delivered first
    deliveries.sort(key=operator.attrgetter('priority'))

    # Add all deliveries to a queue
    #
    # TODO: consider the following, which will affect how deliveries are sorted
    # first, and how they interact with the working threads:
    #
    #  * Check the "data-mover-only" logic. What is "data-mover-only" anyway?
    #
    #  * Number of concurrent publishing threads works on a per-subscriber basis
    #
    #  * Is it better (or not?) to pack deliveries by physical file? I guess
    #    this makes sense only if we need to stage it.
    q = Queue()
    for d in deliveries:
        q.put(d)

    # Let the worker threads do the delivery now
    # We use pool.map with a dummy iterable, but actually use the queue for
    # picking up the work and deciding when each thread finishes
    thread_pool.map(functools.partial(do_delivery, q, srv, stop_evt), range(10))

def filter_hosts_down(deliveries, thread_pool):

    # Convert URLs to host/port pairs
    remotes = set(map(ngamsHttpUtils.host_port, [d.url for d in deliveries]))
    logger.info('Pinging the following remote endpoints: %s', ', '.join('%s:%d' % (h,p) for h,p in remotes))

    # Ping each host/port combination and check if it's alive
    def ping(host_and_port):
        try:
            host, port = host_and_port
            ngamsHighLevelLib.pingServer(host, port, 10)
            return host, port
        except:
            logger.warning('Server at %s:%d is down, will filter out deliveries going there', host, port)
            return None
    remotes_up = filter(None, thread_pool.map(ping, remotes))

    # Make them look like URLs again and check the deliveries against these
    urls_up = ['http://%s:%d' % (h, p) for h, p in remotes_up]
    return filter(lambda d: any(map(lambda u: d.url.startswith(u), urls_up)), deliveries)

def do_delivery(q, srv, stop_evt, _):
    """Continuously steal delivery work from the queue and try to perform it"""

    try:
        check_stop(stop_evt)
        try:
            d = q.get_nowait()
        except Empty:
            # No more work, bye!
            return
        else:
            _do_delivery(srv, d)
    except StopSubscriptionThread:
        logger.info('Exiting from delivery thread')
        return

def stageFile(srvObj, filename):
    """Stage a file, if required"""
    fspi = srvObj.getCfg().getFileStagingPlugIn()
    if not fspi:
        return
    try:
        logger.debug("Invoking FSPI.isFileOffline: %s to check file: %s", fspi, filename)
        isFileOffline = loadPlugInEntryPoint(fspi, 'isFileOffline')
        if isFileOffline(filename) == 1:
            logger.debug("File %s is offline, staging for delivery...", filename)
            stageFiles = loadPlugInEntryPoint(fspi, 'stageFiles')
            stageFiles(filenames = [filename], serverObj = srvObj)
            logger.debug("File %s staging completed for delivery.", filename)
    except Exception as ex:
        logger.error("File staging error: %s", filename)
        raise ex


def _do_delivery(srv, deliv):

    # Make sure the file is staged
    filename = os.path.join(deliv.mount_point, deliv.filename)
    baseName = os.path.basename(filename)
    stageFile(srv, filename)

    # Prepare information for the HTTP transfer
    contDisp = 'attachment; filename="{0}"; file_id={1}'.format(baseName, deliv.file_id)
    hdrs = {}
    pars = []
    if deliv.checksum:
        hdrs[NGAMS_HTTP_HDR_CHECKSUM] = deliv.checksum
        pars.append(('crc_variant', deliv.checksum_type))

    # Go, go, go!
    try:
        start = time.time()
        with open(filename, "rb") as f:
            ret = ngamsHttpUtils.httpPostUrl(deliv.url, f, deliv.mime_type,
                                             pars=pars, hdrs=hdrs,
                                             contDisp=contDisp, timeout=120)
            http_status,_,_,data = ret
            ngams_status = ngamsStatus.to_status((http_status, data), 'dummy', 'dummy')
            if ngams_status.getStatus() != NGAMS_SUCCESS:
                raise DeliveryError()
    except DeliveryError:
        msg = "Error while delivering %r. HTTP status: %d, NGAS message: %s. Will try again later"
        logger.error(msg, deliv, http_status, ngams_status.getMessage())
        return
    except:
        logger.exception("Unexpected error while delivering %r. Will try again later", deliv)
        return

    # Tell us how we did
    duration = time.time() - start
    fsize_mb = getFileSize(filename) / 1024. / 1024.
    logger.info("Successfully delivered %r in %.3f [s] (%.3f [MB/s])",
                deliv, duration, fsize_mb / duration)

    # Most importantly: remove delivery item from the queue
    try:
        logger.info("Removing %r from delivery queue", deliv)
        is_last_file = srv.db.remove_from_delivery_queue(deliv.subscriber_id, deliv.file_id,
                                                         deliv.file_version, deliv.disk_id)
    except:
        logger.exception("Failed to remove %r from queue, expect possible duplications", deliv)
    else:
        # ... and signal the cache system if the file is available for future deletion
        if is_last_file and srv.getCachingActive():
            sqlFileInfo = (deliv.disk_id, deliv.file_id, deliv.file_version)
            ngamsCacheControlThread.requestFileForDeletion(srv, sqlFileInfo)