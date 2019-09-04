#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2019
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
"""Module containing the ngas-fs-monitor client"""
import argparse
import functools
import glob
import logging
import multiprocessing
import os
import signal
import socket
import sys
import time

import lockfile.pidlockfile

from ngamsLib import utils, logutils
from ngamsLib.ngamsCore import mvFile, rmFile, checkCreatePath
from six.moves import queue, cPickle

from . import ngamsPClient


logger = logging.getLogger(__name__)

class archive_request(object):
    """An archiving request. ``file_id`` and ``file_version`` are set after a
    successful archiving and are used for checking the file later"""

    def __init__(self, fname):
        """Creates a request to archive file ``fname``"""
        self.fname = fname
        self.file_id = None
        self.file_version = None
        self.sched_time = time.time()
        self.last_check_attempt = 0

    def move_to(self, dirname):
        logger.info('Moving %s to %s', self.fname, dirname)
        basename = os.path.basename(self.fname)
        new_fname = os.path.join(dirname, basename)
        mvFile(self.fname, new_fname)
        self.fname = new_fname

class ClientException(Exception):
    pass

class ClientWrapper(object):
    """A simple wrapper around an NGAS client that only throws ClientException"""

    def __init__(self, host, port, servers):
        self._client = ngamsPClient.ngamsPClient(host, port, servers)

    def client(self, what, *args, **kwargs):
        method = getattr(self._client, what)
        try:
            return method(*args, **kwargs)
        except socket.error as e:
            raise ClientException(e)

def periodic(period_attr):
    """Wraps a function into a main loop with the given periodicity"""
    def decorator(f):
        @functools.wraps(f)
        def _wrapper(self, *args):
            stop_evt = args[-1]
            period = getattr(self, period_attr)
            while not stop_evt.is_set():
                f(self, *args)
                stop_evt.wait(period)
        return _wrapper
    return decorator

def periodic_queue_consumer(poll_period_attr_name):
    """Wraps a function into a main loop where elements are drawn from a queue
    and given to the target function"""
    def decorator(f):
        @functools.wraps(f)
        def _wrapper(self, *args):
            the_queue = args[0]
            stop_evt = args[-1]
            args = args[1:-1]
            queue_poll_period = getattr(self, poll_period_attr_name)
            while not stop_evt.is_set():
                try:
                    req = the_queue.get_nowait()
                except queue.Empty:
                    stop_evt.wait(queue_poll_period)
                    continue
                try:
                    f(self, req, *args)
                except ClientException as e:
                    logger.exception('Error while communicating with the server, will try again in %f seconds',
                                     self.client_retry_period, exc_info=e.args[0])
                    the_queue.put_nowait(req)
                    stop_evt.wait(self.client_retry_period)
        return _wrapper
    return decorator

def flush(the_queue):
    """Iterates over all elements of the queue while they are removed"""
    try:
        while True:
            yield the_queue.get_nowait()
    except queue.Empty:
        pass

class Monitor(ClientWrapper):
    """Class implementing monitoring + archiving functionality"""

    def __init__(self, root_dir, host='127.0.0.1', port=7777, servers=None,
                 num_streams=1, fs_poll_period=30, archive_poll_period=30,
                 check_poll_period=30, client_retry_period=60,
                 cleanup_timeout=60, archiving_cmd='ARCHIVE'):

        ClientWrapper.__init__(self, host, port, servers)
        self.stopping = False
        self.num_streams = num_streams
        self.fs_poll_period = fs_poll_period
        self.archive_poll_period = archive_poll_period
        self.check_poll_period = check_poll_period
        self.client_retry_period = client_retry_period
        self.cleanup_timeout = cleanup_timeout
        self.archiving_cmd = archiving_cmd
        self.all_tasks = []

        # Paths to be created
        root_dir = os.path.normpath(os.path.abspath(root_dir))
        _relative = lambda p: os.path.join(root_dir, p)
        self.queue_dir = _relative('queue')
        self.archiving_dir = _relative('archiving')
        self.archived_dir = _relative('archived')
        self.badfiles_dir = _relative('bad')
        self.backlog_dir = _relative('backlog')
        self.pickled_check_reqs = os.path.join(self.archived_dir, '.check_requests.pickle')
        for d in (self.queue_dir, self.archiving_dir, self.archived_dir,
                  self.badfiles_dir, self.backlog_dir):
            checkCreatePath(d)

        self.start_tasks()
        logger.debug('Done starting tasks')

    def start_tasks(self):

        logger.info('Starting background tasks')
        self.archive_queue = multiprocessing.Queue()
        self.logs_queue = multiprocessing.Queue()
        self.check_queue = multiprocessing.Queue()

        self.load_pending_checks()

        divisor = 4
        num_check_tasks = (self.num_streams + divisor - 1) // divisor
        stop_evt = multiprocessing.Event()

        def add_task(name, target, *args):
            def _target(*args):
                self._setup_child_logging(self.logs_queue)
                target(*args)
            task = utils.Task(name, _target,
                          mode=utils.Task.PROCESS, stop_evt=stop_evt)
            task.start(*args)
            self.all_tasks.append(task)

        add_task('File system poller', self.fs_polling)
        for i in range(self.num_streams):
            add_task('Archiving %d' % i, self.process_archive_request, self.archive_queue)
        for i in range(num_check_tasks):
            add_task('Checking %d' % i, self.process_check_request, self.check_queue)

        task = utils.Task('Log handling', self.handle_child_log_records)
        task.start()
        self.all_tasks.append(task)

    def handle_child_log_records(self, stop_evt):
        while not stop_evt.is_set():
            try:
                record = self.logs_queue.get(timeout=0.1)
                logger.handle(record)
            except queue.Empty:
                continue

    def _setup_child_logging(self, queue):
        for h in list(logging.root.handlers):
            logging.root.removeHandler(h)
        logging.root.addHandler(logutils.ForwarderHandler(queue.put_nowait))

    def stop(self, timeout=10):
        """Stop this client and its background tasks"""
        if self.stopping:
            return
        self.stopping = True
        for task in self.all_tasks:
            task.stop(timeout)
        self.dump_pending_checks()
        self.restore_pending_archives()

    def load_pending_checks(self):
        if not os.path.exists(self.pickled_check_reqs):
            return
        with open(self.pickled_check_reqs, 'rb') as f:
            pending_reqs = cPickle.load(f)
        for req in pending_reqs:
            self.check_queue.put_nowait(req)

    def dump_pending_checks(self):
        pending_check_requests = list(flush(self.check_queue))
        if pending_check_requests:
            with open(self.pickled_check_reqs, 'wb') as f:
                cPickle.dump(pending_check_requests, f)
        elif os.path.exists(self.pickled_check_reqs):
            rmFile(self.pickled_check_reqs)

    def restore_pending_archives(self):
        for req in flush(self.archive_queue):
            req.move_to(self.queue_dir)

    @periodic('fs_poll_period')
    def fs_polling(self, _stop_evt):
        queue_dir_pattern = os.path.join(self.queue_dir, '*')
        for fname in glob.glob(queue_dir_pattern):
            req = archive_request(fname)
            req.move_to(self.archiving_dir)
            self.archive_queue.put_nowait(req)
            logger.info('Added archive request to queue for %s', fname)

    @periodic_queue_consumer('archive_poll_period')
    def process_archive_request(self, req):

        stat = self.client('archive', req.fname, cmd=self.archiving_cmd)

        # Archive Request after-math.
        status = stat.getStatus()
        msg = stat.getMessage()
        if status != 'SUCCESS':
            if ('NGAMS_ER_DAPI_BAD_FILE' in msg or 'NGAMS_ER_UNKNOWN_MIME_TYPE1' in msg):
                log = "%s classified as bad by server, giving up on archiving"
                logger.error(log, req.fname)
                req.move_to(self.badfiles_dir)
            elif 'NGAMS_WA_BUF_DATA' in msg:
                log = "%s was back-log buffered, assuming eventual correct archiving."
                logger.warning(log, req.fname)
                req.move_to(self.backlog_dir)
            else:
                log = 'Archiving of %s failed (error: %s), will try again later'
                logger.warning(log, req.fname, msg)
                self.archive_queue.put_nowait(req)
            return

        logger.info("%s successfully archived", req.fname)
        file_info = stat.getDiskStatusList()[0].getFileObj(0)
        req.file_id = file_info.getFileId()
        req.file_version = file_info.getFileVersion()
        req.move_to(self.archived_dir)
        self.check_queue.put_nowait(req)

    @periodic_queue_consumer('check_poll_period')
    def process_check_request(self, req):

        # Is file 'ripe' for removal?
        logger.info("Checking if file is ready for removal: %s", req.fname)
        now = time.time()
        if now - req.sched_time < self.cleanup_timeout:
            # Nope, put it in the archive request list/dictionary.
            self.check_queue.put_nowait(req)
            return

        # Handle the CHECKFILE Request
        pars = ("file_id", req.file_id), ("file_version", req.file_version)
        stat = self.client('get_status', 'CHECKFILE', pars=pars)
        msg = stat.getMessage()
        if 'NGAMS_INFO_FILE_OK' in msg:
            logger.info('%s successfully validated, removing', req.fname)
            rmFile(req.fname)
        else:
            log = "%s could not be validated successfully, will try again later: %s"
            logger.warning(log, req.fname, msg)
            self.check_queue.put_nowait(req)


def setup_logging(logdir, level, log_rotation_period):
    checkCreatePath(logdir)
    formatter = logutils.get_formatter(include_pid=True)
    file_hnd = logutils.RenamedRotatingFileHandler(
        os.path.join(logdir, 'monitor.log'), log_rotation_period,
        os.path.join(logdir, 'ROTATED_LOG_%s.log'))
    sysout_hnd = logging.StreamHandler(sys.stdout)
    for hnd in (file_hnd, sysout_hnd):
        hnd.setLevel(level)
        hnd.setFormatter(formatter)
        logging.root.addHandler(hnd)
    logging.root.setLevel(level)


_desc = r'''
A tool that monitors the file system and archives new files into an NGAS server.

This tool works within the boundaries of a working directory (-w option). Within
this, different sub-directories are found:

 * queue/ is constantly monitored for new files. New files appearing under this
   directory are eventually queued for archiving into an NGAS server.
 * archiving/ holds the files that were previously under queue/ and are actively
   queued for archiving. When the tool stops, any file in this directory is put
   back into queue/
 * archived/ holds the files that have been successfully archived into the NGAS
   server, but are pending a further check to validate they have been received
   correctly. After this check has been performed files are finally removed.
   When the tool stops, any file in this directory stays here for future
   checking.
 * bad/ holds files that have been submitted for archiving, but which the NGAS
   server refused to archive as they were deemed "bad". No further action is
   taken on these files.

In other words, this is a file's lifecycle:

    /----------\
    |          |
    v          |
 queue --> archiving --> archived -> (file removed)
              / \
             /   \
            v     v
           bad   backlog

The tool runs infinitely until stopped via SIGINT/SIGTERM (e.g., hitting Ctrl-C).
'''
def main():
    """Main function to execute the tool from the command-line"""

    parser = argparse.ArgumentParser(
        description=_desc, usage='%(prog)s [options]',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    general_group = parser.add_argument_group('General options')
    general_group.add_argument('-w', '--workdir', help='The working directory where this tool wil run, defaults to .', default='.')
    general_group.add_argument('-C', '--command', help='Command used to issue archiving, defaults to ARCHIVE', default='ARCHIVE')
    general_group.add_argument('-s', '--streams', help='Number or parallel archiving streams, defaults to #CPUs', default=multiprocessing.cpu_count(), type=int)
    connection_group = parser.add_argument_group('Connection options')
    connection_group.add_argument('-H', '--host', help='The host to connect to, defaults to 127.0.0.1', default='127.0.0.1')
    connection_group.add_argument('-p', '--port', help='The port to connect to, defaults to 7777', type=int, default=7777)
    connection_group.add_argument('--servers', help='A comma-separated list of host:server addresses', default='')
    timing_group = parser.add_argument_group('Timing options')
    timing_group.add_argument('-f', '--fs-poll-period', help='Period used to poll files, in seconds. Defaults to 30', default=30, type=float)
    timing_group.add_argument('-r', '--client-retry-period', help='Period to pause between intermittent client<->server communication failures, in seconds. Defaults to 30', default=30, type=float)
    timing_group.add_argument('-c', '--cleanup-timeout', help="Time after which successfully archived files will be automatically deleted, in seconds. Defaults to 604800 (1 week)", default=604800)
    logging_group = parser.add_argument_group('Logging options')
    logging_group.add_argument('--log-level', help="Log level to apply when producing logs", default='INFO', choices=['DEBUG', 'INFO', 'WARNING'])
    logging_group.add_argument('--log-rotation-period', help='Log rotation period, in seconds, defaults to 86400 (1 day)', default=86400, type=float)

    opts = parser.parse_args()
    servers = [(s.split(':')[0], s.split(':')[1]) for s in opts.servers.split(',') if s]

    checkCreatePath(opts.workdir)
    pidfile = os.path.join(opts.workdir, 'monitor.pid')

    with lockfile.pidlockfile.PIDLockFile(pidfile, timeout=1):
        setup_logging(os.path.join(opts.workdir, 'log'), opts.log_level, opts.log_rotation_period)
        monitor = Monitor(opts.workdir, host=opts.host, port=opts.port,
                          servers=servers, num_streams=opts.streams,
                          fs_poll_period=opts.fs_poll_period,
                          client_retry_period=opts.client_retry_period,
                          cleanup_timeout=opts.cleanup_timeout,
                          archiving_cmd=opts.command)
        # Let SIGTERM also raise a KeyboardInterrupt
        signal.signal(signal.SIGTERM, signal.getsignal(signal.SIGINT))
        try:
            signal.pause()
        except KeyboardInterrupt:
            monitor.stop(30)
        logging.shutdown()

if __name__ == '__main__':
    main()