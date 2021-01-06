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

import os
import subprocess
import sys
import time

from ngamsLib.ngamsCore import cpFile, terminate_or_kill
from ngamsPClient.fs_monitor import Monitor

from ..ngamsTestLib import tmp_path, save_to_tmp, ngamsTestSuite
from test.ngamsTestLib import pollForFile


class FsMonitorTests(ngamsTestSuite):

    def setUp(self):
        super(FsMonitorTests, self).setUp()
        self.workdir = tmp_path('fs-monitor-workdir')
        self.monitor = None

    def create_monitor(self, *args, **kwargs):
        self.monitor = Monitor(self.workdir, host='127.0.0.1', port=8888,
                               *args, **kwargs)
        self.monitor.start_tasks()

    def queue_smallfits(self, basename='test.fits'):
        cpFile(self.resource('src/SmallFile.fits'),
               os.path.join(self.monitor.queue_dir, basename))

    def assert_monitor_files(self, basename='test.fits', queued=0, archiving=0,
                             archived=0, bad=0, backlog=0):
        dirs = ('queue_dir', 'archiving_dir', 'archived_dir',
                'badfiles_dir', 'backlog_dir')
        amounts = (queued, archiving, archived, bad, backlog)
        for dirname, amount in zip(dirs, amounts):
            fname = os.path.join(getattr(self.monitor, dirname), basename)
            pollForFile(fname, amount)

    def start_srv(self, cfg=(), **kwargs):
        cfg += (('NgamsCfg.ArchiveHandling[1].EventHandlerPlugIn[1].Name', 'test.ngamsTestLib.SenderHandler'),)
        self.prepExtSrv(cfgProps=cfg, **kwargs)

    def tearDown(self):
        if self.monitor:
            self.monitor.stop()
        super(FsMonitorTests, self).tearDown()

    def test_double_stop(self):
        self.create_monitor()
        self.monitor.stop()

    def test_simple_archive(self):
        """Start a server, then a monitor, put a file for the monitor to pick up
        and make sure it was archived"""
        self.start_srv()
        archive_listener = self.notification_listener()
        self.create_monitor(fs_poll_period=0.1, archive_poll_period=0.1, cleanup_timeout=0.1)
        self.queue_smallfits()
        self.assertIsNotNone(archive_listener.wait_for_file(10))
        self.retrieve("TEST.2001-05-08T15:25:00.123", targetFile=tmp_path())
        archive_listener.close()
        self.assert_monitor_files(archived=1)

    def test_archive_after_server_start(self):
        """Start the monitor, trigger a file archival with the server off;
        let the archiving fail, start the server, everything is fine"""

        poll_period = 0.1
        archive_listener = self.notification_listener()
        self.create_monitor(fs_poll_period=poll_period, archive_poll_period=poll_period,
                            client_retry_period=poll_period)
        self.queue_smallfits()
        time.sleep(poll_period * 2)

        self.start_srv()
        self.assertIsNotNone(archive_listener.wait_for_file(10))
        self.retrieve("TEST.2001-05-08T15:25:00.123", targetFile=tmp_path())
        archive_listener.close()

    def test_pending_checks_are_pickled(self):
        """Like a simple archive test, but also makes sure pending checks are
        pickled on stop() and then picked up again"""
        self.test_simple_archive()
        self.monitor.stop()
        self.assert_monitor_files(archived=1)
        self.create_monitor(cleanup_timeout=0.1)
        time.sleep(1)
        self.assert_monitor_files()

    def test_unfinished_archive_goes_to_queue(self):
        """An uncompleted archiving operation should go back to the queue dir"""

        # We start a server that takes 5 seconds to do ARCHIVEs
        save_to_tmp("handleHttpRequest_Block5secs", fname="handleHttpRequest_tmp")
        self.start_srv(srvModule="test.support.ngamsSrvTestDynReqCallBack")
        self.create_monitor(fs_poll_period=0.1, archive_poll_period=0.1)
        queued_fname = os.path.join(self.monitor.queue_dir, 'test.fits')

        cpFile(self.resource('src/SmallFile.fits'), queued_fname)
        self.assert_monitor_files(archiving=1)
        self.terminateAllServer()
        self.monitor.stop()
        self.assert_monitor_files(queued=1)

    def test_badfiles(self):
        """Unknown/bad files end up in the badfiles/ subdir"""
        self.start_srv()
        self.create_monitor(fs_poll_period=0.1, archive_poll_period=0.1)
        self.queue_smallfits(basename='test.file')
        self.assert_monitor_files(basename='test.file', bad=1)

    def test_backlog_buffered_archive(self):
        """A backlog buffered archive eventually gets archived and checked"""
        # Start a server with a DAPI that always fails. Archived files are
        # backlog-buffered
        cfg = (("NgamsCfg.Streams[1].Stream[2].PlugIn",
                "test.support.ngamsRaiseEx_NGAMS_ER_DAPI_1"),
               ("NgamsCfg.JanitorThread[1].SuspensionTime", "0T00:05:00"),
               ('NgamsCfg.Server[1].RequestDbBackend', 'memory'))
        self.start_srv(cfg)
        self.create_monitor(fs_poll_period=0.1, archive_poll_period=0.1,
                            check_poll_period=0.1, cleanup_timeout=1,
                            client_retry_period=0.1)
        self.queue_smallfits()
        time.sleep(1)
        self.assert_monitor_files(backlog=1)

        # Restart with a normal server and a fast janitor thread that will
        # finish the archiving process
        cfg = [["NgamsCfg.JanitorThread[1].SuspensionTime", "0T00:00:01"]]
        self.restart_last_server(cfgProps=cfg)
        filePat = self.ngas_path("%s/saf/2001-05-08/1/" +\
                  "TEST.2001-05-08T15:25:00.123.fits.gz")
        pollForFile(filePat % "FitsStorage1-Main-1", 1)
        pollForFile(filePat % "FitsStorage1-Rep-2", 1)
        self.retrieve("TEST.2001-05-08T15:25:00.123", targetFile=tmp_path())


    def test_cmdline_tool(self):
        """Make sure the command line tool starts correctly and that some of the
        command line parameters are understood"""
        cmdline = [sys.executable, '-m', 'ngamsPClient.fs_monitor',
                   '-w', tmp_path('fs-monitor-workdir'), '--log-level', 'DEBUG',
                   '-p', '8888', '-s', '1']
        proc = subprocess.Popen(cmdline, shell=False)
        self.assertIsNotNone(proc)
        time.sleep(1)
        self.assertEqual(0, terminate_or_kill(proc, 5))

    def test_cmdline_tool_with_servers(self):
        """Make sure the command line tool starts correctly with --servers"""
        cmdline = [sys.executable, '-m', 'ngamsPClient.fs_monitor',
                   '-w', tmp_path('fs-monitor-workdir'), '--log-level', 'DEBUG',
                   '-s', '1', '--servers', '127.0.0.1:8888']
        proc = subprocess.Popen(cmdline, shell=False)
        self.assertIsNotNone(proc)
        time.sleep(1)
        self.assertEqual(0, terminate_or_kill(proc, 5))