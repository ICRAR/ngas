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
# "@(#) $Id: ngamsSubscriptionTest.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  18/11/2003  Created
#
"""
This module contains the Test Suite for the SUBSCRIBE Command.
"""

import base64
import contextlib
import functools
import time

from ngamsLib import ngamsHttpUtils
from .ngamsTestLib import ngamsTestSuite, tmp_path

class ngamsSubscriptionTest(ngamsTestSuite):
    """
    Synopsis:
    Test the Subscription Service.

    Description:
    NG/AMS offers a Data Subscription Service

    Missing Test Cases:
    - Review Test Suite and define Test Cases.
    - Test UNSUBSCRIBE Command.
    """

    def _prep_subscription_cluster(self, *orig_server_list):

        # The current subscription code requires a local user named 'ngas-int',
        # regardless of whether remote authentication is enabled or not
        # To make things simple we add the user to all servers of the cluster
        ngas_int = ('Name', 'ngas-int'), ('Password', base64.b64encode(b'ngas-int'))
        server_list = []
        for srvInfo in orig_server_list:
            port, cfg_pars, send_archive_evt = srvInfo, [], False
            if isinstance(srvInfo, (tuple, list)):
                port, cfg_pars, send_archive_evt = srvInfo
                cfg_pars = list(cfg_pars)
            cfg_pars += [('NgamsCfg.Authorization[1].User[1].' + name, value) for name, value in ngas_int]
            if send_archive_evt:
                cfg_pars.append(('NgamsCfg.ArchiveHandling[1].EventHandlerPlugIn[1].Name', 'test.ngamsTestLib.SenderHandler'))
            server_list.append((port, cfg_pars))

        return self.prepCluster(server_list)

    def test_basic_subscription(self):

        # We configure the second server to send notifications via socket
        # to the listener we start later
        self._prep_subscription_cluster(8888, (8889, [], True))

        subscribe = functools.partial(ngamsHttpUtils.httpGet, 'localhost', 8888, 'SUBSCRIBE', timeout=5)

        # Initial archiving
        self.qarchive(8888, 'src/SmallFile.fits', mimeType='application/octet-stream')

        # Version 2 of the file should only exist after
        # subscription transfer is successful.
        self.retrieve_fail(8889, 'SmallFile.fits', fileVersion=2, targetFile=tmp_path())

        # Create listener that should get information when files get archives
        # in the second server (i.e., the one on the receiving end of the subscription)
        subscription_listener = self.notification_listener()

        # Create subscription
        params = {'url': 'http://localhost:8889/QARCHIVE',
                  'subscr_id': 'HERE-TO-THERE',
                  'priority': 1,
                  'start_date': '%sT00:00:00.000' % time.strftime("%Y-%m-%d"),
                  'concurrent_threads': 1}
        with contextlib.closing(subscribe(pars=params)) as resp:
            self.assertEqual(resp.status, 200)

        # Do not like sleeps but xfer should happen immediately.
        try:
            archive_evt = subscription_listener.wait_for_file(5)
        finally:
            subscription_listener.close()
        self.assertIsNotNone(archive_evt)

        self.assertEqual(2, archive_evt.file_version)
        self.assertEqual('SmallFile.fits', archive_evt.file_id)

        self.retrieve(8889, 'SmallFile.fits', fileVersion=2, targetFile=tmp_path())


    def test_basic_subscription_fail(self):

        src_cfg = (("NgamsCfg.HostSuspension[1].SuspensionTime", '0T00:00:02'), ("NgamsCfg.Log[1].LocalLogLevel", '4'))
        self._prep_subscription_cluster((8888, src_cfg, False), (8889, [], True))

        subscribe = functools.partial(ngamsHttpUtils.httpGet, 'localhost', 8888, 'SUBSCRIBE', timeout=5)
        usubscribe = functools.partial(ngamsHttpUtils.httpGet, 'localhost', 8888, 'USUBSCRIBE', timeout=5)
        unsubscribe = functools.partial(ngamsHttpUtils.httpGet, 'localhost', 8888, 'UNSUBSCRIBE', timeout=5)
        def assert_subscription_status(pars, status):
            with contextlib.closing(subscribe(pars=pars)) as resp:
                self.assertEqual(resp.status, status)

        # Archive these two
        for test_file in ('src/SmallFile.fits', 'src/TinyTestFile.fits'):
            self.qarchive(8888, test_file, mimeType='application/octet-stream')

        # Things haven't gone through tyet
        retrieve = functools.partial(self.retrieve, 8889, targetFile=tmp_path())
        retrieve('SmallFile.fits', fileVersion=2, expectedStatus='FAILURE')

        # Invalid number of concurrent threads
        params = {'url': 'http://localhost:8889/QARCHIVE',
                  'subscr_id': 'TEST',
                  'priority': 1,
                  'start_date': '%sT00:00:00.000' % time.strftime("%Y-%m-%d"),
                  'concurrent_threads': -1}
        assert_subscription_status(params, 400)

        # Invalid start_date -- not a date
        params = {'url': 'http://localhost:8889/QARCHIVE',
                  'subscr_id': 'TEST',
                  'priority': 1,
                  'start_date': 'ERRORT00:00:00.000',
                  'concurrent_threads': 2}
        assert_subscription_status(params, 400)

        # Invalid start_date -- month is invalid
        params = {'url': 'http://localhost:8889/QARCHIVE',
                  'subscr_id': 'TEST',
                  'priority': 1,
                  'start_date': '2010-20-02T00:00:00.000',
                  'concurrent_threads': 2}
        assert_subscription_status(params, 400)

        # Invalid start_date -- time is invalid
        params = {'url': 'http://localhost:8889/QARCHIVE',
                  'subscr_id': 'TEST',
                  'priority': 1,
                  'start_date': '2010-10-02TERROR',
                  'concurrent_threads': 2}
        assert_subscription_status(params, 400)

        # Invalid url -- empty
        params = {'url': '',
                  'subscr_id': 'TEST',
                  'priority': 1,
                  'start_date': '%sT00:00:00.000' % time.strftime("%Y-%m-%d"),
                  'concurrent_threads': 2}
        assert_subscription_status(params, 400)

        # Subscription created, but files shouldn't be transfered
        # because the url contains an invalid path
        params = {'url': 'http://localhost:8889/QARCHIV',
                  'subscr_id': 'TEST',
                  'priority': 1,
                  'start_date': '%sT00:00:00.000' % time.strftime("%Y-%m-%d"),
                  'concurrent_threads': 1}
        assert_subscription_status(params, 200)

        # Let a full subscription iteration go before checking anything
        # We put a time.sleep(3) in there to slow down the resource usage
        # and therefore we need to account for that in this test.
        # In the future we'll have a nicer mechanism that will make this
        # unnecessary (and less error prone to race conditions)
        time.sleep(7)

        # Check after all the failed subscriptions we don't have the file
        retrieve('SmallFile.fits', fileVersion=2, expectedStatus='FAILURE')

        # USUBSCRIBE updates the subscription to valid values
        # After this update the two files should go through
        subscription_listener = self.notification_listener()
        params = {'url': 'http://localhost:8889/QARCHIVE',
                  'subscr_id': 'TEST',
                  'priority': 1,
                  'start_date': '%sT00:00:00.000' % time.strftime("%Y-%m-%d"),
                  'concurrent_threads': 2}
        with contextlib.closing(usubscribe(pars=params)) as resp:
            self.assertEqual(resp.status, 200)

        archive_evts = []
        with contextlib.closing(subscription_listener):
            archive_evts.append(subscription_listener.wait_for_file(5))
            archive_evts.append(subscription_listener.wait_for_file(5))

        self.assertNotIn(None, archive_evts)
        self.assertEqual([2, 2], [x.file_version for x in archive_evts])
        self.assertSetEqual({'SmallFile.fits', 'TinyTestFile.fits'}, set([x.file_id for x in archive_evts]))

        for f in ('SmallFile.fits', 'TinyTestFile.fits'):
            retrieve(f, fileVersion=2)

        # UNSUBSCRIBE and check the newly archived file is not transfered
        subscription_listener = self.notification_listener()
        params = {'subscr_id': 'TEST'}
        with contextlib.closing(unsubscribe(pars=params)) as resp:
            self.assertEqual(resp.status, 200)

        self.qarchive(8888, 'src/SmallBadFile.fits', mimeType='application/octet-stream')
        with contextlib.closing(subscription_listener):
            self.assertIsNone(subscription_listener.wait_for_file(5))

        # Check after all the failed subscriptions we don't have the file
        retrieve('SmallBadFile.fits', fileVersion=1)
        retrieve('SmallBadFile.fits', fileVersion=2, expectedStatus='FAILURE')

    def test_server_starts_after_subscription_added(self):

        # Server should come up properly after a subscription is created
        self.prepExtSrv()
        self.subscribe('http://somewhere/SOMETHING')
        self.restart_last_server()

    def test_url_values(self):

        self.prepExtSrv()

        # empty url
        status = self.subscribe_fail('        ')
        self.assertIn('empty', status.getMessage())

        # Missing scheme
        status = self.subscribe_fail('some/path')
        self.assertIn('no scheme found', status.getMessage().lower())

        # Missing network location
        # These are all interpreted as <scheme>:<path>,
        # even if it looks like a network location to the eye
        for url in ('scheme:some/path', 'host:80/path', 'file:///tmp/file'):
            status = self.subscribe_fail(url)
            self.assertIn('no netloc found', status.getMessage().lower())

        # Scheme is actually not http
        for url in (
            "ftp://host:port/path", # ftp scheme not allowed
            "https://host/path", # https not allowed
            "file://hostname:port/somewhere/over/the/rainbow" # file not allowed
            ):
            status = self.subscribe_fail(url)
            self.assertIn('only http:// scheme allowed', status.getMessage().lower())

    def test_create_remote_subscriptions(self):
        """
        Starts two servers A and B, and configures B to automatically create a
        subscription to A when it starts. Then, archiving a file into A should
        make it into B.
        """

        subscription_pars = (('NgamsCfg.SubscriptionDef[1].Enable', '1'),
                             ('NgamsCfg.SubscriptionDef[1].Subscription[1].HostId', 'localhost'),
                             ('NgamsCfg.SubscriptionDef[1].Subscription[1].PortNo', '8888'),
                             ('NgamsCfg.SubscriptionDef[1].Subscription[1].Command', 'QARCHIVE'))
        self._prep_subscription_cluster(8888, (8889, subscription_pars, True))

        # Listen for archives on server B (B is configured to send us notifications)
        listener = self.notification_listener()

        # File archived onto server A
        self.archive(8888, 'src/SmallFile.fits', mimeType='application/octet-stream')
        with contextlib.closing(listener):
            # The built-in retry period is 10 seconds, so let's wait at maximum
            # for twice that, in case the first attempt fails because the servers
            # are not fully initialized
            self.assertIsNotNone(listener.wait_for_file(20))

        # Double-check that the file is in B
        self.retrieve(8889, 'SmallFile.fits', targetFile=tmp_path())