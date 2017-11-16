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

import SocketServer
import contextlib
import functools
import pickle
import socket
import struct
import sys
import threading
import time

from ngamsLib import ngamsHttpUtils
from ngamsLib.ngamsCore import NGAMS_SUCCESS
from ngamsTestLib import ngamsTestSuite, runTest, sendPclCmd, getNoCleanUp, setNoCleanUp, getClusterName
from ngamsServer import ngamsServer


# The plug-in that we configure the subscriber server with, so we know when
# an archiving has taken place on the subscription receiving end
class SenderHandler(object):

    def handle_event(self, evt):

        # pickle evt as a normal tuple and send it over to the test runner
        evt = pickle.dumps(tuple(evt))

        # send this to the notification_srv
        try:
            s = socket.create_connection(('127.0.0.1', 8887), timeout=5)
            s.send(struct.pack('!I', len(evt)))
            s.send(evt)
            s.close()
        except socket.error as e:
            print(e)

# A small server that receives the archiving event and sets a threading event
class notification_srv(SocketServer.TCPServer):

    allow_reuse_address = True

    def __init__(self, recvevt):
        SocketServer.TCPServer.__init__(self, ('127.0.0.1', 8887), None)
        self.recvevt = recvevt
        self.archive_evt = None

    def finish_request(self, request, _):
        l = struct.unpack('!I', request.recv(4))[0]
        self.archive_evt = ngamsServer.archive_event(*pickle.loads(request.recv(l)))
        self.recvevt.set()

# A class that starts the notification server and can wait until it receives
# an archiving event
class notification_listener(object):

    def __init__(self):
        self.closed = False
        self.recevt = threading.Event()
        self.server = notification_srv(self.recevt)

    def wait_for_file(self, timeout):
        self.server.timeout = timeout
        self.server.handle_request()
        return self.server.archive_evt

    def close(self):
        if self.closed:
            return
        if self.server:
            self.server.server_close()
            self.server = None
            self.closed = True

    __del__ = close

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

    def test_basic_subscription(self):

        # We configure the second server to send notifications via socket
        # to the listener we start later
        cfg = (('NgamsCfg.ArchiveHandling[1].EventHandlerPlugIn[1].Name', 'ngamsSubscriptionTest.SenderHandler'),)
        self.prepCluster("src/ngamsCfg.xml", [[8888, None, None, None], [8889, None, None, None, cfg]])

        qarchive = functools.partial(ngamsHttpUtils.httpGet, 'localhost', 8888, 'QARCHIVE', timeout=5)
        subscribe = functools.partial(ngamsHttpUtils.httpGet, 'localhost', 8888, 'SUBSCRIBE', timeout=5)

        # Initial archiving
        params = {'filename': 'src/SmallFile.fits',
                  'mime_type': 'application/octet-stream'}
        with contextlib.closing(qarchive(pars=params)) as resp:
            self.checkEqual(resp.status, 200, None)

        # Version 2 of the file should only exist after
        # subscription transfer is successful.
        client = sendPclCmd(port = 8889)
        status = client.retrieve('SmallFile.fits', fileVersion=2, targetFile='tmp')
        self.assertEquals(status.getStatus(), 'FAILURE', None)

        # Create listener that should get information when files get archives
        # in the second server (i.e., the one on the receiving end of the subscription)
        subscription_listener = notification_listener()

        # Create subscription
        params = {'url': 'http://localhost:8889/QARCHIVE',
                  'subscr_id': 'HERE-TO-THERE',
                  'priority': 1,
                  'start_date': '%sT00:00:00.000' % time.strftime("%Y-%m-%d"),
                  'concurrent_threads': 1}
        with contextlib.closing(subscribe(pars=params)) as resp:
            self.checkEqual(resp.status, 200, None)

        # Do not like sleeps but xfer should happen immediately.
        try:
            archive_evt = subscription_listener.wait_for_file(5)
        finally:
            subscription_listener.close()
        self.assertIsNotNone(archive_evt)

        self.assertEquals(2, archive_evt.file_version)
        self.assertEquals('SmallFile.fits', archive_evt.file_id)

        status = client.retrieve('SmallFile.fits', fileVersion=2, targetFile='tmp')
        self.assertEquals(status.getStatus(), 'SUCCESS', None)


    def test_basic_subscription_fail(self):

        cfg = (('NgamsCfg.ArchiveHandling[1].EventHandlerPlugIn[1].Name', 'ngamsSubscriptionTest.SenderHandler'),)
        self.prepCluster("src/ngamsCfg.xml", [[8888, None, None, None, [["NgamsCfg.HostSuspension[1].SuspensionTime", '0T00:00:02'], ["NgamsCfg.Log[1].LocalLogLevel", '4']]],
                                              [8889, None, None, None, cfg]])

        qarchive = functools.partial(ngamsHttpUtils.httpGet, 'localhost', 8888, 'QARCHIVE', timeout=5)
        subscribe = functools.partial(ngamsHttpUtils.httpGet, 'localhost', 8888, 'SUBSCRIBE', timeout=5)
        usubscribe = functools.partial(ngamsHttpUtils.httpGet, 'localhost', 8888, 'USUBSCRIBE', timeout=5)
        unsubscribe = functools.partial(ngamsHttpUtils.httpGet, 'localhost', 8888, 'UNSUBSCRIBE', timeout=5)
        def assert_subscription_status(pars, status):
            with contextlib.closing(subscribe(pars=pars)) as resp:
                self.assertEqual(resp.status, status, None)

        # Archive these two
        for test_file in ('src/SmallFile.fits', 'src/TinyTestFile.fits'):
            params = {'filename': test_file,
                      'mime_type': 'application/octet-stream'}
            with contextlib.closing(qarchive(pars=params)) as resp:
                self.checkEqual(resp.status, 200, None)

        # Things haven't gone through tyet
        retrieve = functools.partial(sendPclCmd(port = 8889).retrieve, targetFile='tmp')
        status = retrieve('SmallFile.fits', fileVersion=2)
        self.assertEquals(status.getStatus(), 'FAILURE', None)

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
        status = retrieve('SmallFile.fits', fileVersion=2)
        self.assertEquals(status.getStatus(), 'FAILURE')

        # USUBSCRIBE updates the subscription to valid values
        # After this update the two files should go through
        subscription_listener = notification_listener()
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
            status = retrieve(f, fileVersion=2)
            self.assertEqual(status.getStatus(), 'SUCCESS')

        # UNSUBSCRIBE and check the newly archived file is not transfered
        subscription_listener = notification_listener()
        params = {'subscr_id': 'TEST'}
        with contextlib.closing(unsubscribe(pars=params)) as resp:
            self.assertEqual(resp.status, 200)

        test_file = 'src/SmallBadFile.fits'
        params = {'filename': test_file,
                  'mime_type': 'application/octet-stream'}
        with contextlib.closing(qarchive(pars=params)) as resp:
            self.assertEqual(resp.status, 200)

        with contextlib.closing(subscription_listener):
            self.assertIsNone(subscription_listener.wait_for_file(5))

        # Check after all the failed subscriptions we don't have the file
        status = retrieve('SmallBadFile.fits', fileVersion=1)
        self.assertEqual(status.getStatus(), 'SUCCESS')

        status = retrieve('SmallBadFile.fits', fileVersion=2)
        self.assertEqual(status.getStatus(), 'FAILURE')

    def test_server_starts_after_subscription_added(self):

        self.prepExtSrv()
        client = sendPclCmd()
        status = client.subscribe('http://somewhere/SOMETHING')
        self.assertEqual('SUCCESS', status.getStatus())

        # Cleanly shut down the server, and wait until it's completely down
        old_cleanup = getNoCleanUp()
        setNoCleanUp(True)
        self.termExtSrv(self.extSrvInfo.pop())
        setNoCleanUp(old_cleanup)

        # Server should come up properly
        self.prepExtSrv(delDirs=0, clearDb=0)

    def test_url_values(self):

        self.prepExtSrv()
        client = sendPclCmd()

        # empty url
        status = client.subscribe('        ')
        self.assertEqual('FAILURE', status.getStatus())
        self.assertIn('empty', status.getMessage())

        # Missing scheme
        status = client.subscribe('some/path')
        self.assertEqual('FAILURE', status.getStatus())
        self.assertIn('no scheme found', status.getMessage().lower())

        # Missing network location
        # These are all interpreted as <scheme>:<path>,
        # even if it looks like a network location to the eye
        for url in ('scheme:some/path', 'host:80/path', 'file:///tmp/file'):
            status = client.subscribe(url)
            self.assertEqual('FAILURE', status.getStatus())
            self.assertIn('no netloc found', status.getMessage().lower())

        # Scheme is actually not http
        for url in (
            "ftp://host:port/path", # ftp scheme not allowed
            "https://host/path", # https not allowed
            "file://hostname:port/somewhere/over/the/rainbow" # file not allowed
            ):
            status = client.subscribe(url)
            self.assertEqual('FAILURE', status.getStatus())
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
                             ('NgamsCfg.SubscriptionDef[1].Subscription[1].Command', 'QARCHIVE'),
                             ('NgamsCfg.ArchiveHandling[1].EventHandlerPlugIn[1].Name', 'ngamsSubscriptionTest.SenderHandler'))
        self.prepCluster("src/ngamsCfg.xml",
                        [[8888, None, None, getClusterName()],
                         [8889, None, None, getClusterName(), subscription_pars]])

        # Listen for archives on server B (B is configured to send us notifications)
        listener = notification_listener()

        # File archived onto server A
        stat = sendPclCmd(port=8888).archive('src/SmallFile.fits', mimeType='application/octet-stream')
        self.assertEqual(NGAMS_SUCCESS, stat.getStatus())
        with contextlib.closing(listener):
            self.assertIsNotNone(listener.wait_for_file(10))

        # Double-check that the file is in B
        status = sendPclCmd(port = 8889).retrieve('SmallFile.fits', targetFile='tmp')
        self.assertEquals(status.getStatus(), 'SUCCESS', None)


def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngamsSubscriptionTest"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)
