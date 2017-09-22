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
from contextlib import closing
import httplib
import pickle
import socket
import struct
import sys
import threading
import time
import urllib

from ngamsTestLib import ngamsTestSuite, runTest, sendPclCmd, getNoCleanUp, setNoCleanUp
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

        host = 'localhost:8888'
        method = 'GET'
        cmd = 'QARCHIVE'

        test_file = 'src/SmallFile.fits'
        params = {'filename': test_file,
                  'mime_type': 'application/octet-stream'}
        params = urllib.urlencode(params)
        selector = '{0}?{1}'.format(cmd, params)
        with closing(httplib.HTTPConnection(host, timeout = 5)) as conn:
            conn.request(method, selector, open(test_file, 'rb'), {})
            resp = conn.getresponse()
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
        method = 'GET'
        cmd = 'SUBSCRIBE'
        params = {'url': 'http://localhost:8889/QARCHIVE',
                  'subscr_id': 'HERE-TO-THERE',
                  'priority': 1,
                  'start_date': '%sT00:00:00.000' % time.strftime("%Y-%m-%d"),
                  'concurrent_threads': 1}
        params = urllib.urlencode(params)
        selector = '{0}?{1}'.format(cmd, params)
        with closing(httplib.HTTPConnection(host, timeout = 5)) as conn:
            conn.request(method, selector, '', {})
            resp = conn.getresponse()
            self.checkEqual(resp.status, 200, None)

        # Do not like sleeps but xfer should happen immediately.
        try:
            archive_evt = subscription_listener.wait_for_file(5)
        finally:
            subscription_listener.close()
        self.assertIsNotNone(archive_evt)

        self.assertEquals(2, archive_evt.file_version)
        self.assertEquals('SmallFile.fits', archive_evt.file_id)

        client = sendPclCmd(port = 8889)
        status = client.retrieve('SmallFile.fits', fileVersion=2, targetFile='tmp')
        self.assertEquals(status.getStatus(), 'SUCCESS', None)


    def test_basic_subscription_fail(self):

        cfg = (('NgamsCfg.ArchiveHandling[1].EventHandlerPlugIn[1].Name', 'ngamsSubscriptionTest.SenderHandler'),)
        self.prepCluster("src/ngamsCfg.xml", [[8888, None, None, None, [["NgamsCfg.HostSuspension[1].SuspensionTime", '0T00:00:05'], ["NgamsCfg.Log[1].LocalLogLevel", '4']]],
                                              [8889, None, None, None, cfg]])

        host = 'localhost:8888'
        method = 'GET'
        cmd = 'QARCHIVE'

        test_file = 'src/SmallFile.fits'
        params = {'filename': test_file,
                  'mime_type': 'application/octet-stream'}
        params = urllib.urlencode(params)
        selector = '{0}?{1}'.format(cmd, params)
        with closing(httplib.HTTPConnection(host, timeout = 5)) as conn:
            conn.request(method, selector, open(test_file, 'rb'), {})
            resp = conn.getresponse()
            self.checkEqual(resp.status, 200, None)

        test_file = 'src/TinyTestFile.fits'
        params = {'filename': test_file,
                  'mime_type': 'application/octet-stream'}
        params = urllib.urlencode(params)
        selector = '{0}?{1}'.format(cmd, params)
        with closing(httplib.HTTPConnection(host, timeout = 5)) as conn:
            conn.request(method, selector, open(test_file, 'rb'), {})
            resp = conn.getresponse()
            self.checkEqual(resp.status, 200, None)

        client = sendPclCmd(port = 8889)
        status = client.retrieve('SmallFile.fits', fileVersion=2, targetFile='tmp')
        self.assertEquals(status.getStatus(), 'FAILURE', None)

        method = 'GET'
        cmd = 'SUBSCRIBE'
        params = {'url': 'http://localhost:8889/QARCHIVE',
                  'subscr_id': 'TEST',
                  'priority': 1,
                  'start_date': '%sT00:00:00.000' % time.strftime("%Y-%m-%d"),
                  'concurrent_threads': -1}
        params = urllib.urlencode(params)
        selector = '{0}?{1}'.format(cmd, params)
        with closing(httplib.HTTPConnection(host, timeout = 5)) as conn:
            conn.request(method, selector, '', {})
            resp = conn.getresponse()
            self.checkEqual(resp.status, 400, None)

        params = {'url': 'http://localhost:8889/QARCHIVE',
                  'subscr_id': 'TEST',
                  'priority': 1,
                  'start_date': 'ERRORT00:00:00.000',
                  'concurrent_threads': 2}
        params = urllib.urlencode(params)
        selector = '{0}?{1}'.format(cmd, params)
        with closing(httplib.HTTPConnection(host, timeout = 5)) as conn:
            conn.request(method, selector, '', {})
            resp = conn.getresponse()
            self.checkEqual(resp.status, 400, None)

        params = {'url': 'http://localhost:8889/QARCHIVE',
                  'subscr_id': 'TEST',
                  'priority': 1,
                  'start_date': '2010-20-02T00:00:00.000',
                  'concurrent_threads': 2}
        params = urllib.urlencode(params)
        selector = '{0}?{1}'.format(cmd, params)
        with closing(httplib.HTTPConnection(host, timeout = 5)) as conn:
            conn.request(method, selector, '', {})
            resp = conn.getresponse()
            self.checkEqual(resp.status, 400, None)

        params = {'url': 'http://localhost:8889/QARCHIVE',
                  'subscr_id': 'TEST',
                  'priority': 1,
                  'start_date': '2010-10-02TERROR',
                  'concurrent_threads': 2}
        params = urllib.urlencode(params)
        selector = '{0}?{1}'.format(cmd, params)
        with closing(httplib.HTTPConnection(host, timeout = 5)) as conn:
            conn.request(method, selector, '', {})
            resp = conn.getresponse()
            self.checkEqual(resp.status, 400, None)

        params = {'url': '',
                  'subscr_id': 'TEST',
                  'priority': 1,
                  'start_date': '%sT00:00:00.000' % time.strftime("%Y-%m-%d"),
                  'concurrent_threads': 2}
        params = urllib.urlencode(params)
        selector = '{0}?{1}'.format(cmd, params)
        with closing(httplib.HTTPConnection(host, timeout = 5)) as conn:
            conn.request(method, selector, '', {})
            resp = conn.getresponse()
            self.checkEqual(resp.status, 400, None)

        params = {'url': 'http://localhost:8889/QARCHIV',
                  'subscr_id': 'TEST',
                  'priority': 1,
                  'start_date': '%sT00:00:00.000' % time.strftime("%Y-%m-%d"),
                  'concurrent_threads': 1}
        params = urllib.urlencode(params)
        selector = '{0}?{1}'.format(cmd, params)
        with closing(httplib.HTTPConnection(host, timeout = 5)) as conn:
            conn.request(method, selector, '', {})
            resp = conn.getresponse()
            self.checkEqual(resp.status, 200, None)

        time.sleep(2)

        # Check after all the failed subscriptions we don't have the file
        client = sendPclCmd(port = 8889)
        status = client.retrieve('SmallFile.fits', fileVersion=2, targetFile='tmp')
        self.assertEquals(status.getStatus(), 'FAILURE', None)

        # USUBSCRIBE updates the subscription to valid values
        subscription_listener = notification_listener()
        cmd = 'USUBSCRIBE'
        params = {'url': 'http://localhost:8889/QARCHIVE',
                  'subscr_id': 'TEST',
                  'priority': 1,
                  'start_date': '%sT00:00:00.000' % time.strftime("%Y-%m-%d"),
                  'concurrent_threads': 2}
        params = urllib.urlencode(params)
        selector = '{0}?{1}'.format(cmd, params)
        with closing(httplib.HTTPConnection(host, timeout = 5)) as conn:
            conn.request(method, selector, '', {})
            resp = conn.getresponse()
            self.checkEqual(resp.status, 200, None)

        archive_evts = []
        try:
            archive_evts.append(subscription_listener.wait_for_file(5))
            archive_evts.append(subscription_listener.wait_for_file(5))
        finally:
            subscription_listener.close()

        self.assertNotIn(None, archive_evts)
        self.assertEqual([2, 2], [x.file_version for x in archive_evts])
        self.assertSetEqual({'SmallFile.fits', 'TinyTestFile.fits'}, set([x.file_id for x in archive_evts]))

        client = sendPclCmd(port = 8889)
        status = client.retrieve('SmallFile.fits', fileVersion=2, targetFile='tmp')
        self.assertEquals(status.getStatus(), 'SUCCESS', None)

        client = sendPclCmd(port = 8889)
        status = client.retrieve('TinyTestFile.fits', fileVersion=2, targetFile='tmp')
        self.assertEquals(status.getStatus(), 'SUCCESS', None)

        # UNSUBSCRIBE and check the newly archived file is not transfered
        subscription_listener = notification_listener()
        cmd = 'UNSUBSCRIBE'
        params = {'subscr_id': 'TEST'}
        params = urllib.urlencode(params)
        selector = '{0}?{1}'.format(cmd, params)
        with closing(httplib.HTTPConnection(host, timeout = 5)) as conn:
            conn.request(method, selector, '', {})
            resp = conn.getresponse()
            self.checkEqual(resp.status, 200, None)

        host = 'localhost:8888'
        method = 'GET'
        cmd = 'QARCHIVE'

        test_file = 'src/SmallBadFile.fits'
        params = {'filename': test_file,
                  'mime_type': 'application/octet-stream'}
        params = urllib.urlencode(params)
        selector = '{0}?{1}'.format(cmd, params)
        with closing(httplib.HTTPConnection(host, timeout = 5)) as conn:
            conn.request(method, selector, open(test_file, 'rb'), {})
            resp = conn.getresponse()
            self.checkEqual(resp.status, 200, None)

        try:
            self.assertIsNone(subscription_listener.wait_for_file(5))
        finally:
            subscription_listener.close()

        # Check after all the failed subscriptions we don't have the file
        client = sendPclCmd(port = 8889)
        status = client.retrieve('SmallBadFile.fits', fileVersion=1, targetFile='tmp')
        self.assertEquals(status.getStatus(), 'SUCCESS', None)

        client = sendPclCmd(port = 8889)
        status = client.retrieve('SmallBadFile.fits', fileVersion=2, targetFile='tmp')
        self.assertEquals(status.getStatus(), 'FAILURE', None)

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
