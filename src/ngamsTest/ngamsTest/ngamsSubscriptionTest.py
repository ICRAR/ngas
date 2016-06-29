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

import sys
import time
import urllib
import httplib
from contextlib import closing

from ngamsLib.ngamsCore import *
from ngamsTestLib import ngamsTestSuite, runTest, sendExtCmd, sendPclCmd

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
        self.prepCluster("src/ngamsCfg.xml", [[8888, None, None, None], [8889, None, None, None]])

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
        status = client.retrieve2File(fileId = 'SmallFile.fits',
                                        fileVersion = 2,
                                        targetFile = '/tmp/test.fits')
        self.assertEquals(status.getStatus(), 'FAILURE', None)

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
        time.sleep(5)

        client = sendPclCmd(port = 8889)
        status = client.retrieve2File(fileId = 'SmallFile.fits',
                                        fileVersion = 2,
                                        targetFile = '/tmp/test.fits')
        self.assertEquals(status.getStatus(), 'SUCCESS', None)


    def test_basic_subscription_fail(self):
        self.prepCluster("src/ngamsCfg.xml", [[8888, None, None, None, [["NgamsCfg.HostSuspension[1].SuspensionTime", '0T00:00:05']]],
                                              [8889, None, None, None]])

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
        status = client.retrieve2File(fileId = 'SmallFile.fits',
                                        fileVersion = 2,
                                        targetFile = '/tmp/test.fits')
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
                  'start_date': '%s 00:00:00.000' % time.strftime("%Y-%m-%d"),
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
        status = client.retrieve2File(fileId = 'SmallFile.fits',
                                        fileVersion = 2,
                                        targetFile = '/tmp/test.fits')
        self.assertEquals(status.getStatus(), 'FAILURE', None)

        # USUBSCRIBE for update
        # SUBSCRIBE for insert
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

        time.sleep(5)

        client = sendPclCmd(port = 8889)
        status = client.retrieve2File(fileId = 'SmallFile.fits',
                                        fileVersion = 2,
                                        targetFile = '/tmp/test.fits')
        self.assertEquals(status.getStatus(), 'SUCCESS', None)

        client = sendPclCmd(port = 8889)
        status = client.retrieve2File(fileId = 'TinyTestFile.fits',
                                        fileVersion = 2,
                                        targetFile = '/tmp/test1.fits')
        self.assertEquals(status.getStatus(), 'SUCCESS', None)

        # UNSUBSCRIBE and check the newly archived file is not transfered
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

        time.sleep(5)

        # Check after all the failed subscriptions we don't have the file
        client = sendPclCmd(port = 8889)
        status = client.retrieve2File(fileId = 'SmallBadFile.fits',
                                        fileVersion = 1,
                                        targetFile = '/tmp/test2.fits')
        self.assertEquals(status.getStatus(), 'SUCCESS', None)

        client = sendPclCmd(port = 8889)
        status = client.retrieve2File(fileId = 'SmallBadFile.fits',
                                        fileVersion = 2,
                                        targetFile = '/tmp/test2.fits')
        self.assertEquals(status.getStatus(), 'FAILURE', None)

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
