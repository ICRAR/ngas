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
        self.prepExtSrv(8888)
        self.prepExtSrv(8889)

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
        time.sleep(7)

        client = sendPclCmd(port = 8889)
        status = client.retrieve2File(fileId = 'SmallFile.fits',
                                        fileVersion = 2,
                                        targetFile = '/tmp/test.fits')
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


# EOF
