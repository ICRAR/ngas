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

#
"""
Contains the Test Suite for the BBCP Command.
"""

import sys
import urllib
import urllib2
import httplib
import subprocess
from contextlib import closing
from ngamsTestLib import ngamsTestSuite, runTest
from unittest.case import skipIf

bbcp_cmd = True
cmd = ["which", "bbcp"]
p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
out, err = p.communicate()
if p.returncode == 0:
    bbcp_cmd = False


@skipIf(bbcp_cmd, 'BBCP not found')
class ngamsBBCPArchiveTest(ngamsTestSuite):

    def test_BBCPArchive(self):
        """
        Synopsis:
            Test BBCP archive plugin
        """

        host = 'localhost:8888'
        self.prepExtSrv(cfgFile = 'src/ngamsCfg.xml')
        
        test_file = '/bin/cp'
        query_args = { 'fileUri': test_file,
                       'bnum_streams': 2,
                       'mimeType': 'application/octet-stream'}

        bbcpurl = 'http://%s/BBCPARC?%s' % (host, urllib.urlencode(query_args))

        request = urllib2.Request(bbcpurl)
        with closing(urllib2.urlopen(request, timeout = 50)) as resp:
            self.checkEqual(resp.getcode(), 200, None)



def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngamsBBCPArchiveTest"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)

