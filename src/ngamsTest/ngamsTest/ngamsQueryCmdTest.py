#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2016
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
import sys

from ngamsTestLib import sendPclCmd, ngamsTestSuite, runTest
from ngamsLib import ngamsCore


class ngamsQueryCmdTest(ngamsTestSuite):

    def test_files_list(self):

        self.prepExtSrv()
        client = sendPclCmd()

        # No files archived, there was an error on the previous implementation
        stat = client.sendCmd("QUERY", pars = [['query', 'files_list'], ['format', 'list']])
        self.assertEquals(ngamsCore.NGAMS_SUCCESS, stat.getStatus())

        # One file archived, let's see what happens now
        stat = client.archive("src/SmallFile.fits")
        self.assertEquals(ngamsCore.NGAMS_SUCCESS, stat.getStatus())
        stat = client.sendCmd("QUERY", pars = [['query', 'files_list'], ['format', 'list']])
        self.assertEquals(ngamsCore.NGAMS_SUCCESS, stat.getStatus())

        # Check that the archived file is listed
        data = stat.getData()
        self.assertTrue("TEST.2001-05-08T15:25:00.123" in data)

def run():
    runTest(["ngamsQueryCmdTest"])

if __name__ == "__main__":
    runTest(sys.argv)