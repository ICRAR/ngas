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
import contextlib
import sys

from ngamsTestLib import ngamsTestSuite, runTest, sendPclCmd
from ngamsLib import ngamsHttpUtils

def delete_ngas_file(host, port, file_id, file_version, disk_id, timeout = 10):
    pars = {'file_id': file_id,
            'file_version': file_version,
            'disk_id': disk_id}
    resp = ngamsHttpUtils.httpGet(host, port, 'CACHEDEL', pars=pars, timeout=timeout)
    with contextlib.closing(resp):
        return resp.status

class ngamsCacheDelTest(ngamsTestSuite):

    def tearDown(self):
        ngamsTestSuite.tearDown(self)
    
    def test_cache_delete(self):
        self.prepExtSrv(cache=True)

        r = sendPclCmd().archive('src/SmallFile.fits')
        self.assertEquals(r.getStatus(), 'SUCCESS', None)
        status = delete_ngas_file('localhost', 8888, 'TEST.2001-05-08T15:25:00.123', 1,
                                        r.getDiskStatusList()[0].getDiskId())
        self.assertEquals(status, 200, None)

def run():
    runTest(["ngamsCacheDelTest"])

if __name__ == '__main__':
    runTest(sys.argv)