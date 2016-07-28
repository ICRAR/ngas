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
import sys
import time
import httplib
import urllib
import base64
from contextlib import closing

from ngamsLib import ngamsStatus
from ngamsTestLib import ngamsTestSuite, runTest, sendPclCmd

def delete_ngas_file(hostport, file_id, file_version, disk_id, timeout = 10):
   data = {'file_id': file_id,
           'file_version': file_version,
           'disk_id': disk_id}

   params = urllib.urlencode(data)
   selector = '{0}?{1}'.format('CACHEDEL', params)
   with closing(httplib.HTTPConnection(hostport, timeout = timeout)) as conn:
      conn.request('GET', selector, '')
      resp = conn.getresponse()
      return resp.status, resp.reason


class ngamsCacheDelTest(ngamsTestSuite):

    def tearDown(self):
        ngamsTestSuite.tearDown(self)
    
    def test_cache_delete(self):
        self.prepExtSrv(server_type = 'ngamsCacheServer')

        r = sendPclCmd().archive('src/SmallFile.fits')
        self.assertEquals(r.getStatus(), 'SUCCESS', None)
        
        status, resp = delete_ngas_file('localhost:8888', 'TEST.2001-05-08T15:25:00.123', 1, 
                                        r.getDiskStatusList()[0].getDiskId())
        
        self.assertEquals(status, 200, None)
        
def run():
    runTest(["ngamsCacheDelTest"])

if __name__ == '__main__':
    runTest(sys.argv)