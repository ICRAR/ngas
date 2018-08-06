#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2017
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

import time

from .ngamsTestLib import ngamsTestSuite


class ngamsCacheThreadTest(ngamsTestSuite):
    """Unit tests for the cache thread logic"""

    def get_cache_cfg(self, period, max_cache_size, check):
        cfg = [('Enable', '1'), ('Period', period), ('CheckCanBeDeleted', int(check))]
        if max_cache_size is not None:
            cfg.append(('MaxCacheSize', str(max_cache_size)))

        # Enable the cache thread, but only trigger it when we hit 1 GB (which we won't)
        return [('NgamsCfg.Caching[1].%s' % k, v) for k, v in cfg]

    def start_cache_server(self, period, max_cache_size=None, check=False):
        cfg = self.get_cache_cfg(period, max_cache_size, check)
        return self.prepExtSrv(cfgProps=cfg)

    def test_registered_in_cache(self):
        """When the cache thread is active, files get registred in the cache"""

        # Trigger the cache thread only when we hit 1 GB (which we won't)
        one_gb = 1024 * 1024 * 1024
        _, db = self.start_cache_server('0T00:00:01', max_cache_size=one_gb)

        # Archive a file, it should be registered into the cache
        # Because how the caching thread is currently implemented there might be
        # a small delay between when we inform the caching service about the new
        # file, and when it gets actually registered (both in the central and in
        # the local database). So we sleep a bit
        self.assertArchive('src/SmallFile.fits', mimeType='application/octet-stream')
        time.sleep(3)

        # ARCHIVE puts two copies of the file, both of which should be registered
        # in the cache
        res = db.query2('SELECT file_id FROM ngas_cache')
        self.assertEqual(2, len(res))
        self.assertEqual({'SmallFile.fits'}, set([x[0] for x in res]))

    def _test_delete_from_cache(self, check):
        """After a few archives the cache gets full and old data is purged (or not)"""

        # Only a few bytes! anything that gets archived should quickly disappear
        _, db = self.start_cache_server('0T00:00:01', max_cache_size=10, check=check)
        self.assertArchive('src/SmallFile.fits', mimeType='application/octet-stream', cmd='QARCHIVE')
        time.sleep(3)

        expected = 1 if check else 0
        self.assertEqual(expected, len(db.query2('SELECT file_id FROM ngas_cache')))
        self.assertEqual(expected, len(db.query2('SELECT file_id FROM ngas_files')))

    def test_delete_from_cache(self):
        self._test_delete_from_cache(False)

    def test_dont_delete_from_cache(self):
        self._test_delete_from_cache(True)