#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2018
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

from ngamsLib import ngamsDb, ngamsDiskInfo, ngamsFileInfo
from test import ngamsTestLib

class DbTests(ngamsTestLib.ngamsTestSuite):

    def setUp(self):
        super(DbTests, self).setUp()
        cfg = self.env_aware_cfg()
        self.point_to_sqlite_database(cfg, True)
        self.db = ngamsDb.from_config(cfg, maxpool=1)
        ngamsTestLib.delNgasTbls(self.db)

    def tearDown(self):
        self.db.close()
        super(DbTests, self).tearDown()

    def test_get_file_info_list_with_wildcards(self):
        """Double-check that wildcards in fileId work"""

        disk_info = ngamsDiskInfo.ngamsDiskInfo()
        disk_info.setDiskId('disk-id')
        disk_info.write(self.db)
        file_info = ngamsFileInfo.ngamsFileInfo()
        file_info.setDiskId('disk-id')
        file_info.setFileId('file-id')
        file_info.write('host-id', self.db, genSnapshot=0)
        res = list(self.db.getFileInfoList('disk-id', fileId="*"))
        self.assertEqual(1, len(res))
