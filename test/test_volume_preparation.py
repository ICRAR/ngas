#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2019
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
import functools
import os

from ngamsServer import volumes

from . import ngamsTestLib


class TestVolumePreparation(ngamsTestLib.ngamsTestSuite):

    def assert_volume_info(self, info, disk_type, manufacturer):
        self.assertEqual(info[volumes.NGAS_VOL_INFO_TYPE], disk_type)
        self.assertEqual(info[volumes.NGAS_VOL_INFO_MANUFACT], manufacturer)

    def assert_undef_vinfo(self, info):
        self.assert_volume_info(info, 'UNDEFINED', 'UNDEFINED')

    def test_simple_run(self):
        root = ngamsTestLib.tmp_path()
        volumes.prepare_volume_info_file(root, check_func=self.assert_undef_vinfo)

        # file already exists
        self.assertRaises(ValueError, volumes.prepare_volume_info_file, os.path.join(root))
        # dir doesn't exist
        self.assertRaises(ValueError, volumes.prepare_volume_info_file, os.path.join(root, 'doesnt-exist'))

    def test_overwrite(self):
        root = ngamsTestLib.tmp_path()
        volumes.prepare_volume_info_file(root, check_func=self.assert_undef_vinfo)
        volumes.prepare_volume_info_file(root, overwrite=True, check_func=self.assert_undef_vinfo)

        # Do some checks
        disk_type = 'type'
        manufacturer = 'manufacturer'
        check_func = functools.partial(self.assert_volume_info, disk_type=disk_type,
                                       manufacturer=manufacturer)
        volumes.prepare_volume_info_file(root, overwrite=True, disk_type=disk_type,
                                         manufacturer=manufacturer,
                                         check_func=check_func)