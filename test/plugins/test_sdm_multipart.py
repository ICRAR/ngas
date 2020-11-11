#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2020
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

"""
This module contains the Test Suite for the ngamsSdmMultipart plugin.
"""

import os

from ngamsPlugIns import ngamsSdmMultipart
from test import ngamsTestLib


class NgamsSdmMultipartTest(ngamsTestLib.ngamsTestSuite):

    def test_specific_treatment(self):
        sample_file_name = "A002_X9896b4_X10f"
        sample_file_path = self.resource(os.path.join("src", sample_file_name))

        file_id, file_name, file_type = \
            ngamsSdmMultipart.specific_treatment(sample_file_path)
        self.assertEqual(file_id, "A002/X9896b4/X10f")
        self.assertEqual(file_name, "A002:X9896b4:X10f")
        self.assertEqual(file_type, "multipart/mixed")

    # def test_ngams_sdm_multipart(self):
    #     sample_file_name = "A002_X9896b4_X10f"
    #     sample_file_path = self.resource(os.path.join("src", sample_file_name))
