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

import unittest

from ngamsLib import ngamsCore, ngamsLib

class NgamsLibTests(unittest.TestCase):

    def test_remove_duplicated_extension(self):

        # File with extension, test with same and different extensions
        for fname in ('/path/to/a/file.txt', '../../file.txt', 'file.txt'):
            self.assertEqual(fname, ngamsLib.remove_duplicated_extension(fname))
            self.assertEqual(fname, ngamsLib.remove_duplicated_extension(fname + '.txt'))
            self.assertEqual(fname + '.bin', ngamsLib.remove_duplicated_extension(fname + '.bin'))

        # File without extensions, should always remain unchanged
        for fname in ('/path/to/a/file', '../../file', 'file'):
            self.assertEqual(fname, ngamsLib.remove_duplicated_extension(fname))
            self.assertEqual(fname + '.txt', ngamsLib.remove_duplicated_extension(fname + '.txt'))
            self.assertEqual(fname + '.bin', ngamsLib.remove_duplicated_extension(fname + '.bin'))

    def test_gen_staging_filename(self):
        """Double-checks that filenames are properly escaped"""

        self.assertEqual('_', ngamsCore.to_valid_filename('?'))
        self.assertEqual('__', ngamsCore.to_valid_filename('??'))