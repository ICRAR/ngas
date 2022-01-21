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
from test import ngamsTestLib
import glob, os

class TestExtractFitsHeader(ngamsTestLib.ngamsTestSuite):
    def test_extract_header(self):
        noFitsCompressionAlmaCfg = [['NgamsCfg.Streams[1].Stream[2].PlugInPars', ''], ['NgamsCfg.Processing[1].PlugIn[2].Name', 'eso.ngamsExtractFitsHdrDppi']]
        self.prepExtSrv(cfgProps=noFitsCompressionAlmaCfg)
        self.archive('src/SmallFile.fits', pars=(('ignore_arcfile', 1), ))
        header_file = './SmallFile.hdr'
        self.retrieve('SmallFile.fits', processing=('eso.ngamsExtractFitsHdrDppi'), targetFile=header_file)
        b = os.path.getsize (header_file)
        self.assertEqual(2881, b)


