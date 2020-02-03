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
import os

from test import ngamsTestLib


class TestJanitor(ngamsTestLib.ngamsTestSuite):

    def test_plugin_errors_are_independent(self):
        '''An error in a plug-in shouldn't prevent others from running'''
        cfg = (
            ('NgamsCfg.JanitorThread[1].PlugIn[1].Name', 'test.support.janitor.exception_raiser_plugin'),
            ('NgamsCfg.JanitorThread[1].PlugIn[2].Name', 'test.support.janitor.file_writer_plugin'),
            ('NgamsCfg.JanitorThread[1].SuspensionTime', '0T00:00:01'),
        )
        empty_file = ngamsTestLib.tmp_path('an_empty_file')
        os.environ['NGAS_JANITOR_EMPTY_FILE'] = empty_file
        self.prepExtSrv(cfgProps=cfg)
        ngamsTestLib.pollForFile(empty_file, timeOut=2, pollTime=0.2)

    def test_plugins_detect_stop(self):
        cfg = (
            ('NgamsCfg.JanitorThread[1].PlugIn[1].Name', 'test.support.janitor.long_waiter_plugin'),
        )
        self.prepExtSrv(cfgProps=cfg)