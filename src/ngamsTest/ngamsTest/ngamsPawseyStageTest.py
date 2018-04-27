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

import os
import socket

from . import ngamsTestLib
from ngamsPlugIns.mwa.ngamsMWAPawseyTapeApi import isFileOffline, readDMFStatus,\
    pawseyMWAdmget, releaseFiles

os.environ['PATH'] += ':' + os.path.join(os.path.dirname(__file__)) + '/bin'

class ngamsPawseyStageTest(ngamsTestLib.ngamsTestSuite):

    def test_status(self):

        try:
            status = readDMFStatus('INV')
            self.assertTrue(False)
        except Exception as e:
            pass

        self.assertEquals(readDMFStatus('REG'), 'REG')
        self.assertEquals(readDMFStatus('DUL'), 'DUL')

        try:
            status = readDMFStatus('Error')
            self.assertTrue(False)
        except Exception as e:
            pass

        self.assertEquals(isFileOffline('NMG'), 1)
        self.assertEquals(isFileOffline('OFL'), 1)
        self.assertEquals(isFileOffline('PAR'), 1)

        self.assertEquals(isFileOffline('N/A'), 0)
        self.assertEquals(isFileOffline('REG'), 0)
        self.assertEquals(isFileOffline('DUL'), 0)

        try:
            self.assertEquals(isFileOffline('INV'), 1)
            self.assertTrue(False)
        except Exception as e:
            pass

        try:
            self.assertEquals(isFileOffline('Error'), 1)
            self.assertTrue(False)
        except Exception as e:
            pass

    def test_release(self):
        released = releaseFiles(['DUL', 'DUL', 'NMG', 'OFL'])
        self.assertEquals(len(released), 2)
        self.assertEquals(released[0], 'DUL')
        self.assertEquals(released[1], 'DUL')

        try:
            released = releaseFiles(['DUL', 'DUL', 'NMG', 'OFL', 'Error'])
            self.assertTrue(False)
        except Exception as e:
            pass

    def test_stage(self):
        host = 'fe1.pawsey.ivec.org'
        port = 9898

        try:
            # check if we can connect to pawsey, if not then just ignore
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3)
            sock.connect((host, port))

            try:
                pawseyMWAdmget(['/mnt/mwa01fs/MWA/ngas_data_volume/mfa/2016-01-14/ \
                              2/1136798736_20160114092621_gpubox02_01.fits'],
                              host,
                              port)
            except:
                self.assertTrue(False)

            try:
                pawseyMWAdmget(['test'], host, port)
                self.assertTrue(False)
            except:
                pass
        except socket.error:
            pass #ingore