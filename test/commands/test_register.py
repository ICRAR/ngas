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
#******************************************************************************
#
# "@(#) $Id: ngamsRegisterCmdTest.py,v 1.6 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  18/11/2003  Created
#
"""
This module contains the Test Suite for the REGISTER Command.
"""

import os
import time

from ngamsLib import ngamsFileInfo
from ngamsLib.ngamsCore import checkCreatePath, getHostName, NGAMS_REGISTER_CMD
from ..ngamsTestLib import ngamsTestSuite, tmp_path

TEST_PATH = "FitsStorage2-Main-3/saf/test/"

A002_X35a1b1_Xd_sample = """MIME-Version: 1.0
Content-Type: multipart/mixed; boundary="MIME_boundary-1"; type="text/xml";
Content-Description: ALMA/CORRELATOR/ALMA_ACA/CHANNEL_AVERAGE
Content-Location: uid://A002/X35a1b1/Xd

--MIME_boundary-1
Content-Type: text/xml; charset="UTF-8"
Content-Transfer-Encoding: 8bit
Content-Location: sdmDataHeader.xml

<?xml version="1.0" encoding="UTF-8"?>
<sdmDataHeader xmlns="http://Alma/sdmDataObject/sdmbin" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xvers="http://Alma/xversion" byteOrder="Little_Endian" xvers:schemaVersion="2" xvers:revision="0.0.97" mainHeaderId="sdmDataHeader" projectPath="1/1/3/"><startTime>4831764181680000000</startTime><dataOID xlink:type="locator" xlink:href="uid://A002/X35a1b1/Xd" xlink:title="ALMA ACA Correlator Channel Average Data"/><dimensionality axes="TIM">1</dimensionality><execBlock xlink:href="uid://A002/X35a1b1/X6"/><numAntenna>6</numAntenna><correlationMode>AUTO_ONLY</correlationMode><spectralResolution>CHANNEL_AVERAGE</spectralResolution><processorType>CORRELATOR</processorType><dataStruct xsi:type="AutoData"><baseband name="BB_1"><spectralWindow sw="1" swbb="BB_1" sdPolProducts="XX YY" numSpectralPoint="1" numBin="1" sideband="USB"/></baseband><flags size="12" axes="ANT POL"/><actualTimes size="12" axes="ANT POL"/><actualDurations size="12" axes="ANT POL"/><autoData size="12" axes="ANT POL" normalized="false"/></dataStruct></sdmDataHeader>
"""

Z00_X01_X00_sample = """MIME-Version: 1.0
Content-Type: Multipart/Related; boundary="MIME_boundary"; type="text/xml"; start= "<sb1234_ABCsc_1234ob_1234in1.xml>"
Content-Description: Correlator
alma-uid: uid://Z00/X01/X00

--MIME_boundary
Content-Type: text/xml; charset="UTF-8"
Content-Transfer-Encoding: 8bit
Content-ID: <sb1234_ABCsc_1234ob_1234in1.xml>

<ASDMBinaryTable>
  <ExecblockUID>just a test</ExecblockUID>
</ASDMBinaryTable>
--MIME_boundary
Content-Type: binary/octet-stream
Content-ID: <sb1234_ABCsc_1234ob_1234in1.actualIntDur.bin>

'!'"'#'$'%'&'''(')'*'+'
--MIME_boundary
"""

ngas_log_sample = """2021-03-26T12:34:17.078 [ 3778] [MainThread] [  INFO] ngamsServer.ngamsServer#handleStartUp:2116 Allow Archiving Requests: 1
2021-03-26T12:34:17.079 [ 3778] [MainThread] [  INFO] ngamsServer.ngamsServer#handleStartUp:2117 Allow Retrieving Requests: 1
2021-03-26T12:34:17.079 [ 3778] [MainThread] [  INFO] ngamsServer.ngamsServer#handleStartUp:2118 Allow Processing Requests: 1
2021-03-26T12:34:17.079 [ 3778] [MainThread] [  INFO] ngamsServer.ngamsServer#handleStartUp:2119 Allow Remove Requests: 1
2021-03-26T12:34:17.080 [ 3778] [MainThread] [  INFO] ngamsServer.ngamsServer#handleStartUp:2143 Registering partner site address: arca10.hq.eso.org:7780
2021-03-26T12:34:17.116 [ 3778] [MainThread] [  INFO] ngamsServer.ngamsServer#handleStartUp:2209 NGAMS_INFO_STARTING_SRV:3016:INFO: Starting/initializing NG/AMS Server - Version: 11.0/2018-10-26T07:00:00 - Host: aat-ngas-5:8001 - Port 8001
2021-03-26T12:34:17.161 [ 3778] [MainThread] [  INFO] ngamsServer.ngamsServer#handleStartUp:2219 PID file for this session created: /srv/ngas1/.aat-ngas-5:8001.pid
2021-03-26T12:34:17.161 [ 3778] [MainThread] [  INFO] ngamsServer.ngamsServer#handleStartUp:2223 Auto Online requested - server going to Online State ...
2021-03-26T12:34:17.162 [ 3778] [MainThread] [  INFO] ngamsServer.ngamsServer#loadCfg:1943 Loading NG/AMS Configuration from /etc/ngas.d/ngas-server-8001.xml
2021-03-26T12:34:17.213 [ 3778] [MainThread] [  INFO] ngamsServer.ngamsServer#loadCfg:1958 Successfully loaded NG/AMS Configuration
"""


class ngamsRegisterCmdTest(ngamsTestSuite):
    """
    Synopsis:
    Test Suite for  REGISTER Command.

    Description:
    Test Suite that exercises the REGISTER Command in various usages.

    Missing Test Cases:
    - wait=0/1
    - Register file already registered.
    - Register illegal file.
    - Non-existing file/path.
    - File/path not on NGAS Disk.
    - Unknown mime-type.
    """
    def create_test_sample_file(self, file_name, sample):
        file_path = self.ngas_path(TEST_PATH + file_name)
        checkCreatePath(os.path.dirname(file_path))
        with open(file_path, 'w') as fo:
            fo.write(sample)
        return self.get_status(NGAMS_REGISTER_CMD, (("path", file_path),))

    def copy_to_ngas(self, file_suffix=''):
        src_file = "src/SmallFile.fits"
        tmp_src_file = self.ngas_path("FitsStorage2-Main-3/saf/test/SmallFile.fits" + file_suffix)
        checkCreatePath(os.path.dirname(tmp_src_file))
        self.cp(src_file, tmp_src_file)
        return tmp_src_file

    def copy_and_register(self, file_suffix='', **extra_params):
        tmp_src_file = self.copy_to_ngas(file_suffix=file_suffix)
        return self.get_status(NGAMS_REGISTER_CMD, (("path", tmp_src_file),) + tuple(extra_params.items()))

    def test_RegisterCmd_1(self):
        """
        Synopsis:
        REGISTER Command/register single file compl. path.

        Description:
        Test handling of the REGISTER Command under normal circumstances.

        Expected Result:
        The REGISTER Command should be accepted by the server and should
        be executed successfully.

        Test Steps:
        - Start server.
        - Copy file onto NGAS Disk.
        - Submit REGISTER Command requesting to register the file copied over
          (wait=1).
        - Check response from the server that the request was successfully
          executed.
        - Check the DB info for the registered file.

        Remarks:
        ...
        """
        _, db_obj = self.prepExtSrv()
        status = self.copy_and_register()
        msg = "Incorrect status returned for REGISTER command"
        ref_stat_file = "ref/ngamsRegisterCmdTest_test_RegisterCmd_1_ref"
        self.assert_status_ref_file(ref_stat_file, status, msg=msg)
        disk_id = self.ngas_disk_id("FitsStorage2/Main/3")
        file_prefix = "ngamsRegisterCmdTest_test_RegisterCmd_1"
        file_info_ref = "ref/" + file_prefix + "_FileInfo_ref"
        file_id = "TEST.2001-05-08T15:25:00.123"
        start_time = time.time()

        host_id = getHostName() + ":8888"
        while (time.time() - start_time) < 10:
            tmp_file_res = db_obj.getFileInfoFromFileIdHostId(host_id, file_id, 1, disk_id)
            if tmp_file_res: break
        if not tmp_file_res:
            self.fail("Couldn't get fileInfo result from database within 10 seconds")
        tmp_file_obj = ngamsFileInfo.ngamsFileInfo().unpackSqlResult(tmp_file_res)
        msg = "Incorrect info in DB for registered file"
        self.assert_status_ref_file(file_info_ref, tmp_file_obj, msg=msg)

    def test_register_generic_plugin(self):
        """Tests that a register plugin without parameters can work"""
        cfg = (('NgamsCfg.Register[1].PlugIn[2].Name', 'ngamsPlugIns.ngamsRegisterGenericPlugIn'),
               ('NgamsCfg.Register[1].PlugIn[2].MimeType[1].Name', 'ngas/log'))
        self.prepExtSrv(cfgProps=cfg)
        self.create_test_sample_file("LogFile.log", ngas_log_sample)
        self.retrieve('LogFile.log', targetFile=tmp_path())

    def test_register_alma_plugin(self):
        """Tests that a register plugin without parameters can work"""
        cfg = (('NgamsCfg.MimeTypes[1].MimeTypeMap[8].MimeType', 'text/plain'),
               ('NgamsCfg.MimeTypes[1].MimeTypeMap[8].Extension', 'txt'),
               ('NgamsCfg.Register[1].PlugIn[2].Name', 'ngamsPlugIns.ngamsRegisterAlmaPlugIn'),
               ('NgamsCfg.Register[1].PlugIn[2].MimeType[1].Name', 'text/plain'),
               ('NgamsCfg.Register[1].PlugIn[2].MimeType[2].Name', 'multipart/related'),
               ('NgamsCfg.Register[1].PlugIn[2].MimeType[3].Name', 'unknown'))
        self.prepExtSrv(cfgProps=cfg)
        self.create_test_sample_file('A002_X35a1b1_Xd', A002_X35a1b1_Xd_sample)
        self.create_test_sample_file('Z00_X01_X00', Z00_X01_X00_sample)
        self.retrieve('A002_X35a1b1_Xd', targetFile=tmp_path())
        self.retrieve('Z00_X01_X00', targetFile=tmp_path())

    def test_register_invalid_mime_type(self):
        """Tests registering with an invalid mime type fails with a meaningful message"""
        self.prepExtSrv()
        fname = self.copy_to_ngas(file_suffix=".log")
        status = self.get_status_fail(NGAMS_REGISTER_CMD, (("path", fname),))
        self.assertIn("mime-type", status.getMessage().lower())
