#
#    ALMA - Atacama Large Millimiter Array
#    (c) European Southern Observatory, 2002
#    Copyright by ESO (in the framework of the ALMA collaboration),
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
This module contains the Test Suite for the partner sites feature.
"""

import contextlib
import functools
import os

from ngamsLib import ngamsHttpUtils, ngamsStatus
from ngamsLib.ngamsCore import getHostName
from ngamsLib.ngamsLib import getDomain
from ngamsServer import ngamsFileUtils
from .ngamsTestLib import ngamsTestSuite, tmp_path

class NgasPartnerSiteTest(ngamsTestSuite):
    """
    Synopsis:
    Test the partner site feature.

    Description:
    NGAS offers a partner site feature for proxy requests to remote NGAS hosts

    Missing Test Cases:
    """

    def _prepare_partner_site_cluster(self, *original_server_list, **kwargs):
        server_list = []
        for server_info in original_server_list:
            port, property_list = server_info[0], list(server_info[1])
            server_list.append((port, property_list))

        return self.prepCluster(server_list)

    def test_parse_host_id(self):
        host_id = "ngas.example.com:7777"
        host_name, domain_name, port = ngamsFileUtils.parse_host_id(host_id)
        self.assertEqual(host_name, "ngas.example.com")
        self.assertEqual(domain_name, "example.com")
        self.assertEqual(port, 7777)

    def test_archive_status_retrieve_sequence(self):

        cfg = self.env_aware_cfg()
        if 'sqlite' not in cfg.getDbInterface():
            self.skipTest("This test works only against the sqlite db")

        host_name = getHostName()
        domain_name = getDomain()
        host_name_fqdn = host_name
        if domain_name is not None:
            host_name_fqdn = "{0}.{1}".format(host_name, domain_name)
        sample_file_name = "SmallFile.fits"
        sample_file_path = os.path.join("src", sample_file_name)
#        sample_file_size = os.path.getsize(sample_file_path)
        sample_mime_type = "application/octet-stream"

        # We create two NGAS clusters each containing a single NGAS node
        # We configure the first NGAS cluster to use the second NGAS cluster
        # as a partner site
        partner_host_id = "{0}:9011".format(host_name_fqdn)
        config_list_1 = [("NgamsCfg.Server[1].RootDirectory", "/tmp/ngas1"),
                         ("NgamsCfg.Server[1].IpAddress", "0.0.0.0"),
                         ("NgamsCfg.PartnerSites[1].ProxyMode", "1"),
                         ("NgamsCfg.PartnerSites[1].PartnerSite[1].Address", partner_host_id)]
        self._prepare_partner_site_cluster((9001, config_list_1))

        config_list_2 = [("NgamsCfg.Server[1].RootDirectory", "/tmp/ngas2"),
                         ("NgamsCfg.Server[1].IpAddress", "0.0.0.0")]
        self._prepare_partner_site_cluster((9011, config_list_2))

        # We archive a test sample file on the partner site cluster
        self.archive(9011, sample_file_path, mimeType=sample_mime_type)

        # We check the status of a file ID found on the partner site cluster
        command_status = functools.partial(ngamsHttpUtils.httpGet, host_name, 9001, "STATUS", timeout=5)
        argument_list = {"file_id": sample_file_name}
        with contextlib.closing(command_status(pars=argument_list)) as response:
            self.assertEqual(response.status, 200)
            # Parse the response XML status information
            response_data = response.read()
            status_info = ngamsStatus.ngamsStatus().unpackXmlDoc(response_data, 1)
            disk_info = status_info.getDiskStatusList()[0]
            file_info = disk_info.getFileObjList()[0]
            self.assertEqual(disk_info.getHostId(), "{0}:9011".format(host_name))
            self.assertEqual(file_info.getFileId(), sample_file_name)
            self.assertEqual(file_info.getFileVersion(), 1)
            self.assertEqual(file_info.getFormat(), sample_mime_type)

        # Retrieve a file found on the partner site cluster
        retrieve_file_path = tmp_path(sample_file_name)
        self.retrieve(9001, sample_file_name, targetFile=retrieve_file_path)
#        retrieve_file_size = os.path.getsize(retrieve_file_path)
#        self.assertEqual(sample_file_size, retrieve_file_size)

    def test_status_retrieve_sequence(self):
        host_name = getHostName()
        domain_name = getDomain()
        host_name_fqdn = host_name
        if domain_name is not None:
            host_name_fqdn = "{0}.{1}".format(host_name, domain_name)
#        sample_file_name = "SmallFile.fits"
#        sample_file_path = os.path.join("src", sample_file_name)
        bad_file_name = "dummy.fits"

        # We create two NGAS clusters each containing a single NGAS node
        # We configure the first NGAS cluster to use the second NGAS cluster
        # as a partner site
        partner_host_id = "{0}:9011".format(host_name_fqdn)
        config_list_1 = [("NgamsCfg.Server[1].RootDirectory", "/tmp/ngas1"),
                         ("NgamsCfg.Server[1].IpAddress", "0.0.0.0"),
                         ("NgamsCfg.PartnerSites[1].ProxyMode", "1"),
                         ("NgamsCfg.PartnerSites[1].PartnerSite[1].Address", partner_host_id)]
        self._prepare_partner_site_cluster((9001, config_list_1))

        config_list_2 = [("NgamsCfg.Server[1].RootDirectory", "/tmp/ngas2"),
                         ("NgamsCfg.Server[1].IpAddress", "0.0.0.0")]
        self._prepare_partner_site_cluster((9011, config_list_2))

        # We check the status of a file ID found on the partner site cluster
        command_status = functools.partial(ngamsHttpUtils.httpGet, host_name, 9001, "STATUS", timeout=1000)
        argument_list = {"file_id": bad_file_name}
        with contextlib.closing(command_status(pars=argument_list)) as response:
            self.assertEqual(response.status, 400)
            # Parse the response XML status information
            response_data = response.read()
            status_info = ngamsStatus.ngamsStatus().unpackXmlDoc(response_data, 1)
            self.assertEqual(status_info.getStatus(), "FAILURE")
            self.assertEqual(status_info.getMessage(),
                             "NGAMS_ER_UNAVAIL_FILE:4019:ERROR: File with ID: dummy.fits appears not to be available.")

        # Retrieve a file found on the partner site cluster
        retrieve_file_path = tmp_path(bad_file_name)
        self.retrieve(9001, bad_file_name, targetFile=retrieve_file_path, expectedStatus='FAILURE')
