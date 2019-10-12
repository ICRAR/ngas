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
# "@(#) $Id: ngamsConfigHandlingTest.py,v 1.7 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  24/06/2004  Created
#
"""
This module contains the Test Suite for the handling of the NG/AMS
Configuration, in particular, the handling of the configuration in the
DB is tested.
"""

import os
import re
import tempfile
import unittest

from ngamsLib import ngamsConfig, ngamsDb, ngamsXmlMgr
from .ngamsTestLib import delNgasTbls, ngamsTestSuite, \
    save_to_tmp


dbIdAttr = 'Db-Test'
stdCfgGrIdList = ["ngamsCfg-Test", "ArchiveHandling-Test",
                  "Authorization-Test",
                  "DataCheckThread-Test", dbIdAttr,
                  "HostSuspension-Test", "JanitorThread-Std",
                  "Log-Test", "MimeTypes-Std", "Notification-Test",
                  "Permissions-Test", "Processing-Std", "Register-Std",
                  "Server-Sim", "StorageSets-PATA-8-Dbl", "Streams-4-Dbl",
                  "SubscriptionDef-Test", "SystemPlugIns-Std"]


# Remove zero string length values + values = None, due to a incorrect
# handling of zero length strings in Oracle where ("" = NULL).
def _cleanXmlDoc(xmlDicDump, filter_pattern):

    filter_pattern = re.compile(filter_pattern)
    xmlDocLines = xmlDicDump.split("\n")
    doc = []
    for xmlDocLine in xmlDocLines:

        xmlDocLine = xmlDocLine.strip()
        if not xmlDocLine or filter_pattern.match(xmlDocLine):
            continue

        els = xmlDocLine.split(":")
        key = els[0].strip()
        val = els[1].strip()
        if key[-1] == "]":
            continue
        if not val:
            continue

        doc.append(xmlDocLine)
    return '\n'.join(doc)

def without_db_element(s):
    lines = []
    for l in s.split('\n'):
        stripped = l.strip()
        if stripped.startswith('<Db ') or stripped.startswith('<SessionSql ') or stripped.startswith('</Db>'):
            continue
        lines.append(l)
    return '\n'.join(lines)

class ngamsConfigHandlingTest(ngamsTestSuite):
    """
    Synopsis:
    Handling of the configuration file.

    Description:
    The purpose of this Test Suite is to test the handling of the NG/AMS
    Configuration. The test focuse on the handling of the configuration in the
    NGAS DB and the loading into and loading from of this.

    Missing Test Cases:
    - Should make more tests to check if the server reacts correctly on
      different parameter values.
    - Should be analyzed and more Test Cases added to cover better this
      feature.
    """

    def loadCfg(self,
                dbCfgName,
                cfgFile = "src/ngamsCfg.xml",
                dbCfgGroupIds = stdCfgGrIdList,
                checkCfg = 1,
                delDbTbls = 1,
                createDatabase = True):
        """
        Load configuration into NGAS DB. Return the configuration and DB objects.

        cfgFile:        Name of configuration file to load (string).

        dbCfgGroupIds:  DB Cfg. Group IDs to use when defining the cfg. (list).

        checkCfg:       Check cfg. when loading before writing in the DB
                        (integer/0|1).

        delDbTbls:      Delete the NGAS tables in the DB (integer/0|1).

        Returns:        Tuple with ngamsConfig and ngamsDb Objects (tuple).
        """
        # This ensure the Db.Id attribute is what we want it to be
        cfgObj = self.env_aware_cfg(cfgFile, check=checkCfg, db_id_attr=dbIdAttr)
        revAttr = "NgamsCfg.Header[1].Revision"
        cfgObj.storeVal(revAttr, "TEST-REVISION", "ngamsCfg-Test")

        self.point_to_sqlite_database(cfgObj, createDatabase)
        dbObj = ngamsDb.from_config(cfgObj, maxpool=1)
        if (delDbTbls): delNgasTbls(dbObj)
        cfgObj.writeToDb(dbObj)

        # Create a DB configuration association.
        sql = "INSERT INTO ngas_cfg (cfg_name, cfg_par_group_ids, cfg_comment) VALUES ({0}, {1}, 'Test Cfg')"
        dbObj.query2(sql, args=(dbCfgName, ','.join([str(x) for x in dbCfgGroupIds])))
        return (cfgObj, dbObj)

    def test_Load_1(self):
        """
        Synopsis:
        Test Purpose: Load configuration into DB.

        Description:
        The purpose of this Test Case is to test that the NG/AMS Configuration
        can be loaded properly from an XML configuration document into the
        NGAS DB.

        Expected Result:
        The XML based NG/AMS Configuration should be loaded into the NGAS DB.

        Test Steps:
        - Load configuration into instance of ngamsConfig and write it to
          the NGAS DB.
        - Load the previously loaded configuration from the DB into an object
          and dump this into an NG/AMS XML Configuration.
        - Verify that the contents is as expected after loading into the DB
          and dumping from the DB into an ASCII format in the NGAS XML
          Dictionary format.

        Remarks:
        ...
        """
        _, dbObj = self.loadCfg("test_Load_1")

        # Load + check the configuration from the DB.
        cfgObj2 = ngamsConfig.ngamsConfig()
        # Dump as XML Dictionary.
        cfgObj2.loadFromDb("test_Load_1", dbObj)
        refFile     = "ref/ngamsConfigHandlingTest_test_Load_1_1_ref"
        cleanXmlDoc = _cleanXmlDoc(cfgObj2.dumpXmlDic(), r'^NgamsCfg.Db\[1\]')
        msg = "Incorrect contents of XML Dictionary of cfg. loaded from DB"
        self.assert_ref_file(refFile, cleanXmlDoc, msg=msg)
        # Dump as XML Document.
        refFile     = "ref/ngamsConfigHandlingTest_test_Load_1_2_ref"
        data = without_db_element(cfgObj2.genXmlDoc())
        msg = "Incorrect contents of XML Document of cfg. loaded from DB"
        self.assert_ref_file(refFile, data, msg=msg)


    def test_ServerLoad_1(self):
        """
        Synopsis:
        Test that the NG/AMS Server is loading configuration from DB and
        initializing as expected.

        Description:
        Test that the NG/AMS Server can load the configuration properly from
        the DB and initialize accordingly.

        Expected Result:
        The server should load the DB parameters according the specified
        NGAS Configuration ID specified and should start up properly.

        Test Steps:
        - Load the configuration into the DB.
        - Start an NG/AMS Server specifying an XML document which only
          defines the DB connection + giving the reference to the DB Cfg. ID.
        - Verify that the NG/AMS Server starts up properly.

        Remarks:
        ...
        """
        cfgName = "test_ServerLoad_1"
        cfgObj, _ = self.loadCfg(cfgName)
        cfgFile = save_to_tmp(str(cfgObj.genXmlDoc()))
        self.prepExtSrv(cfgFile=cfgFile, dbCfgName=cfgName, clearDb=0)

        # Archive a file, should be OK.
        self.archive("src/SmallFile.fits")


    def test_ServerLoad_2(self):
        """
        Synopsis:
        Test that the NG/AMS Server is loading configuration from DB and
        initializing as expected.

        Description:
        The purpose of this Test Case is to verify that the NG/AMS Server
        can be initialized properly by loading the configuration from the
        NGAS DB. The parameter disabling handling of Archive Requests is
        set to 0. It is checked that the server after initialization,
        rejects Archive Requests.

        Expected Result:
        The NG/AMS Server should initialize and select disabling of handling
        of Archive Requests. Submitting Archive Requests to the server
        should result in a rejection of the request.

        Test Steps:
        - Create an NG/AMS XML Configuration where handling of Archive Requests
          is disabled.
        - Load this into the DB.
        - Start instance of the NG/AMS Server specifying to use the previously
          loaded configuration.
        - Issue an Archive Request and verify that it is rejected.

        Remarks:
        ...
        """
        cfgName = "test_ServerLoad_2"
        tmpCfg = self.env_aware_cfg().\
                 storeVal("NgamsCfg.Permissions[1].AllowArchiveReq", "0",
                          "Permissions-Test")
        cfgFile = save_to_tmp(tmpCfg.genXmlDoc(0))
        cfgObj, _ = self.loadCfg(cfgName, cfgFile=cfgFile)
        cfgFile = save_to_tmp(str(cfgObj.genXmlDoc()))
        self.prepExtSrv(cfgFile=cfgFile, dbCfgName=cfgName, clearDb=0)

        # Archive a file, should be rejected.
        status = self.archive_fail("src/SmallFile.fits")
        self.assertIn('This NG/AMS is not configured for accepting Archive Requests',
                      status.getMessage())


    def test_ServerLoad_3(self):
        """
        Synopsis:
        Test loading of specific configuration from DB.

        Description:
        Test that the NG/AMS Server is loading configuration from DB and
        initializing as expected without mixing up parameters from other
        configurations in the DB.

        Expected Result:
        The NG/AMS Server should load only parameters for the specified
        configuration Group ID and ignore other parameters in the DB.

        Test Steps:
        - Prepare two XML configurations and load them in the DB. Difference
          is that all attribute values of the second are set to value=0, which
          means that the server could not operate if mixing up parameters from
          the two.
        - Start server.
        - Archive file and check that the file was successfully archived.

        Remarks:
        ...
        """
        cfgName1 = "test_ServerLoad_1"
        cfgObj, _ = self.loadCfg(cfgName1)
        cfgFile = save_to_tmp(cfgObj.genXmlDoc(0))

        # For the second cfg. set all the DB Cfg. Group ID to a common value
        # + set all values to a non-sense value.
        cfgName2 = "test_ServerLoad_2"
        tmpCfg = self.env_aware_cfg()
        for cfgKey in list(tmpCfg._getXmlDic()):
            if ((cfgKey[-1] != "]") and (cfgKey.find("Db[1]") == -1)):
                tmpCfg.storeVal(cfgKey, "0")
        tmpCfg.storeVal("NgamsCfg.Log[1].LocalLogFile",
                        "log/LogFile.nglog", "0")
        tmpCfgFile = save_to_tmp(tmpCfg.genXmlDoc(0))
        self.loadCfg(cfgName2, cfgFile=tmpCfgFile, checkCfg=0, delDbTbls=0,
                dbCfgGroupIds=[0], createDatabase=False)

        # Start server specifying 1st DB cfg.
        self.prepExtSrv(cfgFile=cfgFile, dbCfgName=cfgName1, clearDb=0)
        self.archive("src/SmallFile.fits")

class XmlMgrTests(unittest.TestCase):

    def test_no_xsl_declaration(self):
        """Double-check that documents with "xml:stylesheet" XML elements
        can be parsed correctly"""

        fd, fname = tempfile.mkstemp(text=True)
        try:
            os.write(fd, b"""<?xml version="1.0" encoding="UTF-8"?>
            <?xml-stylesheet type="text/xsl" href="class.xsl"?>
            <foo/>""")
            ngamsXmlMgr.ngamsXmlMgr('foo', fname)
        finally:
            os.close(fd)
            os.remove(fname)