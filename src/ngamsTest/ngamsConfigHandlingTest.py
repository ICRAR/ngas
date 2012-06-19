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

import os, sys
from   ngams import *
import ngamsDb, ngamsConfig
from   ngamsTestLib import *


stdCfgGrIdList = ["ngamsCfg-Test", "ArchiveHandling-Test",
                  "Authorization-Test",
                  "DataCheckThread-Test", "Db-DEVSRV-ngastst2",
                  "HostSuspension-Test", "JanitorThread-Std",
                  "Log-Test", "MimeTypes-Std", "Notification-Test",
                  "Permissions-Test", "Processing-Std", "Register-Std",
                  "Server-Sim", "StorageSets-PATA-8-Dbl", "Streams-4-Dbl",
                  "SubscriptionDef-Test", "SystemPlugIns-Std"]


def loadCfg(dbCfgName,
            cfgFile = "src/ngamsCfg.xml",
            dbCfgGroupIds = stdCfgGrIdList,
            checkCfg = 1,
            delDbTbls = 1):
    """
    Load configuration into NGAS DB. Return the configuration and DB objects.

    cfgFile:        Name of configuration file to load (string).

    dbCfgGroupIds:  DB Cfg. Group IDs to use when defining the cfg. (list).
 
    checkCfg:       Check cfg. when loading before writing in the DB
                    (integer/0|1).

    delDbTbls:      Delete the NGAS tables in the DB (integer/0|1).

    Returns:        Tuple with ngamsConfig and ngamsDb Objects (tuple).
    """
    cfgObj = ngamsConfig.ngamsConfig().load(cfgFile, checkCfg)
    revAttr = "NgamsCfg.Header[1].Revision"
    cfgObj.storeVal(revAttr, "TEST-REVISION", "ngamsCfg-Test")

    refCfgObj = ngamsConfig.ngamsConfig().load(getRefCfg())
    multCons = refCfgObj.getDbMultipleCons()
    dbObj = ngamsDb.ngamsDb(refCfgObj.getDbServer(), refCfgObj.getDbName(),
                            refCfgObj.getDbUser(), refCfgObj.getDbPassword(),
                            interface = refCfgObj.getDbInterface(),
                            parameters = refCfgObj.getDbParameters(), 
                            multipleConnections = multCons)
    if (delDbTbls): delNgasTbls(dbObj)
    cfgObj.writeToDb(dbObj)
    
    # Create a DB configuration association.
    sqlQueryFormat = "INSERT INTO ngas_cfg (cfg_name, cfg_par_group_ids, " +\
                     "cfg_comment) VALUES ('%s', '%s', 'Test Cfg')"
    sqlQuery = sqlQueryFormat %\
               (dbCfgName, str(dbCfgGroupIds)[1:-1].replace("'", ""))
    dbObj.query(sqlQuery)
    return (cfgObj, dbObj)


# Remove zero string length values + values = None, due to a incorrect
# handling of zero length strings in Oracle where ("" = NULL).
def _cleanXmlDoc(xmlDicDump):
    xmlDocLines = xmlDicDump.split("\n")
    cleanXmlDoc = ""
    for xmlDocLine in xmlDocLines:
        xmlDocLine = xmlDocLine.strip()
        if (xmlDocLine == ""): continue
        els = xmlDocLine.split(":")
        key = els[0].strip()
        val = els[1].strip()
        if (key[-1] == "]"): continue
        if (val == ""): continue
        cleanXmlDoc += xmlDocLine + "\n"
    return cleanXmlDoc
            
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
        cfgObj1, dbObj = loadCfg("test_Load_1")

        # Load + check the configuration from the DB.
        cfgObj2 = ngamsConfig.ngamsConfig()
        # Dump as XML Dictionary.
        cfgObj2.loadFromDb("test_Load_1", dbObj)
        refFile     = "ref/ngamsConfigHandlingTest_test_Load_1_1_ref"
        cleanXmlDoc = _cleanXmlDoc(cfgObj2.dumpXmlDic())
        tmpStatFile = saveInFile(None, cleanXmlDoc)
        self.checkFilesEq(refFile, tmpStatFile, "Incorrect contents of " +\
                          "XML Dictionary of cfg. loaded from DB")
        # Dump as XML Document.
        refFile     = "ref/ngamsConfigHandlingTest_test_Load_1_2_ref"
        tmpStatFile = saveInFile(None, str(cfgObj2.genXmlDoc()))
        self.checkFilesEq(refFile, tmpStatFile, "Incorrect contents of " +\
                          "XML Document of cfg. loaded from DB")


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
        loadCfg(cfgName)
        self.prepExtSrv(cfgFile="src/ngamsCfgDbCon.xml", dbCfgName=cfgName,
                        clearDb=0)
        # Archive a file, should be OK.
        statObj = sendPclCmd().archive("src/SmallFile.fits")
        refStatFile = "ref/ngamsConfigHandlingTest_test_ServerLoad_1_1_ref"
        tmpStatFile = saveInFile(None, filterDbStatus1(statObj.dumpBuf()))
        self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect status " +\
                          "returned for Archive Push Request")
 

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
        tmpCfg = ngamsConfig.ngamsConfig().load("src/ngamsCfg.xml").\
                 storeVal("NgamsCfg.Permissions[1].AllowArchiveReq", "0",
                          "Permissions-Test")
        loadCfg(cfgName, cfgFile=saveInFile(None,tmpCfg.genXmlDoc(0)))
        self.prepExtSrv(cfgFile="src/ngamsCfgDbCon.xml", dbCfgName=cfgName,
                        clearDb=0)

        # Archive a file, should be rejected.
        statObj = sendPclCmd().archive("src/SmallFile.fits")
        refStatFile = "ref/ngamsConfigHandlingTest_test_ServerLoad_2_1_ref"
        tmpStatFile = saveInFile(None, filterDbStatus1(statObj.dumpBuf()))
        self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect status " +\
                          "returned for Archive Push Request")
        

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
        loadCfg(cfgName1)
        # For the second cfg. set all the DB Cfg. Group ID to a common value
        # + set all values to a non-sense value.
        cfgName2 = "test_ServerLoad_2"
        tmpCfg = ngamsConfig.ngamsConfig().load("src/ngamsCfg.xml")
        for cfgKey in tmpCfg._getXmlDic().keys():
            if ((cfgKey[-1] != "]") and (cfgKey.find("Db[1]") == -1)):
                tmpCfg.storeVal(cfgKey, "0")
        tmpCfg.storeVal("NgamsCfg.Log[1].LocalLogFile",
                        "/tmp/ngamsTest/NGAS/log/LogFile.nglog", "0")
        tmpCfgFile = saveInFile(None,tmpCfg.genXmlDoc(0))
        loadCfg(cfgName2, cfgFile=tmpCfgFile, checkCfg=0, delDbTbls=0,
                dbCfgGroupIds=[0])

        # Start server specifying 1st DB cfg.
        self.prepExtSrv(cfgFile="src/ngamsCfgDbCon.xml", dbCfgName=cfgName1,
                        clearDb=0)
        statObj = sendPclCmd().archive("src/SmallFile.fits")
        refStatFile = "ref/ngamsConfigHandlingTest_test_ServerLoad_3_1_ref"
        tmpStatFile = saveInFile(None, filterDbStatus1(statObj.dumpBuf()))
        self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect status " +\
                          "returned for Archive Push Request")


def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngamsConfigHandlingTest"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)


# EOF
