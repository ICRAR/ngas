

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
# "@(#) $Id: ngasCheckSystem.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  12/07/2005  Created
#

_doc =\
"""
The tool is used to check the condition of an NGAS installation. This is done
by extracting information about the nodes installed from the DB.

A list of nodes must be given, which are used as contact points for the tests.
All requests will go through these/this unit(s). The contact nodes, will
typically be the master nodes in a cluster configuration.

If problems are found, it is ensured that these are only reported once
per 24 hours.

The tool is actually a frame-work to execute System Check Plug-In Functions.
These are contained in the modules, the list of System Check Plug-In Functions
to executed, should be specified when invoking the tool.

After the tests have been executed, it is possible to have a test report send
to a list of email recipients.

The pre-defined input parameters to the tool are:

%s

It is possible to define new parameter dynamically. These can be used e.g.
in user provided System Check Plug-In Functions.


Remarks:
- Test Report DBM:
Entries in the Test Report DBM are stored in the format:

Key:    <Test ID>_<Srv Host>_<Srv Port>_<Host>_<Port>

Result: [<Test ID>, <Time>, <Last Reported>, <Srv Host>, <Srv Port>, <Host>,
         <Port>, ngasCheckSystem.TEST_SUCCESS|ngasCheckSystem.TEST_FAILURE,
         <Status Info (e.g. error output or NGAS XML Status Document)>]
"""

# IMPL:
# - Make tests as plug-ins.
# - Make it possible to specify which tests to execute.

import sys, os, time, getpass

from ngams import *
import ngamsDb, ngamsDbm, ngamsStatus, ngamsFileInfo, ngamsDiskInfo
import ngamsLib
import ngamsPClient
import ngasUtils, ngasUtilsLib

# Constants.

# Status constants.
TEST_SUCCESS = 0
TEST_FAILURE = 1

# Fields in Test Report DBM Results.
TEST_ID        = 0
TEST_TIME      = 1
LAST_REP_TIME  = 2
SRV_HOST_ID    = 3
SRV_PORT_NO    = 4
HOST_ID        = 5
PORT_NO        = 6
TEST_STAT      = 7
TEST_STAT_INFO = 8

# Definition of predefined command line parameters.
_testPars = [["_startTime", time.time(),
              "Internal Parameter: Start time for running the System Checks."],
             ["DETAILS",    0,
              "If specified, all details about each test (the results) are "+\
              "reported."],
             ["DOMAINS",    None,
              "List of domains to take into account."],
             ["FORCE-NOTIF", 0,
              "Force notification, i.e., an Email Notification Report is " +\
              "send even if no errors were encountered."],
             ["REPORT-INT",  24,
              "Indicates how often a specific error is allowed to be " +\
              "reported. If set to 24, each error encountered will only " +\
              "be reported every 24 hours."],
             ["NOTIF-EMAIL", None,
              "Comma separated list of email recipient that will receive " +\
              "the generated error reports."],
             ["REPORT-ALL",  0,
              "Report all tests executed, also those that succeeded."],
             ["SERVERS",    None,
              "List of servers to take into account for the test. Given as: "+\
              "<Srv 1>:<Port 1>,<Srv 2>,<Port 2>,... ."],
             ["IGNORE-SERVERS", None,
              "Servers to ignore. Given as: <Srv 1>:<Port 1>,..."],
             ["TIMEOUT",    420,
              "Timeout in seconds to apply when waiting for each test case " +\
              "to finish."],
             ["WORKING-DIR",    None,
              "Working directory for the tool."]]
_testParDic = {}
_parFormat = "%s [%s]:\n"
_parDoc = ""
for parInfo in _testPars:
    _testParDic[parInfo[0]] = parInfo[1]
    if (parInfo[0][0] != "_"):
        _parDoc += _parFormat % (parInfo[0], str(parInfo[1]))
        _parDoc += parInfo[2] + "\n\n"
__doc__ = _doc % _parDoc


def testParDic():
    """
    Return reference to test parameter dictionary.

    Returns:  Reference to test parameters dictionary (dictionary).
    """
    return _testParDic


def addTestResult(testId,
                  testParDic,
                  dbmObj,
                  srvNode,
                  srvPort,
                  testNode,
                  testPort,
                  status,
                  statusInfo):
    """
    Add/update a test result in the DBM Test Report.

    testId:      ID of the test (string).

    testParDic:  Dictionary with parameters for running the test
                 (dictionary).

    dbmObj:      Test report DBM (ngamsDbm).
     
    srvNode:     Server (contacted) host (string).
    
    srvPort:     Port of server (contacted) host (integer).
    
    testNode:    Host tested (string).
    
    testPort:    Port number for host tested (integer).
    
    status:      Status of test (SUCCESS|FAILURE).
    
    statusInfo:  Additional information in connection with the test. Could be
                 NG/AMS XML Status Document or some error message (string).

    Returns:     Void.
    """
    info(4,"Entering addTestResult() ...")
    key = "%s_%s_%s_%s_%s" % (testId, srvNode, str(srvPort), testNode,
                              str(testPort))
    if (dbmObj.hasKey(key)):
        testRes = dbmObj.get(key)
    else:
        testRes = [testId] + (TEST_STAT_INFO * [None])
    testRes[TEST_TIME]      = time.time()
    if (not testRes[LAST_REP_TIME]): testRes[LAST_REP_TIME] = 0
    testRes[SRV_HOST_ID]    = srvNode
    testRes[SRV_PORT_NO]    = srvPort
    testRes[HOST_ID]        = testNode
    testRes[PORT_NO]        = testPort
    testRes[TEST_STAT]      = status
    testRes[TEST_STAT_INFO] = statusInfo
    dbmObj.add(key, testRes)
    dbmObj.sync()
    info(4,"Leaving addTestResult()")


def sendCmd(testParDic,
            srvNode,
            srvPort,
            cmd,
            cmdPars):
    """
    Send a command to the specified server.

    testPar:  Dictionary with parameters for running the test
              (dictionary).

    srvNode:  Server node (string).
    
    srvPort:  Server port (integer).
    
    cmd:      Command (string).

    cmdPars:  List of parameters to send (list).

    Returns:  Tuple containing information about the test:

                (<Status (0|1)>, <XML Status>|<Info>|None>
    """
    info(4,"Entering sendCmd() ...")
    try:
        stat = ngamsPClient.ngamsPClient().setTimeOut(testParDic["TIMEOUT"]).\
               sendCmdGen(srvNode, srvPort, cmd, pars=cmdPars)
    except Exception, e:
        return (TEST_FAILURE, str(e))
    info(4,"Leaving sendCmd()")
    if (stat.getStatus() == NGAMS_SUCCESS):
        return (TEST_SUCCESS, stat.genXmlDoc())
    else:
        return (TEST_FAILURE, stat.genXmlDoc())


def execTest(testId,
             testParDic,
             dbmObj,
             srvNode,
             srvPort,
             testNode,
             testPort,
             cmd,
             cmdPars):
    """
    Execute a test +

    testId:     Test ID (string).

    testParDic: Dictionary with parameters for running the test
                (dictionary).
                   
    dbmObj:     Test Report DBM (ngamsDbm).
    
    srvNode:    Server Host (string).
    
    srvPort:    Server Port (integer).

    testNode:   Test node name (string).

    testPort:   Test port number (integer).
      
    cmd:        Command (string).
    
    cmdPars:    List with parameters (list).

    Returns:    Status (integer/SUCCESS=0,FAILURE=1).
    """
    info(4,"Entering execTest() ...")
    stat, testInfo = sendCmd(testParDic, srvNode, srvPort, cmd, cmdPars)
    addTestResult(testId, testParDic, dbmObj, srvNode, srvPort, testNode,
                  testPort, stat, testInfo)
    info(4,"Leaving execTest()")
    return stat


#############################################################################
# Test functions.
#############################################################################
def test_status_host_id(testParDic,
                        dbConObj,
                        dbmObj,
                        srvNode,
                        srvPort,
                        testNode,
                        testPort,
                        pars):
    """
    Execute STATUS?host_id test.

    testParDic:   Test parameter dictionary containing the parameters specified
                  for the test (dictionary).
    
    dbConObj:     NG/AMS DB object connected to the DB (ngamsDb).
    
    dbmObj:       NG/AMS DBM object associated to open DBM. This DBM contains
                  the contents for the tests. See also man-page for
                  ngamsUtils.ngamsCheckSystem (ngamsDbm).
    
    srvNode:      Server node name of server contacted for the test (string).
    
    srvPort:      Port of server node (integer).
    
    testNode:     (Sub-)node to be tested (string).

    testPort:     Port number of sub-node (integer).
    
    pars:         Parameters to pass on to System Check Plug-In (string).

    Returns:      Status of executing the test (integer/0=SUCCESS, 1=FAILURE).
    """
    info(4,"Entering test_status_host_id() ...")
    cmdPars = [["host_id", testNode]]
    stat = execTest("test_status_host_id", testParDic, dbmObj, srvNode,
                    srvPort, testNode, testPort, NGAMS_STATUS_CMD, cmdPars)
    info(4,"Leaving test_status_host_id()")
    return stat


def test_retrieve_files(testParDic,
                        dbConObj,
                        dbmObj,
                        srvNode,
                        srvPort,
                        testNode,
                        testPort,
                        pars):
    """
    Retrieve a set of 10 random files from each disk mounted on the given
    test node.
    """
    return TEST_SUCCESS
#############################################################################


def loadInvokeTest(testPlugIn,
                   testParDic,
                   dbConObj,
                   dbmObj,
                   srvNode,
                   srvPort,
                   testNode,
                   testPort,
                   pars):
    """
    Function to load test plug-in and execute it.

    testPlugIn:   Name of test plug-in (string).

    testParDic:   Dictionary with parameters for running the test (dictionary).

    dbConObj:     DB connection object (ngamsDb).
    
    dbmObj:       DBM test object (ngamsDbm).
    
    srvNode:      Server host (string).
    
    srvPort:      Server port (integer).
    
    testNode:     Test Node (string).
    
    testPort:     Test node port (integer).
    
    pars:         Parameters to transfer to Test Plug-In (string).

    Returns:      SUCCESS=0, FAILURE=1 (integer/0|1).
    """
    # IMPL: Load plug-in.
    piCmdFormat = "%s(testParDic, dbConObj, dbmObj, srvNode, srvPort, " +\
                  "testNode, testPort, pars)"
    stat = eval(piCmdFormat % testPlugIn)
    return stat


def serverLoop(testParDic,
               serverList,
               ignoreSrvList,
               domainList,
               dbCon,
               dbmObj):
    """
    Loop over the contact servers and execute the tests for each node
    in the given domains, going through the contact node.

    testParDic:    Dictionary with parameters for running the test
                   (dictionary).
                   
    serverList:    List of node/ports to contact for the tests (list).

    ignoreSrvList: List of servers to ignore (list).
    
    domainList:    List of domains to take into account (list).

    dbCon:         DB connection object (ngamsDb).

    dbmObj:        DBM object (ngamsDbm).

    Returns:       Number of problems encountered (integer).
    """
    info(4,"Entering serverLoop() ...")
    problemCount = 0

    # Get list of nodes.
    sqlQuery = "SELECT host_id, srv_port FROM ngas_hosts " +\
               "WHERE domain IN ('%s') " +\
               "AND (srv_state='ONLINE' OR srv_suspended=1)"
    sqlQuery = sqlQuery % str(testParDic["DOMAINS"]).strip()
    nodeList = dbCon.query(sqlQuery)
    for server in serverList:
        srvNode, srvPort = server.split(":")
        info(2,"Server: %s:%s" % (srvNode, str(srvPort)))
        for testNode, testPort in nodeList[0]:
            # Check if this server should be ignored.
            if (ngamsLib.elInList(ignoreSrvList, "%s:%s" %\
                                  (testNode, str(testPort)))): continue
            
            info(2,"  - Test node: %s/port: %s" % (testNode, testPort))
            for testCase in ["test_status_host_id", "test_retrieve_files"]:
                info(2,"    - Test: %s ..." % testCase)
                stat = loadInvokeTest(testCase, testParDic, dbCon, dbmObj,
                                      srvNode, srvPort, testNode,testPort,None)
                if (not stat):
                    info(2,"      - Result: SUCCESS")
                else:
                    info(2,"      - Result: FAILURE")
                problemCount += stat

    # Now, loop again over the failing nodes to cross-check.
    for n in range(3):
        problemCount = 0
        for nodeKey in dbmObj.keys():
            testRes = dbmObj.get(nodeKey)
            if (testRes[TEST_STAT] == TEST_FAILURE):
                testCase = testRes[TEST_ID]
                srvNode  = testRes[SRV_HOST_ID]
                srvPort  = testRes[SRV_PORT_NO]
                testNode = testRes[HOST_ID]
                testPort = testRes[PORT_NO]

                # Is this node online at all?
                onlineNodeList = nodeList[0]
                testNodePort = (testNode, testPort)
                if (not ngamsLib.elInList(onlineNodeList, testNodePort)):
                    continue
                
                # Ignore this node?
                testNodePort = "%s:%s" % (testNode, str(testPort))
                if (ngamsLib.elInList(ignoreSrvList, "%s" % testNodePort)):
                    continue
                
                msg = "Retrying test case: %s on node: %s:%s using server: "+\
                      "%s:%s"
                info(2, msg % (testCase, testNode, str(testPort), srvNode,
                               str(srvPort)))
                stat = loadInvokeTest(testCase, testParDic, dbCon, dbmObj,
                                      srvNode, srvPort, testNode, testPort,
                                      None)
                if (stat): problemCount += 1
        # Make a small sleep if problems were found and we want to retry.
        if (problemCount and (n < 2)): time.sleep(10)
            
    info(4,"Leaving serverLoop()")
    return problemCount


def genReport(testParDic,
              dbmObj):
    """
    Generate a report and send it to the email recipients defined.

    testParDic:  Dictionary with parameters for running the test (dictionary).

    dbmObj:      Test report DBM object (ngamsDbm).

    Returns:     Void.
    """
    # Get number of tests + number of errors encountered (=failed tests).
    testIds = dbmObj.keys()
    testCount = 0
    errCount  = 0
    for testId in testIds:
        testRes = dbmObj.get(testId)
        if (testRes[TEST_TIME] >= testParDic["_startTime"]):
            testCount += 1
            if (testRes[TEST_STAT] == TEST_FAILURE): errCount += 1

    # Build up report
    repTime = time.time()
    repTimeIso = ngasUtilsLib.secs2Iso(repTime)
    startTime = ngasUtilsLib.secs2Iso(testParDic["_startTime"])
    endTime   = ngasUtilsLib.secs2Iso(testParDic["_endTime"])
    repFormat = "%-20s %s\n"
    report = 80 * "=" + "\n"
    report += "NGAS SYSTEM CHECK REPORT:\n"
    report += 80 * "=" + "\n"
    report += repFormat % ("Date:", repTimeIso)
    report += repFormat % ("Node:", getHostName())
    report += repFormat % ("User:", getpass.getuser())
    report += repFormat % ("Start Time:", startTime)
    report += repFormat % ("End Time:", endTime)
    duration = ((testParDic["_endTime"] - testParDic["_startTime"]) / 3600.0)
    report += repFormat % ("Duration:", ("%.3f hours" % duration))
    report += repFormat % ("Number of Tests:",  str(testCount))
    report += repFormat % ("Number of Errors:", str(errCount))
    report += 80 * "-" + "\n"
    report += "Test Parameters:\n"
    testPars = testParDic.keys()
    testPars.sort()
    for testPar in testPars:
        if (testPar[0] != "_"):
            report += repFormat % (testPar + ":", testParDic[testPar])
    report += 80 * "-" + "\n"

    # Make status for each test to be reported.
    testIds = dbmObj.keys()
    testIds.sort()
    testCount = 0
    for testId in testIds:
        testRes = dbmObj.get(testId)
        try:
            # Report only a problem every 24 hours.
            if ((repTime - testRes[LAST_REP_TIME]) >=
                (testParDic["REPORT-INT"] * 3600.0)):
                repEntry = 1
            else:
                repEntry = 0
        except:
            repEntry = 1
        if (testParDic["FORCE-NOTIF"] or repEntry):
            if (((testRes[TEST_STAT] == TEST_SUCCESS) and
                 testParDic["REPORT-ALL"]) or
                (testRes[TEST_STAT] == TEST_FAILURE) and
                (testRes[TEST_TIME] >= testParDic["_startTime"])):
                testCount += 1
                report += repFormat % ("Test ID:", testRes[TEST_ID])
                testTime = ngasUtilsLib.secs2Iso(testRes[TEST_TIME])
                report += repFormat % ("Test Time:", testTime)
                lastRepTime = ngasUtilsLib.secs2Iso(testRes[LAST_REP_TIME])
                report += repFormat % ("Last Report Time:", lastRepTime)
                report += repFormat % ("Server:",
                                       ("%s:%s" % (testRes[SRV_HOST_ID],
                                                   str(testRes[SRV_PORT_NO]))))
                report += repFormat % ("Test Node:", ("%s:%s" %\
                                        (testRes[HOST_ID],
                                         str(testRes[PORT_NO]))))
                if (testRes[TEST_STAT] == TEST_SUCCESS):
                    tmpStat = "SUCCESS"
                else:
                    tmpStat = "FAILURE"
                report += repFormat % ("Test Result:", tmpStat)
                if (testParDic["DETAILS"]):
                    report += "Details:\n"
                    report += str(testRes[TEST_STAT_INFO]) + "\n"
                report += 80 * "-" + "\n"
                testRes[LAST_REP_TIME] = repTime
                dbmObj.add(testId, testRes)
            
    report += "\nEOF\n"

    if ((testParDic.has_key("NOTIF-EMAIL") and testCount) or
        (testParDic.has_key("NOTIF-EMAIL") and testParDic["FORCE-NOTIF"])):
        ngasUtilsLib.sendEmail("NGAS SYSTEM CHECK REPORT",
                               testParDic["NOTIF-EMAIL"], report)


def checkSystem(testParDic):
    """
    Carry out a system check for the given system 

    testParDic:   Dictionary with parameters for running the test (dictionary).

    Returns:      Void.
    """
    info(4,"Entering serverLoop() ...")
    checkCreatePath(testParDic["WORKING-DIR"])
    srvList = testParDic["SERVERS"].split(",")
    srvList.sort()
    if (testParDic["IGNORE-SERVERS"]):
        ignoreSrvList = testParDic["IGNORE-SERVERS"].split(",")
        ignoreSrvList.sort()
    else:
        ignoreSrvList = []
    domainList = testParDic["DOMAINS"].split(",")
    domainList.sort()
    dbSrv, db, user, password = ngasUtilsLib.getDbPars()
    dbCon = ngamsDb.ngamsDb(dbSrv, db, user, password, 0)
    srvStr = str(srvList)[1:-1].replace(",", "-").replace(" ", "").\
             replace("'", "")
    domStr = str(domainList)[1:-1].replace(",", "-").replace(" ", "").\
             replace("'", "")
    dbmName = os.path.normpath("%s/NGAS_SYS_CHECK_%s_%s_%s.bsddb" %\
                               (testParDic["WORKING-DIR"], os.environ["USER"],
                                srvStr, domStr))
    dbmObj = ngamsDbm.ngamsDbm(dbmName, writePerm=1)
    testParDic["_testTmpPath"] = os.path.normpath("%s/NGAS_SYS_CHECK_%.6f" %\
                                                  (testParDic["WORKING-DIR"],
                                                   time.time()))
    os.system("mkdir -p %s" % testParDic["_testTmpPath"])
    try:
        serverLoop(testParDic, srvList, ignoreSrvList, domainList, dbCon,
                   dbmObj)
        testParDic["_endTime"] = time.time()
        dbmObj.sync()
        os.system("rm -rf %s" % testParDic["_testTmpPath"])
    except Exception, e:
        dbmObj.sync()
        os.system("rm -rf %s" % testParDic["_testTmpPath"])
        raise Exception, e

    # Handle the report.
    genReport(testParDic, dbmObj)

    info(4,"Leaving serverLoop()")


def correctUsage():
    """
    Return the usage/online documentation in a string buffer.

    Returns:  Man-page (string).
    """
    return __doc__

  
if __name__ == '__main__':
    """
    Main function to execute the tool.
    """
    setDebug(1)
    
    # Parse input parameters.
    testParDic = testParDic()
    idx = 1
    while idx < len(sys.argv):
        parOrg = sys.argv[idx]
        par    = parOrg.upper()
        try:
            if (par.find("--DETAILS") == 0):
                testParDic["DETAILS"] = 1
            elif (par.find("--DOMAINS") == 0):
                testParDic["DOMAINS"] = sys.argv[idx].split("=")[-1]
            elif (par.find("--FORCE-NOTIF") == 0):
                testParDic["FORCE-NOTIF"] = 1
            elif (par.find("--NOTIF-EMAIL") == 0):
                testParDic["NOTIF-EMAIL"] = sys.argv[idx].split("=")[-1]
            elif (par.find("--REPORT-ALL") == 0):
                testParDic["REPORT-ALL"] = 1
            elif (par.find("--REPORT-INT") == 0):
                testParDic["REPORT-INT"] = int(sys.argv[idx].split("=")[-1])
            elif (par.find("--SERVERS") == 0):
                testParDic["SERVERS"] = sys.argv[idx].split("=")[-1]
            elif (par.find("--IGNORE-SERVERS") == 0):
                testParDic["IGNORE-SERVERS"] = sys.argv[idx].split("=")[-1]
            elif (par.find("--VERBOSE") == 0):
                setLogCond(0, "", 0, "", int(sys.argv[idx].split("=")[-1]))
            elif (par.find("--TIMEOUT") == 0):
                testParDic["TIMEOUT"] = float(sys.argv[idx].split("=")[-1])
            elif (par.find("--WORKING-DIR") == 0):
                testParDic["WORKING-DIR"] = sys.argv[idx].split("=")[-1]
            else:
                raise Exception, "Unknown parameter: %s" % parOrg
            idx += 1
        except Exception, e:
            print "\nProblem executing the tool: %s\n" % str(e)
            print correctUsage()  
            sys.exit(1)
    if ((not testParDic["SERVERS"]) or (not testParDic["DOMAINS"]) or
        (not testParDic["NOTIF-EMAIL"]) or (not testParDic["WORKING-DIR"])):
        print correctUsage()  
        raise Exception, "Incorrect/missing command line parameter(s)!"
    try:
        checkSystem(testParDic)
    except Exception, e:
        print "Problem encountered handling request:\n\n%s\n" % str(e)
        sys.exit(1)

# EOF
