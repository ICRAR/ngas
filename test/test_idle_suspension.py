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
# "@(#) $Id: ngamsIdleSuspensionTest.py,v 1.6 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  18/11/2003  Created
#
"""
This module contains the Test Suite for the handling of Idle Suspension.
"""

import collections
import itertools
import time

from six.moves.urllib import parse as urlparse  # @UnresolvedImport

from ngamsLib.ngamsCore import getHostName, NGAMS_CHECKFILE_CMD, rmFile
from .ngamsTestLib import ngamsTestSuite, loadFile, genTmpFilename, unzip, tmp_path


SUSP_EL = "NgamsCfg.HostSuspension[1]"

# Simulated nodes in the cluster.
def get_nodes():
    master = getHostName() + ":8000"
    subNode1 = getHostName() + ":8001"
    subNode2 = getHostName() + ":8002"
    suspended = (subNode1, subNode2)
    return master, suspended

# Log files.
_logPat = tmp_path("NGAS:%d/log/LogFile.nglog")
masterNodeLog = _logPat % 8000
subNode1Log   = _logPat % 8001
subNode2Log   = _logPat % 8002


def prepSimCluster(testObj,
                   noOfSubNodes = 1,
                   cfgParDic = {}):
    """
    Prepare a simulated cluster with 1 master node and 2 sub-nodes. The
    two sub-nodes will suspend themselves within 10 seconds of idling.

    testObj:      Reference to test case object (ngamsTestSuite)

    noOfSubNodes: Number of sub-nodes (integer).

    cfgParDic:    Dictionary with entry for each server in the simluated
                  cluster, for which it is desirable to change configuration
                  parameters (dictionary/list).

    Returns:      Void.
    """

    masterNode, _ = get_nodes()
    subnode_ports = [n + 8001 for n in range(noOfSubNodes)]
    subnode_pars = (("NgamsCfg.HostSuspension[1].IdleSuspension", '1'),
                    ("NgamsCfg.HostSuspension[1].IdleSuspensionTime", '5'),
                    ("NgamsCfg.HostSuspension[1].WakeUpServerHost", masterNode))

    all_ports = [8000] + subnode_ports
    all_pars = (('NgamsCfg.JanitorThread[1].SuspensionTime', '0T00:00:01'),
                ('NgamsCfg.Log[1].LocalLogLevel', '5'))

    cfg = collections.defaultdict(list)
    for port, par in itertools.product(subnode_ports, subnode_pars):
        cfg[port].append(par)
    for port, par in itertools.product(all_ports, all_pars):
        cfg[port].append(par)

    # If special configuration parameters are specified by the Test Case these
    # are stored last in the locCfgParDic to ensure that these are taken.
    for port, pars in cfgParDic.items():
        cfg[port] += pars

    srvs = [(port, pars) for port, pars in cfg.items()]
    return testObj.prepCluster(srvs)


class ngamsIdleSuspensionTest(ngamsTestSuite):
    """
    Synopsis:
    Test Suite for the  Idle Suspension Feature.

    Description:
    The purpose of the Test Suite is to test the Idle Suspension/Wake-Up
    Feature. As it is not possible to suspend the test node, a special
    Suspension and Wake-Up Plug-in have been provided which simulate that
    the server is suspended without actually suspending it.

    Missing Test Cases:
    - CLONE (from suspended sub-node).
    - Check that node is not suspending itself when:
        - DCC is on-going.
        - Request is being handled.
        - Subscription data being delivered
    - Check that a node suspends itself if configured so, within
      the specified period of time.
    - Check that sub-nodes are woken up at:
        - CONFIG?<Different Combinations>&host_id
        - STATUS: Check if more relevant cases.
    (All tests with HTTP Authentication).

    - RETRIEVE?disk_id&file_id&file_version.
    - CHECKDISK (file on suspended sub-node).
    - Check that node is not suspending itself when subscription data
      being delivered
    """

    def test_SuspendNode_1(self):
        """
        Synopsis:
        Test that a node suspends itself within the specified period of time.

        Description:
        The purpose of the test is to verify that the NG/AMS Server suspends
        itself when Idle Suspension is enabled within the specified period
        of time.

        Expected Result:
        After starting the server, it should suspend itself after the given
        period of time.

        Test Steps:
        - Start server with a configuration file specifying idle suspension
          after 10s.
        - Wait till the server indicates in the DB that it is suspended
          with a time-out of 20s.
        - Afterwards, mark the suspended node to unsuspended to be able to
          shut down cleanly.

        Remarks:
        ...
        """
        masterNode, susp_nodes = get_nodes()
        subNode1 = susp_nodes[0]
        dbConObj = prepSimCluster(self)[masterNode][1]
        self.waitTillSuspended(dbConObj, subNode1, 30, susp_nodes)
        self.markNodesAsUnsusp(dbConObj, susp_nodes)


    def test_WakeUpStatus_1(self):
        """
        Synopsis:
        Test that suspended server is woken up when
        STATUS?host_id=<Sub-Node> specifying that node is sent.

        Description:
        If a STATUS Command is send to a node requesting status from another
        node (Proxy Mode), and the target is suspended, the contacted node
        must wake up the suspended target node. The purpose of this test
        is to verify this use case.

        Expected Result:
        The contacted node should identify the target node as suspended,
        and should wake it up prior to forwarding the STATUS Command acting
        as proxy.

        Test Steps:
        - Start simulated cluster specifying that one node can suspend itself.
        - Wait till sub-node has suspended itself.
        - Send STATUS Command to the simulated master requesting status from
          the suspended sub-node.
        - Check the response to the STATUS Command.
        - Cross check that the suspended node is now woken up by sending
          a STATUS Command directly to the sub-node.

        Remarks:
        ...
        """
        masterNode, susp_nodes = get_nodes()
        subNode1 = susp_nodes[0]
        dbConObj = prepSimCluster(self)[masterNode][1]
        self.waitTillSuspended(dbConObj, subNode1, 30, susp_nodes)

        # 1. Send STATUS Command to sub-node using master as proxy.
        self.status(8000, pars=[["host_id", subNode1]])

        # 2. Double-check that sub-node is no longer suspended.
        self.status(8001)

        # Clean up.
        self.markNodesAsUnsusp(dbConObj, susp_nodes)


    def test_WakeUpStatus_2(self):
        """
        Synopsis:
        Test that suspended server is woken up when STATUS?file_access is
        issued, specifying a file on a suspended sub-node.

        Description:
        The purpose of the test is to verify that a suspended sub-node is
        woken up by a master node acting as proxy if a STATUS?file_id is
        send pointing to a file on the suspended sub-node.

        Expected Result:
        The suspended sub-node should be woken up by the master node which
        after the suspended sub-node has come Online should forward the
        request which is executed on the sub-node and the result send back to
        the client through the master node.

        Test Steps:
        - Prepare simulated cluster with unit, which suspends itself.
        - Archive 3 files onto sub-node.
        - Wait till sub-node has suspended itself.
        - Send a STATUS?file_id request to the Master Node specifying one
          of the files previously archived.
        - Check the result returned from the sub-node via the Master Node.

        Remarks:
        ...
        """
        masterNode, susp_nodes = get_nodes()
        subNode1 = susp_nodes[0]
        dbConObj = prepSimCluster(self)[masterNode][1]

        # Archive some files on the two nodes and wait till sub-node
        # has suspended itself.
        self.archive(8000, "src/TinyTestFile.fits")
        self.archive(8000, "src/SmallFile.fits")
        self.archive(8001, "src/SmallFile.fits")
        self.waitTillSuspended(dbConObj, subNode1, 20, susp_nodes)

        # Retrieve information about the file on the suspended sub-node.
        fileId = "TEST.2001-05-08T15:25:00.123"
        statObj = self.status(8000, pars=[["file_access", fileId]])
        refStatFile = "ref/ngamsIdleSuspensionTest_test_WakeUpStatus_2_1_ref"
        msg = "Unexpected reply to STATUS?file_access request"
        self.assert_status_ref_file(refStatFile, statObj, msg=msg)


    def test_WakeUpRetrieve_1(self):
        """
        Synopsis:
        Check that RETRIEVE?file_id and RETRIEVE?file_id&file_version are
        correctly handled when the file specified is stored on suspended node.

        Description:
        The test verifies that a suspended sub-node is woken up when a
        RETRIEVE Request is send to the Master Node and the file properly
        retrieved and send back to the client via the Master Node acting
        as proxy.

        Expected Result:
        The Master Node server should identify the target node as suspended
        and should wake it up before requesting the data. Subsequently the
        data should be send back to the client by the Master Node acting
        as proxy.

        Test Steps:
        - Start simulated cluster with a sub-node suspending itself after
          a short while.
        - Archive a small FITS file 3 times onto the sub-node.
        - Wait till the sub-node has suspended itself.
        - Submit a REQUEST?file_id to the Master Node.
        - Check that the response is as expected.
        - Check that the file has arrived on disk.

        Remarks:
        TODO!: Check that the file has arrived on disk as expected.
        """
        masterNode, susp_nodes = get_nodes()
        subNode1 = susp_nodes[0]
        dbConObj = prepSimCluster(self)[masterNode][1]
        for n in range(3):
            self.archive(8001, "src/SmallFile.fits")
        # Retrieve the file as file_id, file_id/file_version.
        testParsList = [["TEST.2001-05-08T15:25:00.123", -1],
                        ["TEST.2001-05-08T15:25:00.123", 2]]
        for fileId, version in testParsList:
            self.waitTillSuspended(dbConObj, subNode1, 20, susp_nodes)

            # different prefixes to avoid name clashes
            tmpRetFile = genTmpFilename("original_")
            unzippedRetFile = genTmpFilename("unzip_")

            self.retrieve(8000, fileId, fileVersion=version, targetFile=tmpRetFile)
            unzip(tmpRetFile, unzippedRetFile)
            self.checkFilesEq("src/SmallFile.fits", unzippedRetFile, "File retrieved incorrect")


    '''def test_WakeUpRetrieve_2(self):
        """
        Synopsis:
        Check that RETRIEVE?ng_log&host_id is correctly handled
        when the file specified is stored on a suspended node.

        Description:
        The purpose of the test is to verify that a suspended sub-node is
        woken up by a Master Node if a RETRIEVE?ng_log&host_id Request is
        sent to the Master Node specifying the suspended sub-node as target
        node.

        Expected Result:
        The Master Node should identify the target sub-node as suspended and
        should wake it up before forwarding the Retrieve Request. Subsequently
        the Master should send back the requested data to the client.

        Test Steps:
        - Start simulated cluster with a sub-node suspending itself after
          a short while.
        - Wait till the sub-node has suspended itself.
        - Send a RETRIEVE?ng_log&host_id Command to the Master Node specifying
          the sub-node as target.
        - Check that the response returned from the suspended node via the
          Master Node is as expected.

        Remarks:
        TODO!: Check that the requested file has arrived on the destination.
        """
        cfgParDic = {"8001": [["%s.IdleSuspensionTime" % SUSP_EL, "5"]]}
        dbConObj = prepSimCluster(self, cfgParDic=cfgParDic)[masterNode][1]
        waitTillSuspended(self, dbConObj, subNode1, 10)
        pars = [["ng_log", ""], ["host_id", subNode1]]
        targetFile = genTmpFilename()
        sendPclCmd(port=8000, auth=AUTH).\
                              get_status(NGAMS_RETRIEVE_CMD, 0, targetFile, pars)
        time.sleep(2)

        # We have to retrieve it twice, since otherwise, maybe not all info
        # has been written to the log file.
        sendPclCmd(port=8000, auth=AUTH).\
                              get_status(NGAMS_RETRIEVE_CMD, 0, targetFile, pars)
        # Check that the proper log file has been retrieved.
        testStr = ["NGAS Node: %s:8001 woken up after" % getHostName()]
        logBuf = loadFile(targetFile)
        self.checkTags(logBuf, testStr, showBuf=0)


    def test_WakeUpRetrieve_3(self):
        """
        Synopsis:
        Check that RETRIEVE?cfg&host_id is correctly handled when the file
        specified is stored on a suspended node.

        Description:
        Check that a suspended sub-node is woken up if the NG/AMS Configuration
        used on that node is requested via a Master Node acting as proxy.

        Expected Result:
        The Master Node should identify the sub-node as suspended and should
        wake up the suspended sub-node before forwarding the RETRIEVE Command.

        Test Steps:
        - Start simulated cluster with a sub-node suspending itself after a
          short while.
        - Wait till the sub-node has suspended itself.
        - Send Retrieve Request requesting the configuration from the
          suspended sub-node.
        - Check that the cfg. file has been properly retrieved.

        Remarks:
        ...
        """
        cfgParDic = {"8001": [["NgamsCfg.Server[1].ArchiveName", subNode1],
                              ["%s.IdleSuspensionTime" % SUSP_EL, "5"]]}
        dbConObj = prepSimCluster(self, cfgParDic=cfgParDic)[masterNode][1]
        waitTillSuspended(self, dbConObj, subNode1, 10)
        pars = [["cfg", ""], ["host_id", subNode1]]
        targetFile = genTmpFilename()
        sendPclCmd(port=8000, auth=AUTH).\
                              get_status(NGAMS_RETRIEVE_CMD, 0, targetFile, pars)
        # Check that the proper cfg. file has been retrieved.
        testStr = ["ArchiveName=\"%s\"" % subNode1]
        logBuf = loadFile(targetFile)
        self.checkTags(logBuf, testStr, showBuf=1)


    def test_WakeUpRetrieve_4(self):
        """
        Synopsis:
        Check that RETRIEVE?internal&host_id is correctly handled
        when the file specified is stored on a suspended node.

        Description:
        Verify that an 'internal file' hosted on a suspended sub-node is
        correctly retrieved by a Master Node acting as proxy.

        Expected Result:
        Teh contacted Master Node should identify that the requested target
        node is suspended and should wake it up before forwarding the
        request. Subsequently it should send back the requested file.

        Test Steps:
        - Start simluated cluster with a sub-node suspending itself after a
          short while.
        - Wait till the sub-node has suspended itself.
        - Send the Retrieve Request requesting a file on the suspended
          sub-node.
        - Check that the file has been properly returned via the log file
          on the Master Node.
        - Check that the file has been properly retrieved via the log file
          on the sub-node.

        Remarks:
        ...
        """
        cfgParDic = {"8001": [["%s.IdleSuspensionTime" % SUSP_EL, "5"]]}
        dbConObj = prepSimCluster(self, cfgParDic=cfgParDic)[masterNode][1]
        waitTillSuspended(self, dbConObj, subNode1, 10)
        pars = [["internal", "/etc/hosts"], ["host_id", subNode1]]
        targetFile = genTmpFilename()
        sendPclCmd(port=8000, auth=AUTH).\
                              get_status(NGAMS_RETRIEVE_CMD, 0, targetFile, pars)
        time.sleep(2)

        # Check that the file has been retrieved from the proper host.
        # 1. Log file on Master Node.
        masterLogBuf = loadFile(masterNodeLog)
        ipAddr = socket.gethostbyname_ex(getHostName())[2][0]
        retFormat = "Issuing request with URL: http://%s:8001/RETRIEVE?"
        testTags = ["Waking up host with ID: %s ..." % subNode1,
                    "Pinging NG/AMS Server: %s/8001. Timeout: 10" % subNode1,
                    "Successfully pinged NG/AMS Server: %s/8001." % subNode1,
                    "internal=/etc/hosts",
                    "host_id=%s" % subNode1,
                    retFormat % ipAddr]
        self.checkTags(masterLogBuf, testTags, showBuf=0)
        # 2. Log File on Sub-Node.
        subNode1LogBuf = loadFile(subNode1Log)
        testTags = ["NGAS Node: %s woken up after" % subNode1,
                    "RETRIEVE?",
                    "internal=/etc/hosts",
                    "host_id=%s" % subNode1,
                    "Sending header: Content-Disposition: attachment; " +\
                    "filename=hosts"]
        self.checkTags(subNode1LogBuf, testTags, showBuf=0)'''

    def test_WakeUpCheckfile_1(self):
        """
        Synopsis:
        Check that CHECKFILE?file_id&file_version correctly handled
        when the referenced file is stored on a suspended node.

        Description:
        The purpose of the test is to verify that a CHECKFILE Command is
        properly handled by a Master node acting as a proxy and interacting
        with a suspended sub-node.

        Expected Result:
        The contacted Master Node should identify that the sub-node hosting
        the specified file is suspended, and should wake it up before
        forwarding the request. Subsequently it should forward the request to
        the sub-node and send back the result to the client.

        Test Steps:
        - Start simulated cluster with a sub-node suspending itself after a
          short while.
        - Archive 2 FITS file 2 times each.
        - Wait till the sub-node suspends itself.
        - Send a CHECKFILE Command to the Master node specifying to check
          one of the previously archived files.
        - Check response from the sub-node.
        - Check from the log entries in the Master Node that the CHECKFILE
          Command has been successfully executed.
        - Check from the log entries on the sub-node that  the CHECKFILE
          Command has been successfully executed.

        Remarks:
        ...
        """
        masterNode, susp_nodes = get_nodes()
        subNode1 = susp_nodes[0]
        cluster_info = prepSimCluster(self)
        dbConObj = cluster_info[masterNode][1]
        subnode1_cfg = cluster_info[subNode1][0]
        self.archive(8001, "src/TinyTestFile.fits")
        self.archive(8001, "src/SmallFile.fits")
        self.archive(8000, "src/TinyTestFile.fits")
        self.archive(8000, "src/SmallFile.fits")
        self.waitTillSuspended(dbConObj, subNode1, 10, susp_nodes)

        # Execute CHECKFILE Command on a file on the suspended sub-node.
        file_id = "TEST.2001-05-08T15:25:00.123"
        cmdPars = [["file_id", file_id],
                   ["file_version", "1"]]
        statObj = self.get_status(8000, NGAMS_CHECKFILE_CMD, pars=cmdPars)
        refStatFile="ref/ngamsIdleSuspensionTest_test_WakeUpCheckfile_1_1_ref"
        msg = "CHECKFILE Command not executed on sub-node as expected"
        self.assert_status_ref_file(refStatFile, statObj, msg=msg, cfg=subnode1_cfg)

        #tstStr = "NGAMS_INFO_FILE_OK:4056:INFO: Checked file with File ID: "+\
        #       "TEST.2001-05-08T15:25:00.123/File Version: 1/Disk ID"
        #if (statObj.getMessage().find(tstStr) == -1):
        #self.fail("CHECKFILE Command not executed on sub-node as expected")

        # Check that expected log entries found in the Master Node Log File.
        refStatFile="ref/ngamsIdleSuspensionTest_test_WakeUpCheckfile_1_2_ref"
        testTags = [loadFile(refStatFile) % (subNode1,)]
        self.status(8000)
        masterLogBuf = loadFile(masterNodeLog)
        self.checkTags(masterLogBuf, testTags, showBuf=0)
        # Check that expected log entries found in the Sub-Node Log File.
        tmpTag = "File list to check: (1: Location:LOCAL, Host:%s, " +\
                 "Version:1) (2: Location:LOCAL, Host:%s, Version:1)"
        testTags = [('CHECKFILE?file_id=' + urlparse.quote(file_id) +
                    '&file_version=1&time_out=60.0'),
                    tmpTag % (subNode1, subNode1)]
        subNodeLogBuf = loadFile(subNode1Log)
        self.checkTags(subNodeLogBuf, testTags, showBuf=0)


    def test_WakeUpDataCheck_1(self):
        """
        Synopsis:
        Check that a suspended sub-node is woken up when DCC is due and that
        the DCC is executed as expected after the node has been woken up.

        Description:
        Before suspending itself, a sub-node should request to be woken up
        when the time for executing the next DCC is due.

        The purpose of the test is to verify that a sub-node suspending itself
        requests to be woken up at the specified point in time, and that the
        contacted Master Node wakes up a suspended sub-node as expected.

        Expected Result:
        The contacted Master Node should identify that the sub-node is
        suspended and that it has requested to be woken up by the Master Node
        at a given point in time. The Master Node should wake up the node at
        the requested point in time and the sub-node should carry out the DCC.

        Test Steps:
        - Prepare simulated cluster with a sub-node suspending itself after a
          short while and having DCC enabled with a high frequency.
        - Archive two files onto the sub-node.
        - Wait till the sub-node has suspended itself.
        - Wait till the sub-node has been woken up.
        - Check that entries in the log file on the Master Node indicate
          that it has woken up the suspended sub-node.
        - Check that entries in the log file on the sub-node indicate that it
          has been woken up.
        - Wait until the DCC has been executed and check that the result is
          as expected (the DCC summary log appears in the log file on the
          sub-node).

        Remarks:
        ...
        """
        masterNode, susp_nodes = get_nodes()
        subNode1 = susp_nodes[0]

        # Always delete sub-node log file (to run test with a fresh log file).
        rmFile(subNode1Log)

        # Enable DCC in 8001 + define a minimum check cycle of 15s.
        datacheck_period = 15
        cfgParDic = {8001: (("NgamsCfg.DataCheckThread[1].Active", "1"),
                            ("NgamsCfg.DataCheckThread[1].MinCycle", "00T00:00:%d" % datacheck_period))}
        dbConObj = prepSimCluster(self, cfgParDic=cfgParDic)[masterNode][1]

        # Archive two files into subnode 8001 and wait for it to go to suspended state
        self.archive(8001, "src/TinyTestFile.fits")
        self.archive(8001, "src/SmallFile.fits")
        self.waitTillSuspended(dbConObj, subNode1, 30, susp_nodes)

        # Get timestamp for the log indicating that the node suspended itself.
        suspLog = "NG/AMS Server %s suspending itself" % subNode1
        subNodeLog = loadFile(subNode1Log).split("\n")
        suspLogEntry = ""
        for line in subNodeLog:
            if (line.find(suspLog) != -1):
                suspLogEntry = line
                break
        if (not suspLogEntry):
            self.fail("Did not find expected log entry in sub-node " +\
                      "log file: " + suspLog)
        suspLogTime = suspLogEntry.split(" ")[0]

        # Now we have to wait until the sub-node is woken up for DCC.
        self.waitTillWokenUp(dbConObj, subNode1, 60, susp_nodes)

        # Check log output in Master Log File.
        tagFormat = "Waking up server %s"
        testTags = [tagFormat % (subNode1,)]
        self.checkTags(loadFile(masterNodeLog), testTags, showBuf=0)
        # Check log output in Sub-Node Log File.
        self.status(8001)  # Flush log cache.
        time.sleep(2) # and wait a bit
        testTags = ["NGAS Node: %s woken up after" % subNode1]
        self.checkTags(loadFile(subNode1Log), testTags, showBuf=0)

        # Wait until the DCC has been executed.
        startTime = time.time()
        datacheck_executed = False
        dccStatLog = None
        while (time.time() - startTime) < (datacheck_period * 1.5):

            lines = loadFile(subNode1Log).splitlines()
            lines.reverse()

            datacheck_time = None
            for line in lines:
                if "NGAMS_INFO_DATA_CHK_STAT" in line:
                    dccStatLog = line
                    datacheck_time = dccStatLog.split(" ")[0]
                    break
            if datacheck_time and datacheck_time < suspLogTime:
                datacheck_executed = True
                break

            time.sleep(2)

        if not datacheck_executed:
            self.fail("Data Consistency Checking not executed AFTER host " +\
                      "wake-up as expected")