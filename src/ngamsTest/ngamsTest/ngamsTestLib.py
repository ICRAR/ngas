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
# "@(#) $Id: ngamsTestLib.py,v 1.17 2009/02/12 23:17:48 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  19/04/2002  Created
#
"""
This module contains test utilities used to build the NG/AMS Functional Tests.
"""
# TODO: Check for each function if it can be moved to the ngamsTestSuite Class.

import os, sys, time, unittest, socket, getpass, commands, re, glob, subprocess, signal, shutil

from ngamsLib.ngamsCore import getHostName, TRACE, info, \
    ngamsCopyrightString, rmFile, NGAMS_BACK_LOG_TMP_PREFIX, NGAMS_BAD_FILES_DIR, \
    NGAMS_PROC_DIR, error, cleanList, setLogCond, getVerboseLevel, logFlush, \
    cpFile, getLogLevel, NGAMS_FAILURE, NGAMS_SUCCESS, getNgamsVersion, \
    checkIfIso8601
from ngamsLib import ngamsConfig, ngamsDb, ngamsLib
from ngamsPClient import ngamsPClient
from pccUt import PccUtUtils, PccUtTime


try:
    import pcfitsio
except:
    pass

# Global parameters to control the test run.
_noCleanUp   = 0

# Constants.
STD_DISK_STAT_FILT = ["AccessDate", "AvailableMb", "CreationDate", "Date",
                      "HostId", "IngestionDate", "InstallationDate",
                      "ModificationDate", "TotalDiskWriteTime", "Version"]
AUTH               = "bmdhczpuZ2Fz"


###########################################################################
# Handle reference configuration file.
###########################################################################
_refCfg = None
def setRefCfg(cfg):
    """
    Set the Reference Configuration File.

    cfg:       Name of NG/AMS Configuration File to use (string).

    Returns:   Void.
    """
    global _refCfg
    _refCfg = cfg

def getRefCfg():
    """
    Return name of Reference Configuration.

    Returns:  Name of Reference Configuration File (string).
    """
    global _refCfg
    return _refCfg

def mergeRefCfg(targCfg):
    """
    Merges the parameters from the reference configuration into the given
    target configuration.

    targCfg:    Target configuration object (ngamsConfig).

    Return:     Void
    """
    refCfgObj = ngamsConfig.ngamsConfig().load(getRefCfg())
    # Take over the DB parameters from the reference.
    dbParFormat = "NgamsCfg.Db[1].%s"
    dbParList = [["AutoRecover", refCfgObj.getDbAutoRecover()],
                 ["Interface", refCfgObj.getDbInterface()],
                 ["Name", refCfgObj.getDbName()],
                 ["Password", refCfgObj.getDbPassword()],
                 ["Server", refCfgObj.getDbServer()],
                 ["Snapshot", refCfgObj.getDbSnapshot()],
                 ["User", refCfgObj.getDbUser()],
                 ["Verify", refCfgObj.getDbVerify()],
                 ["Parameters", refCfgObj.getDbParameters()],
                 ["MultipleConnections",
                     refCfgObj.getVal("Db[1].MultipleConnections")]]
    for dbPar in dbParList:
        par = dbParFormat % dbPar[0]
        targCfg.storeVal(par, dbPar[1])

###########################################################################

###########################################################################
# Check that it is not possible to connect accidentally to an operating DB.
###########################################################################
if (os.path.exists("/opt/sybase/interfaces")):
    fo = open("/opt/sybase/interfaces")
    intLines = fo.readlines()
    for intLine in intLines:
        if ((intLine.find("ESOECF") != -1) or
            (intLine.find("ESOECF_DIRECT") != -1) or
            (intLine.find("ASTOPP") != -1) or
            (intLine.find("OLASLS") != -1)):
            errMsg = "The NG/AMS Unit Test cannot be executed on a host " +\
                     "that might be able to connect to ESOECF, ASTOPP or " +\
                     "OLASLS. Remove entries for these DB servers from " +\
                     "/opt/sybase/interfaces and run the test again"
            raise Exception, errMsg
###########################################################################

###########################################################################
# In order for this to work sendmail must be running locally.
#
# As root: # /etc/init.d/sendmail start
#
# In addition /etc/sysconfig/sendmail must have the following contents:
#
# DAEMON=yes
# QUEUE=15m
###########################################################################
if (os.path.exists("/etc/mail/sendmail.cf")):
    fo = open("/etc/mail/sendmail.cf")
    lines = fo.readlines()
    fo.close()
    foundDaemonYes = 0
    for line in lines:
        if (line.find("DAEMON") != -1):
    #        if (line.find("yes") != -1):
                foundDaemonYes = 1
    if (not foundDaemonYes):
        raise Exception, "Mail configuration incorrect. Set parameter: " + \
              "DAEMON=yes in /etc/mail/sendmail.cf"
    stat, out = commands.getstatusoutput("ps -efww|grep sendmail")
    psLines = out.split("\n")
    sendMailRunning = 0
    for psLine in psLines:
        if ((psLine.find("sendmail") != -1) and
            (psLine.find("ps -efww|grep sendmail") == -1)):
            sendMailRunning = 1
            break
    if (not sendMailRunning):
        errMsg = "Start local SMTP server as root " + \
                 "(# /etc/init.d/sendmail start)"
        print errMsg
        raise Exception, errMsg

if (os.path.exists("/etc/aliases")):
    # Check that no entry is defined for ngasmgr in /etc/aliases.
    fo = open("/etc/aliases")
    etcAliases = fo.readlines()
    fo.close()
    for line in etcAliases:
        line = line.strip()
        if (len(line)):
            if ((line[0] != "#") and (line.find("ngasmgr:") != -1)):
                errMsg = "Remove entry for ngasmgr in /etc/aliases (%s) and " + \
                         "run newaliases as root before running the tests. " + \
                         "Afterwards remember to restore the original settings!"
                errMsg = errMsg % line
                print line
                raise Exception, errMsg
###########################################################################

###########################################################################
# Check that the binaries of the C-Client API are there.
###########################################################################
try:
    global NGAMS_TEST_IGNORE_CCLIENT
    ignoreCClient = NGAMS_TEST_IGNORE_CCLIENT
except:
    ignoreCClient = 0
if (not ignoreCClient):
    bins = ["ngamsCClient", "ngamsArchiveClient"]
    for bin in bins:
        stat, out = commands.getstatusoutput(bin)
        if (out.find("No such file or directory") != -1):
            raise Exception, "NG/AMS C Client Module appears not " +\
                  "to be compiled!"
###########################################################################

###########################################################################
# START: Utility functions:
###########################################################################
def execCmd(cmd,
            raiseEx = 1,
            timeOut = 30.0):
    """
    Execute a shell command and return the exit status + output
    (stdout/stderr).

    cmd:       Shell command to execute (string).

    raiseEx:   If set to 1 an exception will be raised in case of error
               (integer/0|1).

    timeOut:   Timeout in seconds to wait for command completion (float).

    Returns:   Tuple with exit status + stdout/stderr output (string).
    """
    exitCode, stdOut, stdErr = PccUtUtils.execCmd(cmd, timeOut)
    out = stdOut + stdErr
    if (exitCode and raiseEx):
        errMsg = "Error executing shell command. Exit status: %d, ouput: %s"
        raise Exception, errMsg % (exitCode, out)
    return (exitCode, out)


def getClusterName():
    """
    Return the name of the simulated cluster.
    """
    return "%s:8000" % getHostName()


def getNmu():
    """
    Return the name of the Main Node for the simulated cluster.
    """
    return "%s:8000" % getHostName()


def getNcu11():
    """
    Return the name of the Cluster Node for the test.
    """
    return "%s:8011" % getHostName()


def cleanUp(cfgFile,
            delDirs,
            clearDb,
            retCfgObj):
    """
    Clean up the NG/AMS environment. This comprises deleting DB tables
    (ngas_disks, ngas_files) and to delete files and directories on the disk.

    cfgFile:   Cfg. file to use (string).

    delDirs:   If set to 1, the NG/AMS directories are deleted (integer/0|1).

    clearDb:   If set to 1, the NG/AMS tables are deleted (integer/0|1).

    Returns:   Void or tuple with cfg. object and DB object
               (void | tuple/(ngamsConfig, ngamsDb)).
    """
    T = TRACE(3)

    tmpCfgObj = ngamsConfig.ngamsConfig().load(cfgFile)

    # Delete NG/AMS directories if requested.
    if (delDirs):
        info(3,"Deleting NG/AMS directories ...")
        delNgamsDirs(tmpCfgObj)

    # Clear DB if requested.
    if (clearDb):
        info(3,"Clearing NGAS DB ...")
        multCons = tmpCfgObj.getDbMultipleCons()
        tmpDbObj = ngamsDb.\
                   ngamsDb(tmpCfgObj.getDbServer(), tmpCfgObj.getDbName(),
                           tmpCfgObj.getDbUser(), tmpCfgObj.getDbPassword(),
                           interface = tmpCfgObj.getDbInterface(),
                           parameters = tmpCfgObj.getDbParameters(),
                           multipleConnections = multCons)
        delNgasTbls(tmpDbObj)
        del tmpDbObj
        tmpDbObj = None

    if (retCfgObj):
        return tmpCfgObj
    else:
        pass


def waitReqCompl(clientObj,
                 requestId,
                 timeOut = 10):
    """
    Function to poll periodically an NG/AMS Server (running as an independent
    process) to check for completion of the given request.

    clientObj:      Instance of NG/AMS P-Client Class to be used to contact
                    the remote NG/AMS Server (ngamsPClient).

    requestId:      ID of request (string).

    TimeOut:        Timeout to apply, waiting for the remote NG/AMS Server
                    to finish the processing of the request (integer).

    Returns:        Last NG/AMS Status Object (ngamsStatus).
    """
    startTime = time.time()
    while ((time.time() - startTime) < timeOut):
        res = clientObj.\
              sendCmdGen(clientObj.getHost(), clientObj.getPort(), "STATUS",
                         1, "", [["request_id", str(requestId)]])
        if (res.getCompletionPercent() != None):
            if (float(res.getCompletionPercent()) >= 99.9):
                break
        time.sleep(0.100)
    if ((time.time() - startTime) > timeOut):
        errMsg = "Timeout waiting for request: %s (%s/%s) to finish"
        raise Exception, errMsg % (requestId, clientObj.getHost(),
                                   clientObj.getPort())
    return res


def markNodesAsUnsusp(dbConObj,
                      nodes):
    """
    Mark the sub-nodes as not suspended.

    Returns:   Void.
    """
    for subNode in nodes:
        try:
            dbConObj.resetWakeUpCall(subNode, 1)
        except:
            pass


# TODO: Move into ngamsTestSuite.
def waitTillSuspended(testObj,
                      dbConObj,
                      node,
                      timeOut,
                      nodes,
                      fail = 1):
    """
    Wait till a node has suspended itself (marked as suspended).

    testObj:     Reference to test case object (ngamsTestSuite).

    dbConObj:    DB connection object (ngamsDb).

    node:        NGAS ID for node (string).

    timeOut:     Time-out in seconds to wait for the node to suspend itself
                 (float).

    nodes:       List of nodes in the cluster (list).

    fail:        Clean up + make a unit test fail if the host is not woken up
                 within the given time-out (integr/0|1).

    Returns:     1 if node suspended itself within the given timeout, otherwise
                 0 is returned (integer/0|1).
    """
    startTime = time.time()
    while ((time.time() - startTime) < timeOut):
        nodeSusp = dbConObj.getSrvSuspended(node)
        if (nodeSusp):
            info(2,"Server suspended itself after: %.3fs" %\
                 (time.time() - startTime))
            break
        else:
            time.sleep(0.100)
    if (fail):
        if (nodeSusp):
            return 1
        else:
            markNodesAsUnsusp(dbConObj, nodes)
            testObj.fail("Sub-node did not suspend itself within %ds"%timeOut)
    else:
        return nodeSusp


# TODO: Move into ngamsTestSuite.
def waitTillWokenUp(testObj,
                    dbConObj,
                    node,
                    timeOut,
                    nodes,
                    fail = 1):

    """
    Wait until a suspended node has been woken up.

    See parameters for waitTillSuspended().

    Returns:    1 if node was woken up within the given timeout, otherwise
                0 is returned (integer/0|1).
    """
    startTime = time.time()
    while ((time.time() - startTime) < timeOut):
        nodeSusp = dbConObj.getSrvSuspended(node)
        if (not nodeSusp):
            info(2,"Server woken up after: %.3fs" % (time.time() - startTime))
            break
        else:
            time.sleep(0.100)
    if (fail):
        if (not nodeSusp):
            return 1
        else:
            markNodesAsUnsusp(dbConObj, nodes)
            testObj.fail("Sub-node not woken up within %ds" % timeOut)
    else:
        return (not nodeSusp)


def setNoCleanUp(noCleanUp):
    """
    Set the No Clean Up Flag.

    noCleanUp:     New value for flag (integer/0|1).

    Returns:       Void.
    """
    global _noCleanUp
    _noCleanUp = noCleanUp


def getNoCleanUp():
    """
    Return the No Clean Up Flag.

    Returns:    No Clean Up Flag (integer/0|1).
    """
    global _noCleanUp
    return _noCleanUp


def correctUsage():
    """
    Print out correct usage.

    Returns:   Void.
    """
    print "Input parameters for NG/AMS test programs:\n"
    print "<test program> [-v <level>] [-logLevel <level> " +\
          "-logFile <file>] [-tests <test name>] [-noCleanUp]\n"
    print ngamsCopyrightString()


def cmpFiles(refFile,
             testFile,
             sort = 0):
    """
    Compares two files, and returns the difference. If the files are
    identical, an empty string, '', is returned.

    refFile:     Name of reference file (string).

    testFile:    Name of new file (string).

    sort:        Sort the contents of the file (line-wise) before comparing
                 (integer/0|1).

    Returns:     Difference between the two files (string).
    """
    if (sort):
        testFileLines = open(testFile).readlines()
        testFileLines.sort()
        rmFile(testFile)
        fo = open(testFile, "w")
        for line in testFileLines: fo.write(line)
        fo.close()
    stat, out = commands.getstatusoutput("diff %s %s" % (refFile, testFile))
    return out


def pollForFile(pattern,
                expNoOfCopies = 1,
                timeOut = 10,
                errMsg = None,
                pollTime = 0.2):
    """
    Poll for a given number of files matching the given file pattern.
    There is only polled during the specified timeout.

    pattern:        File pattern to poll for (string/UNIX filename mactching).

    expNoOfCopies:  Desired number of copies to match the pattern (integer).

    timeOut:        Timeout in seconds to wait (float).

    errMsg:         Error message to raise with exception if the conditions
                    where not fulfilled. If not provided, a generic error
                    message is generated (string|None).

    pollTime:       Period for polling (float).

    Returns:
    """
    startTime = time.time()
    while ((time.time() - startTime) < timeOut):
        if (len(glob.glob(pattern)) == expNoOfCopies): break
        time.sleep(pollTime)
    if ((time.time() - startTime) >= timeOut):
        if (errMsg):
            errMsg += pattern,
        else:
            errMsg = "The expected number of matches: %d to pattern: %s was "+\
                     "not obtained within the given timeout: %f"
            errMsg = errMsg % (expNoOfCopies, pattern, timeOut)
        raise Exception, errMsg


def genErrMsg(msg,
              refFile,
              tmpFile):
    """
    Generate an Error Message containing a description of the error
    and the name of the Ref. and Tmp. Files involved in the test.

    msg:       Message (string).

    refFile:   Name of Ref. File (string).

    tmpFile:   Name of Tmp. File (string).

    Returns:   Buffer with message (string).
    """
    diffFile = genTmpFilename()
    commands.getstatusoutput("diff %s %s > %s" % (refFile, tmpFile, diffFile))
    return msg + "\nRef File: " + refFile +\
           "\nTmp File: " + tmpFile +\
           "\nDiff Cmd: diff " + refFile + " " + tmpFile +\
           "\nCopy Cmd: cp " + tmpFile + " " + refFile +\
           "\nDiff Out: %s" % diffFile


def genErrMsgVals(msg,
                  refVal,
                  actVal):
    """
    Generate an Error Message containing a description of the error and the
    Reference Value and Temporary (Actual Value) involved in the test.

    msg:       Message (string).

    refVal:    Reference Value (string).

    actVal:    Temporary Value (string).

    Returns:   Buffer with message (string).
    """
    return msg + "\nRef Val: " + str(refVal) + "\nAct Val: " + str(actVal)


def copyFile(srcFile,
             trgFile):
    """
    Make of copy of referenced source file.

    srcFile:         Source file to copy (string).

    trgFile:         Target file (string).

    Returns:         Void.
    """
    shutil.copy(srcFile, trgFile)


def loadFile(filename):
    """
    Read contents from file and return this.

    filename:    Filename to read in (string).

    Returns:     Buffer containing contents of file (string).
    """
    fo = open(filename)
    buf = fo.read()
    fo.close()
    return buf


def genTmpFilename(prefix = ""):
    """
    Generate a unique, temporary filename.

    Returns:   Returns unique, temporary filename (string).
    """
    return "tmp/%s%.5f_tmp" % (prefix, time.time())


def saveInFile(filename,
               buf):
    """
    Save the contents of a buffer in a file with the given name.

    filename:   Target filename. If specified as None, a temporary filename
                int ngamsTest/tmp is generated (string).

    buf:        Buffer, which contents to store in the file (string).

    Returns:    Name of file in which the data was stored (string).
    """
    if (not filename): filename = genTmpFilename()
    fo = open(filename, "w")
    fo.write(buf)
    fo.close()
    return filename


def delNgasTbls(dbObj):
    """
    Delete the contents of the ngas_files table.

    dbObj:     Instance of NG/AMS server DB object (ngamsDb).

    Returns:   Void.
    """
    if ((dbObj.getDbServer().find("ESOECF") != -1) or
        (dbObj.getDbServer().find("OLASLS") != -1) or
        (dbObj.getDbServer().find("ASTOP") != -1)):
        raise Exception, "Tests cannot be run using ESOECF/OLASLS!!!"

    diskIdList = ('0ec67bec8ad4a744a2e5bbd472e50fa9',
                  '1aca0ec15d89921fb9fb889ff83a88aa',
                  '3334d27666faa151bf198ce0be574ffe',
                  '3f05c85abc183ce3f0cc7ac2965ad669',
                  'd8dbcece4572c9e31c9ef00dbc38751e',
                  'e744f22fc78a83e309166aafed92b667')

    dbObj.query("DELETE FROM ngas_disks WHERE " +\
                "disk_id LIKE 'tmp-ngamsTest-NGAS%' OR " +\
                "disk_id in %s" % str(diskIdList))

    diskHistQuery = "DELETE FROM ngas_disks_hist WHERE hist_descr LIKE " +\
                    "'%tmp-ngamsTest-NGAS%'"
    for diskId in diskIdList:
        diskHistQuery += " OR hist_descr LIKE '%%%s%%'" % diskId
    dbObj.query(diskHistQuery)

    dbObj.query("DELETE FROM ngas_files WHERE disk_id LIKE " +\
                "'tmp-ngamsTest-NGAS%' OR " +\
                "file_id='TEST.2001-05-08T15:25:00.123'")
    #dbObj.query("DELETE FROM ngas_files_retired LIKE 'tmp-ngamsTest-NGAS%'")
    dbObj.query("DELETE FROM ngas_subscr_back_log")  # TODO: Check if refine.
    dbObj.query("DELETE FROM ngas_subscribers")      # TODO: Check if refine.
    dbObj.query("DELETE FROM ngas_cfg_pars")         # TODO: Refine.
    dbObj.query("DELETE FROM ngas_cfg")              # TODO: Refine.


def delNgamsDirs(cfgObj):
    """
    Delete directories used by NG/AMS.

    cfgObj:   Configuration object (ngamsConfig).

    Returns:  Void.
    """
    T = TRACE(3)

    try:
        info(3,"Unmounting possibly mounted, simulated disks ...")
        execCmd("sudo /bin/umount /tmp/ngamsTest/*/*", 0)

        info(3,"Removing directory: " + cfgObj.getRootDirectory())
        os.system("rm -rf " + cfgObj.getRootDirectory())

        backLogDir = os.path.normpath(cfgObj.getBackLogBufferDirectory() +\
                                      "/" + NGAMS_BACK_LOG_TMP_PREFIX)
        info(3,"Removing directory: " + backLogDir)
        os.system("rm -rf " + backLogDir)

        globBadDir = os.path.normpath(cfgObj.getRootDirectory() +\
                                      "/" + NGAMS_BAD_FILES_DIR)
        info(3,"Removing directory: " + globBadDir)
        os.system("rm -rf " + globBadDir)

        procDir = os.path.normpath(cfgObj.getProcessingDirectory() +\
                                   "/" + NGAMS_PROC_DIR)
        info(3,"Removing directory: " + procDir)
        os.system("rm -rf " + procDir)

        info(3,"Removing directory: " + cfgObj.getLocalLogFile())
        os.system("rm -rf " + cfgObj.getLocalLogFile())

        info(3,"Removing directory: /tmp/ngamsTest")
        os.system("rm -rf /tmp/ngamsTest")
    except Exception, e:
        error("Error encountered removing NG/AMS directories: " + str(e))


def checkHostEntry(dbObj,
                   dbHostId,
                   domain = None,
                   ipAddress = None,
                   clusterName = None):
    """
    Check that the given host has an entry in the DB. If this is not the case,
    create it.

    dbObj:               DB connection object (ngamsDb).

    dbHostId:            DB Host ID in to check for (string).

    domain:              Domain name of NGAS Node (string).

    ipAddress:           IP address of the node (string|None).

    clusterName:         Cluster name of NGAS Node (string).

    Returns:             Void.
    """
    try:
        dbObj.getIpFromHostId(dbHostId)
        hostDefined = 1
    except Exception, e:
        hostDefined = 0
    hostInfo = socket.gethostbyname_ex(dbHostId.split(":")[0])
    fullHostName = hostInfo[0]
    #hostAlias = hostInfo[1][0]
    hostAlias = hostInfo[0].split(".")[0]
    ip = hostInfo[2][0]
    if (not domain): domain = fullHostName[(fullHostName.find(".") + 1):]
    if (not ipAddress): ipAddress = ip
    if (not clusterName): clusterName = dbHostId
    if (not hostDefined):
        sqlFormat = "INSERT INTO ngas_hosts (host_id, domain, ip_address, " +\
                    "cluster_name, srv_suspended) VALUES " +\
                    "('%s', '%s', '%s', '%s', 0)"
        sqlQuery = sqlFormat % (dbHostId, domain, ipAddress, clusterName)
    else:
        sqlFormat = "UPDATE ngas_hosts SET domain='%s', ip_address='%s', " +\
                    "cluster_name='%s', srv_suspended=0 WHERE host_id='%s'"
        sqlQuery = sqlFormat % (domain, ipAddress, clusterName, dbHostId)
    dbObj.query(sqlQuery)


def sendPclCmd(host = getHostName(),
               port = 8888,
               auth = None,
               timeOut = 60.0):
    """
    Create an instance of the Python Client with the given host ID + port
    number and return this to the caller.

    host:          Host ID where externally running server is located (string).

    port:          Port number used by externally running server (integer).

    auth:          Authorization Code (string).

    timeOut:       Timeout in seconds (float).

    Returns:       Created instance of Python Client (ngamsPClient).
    """
    return ngamsPClient.ngamsPClient(host, port).setAuthorization(auth).\
           setTimeOut(timeOut)


def sendExtCmd(host,
               port,
               cmd,
               pars = [],
               genStatFile = 1,
               filterTags = [],
               replaceLocalHost = 1):
    """
    Issue the command to an externally running NG/AMS Server and return
    ngamsStatus object. Returns name of file containing a filtered ASCII dump
    of the ngamsStatus object or the ngamsStatus object.

    host:          Host ID where externally running server is located (string).

    port:          Port number used by externally running server (integer).

    cmd:           Command to issue (string).

    pars:          List containing sub-lists with parameters and their
                   values: [[<Par>, <Val>], [<Par>, <Val>], ...]    (list).

    genStatFile:   If set to 1 a status file containing a filtered ASCII
                   dump of the ngamsStatus object is generated and the name
                   of this returned (integer/0|1).

    filterTags:    Additional line contents to filter out (list).

    Returns:       Filename of file containing filtered ASCII dump of status
                   or ngamsStatus object (string|ngamsStatus).
    """
    try:
        statObj = ngamsPClient.ngamsPClient().\
                  sendCmdGen(host, port, cmd, 1, "", pars)
        if (genStatFile):
            tmpStatFile = "tmp/%s_CmdStatus_%s_tmp" %\
                          (cmd, str(int(time.time())))
            buf = statObj.dumpBuf().replace(getHostName(), "___LOCAL_HOST___")
            saveInFile(tmpStatFile, filterDbStatus1(buf, filterTags))
            return tmpStatFile
        else:
            return statObj
    except Exception, e:
        errMsg = "Problem handling command to external server (%s/%d). " +\
                 "Error: %s"
        raise Exception, errMsg % (host, port, str(e))


def filterDbStatus1(statBuf,
                    filterTags = []):
    """
    Filter a status buffer so that non-static information is removed.

    statBuf:        Status ASCII buffer as generated by ngamsStatus.dumpBuf()
                    (string)

    filterTags:     Additional line contents to filter out (list).

    Returns:        Filtered buffer (string).
    """
    # TODO: Use ngamsTestLib.filterOutLines().
    statBufLines = statBuf.split("\n")
    filteredBuf = ""
    for line in statBufLines:
        if ((line.find("Date:") == 0) or (line.find("Version:") == 0) or
            (line.find("InstallationDate:") == 0) or
            (line.find("HostId:") == 0) or (line.find("AvailableMb:") == 0) or
            (line.find("TotalDiskWriteTime:") == 0) or
            (line.find("IngestionDate:") == 0) or
            (line.find("CompletionDate:") == 0) or
            (line.find("CreationDate:") == 0) or
            (line.find("RequestTime:") == 0) or
            (line.find("CompletionTime:") == 0) or
            (line.find("StagingFilename:") == 0) or
            (line.find("ModificationDate:") == 0) or
            (line.find("AccessDate:") == 0)):
            continue
        skipLine = 0
        for filterTag in filterTags:
            if (line.find(filterTag) != -1):
                skipLine = 1
                break
        if (skipLine): continue
        if ((line.find("NG/AMS Server performing exit") != -1) or
            (line.find("Successfully handled command") != -1)):
            line = line.split(" (")[0]
        filteredBuf += line + "\n"
    return filteredBuf


def recvEmail(no):
    """
    Receive an email with the given number and return the contents in a
    string buffer.

    no:       Email number (integer).

    Returns:  Contents of email (string).
    """
    cmd = "echo \"" + str(no) + "\" | mail"
    stat, out = commands.getstatusoutput(cmd)
    delCmd = "echo \"d " + str(no) + "\" | mail"
    dummyStat, dummyOut = commands.getstatusoutput(delCmd)
    return out


def supprVerbLogPreamb(logBuf):
    """
    Supress the first part of a log line as written on stdout, e.g.:

    2005-09-07T10:44:54.703:<mod>:<fct>:169:2074:MainThread:INFO> <text>

    - is converted into:

    -----> <text>

    logLine:   Log line to process (string).

    Returns:   Processed log line (string).
    """
    procLogBuf = ""
    year = PccUtTime.TimeStamp().getTimeStamp().split("-")[0].strip()
    for logLine in logBuf.split("\n"):
        if (logLine.strip().find("%s-" % year) == 0):
            procLogBuf += "-----> %s\n" % logLine[(logLine.find(">") + 1):].\
                          strip()
        else:
            procLogBuf += logLine + "\n"
    return procLogBuf


def filterOutLines(buf,
                   discardTags = [],
                   matchStart = 1):
    """
    Remove the lines containing the tags given in the input parameter.

    buf:            String buffer to be filtered (string).

    discardTags:    List of tags. Lines containing the given tag will
                    be removed (list/string).

    matchStart:     Match from start of line (integer/0|1).

    Returns:        Filtered string buffer (string).
    """
    lines = buf.split("\n")
    filterBuf = ""
    for line in lines:
        discardLine = 0
        for discardTag in discardTags:
            if (matchStart):
                if (line.find(discardTag) == 0):
                    discardLine = 1
                    break
            else:
                if (line.find(discardTag) != -1):
                    discardLine = 1
                    break
        if (not discardLine): filterBuf += line + "\n"
    return filterBuf


def getEmailMsg(remTags = [],
                timeOut = 10.0):
    """
    Retrieve an email message and return the contents (cleaned).

    remTags:    List with additional tags to remove from the email (list).

    timeOut:    Timeout in seconds to apply waiting for emails (float).

    Returns:    Email message, cleaned (string).
    """
    stdRemTags = ["Mail version", "/var/spool/mail", ">N  ", "From ",
                  "Date: ", "From:", "Subject:",
                  "mbox", "Message ", " N ", " U ", "/var/mail/",
                  "To: undisclosed-recipients:"]
    remTags += stdRemTags
    mailCont = ""
    startTime = time.time()
    while ((time.time() - startTime) < timeOut):
        mailCont = recvEmail(1)
        if ((mailCont.strip() != "") and
            (mailCont.find("No mail for ") == -1)):
            break
        else:
            time.sleep(0.2)
    if (mailCont == ""): return ""
    return filterOutLines(mailCont, remTags, matchStart=0)


def flushEmailQueue():
    """
    Flush the email queue of the user running the NG/AMS Unit Tests.

    Returns:   Void.
    """
    stat, stdOut = commands.getstatusoutput("echo \"x\" | mail")
    mailDic = {}
    for line in stdOut.split("\n"):
        line = line.strip()
        if (line != ""):
            lineEls = cleanList(line.split(" "))
            try:
                mailDic[int(lineEls[1])] = 1
            except:
                pass

    # Now delete the mails.
    mailList = mailDic.keys()
    mailList.sort()
    mailList.reverse()
    for mailNo in mailList:
        recvEmail(mailNo)


def runTest(argv):
    """
    Parses and executes the test according to the command line
    parameters given.

    argv:     Arguments given on command line (tuple).

    Returns:  Void.
    """
    setLogCond(0, "", 0, "", -1)
    testModuleName = argv[0].split(".")[0]
    tests = []
    silentExit = 0
    verboseLevel = -1
    logFile = ""
    logLevel = 0
    skip = None
    idx = 1
    while idx < len(argv):
        par = argv[idx].upper()
        try:
            if (par == "-CFG"):
                idx += 1
                setRefCfg(argv[idx])
            elif (par == "-LOGLEVEL"):
                idx += 1
                logLevel = int(argv[idx])
            elif (par == "-LOGFILE"):
                idx += 1
                logFile = argv[idx]
            elif (par == "-V"):
                idx += 1
                verboseLevel = int(argv[idx])
            elif (par == "-TESTS"):
                idx += 1
                tests = argv[idx].split(",")
            elif (par == "-NOCLEANUP"):
                setNoCleanUp(1)
            elif (par == "-SKIP"):
                idx += 1
                skip = argv[idx]
            else:
                correctUsage()
                silentExit = 1
                sys.exit(1)
            idx += 1
        except Exception, e:
            if (not silentExit):
                print "Illegal input parameters: " + str(e) + "\n"
                correctUsage()
            sys.exit(1)

    setLogCond(0, "", logLevel, logFile, verboseLevel)

    if (not getRefCfg()):
        msg = "Must specify a basis configuration (command line option: -cfg)"
        raise Exception, msg

    skipDic = {}
    if (skip):
        for testCase in skip.split(","): skipDic[testCase.strip()] = 1

    # Always ensure that the local "tmp" directory exists
    if not os.path.isdir("tmp"):
        if os.path.exists("tmp"):
            raise Exception("./tmp exists and is not a directory, cannot continue")
        os.mkdir("tmp")

    # Execute the test.
    exec "import " + testModuleName
    if (tests == []):
        # No specific test specified - run all tests.
        testSuite = unittest.makeSuite(eval(testModuleName + "." +\
                                            testModuleName))
    elif (skipDic != {}):
        print "TODO: IMPLEMENT SKIP PARAMETER FOR TEST SUITES!"
        sys.exit(1)
    else:
        testSuite = unittest.TestSuite()
        for testCase in tests:
            testSuite.addTest(eval(testModuleName + "." + testModuleName +\
                                   '("' + testCase + '")'))
    ngamsTextTestRunner(sys.stdout, 1, 0).run(testSuite)


def writeFitsKey(filename,
                 key,
                 value,
                 comment = ""):
    """
    Write or update a FITS keyword in the given FITS file.

    filename:     FITS file to update (string).

    key:          FITS keyword (string).

    value:        Value of keyword (string).

    comment:      Comment of keyword card (string).

    Returns:      Void.
    """
    fo = pcfitsio.fits_open_file(filename, 1)
    pcfitsio.fits_update_key(fo, key, value, comment)
    pcfitsio.fits_close_file(fo)


def remFitsKey(filename,
               key):
    """
    Remove a FITS keyword from the given FITS file.

    filename:   FITS file (string).

    key:        Name of key (string).

    Returns:    Void.
    """
    fo = pcfitsio.fits_open_file(filename, 1)
    pcfitsio.fits_delete_key(fo, key)
    pcfitsio.fits_close_file(fo)


def prepCfg(cfgFile,
            parList):
    """
    Prepare a configuration file based on the input configuration file.
    Return the name of the new configuration file.

    cfgFile:    Configuration file used as base for generating the new
                configuration (string).

    parList:    List of parameters/values to change. The format is:

                  [[<Par>, <Val>], ...]                           (list).

    Returns:    Name of new configuration file generated (string).
    """
    cfgObj = ngamsConfig.ngamsConfig().load(cfgFile)
    for cfgPar in parList:
        parName = cfgPar[0]
        if (parName.find("NgamsCfg.") == -1): parName = "NgamsCfg." + parName
        cfgObj.storeVal(parName, cfgPar[1])
    tmpCfgFileName = genTmpFilename("TMP_CFG_") + ".xml"
    cfgObj.save(tmpCfgFileName, hideCritInfo=0)
    return tmpCfgFileName


def getTestUserEmail():
    """
    Generate a qualified email user account name for the user running
    the test on the local host.

    Returns:   Email recipient (string).
    """
    return getpass.getuser() + "@" + ngamsLib.getCompleteHostName()


def incArcfile(filename,
               step = 0.001):
    """
    Increment the time stamp of the ARCFILE keyword with the given step.

    filename:    Name of FITS file (string).

    step:        Step in seconds (float).

    Returns:     Void.
    """
    fptr = pcfitsio.fits_open_file(filename, 1)
    arcFile = pcfitsio.fits_read_keyword(fptr, "ARCFILE")[0][1:-1]
    idx = arcFile.find(".")
    insId = arcFile[0:idx]
    ts = arcFile[(idx + 1):]
    newMjd = PccUtTime.TimeStamp().initFromTimeStamp(ts).getMjd() +\
             (float(step) / 86400.0)
    newArcFile = insId + "." + PccUtTime.TimeStamp(newMjd).getTimeStamp()
    pcfitsio.fits_modify_key_str(fptr, "ARCFILE", newArcFile, "")
    pcfitsio.fits_close_file(fptr)
    # TODO: Use PCFITSIO to reprocess the checksum.
    commands.getstatusoutput("add_chksum " + filename)


def isFloat(val):
    """
    Check if a given value is a float and return 1 if case yes, otherwise 0.

    val:      Value to check if it is a float (string|float).

    Returns:  Indication if value is float or not (integer/0|1).
    """
    try:
        float(val)
    except:
        return 0
    if (str(val).find(".") != -1):
        return 1
    else:
        return 0


def getThreadId(logFile,
                tagList):
    """
    Return the Thread ID for a given request. The thread ID is identified
    by the tags in the tag list (all must match).

    logFile:     Log File to scan (string).

    tagList:     List of tags that must be present in the log entry from
                 which the Thread ID is extracted (list).

    Returns:     Thread ID if found or None (string|None).
    """
    grepCmd = "grep %s %s" % (tagList[0], logFile)
    for tag in tagList[1:]:
        grepCmd += " | grep %s" % tag
    stat, out = commands.getstatusoutput(grepCmd)
    return out.split(":")[-1][0:-1]

###########################################################################
# END: Utility functions
###########################################################################


class ngamsTestSuite(unittest.TestCase):
    """
    Test Case class for the NG/AMS Functional Tests.
    """

    def __init__(self,
                 methodName = "runTest"):
        """
        Constructor method.

        methodName:    Name of method to run to run test case (string_.
        """
        unittest.TestCase.__init__(self, methodName)
        self.__verboseLevel  = getVerboseLevel()
        self.__extSrvInfo    = []

        # TODO: Get rid of these.
        self.__srvObj        = None
        self.__cfgObj        = None
        self.__fromSrv       = ""
        self.__toSrv         = ""
        self.__foList        = []


    def addSrvInfo(self,
                   pid,
                   port,
                   mtRtDir = None):
        """
        Set the PID and port number used by an external NG/AMS Server running.

        pid:        PID of external server process (integer).

        port:       HTTP socket port number used by external server (integer).

        mtRtDir:    Mount root directory used by the server (string).

        Returns:    Reference to object itself.
        """
        self.__extSrvInfo.append([pid, port, mtRtDir])
        return self


    def prepExtSrv(self,
                   portNo = 8888,
                   delDirs = 1,
                   clearDb = 1,
                   autoOnline = 1,
                   cfgFile = "src/ngamsCfg.xml",
                   multipleSrvs = 0,
                   domain = None,
                   ipAddress = None,
                   clusterName = None,
                   cfgProps = [],
                   dbCfgName = None,
                   srvModule = "ngamsServer/ngamsServer.py",
                   test = 1):
        """
        Prepare a standard server object, which runs as a separate process and
        serves via the standard HTTP interface.

        portNo:        Port number to use by server (integer).

        delDirs:       Delete NG/AMS dirs before executing (integer/0|1).

        clearDb:       Clear the DB (integer/0|1).

        autoOnline:    Bring server to Online automatically (integer/0|1).

        cfgFile:       Configuration file to use when executing the
                       server (string).

        multipleSrvs:  If set to 1, this means that multiple servers might
                       be running on the node (integer/0|1).

        domain:        Domain for server (string).

        ipAddress:     IP address for the host where the server is
                       running (string).

        clusterName:   Name of cluster to which this node belongs (string).

        cfgProps:      With this parameter it is possible to set specific
                       cfg. parameters before starting the server. This
                       must be a list containing sub-lists with the following
                       contents:

                         [[<Cfg. Par.>, <Val>], ...]

                       The Cfg. Par. name, must be given in the XML Dictionary
                       format, e.g.:

                         NgamsCfg.Server[1].PortNo

                       (list).

        dbCfgName:     DB configuration name (string).

        srvModule:     Python module in which the NG/AMS Server class is
                       contained. Must be given relative to the ngams module
                       directory (string).

        test:          Run server in Test Mode (0|1/integer).

        Returns:       Tuple with configuration object and DB object
                       (tuple/(ngamsConfig, ngamsDb)).
        """
        T = TRACE(3)

        verbose = getVerboseLevel()

        # Handle DB connection in Reference Configuration File.
        refCfgObj = ngamsConfig.ngamsConfig().load(getRefCfg())

        if (dbCfgName):
            # If a DB Configuration Name is specified, we first have to
            # extract the configuration information from the DB to
            # create a complete temporary cfg. file.
            multCons = refCfgObj.getDbMultipleCons()
            dbObj =\
                  ngamsDb.ngamsDb(refCfgObj.getDbServer(),
                                  refCfgObj.getDbName(),
                                  refCfgObj.getDbUser(),
                                  refCfgObj.getDbPassword(),
                                  interface = refCfgObj.getDbInterface(),
                                  parameters = refCfgObj.getDbParameters(),
                                  multipleConnections = multCons)
            logFlush()
            cfgObj2 = ngamsConfig.ngamsConfig().loadFromDb(dbCfgName, dbObj)
            del dbObj
            dbObj = None
            tmpCfgFile = saveInFile(None, cfgObj2.genXmlDoc(0))
        else:
            tmpCfgFile = cfgFile

        # Derive NG/AMS Server name for this instance.
        if (not multipleSrvs):
            hostName = getHostName()
        else:
            hostName = "%s:%d" % (getHostName(), portNo)

        tmpCfgObj = ngamsConfig.ngamsConfig().load(tmpCfgFile)

        # Take over the DB parameters from the reference.
        mergeRefCfg(tmpCfgObj)

        # Ensure the SQLite DB is created if working with SQLite.
        # TODO: It would probably be better if we simply run
        # the SQL script that creates the tables
        if (tmpCfgObj.getDbInterface().upper().find("SQLITE") != -1):
            info(1,"Creating SQLite DB file")
            sqliteDb = "tmp/" + hostName +".sqlite"
            cpFile("src/ngas_Sqlite_db_template", sqliteDb)
            tmpCfgObj.storeVal("NgamsCfg.Db[1].Name", sqliteDb)

        # Clean up.
        tmpCfgFile = saveInFile(None, tmpCfgObj.genXmlDoc(0))
        cfgObj = cleanUp(tmpCfgFile, delDirs, clearDb, 1)

        # If configuration parameters should be changed, do this.
        if (cfgProps):
            cfgObj = ngamsConfig.ngamsConfig().load(tmpCfgFile)
            for cfgProp in cfgProps:
                # TODO: Handle Cfg. Group ID.
                cfgObj.storeVal(cfgProp[0], cfgProp[1])
            tmpCfgFile = saveInFile(None, cfgObj.genXmlDoc(0))

        multCons = cfgObj.getDbMultipleCons()
        dbObj = ngamsDb.ngamsDb(cfgObj.getDbServer(), cfgObj.getDbName(),
                                cfgObj.getDbUser(), cfgObj.getDbPassword(),
                                interface = tmpCfgObj.getDbInterface(),
                                parameters = cfgObj.getDbParameters(),
                                multipleConnections = multCons)
        checkHostEntry(dbObj, hostName, domain, ipAddress, clusterName)

        # Update configuration.
        tmpCfg = "tmp/CFG_%d_tmp.xml" %\
                 int(time.time())
        cfgObj.storeVal("NgamsCfg.Server[1].PortNo", str(portNo))
        cfgObj.save(tmpCfg, 0)
        info(3,"DB Name: %s" % cfgObj.getDbName())

        # Execute the server as an external process.
        execCmd = ["ngamsServer", "-cfg", tmpCfg, "-force"]
        if (autoOnline):   execCmd.append("-autoOnline")
        if (verbose):      execCmd.extend(["-v", str(verbose)])
        if (multipleSrvs): execCmd.append("-multipleSrvs")
        if (dbCfgName):    execCmd.extend(["-dbCfgId", dbCfgName])
        if (test):         execCmd.append("-test")
        info(3,"Starting external NG/AMS Server with shell command: " + " ".join(execCmd))

        srvProcess = subprocess.Popen(execCmd)

        # We have to wait until the server is serving.
        pCl = ngamsPClient.ngamsPClient(getHostName(), portNo)
        startTime = time.time()
        stat = None
        count = 0  # Want to have 10 STATUS Commands successfully handled.
        while ((time.time() - startTime) < 20):
            if (stat):
                count += 1
                if (autoOnline):
                    info(2,"Test server running - State: ONLINE")
                    if ((stat.getState() == "ONLINE") and (count == 10)): break
                else:
                    info(2,"Test server running - State: OFFLINE")
                    if ((stat.getState() =="OFFLINE") and (count == 10)): break
            try:
                stat = pCl.status()
            except Exception, e:
                info(3,"Polled server - not yet running ...")
                time.sleep(0.2)

        if ((time.time() - startTime) >= 25):
            self.termExtSrv(srvProcess, portNo)
            raise Exception,"NGAMS TEST LIB> NG/AMS Server did not start " +\
                  "correctly"

        self.addSrvInfo(srvProcess, portNo, cfgObj.getRootDirectory())

        return (cfgObj, dbObj)


    def termExtSrv(self,
                   srvProcess,
                   port):
        """
        Terminate an externally running server.

        pid:       PID of server (integer).

        port:      Port number used by externally running server (integer).

        Returns:   Void.
        """

        info(3,"PID of externally running server. PID: %s, Port: %s " %\
             (str(srvProcess.pid), str(port)))
        info(3,"Killing externally running NG/AMS Server ...")
        pCl = ngamsPClient.ngamsPClient(getHostName(), port)
        try:
            info(1,"Sending OFFLINE command to external server ...")
            stat = pCl.offline(1)
            if getLogLevel() >= 3:
                info(3, "Status OFFLINE command: " + re.sub("\n", "", str(stat.genXmlDoc())))
            status = stat.getStatus()
        except Exception, e:
            info(3,"Error encountered sending OFFLINE command: " + str(e))
            status = NGAMS_FAILURE

        # If OFFLINE was successfully handled, try to
        # shut down the server via a nice EXIT command
        # Otherwise, kill it with -9
        kill9 = True
        if (status == NGAMS_SUCCESS):
            info(1,"External server in Offline State - " +\
                 "sending EXIT command ...")
            try:
                stat = pCl.exit()
                if getLogLevel() >= 3:
                    info(3, "Status EXIT command: " + re.sub("\n", "", str(stat.genXmlDoc())))
            except Exception, e:
                info(3,"Error encountered sending EXIT command: " + str(e))
            else:
                # Wait for the server to be definitively terminated.
                waitLoops = 0
                while srvProcess.poll() is None and waitLoops < 20:
                    time.sleep(0.5)
                    waitLoops += 1

                # ... or force it to die
                kill9 = waitLoops == 20

        if kill9:
            srvProcess.send_signal(signal.SIGKILL)

    def tearDown(self):
        """
        Clean up the test environment.

        Returns:   Void.
        """
        T = TRACE(3)

        # Make an external servers terminate.
        for srvInfo in self.__extSrvInfo:
            self.termExtSrv(srvInfo[0], srvInfo[1])
            # Remove NGAS Root Directories if clean-up requested.
            rootDir = srvInfo[2]
            if ((not getNoCleanUp()) and rootDir):
                execCmd("sudo /bin/umount %s/*" % rootDir, 0)
                execCmd("rm -rf %s" % rootDir, 0)
        # Check that no servers are running.
        stat, out = execCmd("ps -efww | grep autoOnline")
        for line in out.split("\n"):
            if (line.strip() == ""): continue
            pid = cleanList(line.split(" "))[1]
            os.system("kill -9 %s 2> /dev/null" % pid)
        # Remove temporary files in ngams/ngamsTest/tmp.
        if (not getNoCleanUp()):
            execCmd("sudo /bin/umount /tmp/ngamsTest/*/*", 0)
            fileList = glob.glob("tmp/*")
            for tmpFile in fileList:
                rmFile(tmpFile)
            #try:
            #    commands.getstatusoutput("mv %s/CVS %s/.CVS" %(tmpDir,tmpDir))
            #    commands.getstatusoutput("rm -rf %s/*" % tmpDir)
            #except:
            #    pass
            #try:
            #    commands.getstatusoutput("mv %s/.CVS %s/CVS" %(tmpDir,tmpDir))
            #except:
            #    pass

        # Have to reset the log conditions, to avoid that some class created in
        # a subsequent test tries to access this before new log environment
        # has been set up.
        setLogCond(0, "", 0, "", self.__verboseLevel)


    def checkEqual(self,
                   refValue,
                   tstValue,
                   msg):
        """
        Test that the two values given are equal, if not generate an error.

        refValue:    Reference value (all types).

        tstValue:    Value to be tested (all types).

        msg:         Message to give out in connection with error
                     message (string).

        Returns:     Void.
        """
        msg = "\nRef. Value: %s\n" +\
              "Test Value: %s"
        msg = msg % (str(refValue), str(tstValue))
        self.failUnless(refValue == tstValue, msg)


    def checkFilesEq(self,
                     refFile,
                     tmpFile,
                     msg,
                     sort = 0):
        """
        Check if two files are identical. Give out the given message if
        they are not.

        refFile:    Reference file (string).

        tmpFile:    Temporary file to be checked (string).

        msg:        Error message to give out in case of differences (string).

        sort:       Sort the contents of the file before comparing
                    (integer/0|1).

        Returns:    Void.
        """
        self.failUnless("" == cmpFiles(refFile, tmpFile, sort),
                        genErrMsg(msg, refFile, tmpFile))


    def checkTags(self,
                  statBuf,
                  tags,
                  showBuf = 1):
        """
        Checks if a given set of tags are found in the status buffer given.
        If not all tags are found an exception is raised.

        statBuf:    Buffer to check for the appearance of the tags (string).

        tags:       List of tags (list/string).

        showBuf:    Print out the contents of the buffer (integer/0|1).

        Returns:    Void.
        """
        errMsg = ""
        for tag in tags:
            if (statBuf.find(tag) == -1): errMsg += "\n  " + tag
        if (errMsg):
            errMsg = "\nMissing tags:" + errMsg + "\n"
            if (showBuf):
                errMsg += "Status Buffer:\n|%s|" % statBuf
            else:
                errMsg += "Status Buffer:\n|<Contents of buffer too big to " +\
                          "be shown>|"
            info(1,"Error encountered: %s" % errMsg.replace("\n", " | "))
            self.fail(errMsg)


    def prepCluster(self,
                    comCfgFile,
                    serverList):
        """
        Prepare a common, simulated cluster. This consists of 1 to N
        servers running on the same node. It is ensured that each of
        these have a unique NGAS tree and port number.

        It is also checked for each server, if it has an entry in the
        NGAS DB (ngas_hosts). If this is not the case, an entry is created.

        comCfgFile:    Name of configuration to use for setting up the
                       simulated cluster (string).

        serverList:    List containing sub-lists with information about
                       each server. This must be formatted as follows:

                       [[<Port#1>, <Domain>, <IP Addr>, <Cl Name>, <Cfg Pars>],
                        [<Port#2>, <Domain>, <IP Addr>, <Cl Name>, <Cfg Pars>],
                        ...]

                       If the Domain, IP Address or Cluster Name are defined
                       as None, the default (=localhost) is taken. <Cfg Pars>
                       is a list of sub-list specifying (in XML Dictionary
                       format) special configuration parameters for the
                       given server (string).

        Returns:       Dictionary with reference to each server
                       (<Host ID>:<Port No>) pointing to lists with the
                       following contents:

                         [<ngamsConfig object>, <ngamsDb object>]

                                                                 (dictionary).
        """
        # Delete all NGAS Mount Root Directories.
        rmFile("/tmp/ngamsTest/*")

        srvDic = {}
        multSrvs = (len(serverList) - 1)
        cleanUp = 1     # Clean up first time prepSrcExt is called.
        for srvInfo in serverList:
            portNo      = int(srvInfo[0])
            domain      = srvInfo[1]
            ipAddress   = srvInfo[2]
            clusterName = srvInfo[3]
            if (len(srvInfo) > 4):
                cfgParList = srvInfo[4]
            else:
                cfgParList = []

            tmpCfg = ngamsConfig.ngamsConfig().load(comCfgFile)
            mergeRefCfg(tmpCfg)

            # Set port number in configuration and allocate a mount root
            # directory + other directories + generate new, temporary
            # configuration file based on this information.
            srvId = "%s:%d" % (getHostName(), portNo)
            if (multSrvs):
                srvDbHostId = srvId
            else:
                srvDbHostId = getHostName()

            # Create directories.
            if (multSrvs):
                mtRtDir    = "/tmp/ngamsTest/NGAS:%d" % portNo
            else:
                mtRtDir    = "/tmp/ngamsTest/NGAS"
            locLogFile = mtRtDir + "/log/LogFile.nglog"
            tmpCfg.storeVal("NgamsCfg.Header[1].Type", "TEST CONFIG: %s" %\
                            srvId)
            tmpCfg.storeVal("NgamsCfg.Server[1].PortNo", portNo)
            tmpCfg.storeVal("NgamsCfg.Server[1].RootDirectory", mtRtDir)
            tmpCfg.storeVal("NgamsCfg.ArchiveHandling[1]." +\
                            "BackLogBufferDirectory", mtRtDir)
            tmpCfg.storeVal("NgamsCfg.Processing[1].ProcessingDirectory",
                            mtRtDir)
            tmpCfg.storeVal("NgamsCfg.Log[1].LocalLogFile",
                            os.path.normpath(mtRtDir + "/log/LogFile.nglog"))
            # Set special values if so specified.
            for cfgPar in cfgParList: tmpCfg.storeVal(cfgPar[0], cfgPar[1])
            tmpCfgFile = "tmp/%s_tmp.xml" % srvId
            tmpCfg.save(tmpCfgFile, 0)

            # Exceptional handling for SQLite.
            # TODO: It would probably be better if we simply run
            # the SQL script that creates the tables
            if (tmpCfg.getDbInterface().upper().find("SQLITE") != -1):
                # Ensure the SQLite DB is available if working with SQLite.
                sqliteDbTpl = "src/ngas_Sqlite_db_template"
                sqliteDb = tmpCfg.getDbName()
                os.system("cp %s %s" % (sqliteDbTpl, sqliteDb))

            # Check if server has entry in referenced DB. If not, create it.
            multCons = tmpCfg.getDbMultipleCons()
            tmpDbObj =\
                     ngamsDb.ngamsDb(tmpCfg.getDbServer(),
                                     tmpCfg.getDbName(),
                                     tmpCfg.getDbUser(),
                                     tmpCfg.getDbPassword(),
                                     interface = tmpCfg.getDbInterface(),
                                     parameters = tmpCfg.getDbParameters(),
                                     multipleConnections = multCons)
            checkHostEntry(tmpDbObj, srvDbHostId, domain, ipAddress,
                           clusterName)

            # Start server + add reference to server configuration object and
            # server DB object.
            srvCfgObj, srvDbObj = self.prepExtSrv(portNo,
                                                  delDirs = cleanUp,
                                                  clearDb = cleanUp,
                                                  autoOnline = 1,
                                                  cfgFile = tmpCfgFile,
                                                  multipleSrvs = multSrvs,
                                                  domain = domain,
                                                  ipAddress = ipAddress,
                                                  clusterName = clusterName)
            srvDic[srvId] = [srvCfgObj, srvDbObj]
            cleanUp = 0

        return srvDic


    def prepDiskCfg(self,
                    diskCfg,
                    cfgFile = "src/ngamsCfg.xml"):
        """
        Prepare a simulated disk configuration by mounting file based ext3fs
        systems (simulating disks) under the given NGAS Root Mount Point.

        The file based file systems are stored in:

          <NGAS Root Mt Pt>/tmp

        The disks will be stored under:

          <NGAS Root Mt Pt>/Data-Main|Rep-<Slot ID>


        diskCfg:     Dictionary containing a reference to each slot that
                     List containing dictionaries defining the disk
                     configuration. The contents of this is:

                       [{'DiskLabel':      None,
                         'MainDiskSlotId': '<Slot ID>',
                         'RepDiskSlotId':  '<Slot ID>'|None,
                         'Mutex':          '0'|'1',
                         'StorageSetId':   '<ID>',
                         'Synchronize':    '0'|'1',
                         '_SIZE_MAIN_':    '8MB'|'16MB',
                         '_SIZE_REP_':     '8MB'|'16MB'}, ...]


        cfgFile:        Name of configuration file used as base for starting
                        the NG/AMS Server (string).

        Returns:        Name of new, temporary configuration file generated
                        (string).
        """
        cfgObj = ngamsConfig.ngamsConfig().load(cfgFile)
        mergeRefCfg(cfgObj)
        multCons = cfgObj.getDbMultipleCons()
        dbObj  = ngamsDb.ngamsDb(cfgObj.getDbServer(), cfgObj.getDbName(),
                                 cfgObj.getDbUser(), cfgObj.getDbPassword(),
                                 interface = cfgObj.getDbInterface(),
                                 parameters = cfgObj.getDbParameters(),
                                 multipleConnections = multCons)
        stoSetIdx = 0
        xmlKeyPat = "NgamsCfg.StorageSets[1].StorageSet[%d]."
        for diskSetDic in diskCfg:
            stoSetIdx += 1
            for attr in diskSetDic.keys():
                if (attr[0] == "_"): continue
                xmlKey = str(xmlKeyPat + attr) % stoSetIdx
                cfgObj.storeVal(xmlKey, diskSetDic[attr])
            mainDirName = "%s-Main-%s" % (diskSetDic["StorageSetId"],
                                          diskSetDic["MainDiskSlotId"])
            mainLogName = "TEST-DISK-M-00000%d" % stoSetIdx
            if (diskSetDic["RepDiskSlotId"]):
                repDirName  = "%s-Rep-%s" % (diskSetDic["StorageSetId"],
                                             diskSetDic["RepDiskSlotId"])
                repLogName  = "TEST-DISK-R-00000%d" % stoSetIdx
            else:
                repDirName = None
                repLogName = None
            for dirInfo in [[mainDirName, mainLogName,
                             diskSetDic["_SIZE_MAIN_"]],
                            [repDirName, repLogName,
                             diskSetDic["_SIZE_REP_"]]]:
                if (not dirInfo[0]): continue
                diskDir = "%s/%s" % (cfgObj.getRootDirectory(),dirInfo[0])
                if (os.path.exists(diskDir)):
                    execCmd("sudo /bin/umount %s" % diskDir, 0)
                    rmFile(diskDir)
                os.mkdir(diskDir)
                fileSysName = genTmpFilename("FILE_SYS_") + ".gz"
                if (dirInfo[2] == "8MB"):
                    srcFile = "src/8MB_ext3fs.gz"
                    availMb = 8
                else:
                    srcFile = "src/16MB_ext3fs.gz"
                    availMb = 16
                execCmd("cp %s %s" % (srcFile, fileSysName))
                execCmd("gunzip %s" % fileSysName)
                fileSysName = fileSysName[0:-3]
                execCmd("sudo /bin/mount -o loop %s %s" %\
                        (fileSysName, diskDir))
                try:
                    execCmd("sudo /bin/chmod -R a+rwx %s" % diskDir)
                except:
                    pass

                # Generate NgasDiskInfo XML documents for the slot to simulate
                # that the disk is already registered as an NGAS Disk.
                ngasDiskInfo = loadFile("src/NgasDiskInfoTemplate")
                isoDate = PccUtTime.TimeStamp().getTimeStamp()
                diskId = diskDir[1:].replace("/", "-")
                ngasDiskInfo = ngasDiskInfo % (isoDate, getHostName(),
                                               getNgamsVersion(), availMb,
                                               diskId, isoDate, dirInfo[1])
                ngasDiskInfoFile = diskDir + "/NgasDiskInfo"
                saveInFile(ngasDiskInfoFile, ngasDiskInfo)
                # Delete possible entry in the DB for the disk.
                try:
                    if (dbObj.getDiskInfoFromDiskId(diskId) != []):
                        dbObj.deleteDiskInfo(diskId, delFileInfo=1)
                except:
                    pass

        # Make the Stream Elements defined refer to the given Storage Sets.
        streamElNo = 1
        while (1):
            streamElDicKey = "NgamsCfg.Streams[1].Stream[%d]" % streamElNo
            if (cfgObj._getXmlDic().has_key(streamElDicKey)):
                stoSetCount = 1
                for diskInfo in diskCfg:
                    stoSetIdDicKey = "NgamsCfg.Streams[1].Stream[%d]." +\
                                     "StorageSetRef[%d].StorageSetId"
                    stoSetIdDicKey = stoSetIdDicKey % (streamElNo, stoSetCount)
                    cfgObj.storeVal(stoSetIdDicKey, diskInfo["StorageSetId"])
                    stoSetCount += 1
            else:
                break
            streamElNo += 1
        newCfgFileName = genTmpFilename() + ".xml"
        cfgObj.save(newCfgFileName, hideCritInfo=0)

        del dbObj
        return newCfgFileName


    def _substQueryVal(self,
                       queryVal):
        """
        Substitutes the contents of a query value to make it possible to
        make a comparison.

        queryVal:     Query value to check and possibly substitute (string).

        Returns:      Substituted query value (string).
        """
        queryVal = str(queryVal.strip().replace("$HOSTNAME", getHostName()))
        if ((queryVal.find("<DateTime object") != -1) or
            checkIfIso8601(queryVal)):
            queryVal = "_DATETIME_"
        elif (isFloat(queryVal)):
            queryVal = "_FLOAT_"
        elif (queryVal == "' '"):
            # A varchar with value null in Oracle produces None as result.
            # On Sybase ' ' is returned. We skip such results for the moment.
            queryVal = "_SKIP_"
        elif (queryVal == "[[]]"):
            queryVal = "[]"
        return queryVal


    def _checkQuery(self,
                    query,
                    refQuery,
                    queryPlan,
                    queryPlanIdx,
                    refQueryPlan,
                    refQueryPlanIdx):
        """
        Compare two components of an SQL query or query result.

        query:          Query component (string).

        refQuery:       Reference query component (string).

        Returns:        Void.
        """
        refQueryComps = refQuery.split(",")
        queryComps    = query.split(",")
        idx = 0
        while (idx < len(refQueryComps)):
            refQueryEl = self._substQueryVal(refQueryComps[idx])
            queryEl    = self._substQueryVal(queryComps[idx])
            if ((str(refQueryEl).find("_SKIP_") != -1) or
                (str(queryEl).find("_SKIP_") != -1)):
                idx += 1
                continue
            if (refQueryEl != queryEl):
                errMsg = "Mismatch found in query plan.\n\n" +\
                         "Expected component:\n\n%s\n\n" +\
                         "Component found:\n\n%s\n\n" +\
                         "Ref. query:\n\n%s\n\n" +\
                         "Query plan:\n\n%s/%d\n\n" +\
                         "Ref. query plan:\n\n%s/%d"
                self.fail(errMsg % (refQueryEl, queryEl, refQuery,
                                    queryPlan, (queryPlanIdx + 1), refQueryPlan,
                                    refQueryPlanIdx+1))
            idx += 1


    def checkQueryPlan(self,
                       queryPlan,
                       refQueryPlan):
        """
        Verify that contents of the given query plan against the reference
        query plan.

        A query plan contains the following:

        <SQL query>: <Expected Result>

        queryPlan:      Query plan extracted from the log file (string).

        refQueryPlan:   Reference query plan (string).

        Returns:        Void.
        """
        queryPlanLines = loadFile(queryPlan).split("\n")
        refQueryPlanLines = loadFile(refQueryPlan).split("\n")
        idx1 = 0
        idx2 = 0
        while (idx1 < len(refQueryPlanLines)):
            # Skip empty lines, comment lines in the Reference Query Plan.
            if ((refQueryPlanLines[idx1].strip() == "") or
                (refQueryPlanLines[idx1].strip()[0] == "#")):
                idx1 += 1
                continue
            refQuery, refRes = refQueryPlanLines[idx1].split(": ")
            query, queryRes  = queryPlanLines[idx2].split(": ")
            # Check contents of the query against the ref. query:
            self._checkQuery(query, refQuery, queryPlan, idx2, refQueryPlan,
                             idx1)
            # Check contents of the result against the ref. query result:
            self._checkQuery(queryRes, refRes, queryPlan, idx2, refQueryPlan,
                             idx1)
            idx1 += 1
            idx2 += 1


    def checkQueryPlanLogFile(self,
                              logFile,
                              threadId,
                              refQueryPlan):
        """
        Extract the query plan for a given command and verify its correctness
        against a reference query plan.

        logFile:       Log file from which the query plan will be extracted
                       (string).

        threadId:      ID of thread for which the query plan should be checked
                       (string).

        refQueryPlan:  Reference query plan (string).

        Returns:       Void.
        """
        resTag1 = "Result of SQL query"
        resTag2 = "Performing SQL query (using a cursor):"
        logLines = loadFile(logFile).split("\n")
        tmpLogBuf = ""
        for line in logLines:
            if (line.find(threadId) != -1):
                if ((line.find(resTag1) != -1) or (line.find(resTag2) != -1)):
                    tmpLogBuf += line + "\n"
        tmpQueryPlan = genTmpFilename("QUERY_PLAN_")
        saveInFile(tmpQueryPlan, tmpLogBuf)
        queryLines = loadFile(tmpQueryPlan).split("\n")
        queryPlan = ""
        for line in queryLines:
            line = line.strip()
            if (line == ""): continue
            if (line.find(resTag1) != -1):
                sqlQuery, sqlRes = line.split(resTag1)[1].split(": ")
                sqlQuery = sqlQuery.strip()[1:-1]
                sqlRes = sqlRes.split("] [")[0] + "]"
            else:
                sqlQuery = line.split(": ")[1].split(" [ngamsDb")[0]
                sqlRes = ""
            queryPlan += "%s: %s\n" % (sqlQuery, sqlRes)
        tmpQueryPlan2 = genTmpFilename("QUERY_PLAN_2_")
        saveInFile(tmpQueryPlan2, queryPlan)
        self.checkQueryPlan(tmpQueryPlan2, refQueryPlan)


class ngamsTextTestResult(unittest._TextTestResult):
    """
    Class to produce text test output.
    """

    def __init__(self,
                 stream = sys.stderr,
                 descriptions = 1,
                 verbosity = 1):
        """
        Constructor method.

        stream:       Stream on which to write the report, e.g. sys.stderr
                      (stream object).

        descriptions: ?

        verbosity:    ?
        """
        unittest._TextTestResult.__init__(self,stream, descriptions, verbosity)


class ngamsTextTestRunner(unittest.TextTestRunner):
    """
    Test report generator class for the NG/AMS Unit Test.
    """

    def __init__(self,
                 stream = sys.stderr,
                 descriptions = 1,
                 verbosity = 1):
        unittest.TextTestRunner.__init__(self, stream, descriptions, verbosity)


    def _makeResult(self):
        return ngamsTextTestResult(self.stream, self.descriptions,
                                   self.verbosity)


    def run(self,
            test):
        """
        Run the given test case or test suite.
        """
        testName = str(test._tests[0]).split(" ")[1].split(".")[0][1:]
        self.stream.writeln("\nTest: " + testName)
        result = self._makeResult()
        startTime = time.time()
        test(result)
        stopTime = time.time()
        timeTaken = float(stopTime - startTime)
        result.printErrors()
        self.stream.writeln(result.separator2)
        run = result.testsRun
        self.stream.writeln("Ran %d test%s in %.3fs" %
                            (run, run == 1 and "" or "s", timeTaken))
        self.stream.writeln()
        if not result.wasSuccessful():
            self.stream.write("FAILED (")
            failed, errored = map(len, (result.failures, result.errors))
            if failed:
                self.stream.write("failures=%d" % failed)
            if errored:
                if failed: self.stream.write(", ")
                self.stream.write("errors=%d" % errored)
            self.stream.writeln(")")
        else:
            self.stream.writeln("OK\n")
        return result


if __name__ == '__main__':
    """
    Main program to test execution of functions in the module.
    """
    pass


# EOF
