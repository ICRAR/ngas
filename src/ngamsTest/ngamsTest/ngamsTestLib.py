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

import collections
import commands
import contextlib
import functools
import getpass
import glob
import gzip
import importlib
import logging
import multiprocessing.pool
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import unittest
import uuid
import xml.dom.minidom

import astropy.io.fits as pyfits
import pkg_resources
import psutil

from ngamsLib import ngamsConfig, ngamsDb, ngamsLib, ngamsStatus
from ngamsLib.ngamsCore import getHostName, TRACE, \
    ngamsCopyrightString, rmFile, \
    cpFile, NGAMS_FAILURE, NGAMS_SUCCESS, getNgamsVersion, \
    execCmd as ngamsCoreExecCmd, fromiso8601, toiso8601
from ngamsPClient import ngamsPClient


logger = logging.getLogger(__name__)

logging_levels = {
    logging.CRITICAL: 0,
    logging.ERROR: 1,
    logging.WARN: 2,
    logging.INFO: 3,
    logging.DEBUG: 4,
    logging.NOTSET: 5,
    0: logging.CRITICAL,
    1: logging.ERROR,
    2: logging.WARNING,
    3: logging.INFO,
    4: logging.DEBUG,
    5: logging.NOTSET
}

# Global parameters to control the test run.
_noCleanUp   = 0

# Pool used to start/shutdown servers in parallel
srv_mgr_pool = multiprocessing.pool.ThreadPool(5)

# Constants.
STD_DISK_STAT_FILT = ["AccessDate", "AvailableMb", "CreationDate", "Date",
                      "HostId", "IngestionDate", "InstallationDate",
                      "ModificationDate", "TotalDiskWriteTime", "Version"]
AUTH               = "bmdhczpuZ2Fz"


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

def has_program(program):
    try:
        subprocess.check_output(program, shell=False)
    except subprocess.CalledProcessError:
        pass
    except OSError:
        return False
    return True

###########################################################################
# START: Utility functions:
###########################################################################
def checkIfIso8601(timestamp):
    try:
        fromiso8601(timestamp)
        return True
    except ValueError:
        return False

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
    exitCode, stdOut, stdErr = ngamsCoreExecCmd(cmd, timeOut)
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
        res, host, port = clientObj._get("STATUS", pars=[["request_id", str(requestId)]])
        res = res.read()
        res = ngamsStatus.ngamsStatus().unpackXmlDoc(res, 1)
        if res.getStatus() != NGAMS_SUCCESS:
            raise Exception("unsuccessfull STATUS command in %s:%d with request_id: %s" % (host, port, res.getMessage()))
        if (res.getCompletionPercent() != None):
            if (float(res.getCompletionPercent()) >= 99.9):
                break
        time.sleep(0.100)
    if ((time.time() - startTime) > timeOut):
        errMsg = "Timeout waiting for request: %s (%s/%s) to finish"
        raise Exception(errMsg % (requestId, host, port))
    return res


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
    print "<test program> [-v <level>] [-tests <test name>] [-noCleanUp]\n"
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
    return "tmp/%s%s_tmp" % (prefix, str(uuid.uuid4()))


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
    dbObj.query2("DELETE FROM ngas_cache")
    dbObj.query2("DELETE FROM ngas_hosts")
    dbObj.query2("DELETE FROM ngas_disks")
    dbObj.query2("DELETE FROM ngas_disks_hist")
    dbObj.query2("DELETE FROM ngas_files")
    dbObj.query2("UPDATE ngas_containers set parent_container_id = null")
    dbObj.query2("DELETE FROM ngas_containers")
    dbObj.query2("DELETE FROM ngas_subscr_queue")
    dbObj.query2("DELETE FROM ngas_subscr_back_log")
    dbObj.query2("DELETE FROM ngas_subscribers")
    dbObj.query2("DELETE FROM ngas_cfg_pars")
    dbObj.query2("DELETE FROM ngas_cfg")


def delNgamsDirs(cfgObj):
    """
    Delete directories used by NG/AMS.

    cfgObj:   Configuration object (ngamsConfig).

    Returns:  Void.
    """
    try:
        for d in [cfgObj.getRootDirectory(), "/tmp/ngamsTest"]:
            logger.debug("Removing directory: %s", d)
            shutil.rmtree(d, True)
    except Exception:
        logger.exception("Error encountered removing NG/AMS directories")


def sendPclCmd(port = 8888,
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
    return ngamsPClient.ngamsPClient('127.0.0.1', port, timeout=timeOut, auth=auth)

def sendExtCmd(port,
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
    statObj = sendPclCmd(port=port).get_status(cmd, pars=pars)
    if (genStatFile):
        tmpStatFile = "tmp/%s_CmdStatus_%s_tmp" %\
                      (cmd, str(int(time.time())))
        buf = statObj.dumpBuf().replace(getHostName(), "___LOCAL_HOST___")
        saveInFile(tmpStatFile, filterDbStatus1(buf, filterTags))
        return tmpStatFile
    else:
        return statObj


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
        if ((line.find("Date:") == 0) or
            (line.find("Version:") == 0) or
            (line.find("InstallationDate:") == 0) or
            (line.find("HostId:") == 0) or
            (line.find("AvailableMb:") == 0) or
            (line.find("TotalDiskWriteTime:") == 0) or
            (line.find("IngestionDate:") == 0) or
            (line.find("CompletionDate:") == 0) or
            (line.find("CreationDate:") == 0) or
            (line.find("RequestTime:") == 0) or
            (line.find("CompletionTime:") == 0) or
            (line.find("StagingFilename:") == 0) or
            (line.find("ModificationDate:") == 0) or
            (line.find("TotalIoTime") == 0) or
            (line.find("IngestionRate") == 0) or
            (line.find("ContainerId") == 0) or
            (line.find("ModificationDate:") == 0) or
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
            lineEls = filter(None, line.split(" "))
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
    testModuleName = argv[0].split('/')[-1].split(".")[0]
    tests = []
    silentExit = 0
    verboseLevel = 0
    skip = None
    idx = 1
    while idx < len(argv):
        par = argv[idx].upper()
        try:
            if (par == "-V"):
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

    logging.root.addHandler(logging.NullHandler())
    if verboseLevel:
        logging.root.addHandler(logging.StreamHandler(stream=sys.stdout))
        logging.root.setLevel(logging_levels[verboseLevel-1])

    skipDic = {}
    if (skip):
        for testCase in skip.split(","): skipDic[testCase.strip()] = 1

    # Always ensure that the local "tmp" directory exists
    if not os.path.isdir("tmp"):
        if os.path.exists("tmp"):
            raise Exception("./tmp exists and is not a directory, cannot continue")
        os.mkdir("tmp")

    # Execute the test.
    testModule = importlib.import_module(testModuleName)
    testClass = getattr(testModule, testModuleName)
    if (tests == []):
        # No specific test specified - run all tests.
        testSuite = unittest.makeSuite(testClass)
    elif (skipDic != {}):
        print "TODO: IMPLEMENT SKIP PARAMETER FOR TEST SUITES!"
        sys.exit(1)
    else:
        testSuite = unittest.TestSuite()
        for testCase in tests:
            testSuite.addTest(testClass(testCase))
    res = ngamsTextTestRunner(sys.stdout, 1, 0).run(testSuite)
    sys.exit(0 if res.wasSuccessful() else 1)


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
    pyfits.setval(filename, key, value=value, comment=comment)

def remFitsKey(filename,
               key):
    """
    Remove a FITS keyword from the given FITS file.

    filename:   FITS file (string).

    key:        Name of key (string).

    Returns:    Void.
    """
    pyfits.delval(filename, key)


def db_aware_cfg(cfg_filename, check=0, db_id_attr="Db-Test"):
    """
    Load the configuration stored in `cfg_filename` and replace the Db element
    with whatever is in the NGAS_DB_CONF environment variable, if present
    """

    if 'NGAS_TESTDB' not in os.environ or not os.environ['NGAS_TESTDB']:
        return ngamsConfig.ngamsConfig().load(cfg_filename, check)

    new_db = xml.dom.minidom.parseString(os.environ['NGAS_TESTDB'])
    new_db.documentElement.attributes['Id'].value = db_id_attr
    root = xml.dom.minidom.parseString(loadFile(cfg_filename)).documentElement
    for n in root.childNodes:
        if n.localName != 'Db':
            continue

        root.removeChild(n)
        root.appendChild(new_db.documentElement)
        cfg_filename = saveInFile(genTmpFilename('CFG'), root.toprettyxml())
        return ngamsConfig.ngamsConfig().load(cfg_filename, check)

    raise Exception('Db element not found in original configuration')


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
    # ARCFILE looks like SOMEID.YYYY-mm-DDTHH:MM:SS.sss
    arcFile = pyfits.getval(filename, 'ARCFILE')
    idx = arcFile.find('.')
    insId = arcFile[:idx]
    ts = arcFile[idx+1:]
    ts = toiso8601(fromiso8601(ts) + step)
    pyfits.setval(filename, 'ARCFILE', value="%s.%s" % (insId, ts))
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
    tid =  out.split("[")[1].split("]")[0].strip()
    return tid

def unzip(infile, outfile):
    with contextlib.nested(gzip.open(infile, 'rb'), open(outfile, 'w')) as (gz, out):
        for data in iter(functools.partial(gz.read, 1024), ''):
            out.write(data)


###########################################################################
# END: Utility functions
###########################################################################

ServerInfo = collections.namedtuple('ServerInfo', ['proc', 'port', 'rootDir'])

class ngamsTestSuite(unittest.TestCase):
    """
    Test Case class for the NG/AMS Functional Tests.
    """

    # Used to serialize the startup of server processes
    # due to the subprocess module not handling threaded code well
    # in python 2.7
    _proc_startup_lock = threading.Lock()

    def __init__(self,
                 methodName = "runTest"):
        """
        Constructor method.

        methodName:    Name of method to run to run test case (string_.
        """
        unittest.TestCase.__init__(self, methodName)
        logger.info("Starting test %s" % (methodName,))
        self.extSrvInfo    = []
        self.__mountedDirs   = []

    def assertStatus(self, status, expectedStatus='SUCCESS'):
        self.assertIsNotNone(status)
        self.assertEquals(expectedStatus, status.getStatus())

    def prepExtSrv(self,
                   port = 8888,
                   delDirs = 1,
                   clearDb = 1,
                   autoOnline = 1,
                   cfgFile = "src/ngamsCfg.xml",
                   multipleSrvs = 0,
                   cfgProps = [],
                   dbCfgName = None,
                   srvModule = None,
                   force=False):
        """
        Prepare a standard server object, which runs as a separate process and
        serves via the standard HTTP interface.

        port:          Port number to use by server (integer).

        delDirs:       Delete NG/AMS dirs before executing (integer/0|1).

        clearDb:       Clear the DB (integer/0|1).

        autoOnline:    Bring server to Online automatically (integer/0|1).

        cfgFile:       Configuration file to use when executing the
                       server (string).

        multipleSrvs:  If set to 1, this means that multiple servers might
                       be running on the node (integer/0|1).

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

        verbose = logging_levels[logger.getEffectiveLevel()] + 1

        if (dbCfgName):
            # If a DB Configuration Name is specified, we first have to
            # extract the configuration information from the DB to
            # create a complete temporary cfg. file.
            cfgObj = db_aware_cfg(cfgFile)
            dbObj = ngamsDb.from_config(cfgObj)
            cfgObj2 = ngamsConfig.ngamsConfig().loadFromDb(dbCfgName, dbObj)
            del dbObj
            dbObj = None
            logger.debug("Successfully read configuration from database, root dir is %s", cfgObj2.getRootDirectory())
            cfgFile = saveInFile(None, cfgObj2.genXmlDoc(0))

        cfgObj = db_aware_cfg(cfgFile)

        # Change what needs to be changed, like the position of the Sqlite
        # database file when necessary, the custom configuration items, and the
        # port number
        skip_database_creation = multipleSrvs or not clearDb
        self.point_to_sqlite_database(cfgObj, not multipleSrvs and not dbCfgName and not skip_database_creation)
        if (cfgProps):
            for cfgProp in cfgProps:
                # TODO: Handle Cfg. Group ID.
                cfgObj.storeVal(cfgProp[0], cfgProp[1])
        cfgObj.storeVal("NgamsCfg.Server[1].PortNo", str(port))

        # Now connect to the database and perform any cleanups before we start
        # the server, like removing existing NGAS dirs and clearing tables
        dbObj = ngamsDb.from_config(cfgObj)
        if (delDirs):
            logger.debug("Deleting NG/AMS directories ...")
            delNgamsDirs(cfgObj)
        if (clearDb):
            logger.debug("Clearing NGAS DB ...")
            delNgasTbls(dbObj)

        # Dump configuration into the filesystem so the server can pick it up
        tmpCfg = genTmpFilename("CFG_") + ".xml"
        cfgObj.save(tmpCfg, 0)

        # Execute the server as an external process.
        srvModule = srvModule or 'ngamsServer.ngamsServer'
        this_dir = os.path.normpath(pkg_resources.resource_filename(__name__, '.'))  # @UndefinedVariable
        parent_dir = os.path.dirname(this_dir)
        execCmd  = [sys.executable, '-m', srvModule]
        execCmd += ["-cfg", os.path.abspath(tmpCfg), "-v", str(verbose)]
        execCmd += ['-path', parent_dir]
        if force:        execCmd.append('-force')
        if autoOnline:   execCmd.append("-autoOnline")
        if multipleSrvs: execCmd.append("-multipleSrvs")
        if dbCfgName:    execCmd.extend(["-dbCfgId", dbCfgName])

        logger.info("Starting external NG/AMS Server in port %d with command: %s", port, " ".join(execCmd))
        with self._proc_startup_lock:
            srvProcess = subprocess.Popen(execCmd, shell=False)

        # We have to wait until the server is serving.
        server_info = ServerInfo(srvProcess, port, cfgObj.getRootDirectory())
        pCl = sendPclCmd(port=port)
        startTime = time.time()
        while True:

            # Check if the server actually didn't start up correctly
            ecode = srvProcess.poll()
            if ecode is not None:
                raise Exception("Server exited with code %d during startup" % (ecode,))

            # "ping" the server
            try:
                stat = pCl.status()
            except socket.error:
                logger.debug("Polled server - not yet running ...")
                time.sleep(0.2)
                continue
            except:
                logger.error("Error while STATUS-ing server, shutting down")
                self.termExtSrv(server_info)
                raise

            # Check the status is what we expect
            state = "ONLINE" if autoOnline else "OFFLINE"
            logger.debug("Test server running - State: %s", state)
            if stat.getState() == state:
                break

            # Took too long?
            if ((time.time() - startTime) >= 20):
                self.termExtSrv(server_info)
                raise Exception("Server did not start correctly within 20 [s]")

        self.extSrvInfo.append(server_info)

        return (cfgObj, dbObj)


    def termExtSrv(self, srvInfo):
        """
        Terminate an externally running server.
        """

        srvProcess, port, rootDir = srvInfo

        if srvProcess.poll() is not None:
            logger.debug("Server process %d (port %d) already dead x(, no need to terminate it again", srvProcess.pid, port)
            srvProcess.wait()
            return

        # Check the full hierarchy of processes for this server early on
        # After shutdown hopefully all of them successfully finished
        parent = psutil.Process(srvProcess.pid)
        childrenb4 = parent.children(recursive=True)

        logger.debug("Killing externally running NG/AMS Server. PID: %d, Port: %d ", srvProcess.pid, port)
        try:
            pCl = sendPclCmd(port=port, timeOut=10)
            stat = pCl.status()
            if stat.getState() != "OFFLINE":
                logger.info("Sending OFFLINE command to external server ...")
                stat = pCl.offline(1)
            status = stat.getStatus()
        except Exception, e:
            logger.info("Error encountered sending OFFLINE command: %s", str(e))
            status = NGAMS_FAILURE

        # If OFFLINE was successfully handled, try to
        # shut down the server via a nice EXIT command
        # Otherwise, kill it with -9
        kill9 = True
        if (status == NGAMS_SUCCESS):
            logger.info("External server in Offline State - sending EXIT command ...")
            try:
                stat = pCl.exit()
            except Exception, e:
                logger.info("Error encountered sending EXIT command: %s", str(e))
            else:
                # Wait for the server to be definitively terminated.
                waitLoops = 0
                while srvProcess.poll() is None and waitLoops < 20:
                    time.sleep(0.5)
                    waitLoops += 1

                # ... or force it to die
                kill9 = waitLoops == 20

        try:
            if kill9:
                try:
                    parent = psutil.Process(srvProcess.pid)
                except psutil.NoSuchProcess:
                    pass
                else:
                    children = parent.children(recursive=True)
                    for process in children:
                        os.kill(process.pid, signal.SIGKILL)
                    srvProcess.kill()
                    srvProcess.wait()
                    logger.info("Server process had %d to be merciless killed, sorry :(", srvProcess.pid)

            else:
                srvProcess.wait()
                logger.info("Finished server process %d gracefully :)", srvProcess.pid)
                for orphan in childrenb4:
                    if orphan.is_running():
                        logger.warning("Killing orphan child process %d", orphan.pid)
                        orphan.kill()
        except Exception:
            logger.exception("Error while finishing server process %d, port %d", srvProcess.pid, port)
            raise
        finally:
            if ((not getNoCleanUp()) and rootDir):
                shutil.rmtree(rootDir, True)

    def terminateAllServer(self):
        srv_mgr_pool.map(self.termExtSrv, self.extSrvInfo)
        self.extSrvInfo = []

    def setUp(self):
        # Make sure there is a 'tmp' directory here, since most of the tests
        # depend on it
        if not os.path.isdir('tmp'):
            os.mkdir('tmp')

    def tearDown(self):
        """
        Clean up the test environment.

        Returns:   Void.
        """
        T = TRACE(3)

        if (not getNoCleanUp()):
            for d in self.__mountedDirs:
                execCmd("sudo /bin/umount %s" % (d,), 0)

        self.terminateAllServer()

        # Remove temporary files in ngams/ngamsTest/tmp.
        if (not getNoCleanUp()):
            shutil.rmtree('tmp', True)

        # There's this file that gets generated by many tests, so we clean it up
        # generically here
        fname = 'TEST.2001-05-08T15:25:00.123.fits.gz'
        if os.path.exists(fname):
            os.unlink(fname)

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
            logger.info("Error encountered: %s", errMsg.replace("\n", " | "))
            self.fail(errMsg)

    def start_srv_in_cluster(self, multSrvs, comCfgFile, srvInfo):
        """
        Starts a given server which is part of a cluster of servers
        """

        portNo      = int(srvInfo[0])
        if (len(srvInfo) > 4):
            cfgParList = srvInfo[4]
        else:
            cfgParList = []

        # Set port number in configuration and allocate a mount root
        hostName = getHostName()
        srvId = "%s:%d" % (hostName, portNo)
        if (multSrvs):
            mtRtDir = "/tmp/ngamsTest/NGAS:%d" % portNo
            rmFile("/tmp/ngamsTest/NGAS:%d" %(portNo,))
        else:
            mtRtDir = "/tmp/ngamsTest/NGAS"
        rmFile(mtRtDir)

        # Set up our server-specific configuration
        cfg = ngamsConfig.ngamsConfig().load(comCfgFile)
        cfg.storeVal("NgamsCfg.Header[1].Type", "TEST CONFIG: %s" % srvId)
        cfg.storeVal("NgamsCfg.Server[1].PortNo", portNo)
        cfg.storeVal("NgamsCfg.Server[1].RootDirectory", mtRtDir)
        cfg.storeVal("NgamsCfg.ArchiveHandling[1].BackLogBufferDirectory", mtRtDir)
        cfg.storeVal("NgamsCfg.Processing[1].ProcessingDirectory", mtRtDir)
        cfg.storeVal("NgamsCfg.Log[1].LocalLogFile", os.path.normpath(mtRtDir + "/log/LogFile.nglog"))

        # Set special values if so specified.
        for cfgPar in cfgParList:
            cfg.storeVal(cfgPar[0], cfgPar[1])

        # And dump it into our server-specific configuration file
        tmpCfgFile = "tmp/%s_tmp.xml" % srvId
        cfg.save(tmpCfgFile, 0)

        # Check if server has entry in referenced DB. If not, create it.
        db = ngamsDb.from_config(cfg)
        db.close()

        # Start server + add reference to server configuration object and
        # server DB object.
        srvCfgObj, srvDbObj = self.prepExtSrv(portNo,
                                              delDirs = 0,
                                              clearDb = 0,
                                              autoOnline = 1,
                                              cfgFile = tmpCfgFile,
                                              multipleSrvs = multSrvs)
        return [srvId, srvCfgObj, srvDbObj]

    def prepCluster(self,
                    comCfgFile,
                    serverList,
                    createDatabase = True):
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

        # Create the shared database first of all
        tmpCfg = db_aware_cfg(comCfgFile)
        self.point_to_sqlite_database(tmpCfg, createDatabase)
        if createDatabase:
            db = ngamsDb.from_config(tmpCfg)
            delNgasTbls(db)
            db.close()

        multSrvs = len(serverList) > 1

        # Start them in parallel now that we have all set up for it
        res = srv_mgr_pool.map(functools.partial(self.start_srv_in_cluster, multSrvs, comCfgFile), serverList)

        # srvId: (cfgObj, dbObj)
        return {r[0]: (r[1], r[2]) for r in res}

    def point_to_sqlite_database(self, cfgObj, create):
        # Exceptional handling for SQLite.
        # TODO: It would probably be better if we simply run
        # the SQL script that creates the tables
        if create and 'sqlite' in cfgObj.getDbInterface().lower():
            cpFile("src/ngas_Sqlite_db_template", "tmp/ngas.sqlite")

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
        dbObj  = ngamsDb.from_config(cfgObj)

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
                self.__mountedDirs.append(diskDir)
                try:
                    execCmd("sudo /bin/chmod -R a+rwx %s" % diskDir)
                except:
                    pass

                # Generate NgasDiskInfo XML documents for the slot to simulate
                # that the disk is already registered as an NGAS Disk.
                ngasDiskInfo = loadFile("src/NgasDiskInfoTemplate")
                isoDate = toiso8601()
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
        queryVal = str(queryVal.strip().replace("$HOSTNAME", getHostName() + ":8888"))
        if ((queryVal.find("<DateTime object") != -1) or
            checkIfIso8601(queryVal.lstrip('u').strip("'"))):
            queryVal = "_DATETIME_"
        elif (isFloat(queryVal)):
            queryVal = "_FLOAT_"
        elif (queryVal == "u' '"):
            # A varchar with value null in Oracle produces None as result.
            # On Sybase ' ' is returned. We skip such results for the moment.
            queryVal = "_SKIP_"
        elif (queryVal == "[[]]"):
            queryVal = "[]"
        return queryVal


    def _checkQuery(self,
                    query,
                    refQuery,
                    refQueryPlan,
                    query_idx):
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
                         "Query number: %d\n" + \
                         "Query plan:\n\n%s\n\n" +\
                         "Ref. query plan:\n\n%s"
                self.fail(errMsg % (refQueryEl, queryEl, query_idx, refQuery, refQueryPlan))
            idx += 1


    def checkQueryPlan(self,
                       queryPlanLines,
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
        refQueryPlanLines = filter(None, loadFile(refQueryPlan).split("\n"))
        self.assertEqual(len(refQueryPlanLines), len(queryPlanLines))
        for i, (query, refQuery) in enumerate(zip(queryPlanLines, refQueryPlanLines), 1):
            self._checkQuery(query, refQuery, refQueryPlan, i)


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
        resTag1 = "Performing SQL query with parameters: "
        resTag2 = "Performing SQL query (using a cursor):"
        with open(logFile, 'r') as f:
            queryLines = [l for l in f if threadId in l and (resTag1 in l or resTag2 in l)]

        queryPlan = []
        for line in queryLines:
            line = line.strip()
            if resTag1 in line:
                sqlQuery = line.split(resTag1)[1].strip()
            else:
                sqlQuery = line.split(": ")[1].split(" [ngamsDb")[0]
            queryPlan.append(sqlQuery)

        self.checkQueryPlan(queryPlan, refQueryPlan)

    def markNodesAsUnsusp(self, dbConObj, nodes):
        """
        Mark the sub-nodes as not suspended.
        """
        for subNode in nodes:
            try:
                dbConObj.resetWakeUpCall(subNode, 1)
            except:
                pass

    def waitTillSuspended(self, dbConObj, node, timeOut, nodes):
        """
        Wait until *node* has suspended itself (i.e., marked itself as
        suspended in the database).
        """
        startTime = time.time()
        while (time.time() - startTime) < timeOut:
            nodeSusp = dbConObj.getSrvSuspended(node)
            if nodeSusp:
                logger.info("Server suspended itself after: %.3fs",
                            (time.time() - startTime))
                return 1
            else:
                time.sleep(0.1)

        self.markNodesAsUnsusp(dbConObj, nodes)
        self.fail("Sub-node %s did not suspend itself within %d [s]" % (node, timeOut))

    def waitTillWokenUp(self, dbConObj, node, timeOut, nodes):
        """
        Wait until a suspended node has been woken up.
        """
        startTime = time.time()
        while (time.time() - startTime) < timeOut:
            nodeSusp = dbConObj.getSrvSuspended(node)
            if (not nodeSusp):
                logger.info("Server woken up after: %.3fs", (time.time() - startTime))
                return 1
            else:
                time.sleep(0.1)

        self.markNodesAsUnsusp(dbConObj, nodes)
        self.fail("Sub-node not woken up within %ds" % timeOut)

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
