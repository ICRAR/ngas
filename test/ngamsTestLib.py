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

import asyncore
import collections
import contextlib
import email
import errno
import functools
import getpass
import glob
import gzip
import logging
import multiprocessing.pool
import os
import shutil
import signal
import smtpd
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import xml.dom.minidom

import astropy.io.fits as pyfits
import pkg_resources
import psutil

from ngamsLib import ngamsConfig, ngamsDb, ngamsLib, utils
from ngamsLib.ngamsCore import getHostName, rmFile, \
    NGAMS_FAILURE, NGAMS_SUCCESS, getNgamsVersion, \
    execCmd as ngamsCoreExecCmd, fromiso8601, toiso8601, getDiskSpaceAvail
from ngamsPClient import ngamsPClient


logger = logging.getLogger(__name__)

logging_levels = {
    logging.CRITICAL: 0,
    logging.ERROR: 1,
    logging.WARN: 2,
    logging.INFO: 3,
    logging.DEBUG: 4,
    logging.NOTSET: 5,
}


# Pool used to start/shutdown servers in parallel
srv_mgr_pool = multiprocessing.pool.ThreadPool(5)

# Almost all unit tests generate temporary files. These were originally stored
# under 'tmp' relative to the cwd. We now instead try different approaches,
# using a temporary directory under the system's tmp directory,
# or under /dev/shm which would yield faster test runs
_tmp_root_base = os.environ.get('NGAS_TESTS_TMP_DIR_BASE', '')
if not _tmp_root_base:
    _tmp_root_base = tempfile.gettempdir()
    if os.path.isdir('/dev/shm') and getDiskSpaceAvail('/dev/shm') > 1024:
        _tmp_root_base = '/dev/shm'
if not os.path.isdir(_tmp_root_base):
    raise ValueError('%s is not a directory, cannot use it as the base for NGAS temporary files')
tmp_root = os.path.join(_tmp_root_base, 'ngas')

def save_to_tmp(text, fname=None, prefix=None, suffix=None):
    if bool(fname) and bool(prefix or suffix):
        raise ValueError("Either fname or prefix/suffix can be given (or none)")
    if fname:
        fname = os.path.join(tmp_root, fname)
    else:
        fname = tempfile.mktemp(dir=tmp_root, prefix=prefix or '', suffix=suffix or '')
    with open(fname, 'wt') as f:
        f.write(text)
    return fname

def tmp_path(*p):
    return os.path.join(tmp_root, *p)

def as_ngas_disk_id(s):
    return s.strip('/').replace('/', '-')

def filter_and_replace(s, filters=[], startswith_filters=[], replacements={}, split_by_newline=False):
    '''filters lines in s through filters, startswith_fitlers, performing replacements through filtered lines'''
    new_s = []
    lines = s.split('\n') if split_by_newline else s.splitlines()
    for line in lines:
        if any(map(lambda f: f in line, filters)):
            continue
        if any(map(lambda sw: line.startswith(sw), startswith_filters)):
            continue
        for match, replacement in replacements.items():
            line = line.replace(match, replacement)
        new_s.append(line)
    return '\n'.join(new_s)

def _to_abs(path):
    '''Return ``path`` as an absolute path. If ``path`` is relative, it is
    interpreted as relative to the test/ package'''
    if not os.path.isabs(path):
        path = pkg_resources.resource_filename('test', path)
        path = os.path.abspath(path)
    return path

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
            raise Exception(errMsg)
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
        raise Exception("Mail configuration incorrect. Set parameter: " + \
              "DAEMON=yes in /etc/mail/sendmail.cf")
    out = subprocess.check_output("ps -efww|grep sendmail", shell=True)
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
        raise Exception(errMsg)

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
                raise Exception(errMsg)
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
        raise Exception(errMsg % (exitCode, out))
    return (exitCode, out)


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
        status = clientObj.status(pars=[["request_id", str(requestId)]])
        if (status.getCompletionPercent() != None):
            if (float(status.getCompletionPercent()) >= 99.9):
                return status
        time.sleep(0.100)
    errMsg = "Timeout waiting for request: %s to finish"
    raise Exception(errMsg % (requestId,))


_noCleanUp   = int(os.environ.get('NGAS_TESTS_NO_CLEANUP', 0))
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
    _, out, err = ngamsCoreExecCmd(['diff', refFile, testFile], shell=False)
    return out + err


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
        raise Exception(errMsg)


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
    ngamsCoreExecCmd("diff %s %s > %s" % (refFile, tmpFile, diffFile), shell=True)
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


def loadFile(filename, mode='t'):
    """
    Read contents from file and return this.

    filename:    Filename to read in (string).

    Returns:     Buffer containing contents of file (string).
    """
    with open(filename, mode='r' + mode) as f:
        return f.read()


def genTmpFilename(prefix="", suffix=""):
    """
    Generate a unique, temporary filename under the temporal root directory.

    Returns:   Returns unique, temporary filename (string).
    """
    return tempfile.mktemp(suffix, prefix, tmp_root)


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


def _old_buf_style(s):
    if s:
        s += '\n'
    return s

def recvEmail(no):
    """
    Receive an email with the given number and return the contents in a
    string buffer.

    no:       Email number (integer).

    Returns:  Contents of email (string).
    """
    cmd = "echo \"" + str(no) + "\" | mail"
    _, out, _ = ngamsCoreExecCmd(cmd, shell=True)
    delCmd = "echo \"d " + str(no) + "\" | mail"
    ngamsCoreExecCmd(delCmd, shell=True)
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
    if matchStart:
        s = filter_and_replace(buf, startswith_filters=discardTags, split_by_newline=True)
    else:
        s = filter_and_replace(buf, filters=discardTags, split_by_newline=True)
    return _old_buf_style(s)


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
    _, stdout, _ = ngamsCoreExecCmd('echo "x" | mail')
    mailDic = {}
    for line in utils.b2s(stdout).split("\n"):
        line = line.strip()
        if (line != ""):
            lineEls = filter(None, line.split(" "))
            try:
                mailDic[int(lineEls[1])] = 1
            except:
                pass

    # Now delete the mails.
    mailList = list(mailDic)
    mailList.sort(reverse=True)
    for mailNo in mailList:
        recvEmail(mailNo)


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

    cfg_filename = _to_abs(cfg_filename)

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
        cfg_filename = save_to_tmp(root.toprettyxml(), prefix='db_aware_cfg_', suffix='.xml')
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
    # TODO: Use astropy to reprocess the checksum?


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
    out = utils.b2s(subprocess.check_output(grepCmd, shell=True))
    tid =  out.split("[")[2].split("]")[0].strip()
    return tid

def unzip(infile, outfile):
    with gzip.open(infile, 'rb') as gz, open(outfile, 'wb') as out:
        shutil.copyfileobj(gz, out)


###########################################################################
# END: Utility functions
###########################################################################

_to_email_message = email.message_from_string
if six.PY3:
    _to_email_message = email.message_from_bytes

class InMemorySMTPServer(smtpd.SMTPServer):
    """In-memory SMTP server, saves messages into a public list"""

    message = collections.namedtuple('message', 'mailfrom rcpts data')

    def __init__(self, port):

        # decode_data is new in 3.5, defaults to True in 3.5, False in 3.6+
        # Thus, we need to explicitly give it to reliably use message_from_bytes
        # later
        kwargs = {}
        if sys.version_info[0:2] >= (3, 5):
            kwargs['decode_data'] = False
        smtpd.SMTPServer.__init__(self, ('127.0.0.1', port), None, **kwargs)
        self.port = port
        self.messages = []
        # email sending on the server can be asynchronous from the instructions
        # used to trigger them in the tests
        self.recv_cond = threading.Condition()
        self.thread = threading.Thread(target=asyncore.loop, args=(0.1,))
        self.thread.daemon = True
        self.thread.start()

    def pop(self, timeout=10):
        with self.recv_cond:
            while not self.messages:
                if not self.recv_cond.wait(timeout=timeout):
                    raise RuntimeError('email expected but none arrived')
            return _to_email_message(self.messages.pop().data)

    def close(self):
        smtpd.SMTPServer.close(self)
        self.thread.join(timeout=1)
        if self.thread.is_alive():
            raise RuntimeError('asyncore loop still running')

    def process_message(self, peer, mailfrom, rcpttos, data, **_):
        with self.recv_cond:
            self.messages.append(InMemorySMTPServer.message(mailfrom, rcpttos, data))
            self.recv_cond.notify_all()

ServerInfo = collections.namedtuple('ServerInfo', ['proc', 'port', 'rootDir', 'cfg_file', 'daemon'])

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
        self.client = None
        self._clients = None
        self.smtp_server = None

    def _add_client(self, port):
        # We overwrite any client that we may have created on that port already
        if self.client is None:
            self.client = self.get_client(port)
            return
        if self._clients is None:
            old_port = self.client.servers[0][1]
            if old_port == port:
                self.client = self.get_client(port)
                return
            self._clients = {self.client.servers[0][1]: self.client,
                             port: self.get_client(port)}
            self.client = lambda x: self._clients[x]
        else:
            self._clients[port] = self.get_client(port)

    def _remove_client(self, port):
        if self._clients is not None:
            del self._clients[port]
            if len(self._clients) == 1:
                _, self.client = self._clients.popitem()
                self._clients = None
        else:
            self.client = None

    def assert_ngas_status(self, method, *args, **kwargs):
        expectedStatus = kwargs.pop('expectedStatus', 'SUCCESS')
        status = method(*args, **kwargs)
        self.assertIsNotNone(status)
        self.assertEqual(expectedStatus, status.getStatus())
        return status

    def point_to_ngas_root(self, cfg, root_dir=None):
        if not isinstance(cfg, ngamsConfig.ngamsConfig):
            cfg = ngamsConfig.ngamsConfig().load(cfg)
        root_dir = root_dir or os.path.join(tmp_root, 'NGAS')
        cfg.storeVal('NgamsCfg.Server[1].RootDirectory', root_dir)

        # Dump configuration into the filesystem so the server can pick it up
        cfg_fname = os.path.abspath(genTmpFilename("CFG_") + ".xml")
        cfg.save(cfg_fname, 0)
        return cfg_fname

    def get_client(self, port=8888, auth=None, timeout=60.0):
        return ngamsPClient.ngamsPClient(port=port, auth=auth, timeout=timeout)

    def _assert_client_call(self, method, *args, **kwargs):
        '''Generic assertion on the NGAS status returned by clients'''
        client = self.client
        if callable(client):
            client = client(args[0])
            args = args[1:]

        # We concede this hack for the simplicity of test writing: relative
        # paths for archive/qarchive are relative to the test/ package
        if (method in ('archive', 'qarchive') and
            not os.path.isabs(args[0]) and
            not ngamsPClient.is_known_pull_url(args[0])):
            args = (_to_abs(args[0]),) + args[1:]

        return self.assert_ngas_status(getattr(client, method), *args, **kwargs)

    def __getattr__(self, name):
        '''Catch-all for client-like calls that assert the returned status'''
        expected_status = 'SUCCESS'
        if name.endswith("_fail"):
            name = name[:-5]
            expected_status = 'FAILURE'
        if hasattr(ngamsPClient.ngamsPClient, name):
            return functools.partial(self._assert_client_call, name,
                                     expectedStatus=expected_status)
        raise AttributeError

    def start_smtp_server(self):
        if self.smtp_server:
            return
        self.smtp_server = InMemorySMTPServer(utils.find_available_port(1025))

    def prepExtSrv(self,
                   port = 8888,
                   delDirs = 1,
                   clearDb = 1,
                   autoOnline = 1,
                   cache = False,
                   cfgFile = "src/ngamsCfg.xml",
                   root_dir=None,
                   cfgProps = [],
                   dbCfgName = None,
                   srvModule = None,
                   force=False,
                   daemon = False):
        """
        Prepare a standard server object, which runs as a separate process and
        serves via the standard HTTP interface.

        port:          Port number to use by server (integer).

        delDirs:       Delete NG/AMS dirs before executing (integer/0|1).

        clearDb:       Clear the DB (integer/0|1).

        autoOnline:    Bring server to Online automatically (integer/0|1).

        cfgFile:       Configuration file to use when executing the
                       server (string).

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
        if srvModule and daemon:
            raise ValueError("srvModule cannot be used in daemon mode")

        if not utils.is_port_available(port):
            raise RuntimeError("Port %d is not available for test server to use" % port)

        cfgFile = _to_abs(cfgFile)

        verbose = logging_levels[logger.getEffectiveLevel()] + 1

        if (dbCfgName):
            # If a DB Configuration Name is specified, we first have to
            # extract the configuration information from the DB to
            # create a complete temporary cfg. file.
            cfgObj = db_aware_cfg(cfgFile)
            with contextlib.closing(ngamsDb.from_config(cfgObj, maxpool=1)) as db:
                cfgObj2 = ngamsConfig.ngamsConfig().loadFromDb(dbCfgName, db)
            logger.debug("Successfully read configuration from database, root dir is %s", cfgObj2.getRootDirectory())
            cfgFile = save_to_tmp(cfgObj2.genXmlDoc(0))

        cfgObj = db_aware_cfg(cfgFile)

        # Change what needs to be changed, like the position of the Sqlite
        # database file when necessary, the custom configuration items, and the
        # port number
        self.point_to_sqlite_database(cfgObj, not dbCfgName and clearDb)
        if (cfgProps):
            for cfgProp in cfgProps:
                # TODO: Handle Cfg. Group ID.
                cfgObj.storeVal(cfgProp[0], cfgProp[1])
        cfgObj.storeVal("NgamsCfg.Server[1].PortNo", str(port))
        if cache:
            cfgObj.storeVal("NgamsCfg.Caching[1].Enable", '1')

        # Now connect to the database and perform any cleanups before we start
        # the server, like removing existing NGAS dirs and clearing tables
        root_dir = root_dir or tmp_path('NGAS')
        dbObj = ngamsDb.from_config(cfgObj, maxpool=1)
        if delDirs:
            shutil.rmtree(root_dir, True)
        if (clearDb):
            logger.debug("Clearing NGAS DB ...")
            delNgasTbls(dbObj)

        # Point the configuration to the root directory
        tmpCfg = self.point_to_ngas_root(cfgObj, root_dir)

        # Execute the server as an external process.
        if daemon:
            srvModule = 'ngamsServer.ngamsDaemon'
        else:
            srvModule = srvModule or 'ngamsServer.ngamsServer'

        execCmd  = [sys.executable, '-m', srvModule]
        if daemon:
            execCmd += ['start']
        execCmd += ["-cfg", tmpCfg, "-v", str(verbose)]
        execCmd += ['-path', os.path.dirname(_to_abs('.'))]
        if force:        execCmd.append('-force')
        if autoOnline:   execCmd.append("-autoonline")
        if dbCfgName:    execCmd.extend(["-dbcfgid", dbCfgName])

        # Make sure spawned servers use the same tmp dir base as we do, since
        # some of them use unit-test-provided code to perform some checks, and
        # sometimes communicate through files in the temporary area
        environ = os.environ.copy()
        environ['NGAS_TESTS_TMP_DIR_BASE'] = _tmp_root_base

        logger.info("Starting external NG/AMS Server in port %d with command: %s", port, " ".join(execCmd))
        with self._proc_startup_lock:
            srvProcess = subprocess.Popen(execCmd, shell=False, env=environ)

        # We have to wait until the server is serving.
        server_info = ServerInfo(srvProcess, port, cfgObj.getRootDirectory(), tmpCfg, daemon)
        client = self.get_client(port=port, timeout=5)

        def give_up():
            try:
                self.termExtSrv(server_info)
            except:
                pass

        # A daemon server should start rather quickly, as it forks out the actual
        # server process and then exists (with a 0 status when successful)
        if server_info.daemon:
            srvProcess.wait()
            if srvProcess.poll() != 0:
                give_up()
                raise Exception("Daemon server failed to start")

        startTime = time.time()
        while True:

            # Took too long?
            if ((time.time() - startTime) >= 20):
                give_up()
                raise Exception("Server did not start correctly within 20 [s]")

            # Check if the server actually didn't start up correctly
            if not server_info.daemon:
                ecode = srvProcess.poll()
                if ecode is not None:
                    raise Exception("Server exited with code %d during startup" % (ecode,))

            # "ping" the server and check that the status is what we expect
            try:
                stat = client.status()
                state = "ONLINE" if autoOnline else "OFFLINE"
                logger.debug("Test server running - State: %s", state)
                if stat.getState() == state:
                    break
            except Exception as e:

                # Not up yet, try again later
                if isinstance(e, socket.error) and e.errno == errno.ECONNREFUSED:
                    logger.debug("Polled server - not yet running ...")
                    time.sleep(0.1)
                    continue

                # We are having this funny situation in MacOS builds, when
                # intermitently the client times out while trying to connect
                # to the server. This happens very rarely, but when it does it
                # always seems to coincide with the moment the server starts
                # listening for connections.
                #
                # The network traffic during these connect timeout situations
                # seems completely normal, and just like the rest of the
                # connection attempts before the timeout occurs. In particular,
                # all these connection attempts result in a SYN packet sent by
                # the client, and a RST/ACK packet quickly coming back quickly
                # from the server side, meaning that the port is closed. In the
                # case of the connect timeout however, the client hangs for all
                # the time it is allowed to wait before timing out.
                #
                # This could well be a problem with python 2.7's implementation of
                # socket.connect, which issues on initial connect(), followed by a
                # select()/poll() depending on the situation. Without certainty,
                # I imagine this might lead to some sort of very thing race condition
                # between the connect and the select/poll call. Since I'm far
                # from being and Apple guru, I will take the simplest solution
                # for the time being and assume that a socket.timeout is simply
                # a transient error that will solve itself in the next round.
                # This change is also accompanied by a decrease on the timeout
                # used by the client that issuea these requests (it was 60 seconds,
                # we decreased it to 5 which makes more sense).
                elif isinstance(e, socket.timeout):
                    logger.warning("Timed out when connecting to server, will try again")
                    continue

                logger.exception("Error while STATUS-ing server, shutting down")
                give_up()

                raise

        self.extSrvInfo.append(server_info)
        self._add_client(port)
        return (cfgObj, dbObj)


    def termExtSrv(self, srvInfo, auth=None):
        """
        Terminate an externally running server.
        """

        srvProcess, port, rootDir, cfg_file, daemon = srvInfo

        # Started as a daemon, stopped as a daemon
        # Here we trust that the daemon will shut down all the processes nicely,
        # but we could be more thorough in the future I guess and double-check
        # that everything is shut down correctly
        if daemon:
            execCmd  = [sys.executable, '-m', 'ngamsServer.ngamsDaemon', 'stop']
            execCmd += ["-cfg", cfg_file]
            with self._proc_startup_lock:
                daemon_stop_proc = subprocess.Popen(execCmd, shell=False)
            daemon_stop_proc.wait()
            if daemon_stop_proc.poll() != 0:
                raise Exception("Daemon process didn't stop correctly")
            return

        # The rest if for stopping a server that was NOT started inside a daemon
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
            client = self.get_client(port=port, auth=auth, timeout=10)
            stat = client.status()
            if stat.getState() != "OFFLINE":
                logger.info("Sending OFFLINE command to external server ...")
                stat = client.offline(1)
            status = stat.getStatus()
        except Exception as e:
            logger.info("Error encountered sending OFFLINE command: %s", str(e))
            status = NGAMS_FAILURE

        # If OFFLINE was successfully handled, try to
        # shut down the server via a nice EXIT command
        # Otherwise, kill it with -9
        kill9 = True
        if (status == NGAMS_SUCCESS):
            logger.info("External server in Offline State - sending EXIT command ...")
            try:
                stat = client.exit()
            except Exception as e:
                logger.info("Error encountered sending EXIT command: %s", str(e))
            else:
                # Wait for the server to be definitively terminated.
                waitLoops = 0
                while srvProcess.poll() is None and waitLoops < 100:
                    time.sleep(0.1)
                    waitLoops += 1

                # ... or force it to die
                kill9 = waitLoops == 100

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
        for srv_info in self.extSrvInfo:
            self._remove_client(srv_info.port)
        self.extSrvInfo = []

    def setUp(self):
        # Make sure there the temporary directory is there
        # using the exist_ok kwarg in makedirs is not possible, as it was
        # introduced in python3 only
        try:
            os.makedirs(tmp_root)
        except OSError:
            pass

    def tearDown(self):
        """
        Clean up the test environment.

        Returns:   Void.
        """
        if (not getNoCleanUp()):
            for d in self.__mountedDirs:
                execCmd("sudo /bin/umount %s" % (d,), 0)

        self.terminateAllServer()

        if self.smtp_server:
            self.smtp_server.close()

        # Remove temporary files
        if (not getNoCleanUp()):
            shutil.rmtree(tmp_root, True)

        # There's this file that gets generated by many tests, so we clean it up
        # generically here
        fname = 'TEST.2001-05-08T15:25:00.123.fits.gz'
        if os.path.exists(fname):
            os.unlink(fname)

    def resource(self, filename):
        '''Returns the actual filename for a given test resource'''
        return _to_abs(filename)

    def cp(self, fname, tgt):
        '''Copies ``fname`` (a test resource) to ``tgt``'''
        return shutil.copy(_to_abs(fname), tgt)

    def ngas_root(self, port=None):
        '''Get the NGAS root directory for the running server. If more than one
        server is running a port must be given'''
        if len(self.extSrvInfo) == 1:
            return self.extSrvInfo[0].rootDir
        if port is None:
            return None
        for srv_info in self.extSrvInfo:
            if srv_info.port == port:
                return srv_info.rootDir
        raise RuntimeError('No NGAS server found running on port %d' % (port,))

    def ngas_path(self, *p, **kwargs):
        port = kwargs.pop('port', None)
        return os.path.normpath(os.path.join(self.ngas_root(port=port), *p))

    def ngas_disk_id(self, *p, **kwargs):
        port = kwargs.pop('port', None)
        return as_ngas_disk_id(self.ngas_path(port=port, *p))

    def _get_standard_replacements(self, cfg=None, port=None):
        replacements = {'%HOSTNAME%': getHostName()}
        ngas_root = None
        if cfg is None:
            ngas_root = self.ngas_root(port)
        else:
            ngas_root = cfg.getRootDirectory()
        if ngas_root:
            replacements['%NGAS_ROOT%'] = ngas_root
            replacements['%NGAS_ROOT_DISK_ID%'] = ngas_root[1:].replace('/', '-')
        return replacements

    def assert_status_ref_file(self, ref_file, status, filters=(),
                               startswith_filters=(), replacements={},
                               msg='', status_dump_args=(), cfg=None,
                               port=None):
        data = status.dumpBuf(*status_dump_args)
        self.assert_ref_file(ref_file, data, filters=filters,
                             startswith_filters=startswith_filters,
                             replacements=replacements, msg=msg, cfg=cfg,
                             port=port)

    _common_startswith_filters = (
        "Date:", "Version:", "InstallationDate:", "HostId:", "AvailableMb:",
        "TotalDiskWriteTime:", "IngestionDate:", "CompletionDate:", "CreationDate:",
        "RequestTime:", "CompletionTime:", "StagingFilename:", "ModificationDate:",
        "TotalIoTime", "IngestionRate", "ContainerId", "ModificationDate:",
        "BytesStored:", "FileSize:", "Checksum:", "AccessDate:")
    def assert_ref_file(self, ref_file, data, filters=(), startswith_filters=(),
                        replacements={}, msg='', cfg=None, port=None):

        # Users can override the standard replacements
        user_replacements = replacements
        replacements = self._get_standard_replacements(cfg, port)
        replacements.update(user_replacements)

        # Normalise data coming from the unit test execution
        # This is slightly more complicated than the normalisation of data
        # coming from the reference file because way more things are filtered out
        # from the unit test data
        data = filter_and_replace(data, filters=filters,
                                  startswith_filters=tuple(startswith_filters) + self._common_startswith_filters,
                                  replacements=replacements, split_by_newline=True)
        new_buf = []
        for line in _old_buf_style(data).splitlines():
            if ((line.find("NG/AMS Server performing exit") != -1) or
                (line.find("Successfully handled command") != -1)):
                line = line.split(" (")[0]
            new_buf.append(line)
        data = _old_buf_style('\n'.join(new_buf))

        # Clean up data coming from the reference file
        ref = filter_and_replace(loadFile(_to_abs(ref_file)), startswith_filters=startswith_filters,
                                 replacements=replacements)

        errors = []
        for i, (refline, statline) in enumerate(zip(ref.splitlines(), data.splitlines()), 1):
            if refline != statline:
                errors.append((i, refline, statline))
        if not errors:
            return

        msg += '\nRef File: ' + ref_file + '\n'
        errors_as_msgs = map(lambda e: 'Line %d\n< %s\n> %s' % (e[0], e[1], e[2]), errors)
        self.fail(msg + '\n'.join(errors_as_msgs))


    def checkFilesEq(self, refFile, tmpFile, msg, sort = 0):
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
        self.assertTrue(not cmpFiles(_to_abs(refFile), tmpFile, sort),
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

        port, cfg_pars = srvInfo, []
        if isinstance(srvInfo, (tuple, list)):
            port, cfg_pars = srvInfo

        # Set port number in configuration and allocate a mount root
        hostName = getHostName()
        srvId = "%s:%d" % (hostName, port)
        mtRtDir = os.path.join(tmp_root, "NGAS:%d" % port if multSrvs else "NGAS")
        rmFile(mtRtDir)

        # Set up our server-specific configuration
        cfg = ngamsConfig.ngamsConfig().load(comCfgFile)
        cfg.storeVal("NgamsCfg.Header[1].Type", "TEST CONFIG: %s" % srvId)
        cfg.storeVal("NgamsCfg.Server[1].PortNo", port)

        # Set special values if so specified.
        for cfgPar in cfg_pars:
            cfg.storeVal(cfgPar[0], cfgPar[1])

        # And dump it into our server-specific configuration file
        tmpCfgFile = genTmpFilename(prefix=srvId, suffix='xml')
        cfg.save(tmpCfgFile, 0)

        # Start server + add reference to server configuration object and
        # server DB object.
        cfg, db = self.prepExtSrv(port=port, delDirs=0, clearDb=0, autoOnline=1,
                                  cfgFile=tmpCfgFile, root_dir=mtRtDir)
        return [srvId, port, cfg, db]

    def prepCluster(self, server_list, cfg_file='src/ngamsCfg.xml', createDatabase=True,
                    cfg_props=()):
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

                       [[<Port#1>, <Cfg Pars>],
                        [<Port#2>, <Cfg Pars>],
                        ...]

                       <Cfg Pars>
                       is a list of sub-list specifying (in XML Dictionary
                       format) special configuration parameters for the
                       given server (string).

        Returns:       Dictionary with reference to each server
                       (<Host ID>:<Port No>) pointing to lists with the
                       following contents:

                         [<ngamsConfig object>, <ngamsDb object>]

                                                                 (dictionary).
        """

        # Create the shared database first of all and generate a new config file
        tmpCfg = db_aware_cfg(cfg_file)
        self.point_to_sqlite_database(tmpCfg, createDatabase)
        if createDatabase:
            with contextlib.closing(ngamsDb.from_config(tmpCfg, maxpool=1)) as db:
                delNgasTbls(db)
        cfg_file = genTmpFilename(suffix='.xml')
        tmpCfg.save(cfg_file, 0)
        cfg_file = prepCfg(cfg_file, cfg_props)

        multSrvs = len(server_list) > 1

        # Start them in parallel now that we have all set up for it
        res = srv_mgr_pool.map(functools.partial(self.start_srv_in_cluster, multSrvs, cfg_file), server_list)

        # srvId: (cfgObj, dbObj), in same input order
        d = collections.OrderedDict()
        for r in res:
            d[r[0]] = (r[2], r[3])
            self._add_client(r[1])

        return d

    def point_to_sqlite_database(self, cfgObj, create):
        # Exceptional handling for SQLite.
        if 'sqlite' in cfgObj.getDbInterface().lower():

            sqlite_file = os.path.join(tmp_root, 'ngas.sqlite')
            if create:
                rmFile(sqlite_file)
                import sqlite3
                fname = 'ngamsCreateTables-SQLite.sql'
                script = utils.b2s(pkg_resources.resource_string('ngamsSql', fname))  # @UndefinedVariable
                with contextlib.closing(sqlite3.connect(sqlite_file)) as conn:  # @UndefinedVariable
                    conn.executescript(script)

            # Make sure the 'database' attribute is an absolute path
            # This is because there are tests that start the server
            # in daemon mode, in which case the process's cwd is /
            #
            # The "dbCfgGroupId" is needed when dumping configuration objects
            # into the ngas configuration database tables (which is exercised
            # by the unit tests, but in reality not really used much).
            params = cfgObj.getDbParameters()
            if 'database' in params and not params['database'].startswith('/'):
                abspath = sqlite_file
                cfgObj.storeVal("NgamsCfg.Db[1].database", abspath, cfgObj.getVal('NgamsCfg.Id'))

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
        dbObj  = ngamsDb.from_config(cfgObj, maxpool=1)

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
                ngasDiskInfo = _to_abs("src/NgasDiskInfoTemplate")
                isoDate = toiso8601()
                diskId = diskDir[1:].replace("/", "-")
                ngasDiskInfo = ngasDiskInfo % (isoDate, getHostName(),
                                               getNgamsVersion(), availMb,
                                               diskId, isoDate, dirInfo[1])
                ngasDiskInfoFile = diskDir + "/NgasDiskInfo"
                with open(ngasDiskInfoFile, 'wt') as f:
                    f.write(ngasDiskInfo)
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
        newCfgFileName = genTmpFilename(suffix=".xml")
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

        for match, replacement in self._get_standard_replacements().items():
            queryVal = queryVal.replace(match, replacement)

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
        refQueryPlanLines = list(filter(None, loadFile(_to_abs(refQueryPlan)).splitlines()))
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


# EOF
