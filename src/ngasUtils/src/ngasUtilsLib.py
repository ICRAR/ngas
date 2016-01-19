
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
# "@(#) $Id: ngasUtilsLib.py,v 1.4 2008/12/15 22:09:52 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  21/01/2004  Created
#

"""
Utility functions used by the tool in the NGAS Utils module.
"""

import sys, os, base64, getpass, smtplib, glob, commands

from ngams import *
import ngamsPClient


NGAS_RC_FILE            = "~/.ngas"

NGAS_RC_PAR_ACC_CODE    = "AccessCode"
NGAS_RC_PAR_DB_SRV      = "DbServer"
NGAS_RC_PAR_DB_USER     = "DbUser"
NGAS_RC_PAR_DB_PWD      = "DbPassword"
NGAS_RC_PAR_DB_NAME     = "DbName"
NGAS_RC_PAR_SMTP_HOST   = "SmtpHost"
NGAS_RC_PAR_NOTIF_EMAIL = "EmailNotification"
NGAS_RC_PAR_HOST        = "NgasHost"
NGAS_RC_PAR_PORT        = "NgasPort"


def getNgasResourceFile():
    """
    Get the complete name of the NGAS resource file.

    Returns:  Complete filename of NGAS resource file (string).
    """
    return os.path.expanduser(NGAS_RC_FILE)


def getParNgasRcFile(par):
    """
    Retrieve a parameter from the NGAS resource file.

    The function has a built-in check to ensure that it is not possible
    to execute the tool on a test machine. A test machine has one of the
    following substrings in its name: 'wg0ngas', 'dev', 'tst'.

    par:      Parameter name (string).

    Returns:  Value of parameter or None (string|None).
    """
    # Determine if running on a test system.
    testSystems = ['wg0ngas', 'dev', 'tst']
    testSystem = 0
    for tstSysPat in testSystems:
        if (getHostName().find(tstSysPat) != -1): testSystem = 1
    try:
        fo = open(getNgasResourceFile())
    except Exception, e:
        raise Exception, "Error accessing NGAS Resource File: " + str(e)
    fileLines = fo.readlines()
    fo.close()
    for line in fileLines:
        try:
            val = line[line.find("=") + 1:].strip()
        except Exception, e:
            msg = "Problem parsing line |%s| from NGAS " +\
                  "Utilities Resource File. Error: %s"
            raise Exception, msg % (line, str(e))
        # Some basic on-the-fly checks:
        #
        #   1. Only allow to execute with ESOECF/OLASLS/ASTOP if not test
        #      system.
        #   2. Do not allow to send Email Notification to email address
        #      containing the substrings 'sao' and 'ngasgop'.
        #   3. Only allow to execute on system with hostname containing
        #      substring mentioned in the function man-page.
        if (testSystem):
            if (line.find("DbServer") != -1):
                if ((val == "ESOECF") or (val == "OLASLS") or (val =="ASTOP")):
                    raise Exception, "Cannot connect to operational DB: %s" %\
                          val
            elif (line.find("EmailNotification") != -1):
                if ((val.find("ngasgop") != -1) or (val.find("sao") != -1)):
                    raise Exception, "Cannot send Email Notification " +\
                          "Messages to operators: %s" % val
            elif (line.find("NgasHost") != -1):
                if ((val.find("wg0ngas") == -1) and (val.find("dev") == -1) and
                    (val.find("tst") == -1) and (val != "$HOSTNAME")):
                    raise Exception, "Cannot connect to operational NGAS "+\
                          "System: %s" % val
        if (line.find(par) != -1):
            # Resolve a possible environment variable.
            if (val[0] == "$"): val = os.environ[val[1:]]
            return val
    return None


def encryptAccessCode(accessCode):
    """
    Encode an Access Code as used by the NGAS Utilities.

    accessCode:     Access Code as typed by the user (string).

    Returns:        Encoded Access Code (string).
    """
    return base64.encodestring(accessCode)
    

def decryptAccessCode(encryptedAccessCode):
    """
    Decode an Access Code as used by the NGAS Utilities.

    encryptedAccessCode:     Encrypted Access Code (string).

    Returns:                 Decoded Access Code (string).
    """
    return base64.decodestring(encryptedAccessCode)
    

def checkAccessCode(accessCode):
    """
    Check the given access code against the one defined in the NGAS
    resource file. In case of match, it returns 1 otherwise 0.

    accessCode:   Access Code as given by the user (string).

    Returns:      Void.
    """
    accCode = decryptAccessCode(getParNgasRcFile(NGAS_RC_PAR_ACC_CODE))
    if (accCode == accessCode):
        return
    else:
        raise Exception, "Incorrect Access Code given!!"


def input(msg):
    """
    Simple function to prompt the user for input. The string entered
    is stripped before returning it to the user.

    msg:      Message to print (string).

    Returns:  Information entered by the user (string).
    """
    return raw_input("INPUT> " + msg + " ").strip()


def getDbPars():
    """
    Extract the DB parameters from the NGAS resource file. The DB password
    is decrypted.

    Returns:   Tuple with the DB parameters
                 (<DB Srv>, <DB>, <User>, <Pwd>) (tuple)
    """
    server   = getParNgasRcFile(NGAS_RC_PAR_DB_SRV)
    db       = getParNgasRcFile(NGAS_RC_PAR_DB_NAME)
    user     = getParNgasRcFile(NGAS_RC_PAR_DB_USER)
    password = getParNgasRcFile(NGAS_RC_PAR_DB_PWD)
    return (server, db, user, password)


def sendEmail(subject,
              to,
              msg,
              contentType = None,
              attachmentName = None):
    """
    Send an e-mail to the recipient with the given subject.
    
    smtpHost:       Mail server to use for sending the mail (string).
    
    subject:        Subject of mail message (string).
    
    to:             Recipient, e.g. user@test.com (string).
        
    fromField:      Name for the from field (string).
        
    msg:            Message to send (string).

    contentType:    Mime-type of message (string).
    
    attachmentName: Name of attachment in mail (string).
              
    Returns:        Void.
    """
    smtpHost = getParNgasRcFile(NGAS_RC_PAR_SMTP_HOST)
    emailList = to.split(",")
    fromField = getpass.getuser() + "@" + os.uname()[1].split(".")[0]
    for emailAdr in emailList:
        try:
            hdr = "Subject: " + subject + "\n"
            if (contentType): hdr += "Content-Type: " + contentType + "\n"
            if (attachmentName):
                hdr += "Content-Disposition: attachment; filename=" +\
                       attachmentName + "\n"
            tmpMsg = hdr + "\n" + msg
            server = smtplib.SMTP(smtpHost)
            server.sendmail("From: " + fromField, "Bcc: " + emailAdr, tmpMsg)
        except Exception, e:
            print "Error sending email to recipient: " + str(emailAdr) +\
                  ". Error: " + str(e)


def dccMsg2FileList(dccMsgFile,
                    targetFile):
    """
    Converts a DCC (Data Consistency Checking, Inconsistency Notification
    Message, e.g.:

    Notification Message:
    ...
    Problem Description                      File ID                       ...
    -----------------------------------------------------------------------...
    ERROR: File in DB missing on disk        WFI.1999-07-07T21:01:45.296   ...
    ERROR: File in DB missing on disk        WFI.1999-07-07T21:00:37.187   ...
    ...
    -----------------------------------------------------------------------...

    - into a File List of the format:

    <Disk ID> <File ID> <File Version>
    ...

    Can also convert a DCC message of the form:

    Notification Message:
    ...
    ------------------------------------------------------------------------...
    NON REGISTERED FILES FOUND ON STORAGE DISKS:

    Filename:
    ------------------------------------------------------------------------...
    /tmp/ngamsTest/NGAS/FitsStorage1-Main-1/ShouldNotBeHere1
    /tmp/ngamsTest/NGAS/FitsStorage1-Main-1/.db/ShouldNotBeHere2
    ...
    ------------------------------------------------------------------------...

    - into a File List of the format:

    <Complete Path 1>
    <Complete Path 2>
    ...
    

    dccMsgFile:   File containing the DCC Inconsist. Msg. (string).

    Returns:      Void.
    """
    fo = open(dccMsgFile)
    dccMsg = fo.read()
    dccMsgLines = dccMsg.split("\n")
    fo.close()
    fo = open(targetFile, "w")
    if (dccMsg.find("NON REGISTERED FILES FOUND ON STORAGE DISK") != -1):
        fileListBuf = ""
        # Skip until line starting with '/' found.
        totLines = len(dccMsgLines)
        fileIdIdx = -1
        lineIdx = 0
        while (lineIdx < totLines):
            line = dccMsgLines[lineIdx].strip()
            if (line == ""):
                lineIdx += 1
                continue
            if (line[0] == "/"): break
            lineIdx += 1
        while (lineIdx < totLines):
            line = dccMsgLines[lineIdx].strip()
            if (line[0] == "-"): break
            fileListBuf += "%s\n" % line
            lineIdx += 1
        fo.write(fileListBuf)
    else:
        fileIdIdx = -1
        lineIdx = 0
        while (1):
            line = dccMsgLines[lineIdx]
            if (line.find("Problem Description") != -1):
                fileIdIdx = line.find("File ID")
                break
            lineIdx += 1
        lineIdx += 2

        # Read in lines until "-----..." is encountered.
        while (1):
            if (dccMsgLines[lineIdx].find("-----") != -1): break
            lineEls = dccMsgLines[lineIdx][fileIdIdx:].split(" ")
            elList = cleanList(lineEls)
            fileId = elList[0]
            fileVersion = elList[1]
            diskId = elList[2].split(":")[1]
            fo.write("%s %s %s\n" % (diskId, fileId, fileVersion))
            lineIdx += 1
    fo.close()
        

def dccRep2FileList(dccRep):
    """
    Converts a DCC Report to a File List.

    dccRep:    DCC Report to convert (string).
    
    Returns:   Corresponding File list (string).
    """
    # IMPL: Note: This function should replace dccMsg2FileList().
    if (dccRep.find("DATA CHECK REPORT") == -1):
        raise Exception, "The specified text appears not to be a DCC Report"
    fileListBuf = ""
    for line in dccRep.split("\n"):
        if ((line.find("ERROR: File in DB missing on disk") != -1) or
            (line.find("ERROR: Inconsistent checksum found") != -1)):
            cleanLst = cleanList(line.split(" "))
            diskId  = cleanLst[-1].split(":")[-1]
            fileId  = cleanLst[-3]
            fileVer = cleanLst[-2]
            fileListBuf += "\n%s %s %s" % (diskId, fileId, fileVer)
    return fileListBuf


def parseFileList(fileListFile):
    """
    Function that parses the the given file list listing entries like this:

       <Field 1> <Field 2> <Field 3>, ...
       <Field 1> <Field 2> <Field 3>, ...
       ...

    - and returns the entries in a list:

      [[<Field 1>, <Field 2>, <Field 3>, ...], ...]

    If the File List input file specified contains a DCC Report, this will
    be handled as well.

    fileListFile:    Name of file in which the file entries are
                     contained (string).

    Returns:         List containing the info for the files to
                     register (list).
    """
    fileRefList = []
    fo = open(fileListFile)
    fileBuf = fo.read()
    fo.close()

    # Check if the File List should be derived from a DCC Report.
    if (fileBuf.find("DATA CHECK REPORT") != -1):
        fileBuf = dccRep2FileList(fileBuf)

    # Parse the file list and create a Python list with the info.
    fileLines = fileBuf.split("\n")
    lineNo = 0
    for line in fileLines:
        lineNo += 1
        line = line.strip()
        if (not line): continue
        if (line[0] != "#"):
            fields = []
            lineEls = line.split(" ")
            for el in lineEls:
                if (el.strip()): fields.append(el.strip())
            fileRefList.append(fields)
    return fileRefList


def checkDelTmpDirs(dirNamePat,
                    timeOut = 600):
    """
    Check if there are temporary directories files according to the given
    pattern, which are older than the time out specified. In case yes,
    remove these if possible.

    dirNamePat:     Pattern to check (string).

    timeOut:        Timeout in seconds, if entries older than this
                    are found, they are deleted (integer).

    Returns:        Void.
    """
    fileList = glob.glob(dirNamePat)
    for file in fileList:
        lastPath = file.split("/")[-1]
        if ((lastPath != ".") and (lastPath != "..")):
            creationTime = getFileCreationTime(file)
            if ((time.time() - creationTime) > timeOut):
                commands.getstatusoutput("rm -rf " + file)


def checkSrvRunning(host = None,
                    port = 7777):
    """
    Function to check if server is running. It returns the status of the
    server, which is one of the following values:

      - NOT-RUNNING: Server is not running.
      - OFFLINE:     Server is running and is in OFFLINE State.
      - ONLINE:      Server is running and is in ONLINE State.

    host:     Host to check if server is running (string).

    port:     Port number used by server (integer).

    Returns:  Status (string).
    """
    if host is None:
        host = getHostName()
    try:
        res = ngamsPClient.ngamsPClient().\
              sendCmdGen(host, port, NGAMS_STATUS_CMD)
        status = res.getState()
    except Exception, e:
        status = NGAMS_NOT_RUN_STATE
    return status


def secs2Iso(timeSecs):
    """
    Convert a time, given in seconds since epoch to an ISO8601 timestamp.

    timeSecs:   Seconds since epoch (integer).

    Returns:    ISO8601 timestamp (string).
    """
    return PccUtTime.TimeStamp().initFromSecsSinceEpoch(timeSecs).\
           getTimeStamp()

#############################################################################
# Tools to handle command line options.    
#############################################################################
# IMPL: Use a class (ngasOptions) to handle the options rather than a
#       dictionary.
NGAS_OPT_NAME = 0
NGAS_OPT_ALT  = NGAS_OPT_NAME + 1
NGAS_OPT_VAL  = NGAS_OPT_ALT + 1
NGAS_OPT_TYPE = NGAS_OPT_VAL + 1
NGAS_OPT_SYN  = NGAS_OPT_TYPE + 1
NGAS_OPT_DOC  = NGAS_OPT_SYN + 1

NGAS_OPT_OPT  = "OPTIONAL"
NGAS_OPT_MAN  = "MANDATORY"
NGAS_OPT_INT  = "INTERNAL"

_stdOptions = [["help", [], 0, NGAS_OPT_OPT, "",
                "Print out man-page and exit."],
               ["debug", [], 0, NGAS_OPT_OPT, "",
                "Run in debug mode."],
               ["version", [], 0, NGAS_OPT_OPT, "",
                "Print out version."],
               ["verbose", [], 0, NGAS_OPT_OPT, "=<Verbose Level [0; 5]>",
                "Switch verbose mode logging on ([0; 5])."],
               ["logFile", [], None, NGAS_OPT_OPT, "=<Log File>", 
                "Log file into which info will be logged during execution."],
               ["logLevel", [], 0, NGAS_OPT_OPT, "=<Log Level [0; 5]>", 
                "Level applied when logging into the specified log file."],
               ["notifEmail", [], None, NGAS_OPT_OPT, "=<Email Recep. List>", 
                "Comma separated list of email recipients which will " +\
                "receive status reports in connection with the tool " +\
                "execution."],
               ["accessCode", [], None, NGAS_OPT_OPT, "=<Access Code>",
                "Access code to access the NGAS system with the NGAS " +\
                "Utilities."]]


def genOptDicAndDoc(toolOptions):
    """
    From the options defined, generate a dictionary with this info and
    generate a man-page.

    toolOptions:    Tool options (list).

    Returns:        Tuple with dictionary and man-page (tuple).
    """
    optionList = _stdOptions + toolOptions
    optFormat = "--%s%s [%s] (%s):\n"
    optDic = {}
    optDoc = ""
    for optInfo in optionList:
        optDic[optInfo[NGAS_OPT_NAME]] = optInfo
        optDic[optInfo[NGAS_OPT_NAME].upper()] = optInfo
        if (optInfo[NGAS_OPT_NAME][0] != "_"):
            optDoc += optFormat % (optInfo[NGAS_OPT_NAME],
                                   optInfo[NGAS_OPT_SYN], 
                                   str(optInfo[NGAS_OPT_VAL]),
                                   optInfo[NGAS_OPT_TYPE])
            optDoc += optInfo[NGAS_OPT_DOC] + "\n\n"
    optDoc += "\n" + NGAMS_COPYRIGHT_TEXT
    return (optDic, optDoc)

    
def parseCmdLine(argv,
                 optDic):
    """
    Parse the command line parameters and pack the values into the Options
    Dictionary. Some basic checks are carried out.

    argv:        List with arguments as contained in sys.argv (list).
    
    optDic:      Dictionary with information about options (dictionary).

    Returns:     Returns reference to updated Options Dictionary (dictionary).
    """
    # Parse input parameters.
    idx = 1
    while idx < len(argv):
        info(2,"Parsing option: %s" % argv[idx])
        tmpInfo = argv[idx].split("=")
        if (len(tmpInfo) >= 2):
            eqIdx = argv[idx].find("=")
            parOrg = argv[idx][0:eqIdx].strip()[2:]
            parVal = argv[idx][(eqIdx + 1):].strip()
        else:
            parOrg = tmpInfo[0][2:]
            parVal = 1
        parUp = parOrg.upper()
        if (not optDic.has_key(parUp)):
            raise Exception, "Unknown option: %s" % parOrg
        optDic[parUp][NGAS_OPT_VAL] = parVal
        if (parUp.find("HELP") != -1): return optDic
        idx += 1
    setDebug(optDic["debug"][NGAS_OPT_VAL])
    setLogCond(0, "",
               int(optDic["logLevel"][NGAS_OPT_VAL]),
               optDic["logFile"][NGAS_OPT_VAL],
               int(optDic["verbose"][NGAS_OPT_VAL]))

    # Check if mandatory options are all specified.
    for opt in optDic.keys():
        if (opt.upper()[0] == opt[0]): continue
        if ((optDic[opt][NGAS_OPT_TYPE] == NGAS_OPT_MAN) and
            (optDic[opt][NGAS_OPT_VAL] == None)):
            raise Exception, "Undefined, mandatory option: \"%s\"" % opt

    return optDic


def getCheckAccessCode(optDic):
    """
    Check the access code according to the value found for this parameter
    in the Options Dictionary. If not defined in the Options Dictionary, the
    user is prompted for the access code.

    optDic:    Dictionary containing info about options (dictionary).

    Returns:   Void.
    """
    accCode = optDic["accessCode"][NGAS_OPT_VAL]
    if (not accCode): accCode = input("Enter Access Code:")
    checkAccessCode(accCode)


def optDic2ParDic(optDic):
    """
    Convert an NGAS Utils option dictionary into a simple dictionary containing
    the options/parameters as keys and the associated, assigned values.

    optDic:    Dictionary containing info about options (dictionary).

    Returns:   Parameter dictionary (dictionary).
    """
    parDic = {}
    for opt in optDic.keys():
        parDic[opt] = optDic[opt][NGAS_OPT_VAL]
    return parDic

#############################################################################

# EOF
