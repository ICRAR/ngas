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
# "@(#) $Id: ngasUtilsTest.py,v 1.2 2008/08/19 20:37:46 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  01/09/2009  Created
#
"""
Test program that runs all the NGAS Utilities Functional Test Suites.

To launch all tests, invoke the tool as follows:

$ python ngasUtilsTest.py -notifemail <Email Addr 1>,<Email Addr 2>,...

A status report will be printed on stdout and sent to the email recipients
listed.
"""

import os, sys, glob, getpass, profile, pstats, time
import pcc, PccUtUtils, PccUtTime
from ngams import *
import ngamsLib, ngamsConfig, ngamsHighLevelLib
try:
    import ngamsTestLib
    import ngasUtilsTestLib
except Exception, e:
    print str(e)
    sys.exit(1)


NGAMS_TEST_MAX_TS_TIME = 800


def getTestList():
    """
    Generate a list containing the names of the test modules, which should
    be executed.

    Returns:   List with names of test Python modules (list/string).
    """
    testModList = []
    globPat = os.path.normpath(NGAMS_SRC_DIR + "/../ngasUtils/test/*Test.py")
    fileList = glob.glob(globPat)
    fileList.sort()
    supprTests = []
    for file in fileList:
        testMod = os.path.basename(file).split(".")[0]
        try:
            supprTests.index(testMod)
            print "===> NOTE: Test Suite: %-32s disabled - RE-ENABLE!" %\
                  testMod
            continue
        except:
            pass
        if (file.find("ngasUtilsTest.py") == -1): testModList.append(testMod)
    return testModList


def runAllTests(notifEmail = None,
                skip = None):
    """
    Run all tests in ngasUtils/test.

    notifEmail:  List of email recipients that should be notified about the
                 test results (list/email addresses).

    skip:        Test Suites or Test Cases to skip. Comma separated list
                 (string).

    Returns:     Void.
    """
    skipDic = {}
    if (skip):
        for test in skip.split(","): skipDic[test] = 1
    testModList = getTestList()
    startTime = time.time()
    failModDic = {}
    noOfTests = len(testModList)
    testCount = 0
    format = "Running Test Suite: %-32s %-8s"
    line = "\nNGAS UTILITIES FUNCTIONAL TESTS - TEST REPORT\n"
    print line
    testRep = line + "\n"
    line = "Date:           %s" % PccUtTime.TimeStamp().getTimeStamp()
    print line
    testRep += line + "\n"
    line = "Host:           %s" % getHostName()
    print line
    testRep += line + "\n"
    line = "NG/AMS Version: %s\n" % getNgamsVersion()
    print line
    testRep += line + "\n"
    for mod in testModList:
        if (skipDic.has_key(mod)): continue
        testCount += 1
        line = format % (mod, ("(#%d/%d)" % (testCount, noOfTests)))
        testRep += line
        sys.stdout.write(line)
        sys.stdout.flush()
        suiteStartTime = time.time()
        stat, stdout, stderr = PccUtUtils.execCmd("python " + mod + ".py",
                                                  NGAMS_TEST_MAX_TS_TIME)
        testTime = (time.time() - suiteStartTime)
        if (testTime >= NGAMS_TEST_MAX_TS_TIME):
            failModDic[mod] = "TIME-OUT"
            stat = " - %-5.1fs - TIME-OUT!!\n" % testTime
        elif (stdout.find("FAILED") != -1):
            failModDic[mod] = stdout + " --- " + stderr
            stat = " - %-5.1fs - FAILURE!!\n" % testTime
        else:
            stat = " - %-5.1fs - SUCCESS\n" % testTime
        sys.stdout.write(stat)
        sys.stdout.flush()
        testRep += stat
    execTime = (time.time() - startTime)
    line = "\n\nExecuted %d Test Suites in %.3f seconds\n\n" %\
           (len(testModList), execTime)
    print line
    testRep += line

    if (failModDic):
        line = 80 * "="
        print line
        testRep += line
        line = "\n\nFAILED TEST SUITES:\n\n"
        print line
        testRep += line
        failMods = failModDic.keys()
        failMods.sort()
        for failMod in failMods:
            line = "\n%s:\n%s\n" % (failMod, failModDic[failMod])
            print line
            testRep += line + "\n"
            line = 80 * "-"
            print line
            testRep += line
        line = 80 * "="
        print line
        testRep += line

    # Send out email with test report if requested.
    if (notifEmail):
        notifEmail = notifEmail.split(",")
        par = "NgamsCfg.Server[1].MountRootDirectory"
        ngamsCfgObj = ngamsConfig.ngamsConfig().storeVal(par, "/NGAS")
        ngamsHighLevelLib.sendEmail(ngamsCfgObj, "localhost",
                                    "NGAS UTILITIES FUNCTIONAL TESTS REPORT",
                                    notifEmail, "ngas@%s" %\
                                    ngamsLib.getCompleteHostName(), testRep)


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    skip = None
    status = 0
    tests = []
    notifEmail = None
    idx = 1
    while idx < len(sys.argv):
        par = sys.argv[idx].upper()
        if (par == "-SKIP"):
            idx += 1
            skip = sys.argv[idx]
        elif (par == "-STATUS"):
            status = 1
        elif (par == "-TESTS"):
            idx += 1
            tests = sys.argv[idx].split(",")
        elif (par == "-NOTIFEMAIL"):
            idx += 1
            notifEmail = sys.argv[idx]
        else:
            correctUsage()
            sys.exit(1)
        idx += 1
    if (notifEmail == None):
        raise Exception, "Specify parameter: -notifEmail <Rec List>"
    if (notifEmail == ""): notifEmail = None
    if (status):
        genStatus()
    elif (tests != []):
        for testMod in tests:
            exec "import " + testMod
            exec testMod + ".run()"
    else:
        runAllTests(notifEmail, skip)
        

# EOF
